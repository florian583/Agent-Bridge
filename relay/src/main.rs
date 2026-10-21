use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    io::{BufRead, Write},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::mpsc,
    task::{AbortHandle, JoinSet},
};

const MAX_FRAME: usize = 2 * 1024 * 1024;
const MAX_ACTIVE: usize = 16;
const CONTRACT: &str = include_str!("../contract.json");

struct Relay {
    socket: String,
    conversation: Option<String>,
    nested: bool,
    deadline: Duration,
    contract: Value,
}

fn protocol_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
fn response(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"result":result})
}
fn wrapper_error(kind: &str, message: &str) -> Value {
    json!({"ok":false,"error":format!("{kind}: {message}"),"error_type":kind})
}
fn tool_result(value: Value) -> Value {
    json!({"content":[{"type":"text","text":serde_json::to_string(&value).expect("JSON value")}],"structuredContent":value,"isError":false})
}
fn matches_schema(value: &Value, schema: &Value) -> bool {
    if let Some(options) = schema["anyOf"].as_array() {
        return options.iter().any(|s| matches_schema(value, s));
    }
    match schema["type"].as_str() {
        Some("string") => value.is_string(),
        Some("null") => value.is_null(),
        Some("boolean") => value.is_boolean(),
        Some("number") => value.is_number(),
        Some("integer") => value.is_i64() || value.is_u64(),
        _ => false,
    }
}
fn arguments(tool: &Value, input: Option<&Value>) -> Result<Map<String, Value>, &'static str> {
    let empty = Map::new();
    let supplied = match input {
        None => &empty,
        Some(v) => v.as_object().ok_or("arguments must be an object")?,
    };
    let schema = &tool["inputSchema"];
    let properties = schema["properties"]
        .as_object()
        .ok_or("invalid tool schema")?;
    if supplied.keys().any(|k| !properties.contains_key(k)) {
        return Err("unknown argument");
    }
    let mut output = Map::new();
    for (name, property) in properties {
        if let Some(value) = supplied.get(name) {
            if !matches_schema(value, property) {
                return Err("invalid argument type");
            }
            output.insert(name.clone(), value.clone());
        } else if let Some(value) = property.get("default") {
            output.insert(name.clone(), value.clone());
        } else {
            return Err("missing required argument");
        }
    }
    if tool["name"] == "get_transcript" {
        let kinds = output
            .get("kinds")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(|s| {
                Value::Array(
                    s.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(|s| json!(s))
                        .collect(),
                )
            })
            .unwrap_or(Value::Null);
        output.insert("kinds".into(), kinds);
    }
    Ok(output)
}
impl Relay {
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, String> {
        let envelope = json!({"method":method,"params":params,"conversation_id":self.conversation,"client_pid":std::process::id(),"runtime_context":if self.nested {"worker"} else {"coordinator"}});
        let mut encoded = serde_json::to_vec(&envelope).map_err(|_| "invalid request")?;
        encoded.push(b'\n');
        if encoded.len() > MAX_FRAME {
            return Err("request too large".into());
        }
        tokio::time::timeout(self.deadline, async {
            let mut socket = UnixStream::connect(&self.socket)
                .await
                .map_err(|_| "daemon connection failed")?;
            socket
                .write_all(&encoded)
                .await
                .map_err(|_| "daemon write failed")?;
            let mut reader = BufReader::new(socket);
            let mut frame = Vec::new();
            loop {
                let available = reader.fill_buf().await.map_err(|_| "daemon read failed")?;
                if available.is_empty() {
                    return Err("daemon closed before complete response".into());
                }
                let end = available.iter().position(|b| *b == b'\n').map(|p| p + 1);
                let length = end.unwrap_or(available.len());
                if frame.len() + length > MAX_FRAME {
                    return Err("daemon response too large".into());
                }
                frame.extend_from_slice(&available[..length]);
                reader.consume(length);
                if end.is_some() {
                    break;
                }
            }
            let reply: Value =
                serde_json::from_slice(&frame).map_err(|_| "invalid daemon response")?;
            if reply["ok"] != true {
                return Err(reply["error"]
                    .as_str()
                    .unwrap_or("daemon request failed")
                    .to_owned());
            }
            reply
                .get("result")
                .cloned()
                .ok_or_else(|| "missing daemon result".into())
        })
        .await
        .map_err(|_| "daemon request deadline exceeded".to_string())?
    }
    async fn invoke(&self, name: &str, params: Map<String, Value>) -> Result<Value, String> {
        if self.nested
            && matches!(
                name,
                "dispatch_task" | "set_preferences" | "cancel_task" | "end_session"
            )
        {
            return Err("nested worker mutation is disabled".into());
        }
        if name == "list_agents" {
            let agents = self.rpc("list_agents", json!({})).await?;
            let env = self.rpc("env_status", json!({})).await?;
            let coordinator = self.rpc("coordinator_status", json!({})).await?;
            return Ok(json!({"ok":true,"agents":agents,"env":env,"coordinator":coordinator}));
        }
        let result = self
            .rpc(
                if name == "list_sessions" {
                    "session_scope"
                } else {
                    name
                },
                if name == "list_sessions" {
                    json!({})
                } else {
                    Value::Object(params.clone())
                },
            )
            .await?;
        let object = result
            .as_object()
            .ok_or("daemon result must be an object")?;
        let mut output = Map::from_iter([("ok".to_owned(), json!(true))]);
        output.extend(object.clone());
        if name == "list_sessions" {
            output.insert(
                "sessions".into(),
                self.rpc(name, Value::Object(params)).await?,
            );
        }
        Ok(Value::Object(output))
    }
    async fn request(&self, id: Value, method: &str, params: &Value) -> Value {
        match method {
            "initialize" => {
                let requested = params["protocolVersion"].as_str().unwrap_or("");
                let version = match requested {
                    "2024-11-05" | "2025-03-26" | "2025-06-18" | "2025-11-25" => requested,
                    _ => "2025-11-25",
                };
                response(
                    id,
                    json!({"protocolVersion":version,"capabilities":{"tools":{}},"serverInfo":{"name":"agent-bridge-relay","version":env!("CARGO_PKG_VERSION")},"instructions":self.contract["instructions"]}),
                )
            }
            "ping" => response(id, json!({})),
            "tools/list" => response(id, json!({"tools":self.contract["tools"]})),
            "tools/call" => {
                let Some(name) = params["name"].as_str() else {
                    return protocol_error(id, -32602, "missing tool name");
                };
                let Some(tool) = self.contract["tools"]
                    .as_array()
                    .and_then(|t| t.iter().find(|t| t["name"] == name))
                else {
                    return protocol_error(id, -32602, "unknown tool");
                };
                let args = match arguments(tool, params.get("arguments")) {
                    Ok(v) => v,
                    Err(e) => return protocol_error(id, -32602, e),
                };
                let value = match self.invoke(name, args).await {
                    Ok(v) => v,
                    Err(e) => wrapper_error("RuntimeError", &e),
                };
                response(id, tool_result(value))
            }
            _ => protocol_error(id, -32601, "method not found"),
        }
    }
}

fn read_frame(reader: &mut impl BufRead) -> std::io::Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(std::io::ErrorKind::UnexpectedEof.into())
            };
        }
        let end = available.iter().position(|b| *b == b'\n').map(|p| p + 1);
        let length = end.unwrap_or(available.len());
        if frame.len() + length > MAX_FRAME {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        frame.extend_from_slice(&available[..length]);
        reader.consume(length);
        if end.is_some() {
            return Ok(Some(frame));
        }
    }
}
async fn emit(output: &mpsc::Sender<Value>, value: Value) -> Result<(), ()> {
    tokio::time::timeout(Duration::from_secs(5), output.send(value))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

struct Frame(Vec<u8>);
impl Write for Frame {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len() + bytes.len() >= MAX_FRAME {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn serve(relay: Arc<Relay>) {
    let (input_tx, mut input) = mpsc::channel(1);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        while let Ok(Some(frame)) = read_frame(&mut reader) {
            if input_tx.blocking_send(frame).is_err() {
                break;
            }
        }
    });
    let (output, mut output_rx) = mpsc::channel::<Value>(1);
    let (failed_tx, mut failed) = mpsc::channel::<()>(1);
    std::thread::spawn(move || {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        while let Some(value) = output_rx.blocking_recv() {
            let mut frame = Frame(Vec::new());
            if serde_json::to_writer(&mut frame, &value).is_err() {
                frame.0.clear();
                let error = protocol_error(value["id"].clone(), -32603, "response too large");
                if serde_json::to_writer(&mut frame, &error).is_err() {
                    break;
                }
            }
            frame.0.push(b'\n');
            if writer.write_all(&frame.0).is_err() || writer.flush().is_err() {
                break;
            }
        }
        let _ = failed_tx.blocking_send(());
    });
    let mut jobs = JoinSet::new();
    let mut initialization_requested = false;
    let mut initialized = false;
    let mut active: HashMap<String, (AbortHandle, bool)> = HashMap::new();
    loop {
        tokio::select! {
            _ = failed.recv() => break,
            done = jobs.join_next(), if !jobs.is_empty() => {
                match done {
                    Some(Ok((key,reply))) => {
                        let cancelled = active.remove(&key).is_some_and(|(_, cancelled)| cancelled);
                        if !cancelled && emit(&output, reply).await.is_err() { break; }
                    }
                    Some(Err(error)) => active.retain(|_, (handle, _)| handle.id() != error.id()),
                    None => {},
                }
            }
            frame = input.recv() => {
                let Some(frame) = frame else { break; };
                let request: Value = match serde_json::from_slice(&frame) { Ok(v)=>v,Err(_)=> { if emit(&output, protocol_error(Value::Null,-32700,"parse error")).await.is_err(){break;} continue; } };
                let valid_id = request.get("id").filter(|id|id.is_string() || id.is_i64() || id.is_u64());
                if request["jsonrpc"] != "2.0" || !request["method"].is_string() || request.get("id").is_some() && valid_id.is_none() || request.get("params").is_some_and(|p|!p.is_object()) {
                    if emit(&output, protocol_error(Value::Null,-32600,"invalid request")).await.is_err(){break;} continue;
                }
                let method = request["method"].as_str().expect("validated method");
                let Some(id) = valid_id.cloned() else {
                    if method == "notifications/initialized" && initialization_requested { initialized = true; }
                    if method == "notifications/cancelled" {
                        let key = request["params"]["requestId"].to_string();
                        if let Some((handle, cancelled)) = active.get_mut(&key) { *cancelled = true; handle.abort(); }
                    }
                    continue;
                };
                if method == "initialize" {
                    let p = &request["params"];
                    if initialization_requested || !p["protocolVersion"].is_string() || !p["capabilities"].is_object() || !p["clientInfo"]["name"].is_string() || !p["clientInfo"]["version"].is_string() {
                        if emit(&output, protocol_error(id, -32602, "invalid or repeated initialize")).await.is_err() { break; }
                        continue;
                    }
                    initialization_requested = true;
                } else if method != "ping" && !initialized {
                    if emit(&output, protocol_error(id, -32600, "server is not initialized")).await.is_err() { break; }
                    continue;
                }
                let key = id.to_string();
                if active.contains_key(&key) || jobs.len() >= MAX_ACTIVE {
                    if emit(&output, protocol_error(id,-32600,"duplicate active request ID or concurrency limit reached")).await.is_err(){break;} continue;
                }
                let relay = relay.clone(); let task_key = key.clone();
                let handle = jobs.spawn(async move {
                    let reply = relay.request(id,request["method"].as_str().expect("validated method"),&request["params"]).await;
                    (task_key,reply)
                });
                active.insert(key,(handle, false));
            }
        }
    }
    jobs.abort_all();
}
fn main() {
    let socket = std::env::var("AGENT_BRIDGE_DAEMON_SOCKET")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            eprintln!("AGENT_BRIDGE_DAEMON_SOCKET is required");
            std::process::exit(2)
        });
    let mut deadline = 195.0;
    let mut args = std::env::args().skip(1);
    if let Some(arg) = args.next() {
        let parsed = if arg == "--rpc-timeout-seconds" {
            args.next().and_then(|s| s.parse::<f64>().ok())
        } else {
            None
        };
        match parsed {
            Some(n) if n.is_finite() && n > 0.0 && n <= 195.0 && args.next().is_none() => {
                deadline = n
            }
            _ => {
                eprintln!(
                    "usage: agent-bridge-relay [--rpc-timeout-seconds SECONDS] (0 < SECONDS <= 195)"
                );
                std::process::exit(2);
            }
        }
    }
    let relay = Arc::new(Relay {
        socket,
        conversation: std::env::var("AGENT_BRIDGE_CONVERSATION_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("CODEX_THREAD_ID")
                    .ok()
                    .filter(|s| !s.is_empty())
            }),
        nested: std::env::var("AGENT_BRIDGE_PARENT_CONTEXT").as_deref() == Ok("worker"),
        deadline: Duration::from_secs_f64(deadline),
        contract: serde_json::from_str(CONTRACT).expect("embedded contract"),
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("async runtime");
    runtime.block_on(async {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! { _ = serve(relay) => {}, _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
    });
    // Stdio threads may be blocked by an idle peer; process exit closes only relay-owned descriptors.
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_frames() {
        assert_eq!(read_frame(&mut &b"{}\n"[..]).unwrap().unwrap(), b"{}\n");
        assert!(read_frame(&mut &b"{}"[..]).is_err());
        assert!(read_frame(&mut &vec![b'x'; MAX_FRAME + 1][..]).is_err());
        let mut exact = vec![b' '; MAX_FRAME];
        exact[MAX_FRAME - 1] = b'\n';
        assert_eq!(
            read_frame(&mut &exact[..]).unwrap().unwrap().len(),
            MAX_FRAME
        );
    }
    #[test]
    fn output_frame_reserves_newline() {
        let mut frame = Frame(Vec::new());
        frame.write_all(&vec![b'x'; MAX_FRAME - 1]).unwrap();
        assert!(frame.write_all(b"x").is_err());
        assert_eq!(frame.0.len(), MAX_FRAME - 1);
    }

    #[test]
    fn primitive_types_are_not_coerced() {
        assert!(!matches_schema(&json!(true), &json!({"type":"integer"})));
        assert!(!matches_schema(&json!(1.5), &json!({"type":"integer"})));
        assert!(!matches_schema(&json!("1"), &json!({"type":"number"})));
        assert!(matches_schema(&json!(1), &json!({"type":"number"})));
    }

    #[test]
    fn canonical_arguments() {
        let contract: Value = serde_json::from_str(CONTRACT).unwrap();
        let tools = contract["tools"].as_array().unwrap();
        let wait = tools.iter().find(|t| t["name"] == "wait_task").unwrap();
        assert_eq!(
            arguments(wait, Some(&json!({"task_id":"x"}))).unwrap()["timeout_sec"],
            180.0
        );
        assert!(arguments(wait, Some(&json!({"task_id":false}))).is_err());
        assert!(arguments(wait, None).is_err());
        let transcript = tools
            .iter()
            .find(|t| t["name"] == "get_transcript")
            .unwrap();
        assert_eq!(
            arguments(
                transcript,
                Some(&json!({"session_id":"s","kinds":" a, ,b "}))
            )
            .unwrap()["kinds"],
            json!(["a", "b"])
        );
    }
}
