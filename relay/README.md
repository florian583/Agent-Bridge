# Native shared-daemon MCP relay

`agent-bridge-relay` replaces the Python MCP client in shared-daemon mode. It speaks newline-delimited MCP JSON-RPC on stdin/stdout and forwards tool calls to the existing private Unix socket. It does not start a daemon, spawn workers, retry calls, or own worker lifetime. Standalone mode remains a Python server.

## Build and connect

```sh
cargo build --release --locked --manifest-path relay/Cargo.toml
AGENT_BRIDGE_DAEMON_SOCKET="$HOME/.agent-bridge-daemon/daemon.sock" relay/target/release/agent-bridge-relay
```

Supported platforms: macOS and Linux. Use a versioned installation, then change the MCP client's command to the binary's absolute path. Preserve the daemon socket and conversation/context environment. Existing Python clients continue running until their host reconnects; replacing configuration does not reclaim their memory immediately. Do not terminate coordinators or daemon-owned workers to upgrade a relay.

The relay forwards `AGENT_BRIDGE_CONVERSATION_ID`, falling back to `CODEX_THREAD_ID`. If neither exists, it sends null. The daemon resolves the caller's live PID/creation-time ancestry binding on every request, before considering the supplied conversation. This preserves scope changes after clear/resume and rejects unrelated process generations. The relay never synthesizes a conversation identity. `AGENT_BRIDGE_PARENT_CONTEXT=worker` blocks mutation tools locally and is forwarded for independent daemon enforcement.

## Bounds and disconnects

- At most 16 active requests; request IDs must be unique while active.
- Each stdin, daemon and stdout frame is limited to 2 MiB including its newline. Oversized input disconnects; oversized responses fail explicitly.
- Each daemon RPC has a 195-second total deadline. `--rpc-timeout-seconds` can shorten it but cannot exceed 195 seconds.
- One pending input frame and one pending output message. A blocked output queue disconnects after five seconds. Stdout contains protocol messages only.
- Cancellation aborts the relay's pending socket operation only. EOF, SIGTERM and SIGINT close relay descriptors without sending `cancel_task` or `end_session`.
- No implicit replay, including when a dispatch was accepted but its reply was lost. Reuse the original daemon request UUID explicitly when recovery is required.

Tool descriptions, schemas, defaults and instructions come from the canonical Python server. Valid requests preserve the Python wrapper's structuredContent and JSON text result. Unknown argument names and incorrect primitive types are rejected rather than coerced. The daemon remains responsible for business rules, ownership, provider settings, model routing, and durable deduplication.

## Verification and contract updates

With the project dependencies installed:

```sh
PYTHONPATH=src python relay/scripts/export_contract.py
cargo fmt --check --manifest-path relay/Cargo.toml
cargo clippy --locked --manifest-path relay/Cargo.toml -- -D warnings
cargo test --locked --manifest-path relay/Cargo.toml
cargo build --release --locked --manifest-path relay/Cargo.toml
BRIDGE_PYTHON="$(command -v python)" python relay/tests/test_protocol.py -v
PYTHONPATH=src python relay/tests/test_daemon_integration.py -v
BRIDGE_PYTHON="$(command -v python)" python relay/scripts/benchmark_clients.py
```

The protocol suite uses an isolated fake Unix daemon. The ownership suite uses the real daemon with its fake test adapter, never a provider CLI. The benchmark compares three initialized clients per implementation, using identical mocked tool calls, median startup/RSS and macOS physical footprint when available. CPU is sampled while clients are idle. Deployment measurements must distinguish new relay processes from old Python clients that have not reconnected.

When Python tools change, regenerate `contract.json` and update forwarding if their wrapper semantics changed. CI verifies the generated contract and runs wrapper parity tests, Rust checks and ownership integration on macOS and Linux. The relay supports initialization versions 2024-11-05, 2025-03-26, 2025-06-18 and 2025-11-25; it advertises tools only.

## Studio measurement

The recorded three-client sample in `benchmarks/studio-20261021.json` measured median physical footprint of 1.45 MiB for Rust versus 62.22 MiB for Python (97.7% lower). Median initialization was 3.60 ms versus 456.94 ms. Both had zero measured idle CPU over five seconds. These are local mocked-transport measurements, not provider latency benchmarks. Seven reconnected clients would save approximately 425 MiB of physical memory at these sample medians.
