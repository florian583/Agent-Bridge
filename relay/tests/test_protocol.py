"""Black-box MCP compatibility against an isolated Unix-socket daemon."""

import contextlib
import json
import os
import queue
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BINARY = Path(os.environ.get("RELAY_BINARY", ROOT / "target/release/agent-bridge-relay"))
PYTHON = os.environ.get("BRIDGE_PYTHON")
MAX_FRAME = 2 * 1024 * 1024


class FakeDaemon:
    def __init__(self, handler=None):
        self.temp = tempfile.TemporaryDirectory(prefix="relay-")
        self.path = str(Path(self.temp.name) / "daemon.sock")
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.bind(self.path)
        self.socket.listen()
        self.socket.settimeout(0.1)
        self.stop = threading.Event()
        self.requests = []
        self.handlers = []
        self.handler = handler or self.reply
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    @staticmethod
    def reply(request):
        method = request["method"]
        if method in ("list_agents", "list_sessions"):
            result = [{"marker": method}]
        elif method == "session_scope":
            result = {"scope": "conversation", "conversation_id": request["conversation_id"]}
        else:
            result = {"marker": method, "params": request["params"]}
        return {"ok": True, "result": result}

    def serve(self):
        while not self.stop.is_set():
            try:
                connection, _ = self.socket.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            thread = threading.Thread(target=self.handle, args=(connection,), daemon=True)
            self.handlers.append(thread)
            thread.start()

    def handle(self, connection):
        with connection:
            connection.settimeout(5)
            try:
                with connection.makefile("rb") as reader:
                    frame = reader.readline(MAX_FRAME + 1)
                    request = json.loads(frame)
                    self.requests.append(request)
                    reply = self.handler(request)
                    if reply is not None:
                        connection.sendall(reply if isinstance(reply, bytes) else json.dumps(reply).encode() + b"\n")
            except (OSError, ValueError):
                pass

    def close(self):
        self.stop.set()
        self.socket.close()
        self.thread.join(2)
        for thread in self.handlers:
            thread.join(3)
        self.temp.cleanup()


class Client:
    def __init__(self, daemon, reference=False, extra_env=None, extra_args=None):
        env = {
            **os.environ,
            "AGENT_BRIDGE_DAEMON_SOCKET": daemon.path,
            "AGENT_BRIDGE_CONVERSATION_ID": "test-conversation",
            "AGENT_BRIDGE_SKIP_SKILL": "1",
            "PYTHONPATH": str(ROOT.parent / "src"),
        }
        env.pop("AGENT_BRIDGE_PARENT_CONTEXT", None)
        env.pop("CODEX_THREAD_ID", None)
        env.update(extra_env or {})
        cmd = (
            [PYTHON, "-c", 'from agent_bridge.server import mcp; mcp.run(transport="stdio")']
            if reference
            else [str(BINARY), *(extra_args or [])]
        )
        self.process = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
        )
        self.queue = queue.Queue()
        self.pending = {}
        self.stderr = []

        def read():
            for line in self.process.stdout:
                try:
                    self.queue.put(json.loads(line))
                except ValueError:
                    self.queue.put({"invalid_stdout": True})

        def errors():
            for line in self.process.stderr:
                self.stderr.append(line)

        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()
        self.errors = threading.Thread(target=errors, daemon=True)
        self.errors.start()
        self.send(
            {
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "relay-test", "version": "1"},
                },
            }
        )
        self.initialized = self.receive(1)
        self.send({"method": "notifications/initialized"})

    def send(self, value):
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", **value}).encode() + b"\n")
        self.process.stdin.flush()

    def receive(self, request_id, timeout=10):
        if request_id in self.pending:
            return self.pending.pop(request_id)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.queue.get(timeout=max(0.01, deadline - time.monotonic()))
            if value.get("id") == request_id:
                return value
            self.pending[value.get("id")] = value
        raise TimeoutError(request_id)

    def call(self, name, arguments, request_id=2):
        self.send({"id": request_id, "method": "tools/call", "params": {"name": name, "arguments": arguments}})
        return self.receive(request_id)

    def close(self):
        if not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=3)
            raise AssertionError("relay did not exit after input EOF") from None
        self.reader.join(2)
        self.errors.join(2)
        self.process.stdout.close()
        self.process.stderr.close()


def content(reply):
    result = reply["result"]
    value = result["structuredContent"]
    assert json.loads(result["content"][0]["text"]) == value
    return value


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.daemon = FakeDaemon()
        self.addCleanup(self.daemon.close)

    def client(self, **kwargs):
        client = Client(self.daemon, **kwargs)
        self.addCleanup(client.close)
        return client

    def test_contract_and_initialize(self):
        client = self.client()
        self.assertEqual(client.initialized["result"]["protocolVersion"], "2025-06-18")
        client.send({"id": 2, "method": "tools/list"})
        self.assertEqual(
            client.receive(2)["result"]["tools"], json.loads((ROOT / "contract.json").read_text())["tools"]
        )
        self.assertEqual(self.daemon.requests, [])

    @unittest.skipUnless(PYTHON, "set BRIDGE_PYTHON to compare canonical Python MCP")
    def test_all_tool_wrappers_match_python(self):
        rust = self.client()
        python = self.client(reference=True)
        cases = [
            ("list_agents", {}),
            ("set_preferences", {}),
            ("dispatch_task", {"agent": "fake", "message": "hello", "cwd": "/tmp", "request_id": "request-1"}),
            ("wait_task", {"task_id": "task-1"}),
            ("check_task", {"task_id": "task-1"}),
            ("inspect_task", {"task_id": "task-1"}),
            ("get_result", {"task_id": "task-1"}),
            ("get_transcript", {"session_id": "session-1", "kinds": " output, tool_call "}),
            ("cancel_task", {"task_id": "task-1"}),
            ("list_sessions", {}),
            ("end_session", {"session_id": "session-1"}),
        ]
        for i, (name, arguments) in enumerate(cases, 10):
            with self.subTest(tool=name):
                self.assertEqual(content(rust.call(name, arguments, i)), content(python.call(name, arguments, i)))
        for request in self.daemon.requests:
            self.assertEqual(request["conversation_id"], "test-conversation")
            self.assertEqual(request["runtime_context"], "coordinator")
            self.assertIsInstance(request["client_pid"], int)

    def test_invalid_arguments_and_unknown_tool_never_reach_daemon(self):
        client = self.client()
        for i, (name, args) in enumerate(
            [
                ("dispatch_task", {}),
                ("check_task", {"task_id": 42}),
                ("inspect_task", {"task_id": "t", "limit": "bad"}),
                ("unknown", {}),
            ],
            2,
        ):
            reply = client.call(name, args, i)
            self.assertTrue(
                "error" in reply
                or reply.get("result", {}).get("isError")
                or not reply.get("result", {}).get("structuredContent", {}).get("ok", True)
            )
        self.assertEqual(self.daemon.requests, [])

    def test_nested_mutations_fail_closed(self):
        client = self.client(extra_env={"AGENT_BRIDGE_PARENT_CONTEXT": "worker"})
        for i, (name, args) in enumerate(
            [
                ("dispatch_task", {"agent": "fake", "message": "x", "cwd": "/tmp"}),
                ("set_preferences", {}),
                ("cancel_task", {"task_id": "t"}),
                ("end_session", {"session_id": "s"}),
            ],
            2,
        ):
            reply = client.call(name, args, i)
            self.assertTrue("error" in reply or not content(reply).get("ok", True))
        self.assertEqual(self.daemon.requests, [])
        self.assertTrue(content(client.call("check_task", {"task_id": "t"}, 9))["ok"])
        self.assertEqual(self.daemon.requests[-1]["runtime_context"], "worker")

    def test_wait_does_not_block_ping_or_other_calls(self):
        gate = threading.Event()
        self.addCleanup(gate.set)
        original = self.daemon.handler

        def handler(request):
            if request["method"] == "wait_task":
                gate.wait(2)
            return original(request)

        self.daemon.handler = handler
        client = self.client()
        client.send({"id": 2, "method": "tools/call", "params": {"name": "wait_task", "arguments": {"task_id": "t"}}})
        client.send({"id": 3, "method": "ping"})
        self.assertEqual(client.receive(3, 1)["result"], {})
        self.assertTrue(content(client.call("check_task", {"task_id": "t"}, 4))["ok"])
        gate.set()
        self.assertTrue(content(client.receive(2))["ok"])

    def test_lost_dispatch_response_is_not_retried(self):
        self.daemon.handler = lambda request: None
        client = self.client()
        reply = client.call("dispatch_task", {"agent": "fake", "message": "x", "cwd": "/tmp", "request_id": "once"})
        self.assertFalse(content(reply)["ok"])
        self.assertEqual(len(self.daemon.requests), 1)

    def test_oversized_daemon_reply_fails_bounded(self):
        self.daemon.handler = lambda request: b"x" * (MAX_FRAME + 1) + b"\n"
        client = self.client()
        self.assertFalse(content(client.call("check_task", {"task_id": "t"}))["ok"])

    def test_client_eof_does_not_cancel_accepted_work(self):
        accepted, completed = threading.Event(), threading.Event()

        def handler(request):
            accepted.set()
            time.sleep(0.3)
            completed.set()
            return {"ok": True, "result": {"accepted": True}}

        self.daemon.handler = handler
        client = self.client()
        client.send(
            {
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "dispatch_task",
                    "arguments": {"agent": "fake", "message": "x", "cwd": "/tmp", "request_id": "once"},
                },
            }
        )
        self.assertTrue(accepted.wait(2))
        client.process.stdin.close()
        client.process.wait(timeout=2)
        self.assertTrue(completed.wait(2))
        self.assertEqual([r["method"] for r in self.daemon.requests], ["dispatch_task"])

    def test_cancel_notification_does_not_cancel_worker(self):
        accepted, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        original = self.daemon.handler

        def handler(request):
            if request["method"] == "wait_task":
                accepted.set()
                release.wait(2)
            return original(request)

        self.daemon.handler = handler
        client = self.client()
        client.send({"id": 2, "method": "tools/call", "params": {"name": "wait_task", "arguments": {"task_id": "t"}}})
        self.assertTrue(accepted.wait(2))
        client.send({"method": "notifications/cancelled", "params": {"requestId": 2}})
        client.send({"id": 3, "method": "ping"})
        self.assertEqual(client.receive(3, 1)["result"], {})
        release.set()
        self.assertEqual([r["method"] for r in self.daemon.requests], ["wait_task"])

    def test_deadline_does_not_retry(self):
        self.daemon.handler = lambda request: time.sleep(0.3) or {"ok": True, "result": {}}
        client = self.client(extra_args=["--rpc-timeout-seconds", "0.05"])
        start = time.monotonic()
        self.assertFalse(content(client.call("check_task", {"task_id": "t"}))["ok"])
        self.assertLess(time.monotonic() - start, 1)
        self.assertEqual(len(self.daemon.requests), 1)

    def test_sigterm_exits_without_waiting_for_input(self):
        client = self.client()
        client.process.terminate()
        client.process.wait(timeout=2)
        self.assertEqual(self.daemon.requests, [])

    def test_missing_socket_never_starts_fallback(self):
        env = dict(os.environ)
        env.pop("AGENT_BRIDGE_DAEMON_SOCKET", None)
        process = subprocess.run([str(BINARY)], input=b"", capture_output=True, env=env, timeout=2)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(process.stdout, b"")

    def test_stalled_stdout_disconnects(self):
        env = {**os.environ, "AGENT_BRIDGE_DAEMON_SOCKET": self.daemon.path}
        process = subprocess.Popen(
            [str(BINARY)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
        )
        try:
            process.stdin.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "clientInfo": {"name": "blocked-output", "version": "1"},
                        },
                    }
                ).encode()
                + b"\n"
            )
            process.stdin.flush()
            self.assertEqual(json.loads(process.stdout.readline())["id"], 1)
            frames = [{"jsonrpc": "2.0", "method": "notifications/initialized"}]
            frames.extend({"jsonrpc": "2.0", "id": i, "method": "tools/list"} for i in range(2, 25))
            process.stdin.write(b"".join(json.dumps(f).encode() + b"\n" for f in frames))
            process.stdin.flush()
            process.wait(timeout=8)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=2)
            for pipe in (process.stdin, process.stdout, process.stderr):
                with contextlib.suppress(BrokenPipeError):
                    pipe.close()
        self.assertEqual(self.daemon.requests, [])

    def test_frame_limit_and_invalid_utf8(self):
        for raw in (b"\xff\n", b"x" * (MAX_FRAME + 1) + b"\n"):
            with self.subTest(size=len(raw)):
                client = self.client()
                try:
                    client.process.stdin.write(raw)
                    client.process.stdin.flush()
                except BrokenPipeError:
                    pass
                # Either close the connection or return a bounded parse error.
                time.sleep(0.05)
                if client.process.poll() is None:
                    reply = client.queue.get(timeout=2)
                    self.assertIn("error", reply)
                self.assertEqual(self.daemon.requests, [])


if __name__ == "__main__":
    unittest.main()
