from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from agent_bridge.daemon import Daemon
from agent_bridge.daemon_client import DaemonClient
from agent_bridge.daemon_registry import DaemonRegistry
from agent_bridge.models import TaskStatus
from agent_bridge.native_usage import KimiUsageReader
from agent_bridge.transcript import append_event


@pytest.fixture
def short_home():
    with TemporaryDirectory(prefix="ab-", dir="/tmp") as folder:
        yield Path(folder)


@pytest.fixture
async def daemon(short_home, monkeypatch):
    monkeypatch.setenv("AGENT_BRIDGE_ENABLE_FAKE", "1")
    home = short_home / "runtime"
    # macOS Unix socket paths have a small fixed length; tmp_path stays short here.
    d = Daemon(DaemonRegistry.create(home), home / "s")
    await d.start()
    try:
        yield d
    finally:
        await d.close()


def dispatch_args(tmp_path):
    return dict(agent="fake", message="test", cwd=str(tmp_path), request_id=str(uuid.uuid4()))


async def test_dispatch_rejects_worktree_collection_before_task_creation(daemon, tmp_path):
    for name in ("lane-a", "lane-b"):
        lane = tmp_path / name
        lane.mkdir()
        (lane / ".git").write_text("gitdir: /tmp/example\n")
    client = DaemonClient(daemon.socket, "well")
    with pytest.raises(RuntimeError, match="exact lane worktree"):
        await client.call("dispatch_task", **dispatch_args(tmp_path))
    assert not daemon.registry.tasks


async def test_scoped_clients_and_reconnect(daemon, tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_BRIDGE_FAKE_DELAY", "0.3")
    a, b = DaemonClient(daemon.socket, "well"), DaemonClient(daemon.socket, "peetchr")
    first = await a.call("dispatch_task", **dispatch_args(tmp_path))
    task_id = first["task_id"]
    assert first["persisted"] is True
    assert first["request_id"]
    assert (daemon.registry.home / "conversations/well" / f"{task_id}.json").is_file()
    assert daemon.registry.store.load()["tasks"][0]["conversation_id"] == "well"
    del a
    assert len(await b.call("list_sessions")) == 0
    for method, args in (("check_task", {"task_id": task_id}),
                         ("cancel_task", {"task_id": task_id}),
                         ("get_result", {"task_id": task_id}),
                         ("inspect_task", {"task_id": task_id}),
                         ("get_transcript", {"session_id": first["session_id"]}),
                         ("end_session", {"session_id": first["session_id"]})):
        with pytest.raises(RuntimeError, match="unavailable"):
            await b.call(method, **args)
    resumed = DaemonClient(daemon.socket, "well")
    result = await resumed.call("wait_task", task_id=task_id, timeout_sec=5)
    assert result["status"] == "completed"
    assert len(await resumed.call("list_sessions")) == 1
    inspected = await resumed.call("inspect_task", task_id=task_id)
    assert inspected["activity_signal"] == "terminal"
    assert inspected["next_cursor"].startswith(task_id+":")
    assert await b.call("events", after=0) == []
    events = await resumed.call("events", after=0)
    assert any(e["status"] == "completed" for e in events)


async def test_lost_response_deduplicated(daemon, tmp_path):
    params = dispatch_args(tmp_path)
    _reader, writer = await asyncio.open_unix_connection(str(daemon.socket))
    writer.write(json.dumps({"method": "dispatch_task", "conversation_id": "a", "params": params}).encode()+b"\n")
    await writer.drain()
    writer.close()
    await writer.wait_closed()
    a = DaemonClient(daemon.socket, "a")
    # Server and retry may race: registry lock + same key permits only one task.
    result = await a.call("dispatch_task", **params)
    await a.call("wait_task", task_id=result["task_id"], timeout_sec=5)
    await asyncio.sleep(0.05)
    assert len(daemon.registry.tasks) == 1
    again = await a.call("dispatch_task", **params)
    assert again["reused"] and again["task_id"] == result["task_id"]
    with pytest.raises(RuntimeError, match="different dispatch"):
        await a.call("dispatch_task", **{**params, "message": "different"})
    b = await DaemonClient(daemon.socket, "b").call("dispatch_task", **params)
    assert b["task_id"] != result["task_id"]


async def test_storage_failure_never_launches(daemon, tmp_path, monkeypatch):
    started = []
    async def run(task_id):
        started.append(task_id)
    monkeypatch.setattr(daemon.registry, "_run_task", run)
    def fail(state):
        raise OSError("simulated full disk")
    monkeypatch.setattr(daemon.registry.store, "save", fail)
    with pytest.raises(RuntimeError, match="durable storage"):
        await DaemonClient(daemon.socket, "a").call("dispatch_task", **dispatch_args(tmp_path))
    assert not started
    assert all(t.status == TaskStatus.failed for t in daemon.registry.tasks.values())
    monkeypatch.undo()


async def test_nested_and_unknown_methods_denied(daemon, tmp_path):
    c = DaemonClient(daemon.socket, "a", nested=True)
    with pytest.raises(RuntimeError, match="nested"):
        await c.call("dispatch_task", **dispatch_args(tmp_path))
    assert (await c.call("coordinator_status"))["dispatch_enabled"] is False
    with pytest.raises(RuntimeError, match="unsupported"):
        await c.call("stop")
    with pytest.raises(RuntimeError, match="UUID required"):
        await DaemonClient(daemon.socket, "a").call("dispatch_task", agent="fake", message="a", cwd=str(tmp_path))


async def test_singleton_lock_and_socket_permissions(daemon):
    other = Daemon(DaemonRegistry.create(daemon.registry.home), daemon.socket)
    with pytest.raises(RuntimeError, match="another daemon"):
        await other.start()
    assert daemon.socket.stat().st_mode & 0o777 == 0o600
    assert daemon.registry.home.stat().st_mode & 0o777 == 0o700


async def test_cached_probe_single_flight(daemon, monkeypatch):
    calls = []
    async def probe():
        calls.append(1)
        await asyncio.sleep(0.01)
        return [{"agent": "fake"}]
    monkeypatch.setattr(daemon.registry, "list_agents", probe)
    replies = await asyncio.gather(*(DaemonClient(daemon.socket, str(i)).call("list_agents") for i in range(20)))
    assert len(calls) == 1 and len(replies) == 20


async def test_thirty_clients_fifty_workers_no_cross_talk(daemon, tmp_path):
    clients = [DaemonClient(daemon.socket, f"conversation-{i}") for i in range(30)]
    rows = await asyncio.gather(*(clients[i % 30].call("dispatch_task", **dispatch_args(tmp_path)) for i in range(50)))
    outcomes = await asyncio.gather(*(clients[i % 30].call("wait_task", task_id=row["task_id"], timeout_sec=5) for i, row in enumerate(rows)))
    assert all(r["status"] == "completed" for r in outcomes)
    assert len({r["task_id"] for r in rows}) == 50
    for i, client in enumerate(clients):
        events = await client.call("events", after=0)
        expected = {row["task_id"] for j, row in enumerate(rows) if j % 30 == i}
        assert {e["task_id"] for e in events} == expected


async def test_quiet_transcript_flushes(daemon):
    append_event("quiet", "message_chunk", {"text": "pending"}, daemon.registry.home)
    target = daemon.registry.home / "transcripts/quiet.jsonl"
    for _ in range(30):
        if target.exists():
            break
        await asyncio.sleep(0.1)
    assert "pending" in target.read_text()


async def test_native_failure_and_usage_finalized_before_result(daemon, tmp_path):
    client = DaemonClient(daemon.socket, "a")
    row = await client.call("dispatch_task", **dispatch_args(tmp_path))
    await client.call("wait_task", task_id=row["task_id"], timeout_sec=5)
    task = daemon.registry.tasks[row["task_id"]]
    session = daemon.registry.sessions[row["session_id"]]
    task.agent = "kimi"
    session.native_session_id = "native-test"
    wire = tmp_path/"wire.jsonl"
    wire.write_text(json.dumps({"type": "usage.record", "time": 1000, "agentId": "main",
                               "usage": {"inputOther": 10, "inputCacheRead": 20, "inputCacheCreation": 0, "output": 5}})+"\n"
                    + json.dumps({"type": "agent.turn.ended", "time": 1000, "outcome": "failed", "errorMessage": "quota exhausted"})+"\n")
    daemon.registry._usage_readers[task.task_id] = KimiUsageReader(wire, 0)
    await daemon.registry.refresh_usage(task, session)
    assert task.status == TaskStatus.failed
    assert task.stop_reason == "provider_error"
    assert task.usage["total_tokens"] == 35
    assert task.task_id not in daemon.registry._usage_readers


async def test_missing_terminal_wire_drops_reader_cache(daemon, tmp_path):
    client = DaemonClient(daemon.socket, "a")
    row = await client.call("dispatch_task", **dispatch_args(tmp_path))
    await client.call("wait_task", task_id=row["task_id"], timeout_sec=5)
    task = daemon.registry.tasks[row["task_id"]]
    session = daemon.registry.sessions[row["session_id"]]
    task.agent, session.native_session_id = "kimi", "native-test"
    daemon.registry._usage_readers[task.task_id] = KimiUsageReader(tmp_path/"missing", 0)
    with pytest.raises(OSError):
        await daemon.registry.refresh_usage(task, session)
    assert task.task_id not in daemon.registry._usage_readers


async def test_durable_key_survives_restart(tmp_path, short_home, monkeypatch):
    monkeypatch.setenv("AGENT_BRIDGE_ENABLE_FAKE", "1")
    home = short_home/"runtime"
    first = Daemon(DaemonRegistry.create(home), home/"s")
    await first.start()
    params = dispatch_args(tmp_path)
    a = DaemonClient(first.socket, "a")
    row = await a.call("dispatch_task", **params)
    await a.call("wait_task", task_id=row["task_id"], timeout_sec=5)
    await first.close()
    # Old PID still lives in this in-process test; owner stamps match this process.
    second = Daemon(DaemonRegistry.create(home), home/"s")
    await second.start()
    try:
        replay = await DaemonClient(second.socket, "a").call("dispatch_task", **params)
        assert replay["reused"] and replay["task_id"] == row["task_id"]
        assert len(second.registry.tasks) == 1
    finally:
        await second.close()


async def test_killed_stdio_client_does_not_cancel_daemon_task(daemon, tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_BRIDGE_FAKE_DELAY", "0.5")
    env = {**os.environ, "AGENT_BRIDGE_DAEMON_SOCKET": str(daemon.socket),
           "AGENT_BRIDGE_CONVERSATION_ID": "host-process", "AGENT_BRIDGE_SKIP_SKILL": "1",
           "AGENT_BRIDGE_PARENT_CONTEXT": ""}
    proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "agent_bridge.server",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL, env=env)
    async def rpc(ident, method, params):
        proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": ident, "method": method, "params": params}).encode()+b"\n")
        await proc.stdin.drain()
        while True:
            response = json.loads(await asyncio.wait_for(proc.stdout.readline(), timeout=10))
            if response.get("id") == ident:
                return response
    try:
        await rpc(1, "initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                  "clientInfo": {"name": "daemon-canary", "version": "1"}})
        proc.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        await proc.stdin.drain()
        response = await rpc(2, "tools/call", {"name": "dispatch_task", "arguments": dispatch_args(tmp_path)})
        result = response["result"].get("structuredContent") or json.loads(response["result"]["content"][0]["text"])
        assert result["ok"], result
        proc.kill()
        await proc.wait()
        resumed = DaemonClient(daemon.socket, "host-process")
        completed = await resumed.call("wait_task", task_id=result["task_id"], timeout_sec=5)
        assert completed["status"] == "completed"
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


async def test_usage_limits_shared_across_connections_but_nested_cannot_write(daemon, tmp_path):
    a = DaemonClient(daemon.socket, 'limit-owner')
    b = DaemonClient(daemon.socket, 'other-owner')
    nested = DaemonClient(daemon.socket, 'nested-owner', nested=True)
    await a.call('report_usage_limit', agent='fake', reason='monthly_limit')
    status = await b.call('check_availability', agent='fake')
    assert status['agents'][0]['status'] == 'blocked'
    with pytest.raises(RuntimeError, match='retained usage limit'):
        await b.call('dispatch_task', **dispatch_args(tmp_path))
    with pytest.raises(RuntimeError, match='nested'):
        await nested.call('clear_usage_limit', agent='fake')
    status = await nested.call('check_availability', agent='fake')
    assert status['coordinator']['dispatch_enabled'] is False
    await b.call('clear_usage_limit', agent='fake')
    assert not daemon.registry._usage_limits.active('fake', None)
