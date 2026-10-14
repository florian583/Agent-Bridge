"""One worker-owning runtime; bounded local RPC clients never own task lifetime."""
from __future__ import annotations

import asyncio
import contextlib
import fcntl
import inspect
import json
import logging
import os
import socket
import struct
import sys
import time
import uuid
from pathlib import Path
from typing import Any, TextIO

import psutil

from agent_bridge.binding import binding_path, binding_root, is_host_process
from agent_bridge.daemon_protocol import MAX_FRAME, conversation_id
from agent_bridge.daemon_registry import DaemonRegistry
from agent_bridge.models import TERMINAL_STATUSES
from agent_bridge.persist import atomic_write_json
from agent_bridge.transcript import flush_pending

log = logging.getLogger(__name__)
METHODS = frozenset({
    "list_agents", "env_status", "coordinator_status", "set_preferences",
    "dispatch_task", "wait_task", "check_task", "get_result", "get_transcript",
    "cancel_task", "list_sessions", "session_scope", "end_session", "events", "health", "inspect_task",
})
MUTATIONS = frozenset({"set_preferences", "dispatch_task", "cancel_task", "end_session"})
UNSCOPED = frozenset({"health", "list_agents", "env_status", "coordinator_status"})


def bound_conversation(client_pid: Any) -> str | None:
    """Recover a host's current SessionStart binding from the daemon process.

    MCP clients can lose access to process ancestry after a host resume. The
    worker-owning daemon has a stable view of the caller's process tree.
    """
    if not isinstance(client_pid, int) or client_pid <= 0:
        return None
    try:
        client = psutil.Process(client_pid)
        processes = [client, *client.parents()[:16]]
    except psutil.Error:
        return None
    for process in processes:
        try:
            row = json.loads(binding_path(process).read_text())
            return conversation_id(row["session_id"])
        except (OSError, KeyError, ValueError, RuntimeError, psutil.Error):
            continue
    return None


def create_conversation_binding(client_pid: Any) -> str | None:
    """Give an unbound coordinator a stable new scope, once per host process."""
    if not isinstance(client_pid, int) or client_pid <= 0:
        return None
    try:
        client = psutil.Process(client_pid)
        processes = [client, *client.parents()[:16]]
    except psutil.Error:
        return None
    for process in processes:
        try:
            if not is_host_process(process):
                continue
            path = binding_path(process)
            # Another request or SessionStart may have bound it since lookup.
            try:
                return conversation_id(json.loads(path.read_text())["session_id"])
            except (OSError, KeyError, ValueError, RuntimeError):
                pass
            session = uuid.uuid4().hex
            binding_root().mkdir(parents=True, exist_ok=True, mode=0o700)
            binding_root().chmod(0o700)
            atomic_write_json(path, {"session_id": session, "source": "daemon_fallback"})
            log.warning("conversation_binding_created host_pid=%s session=%s", process.pid, session)
            return session
        except (OSError, psutil.Error):
            continue
    return None


def peer_pid(writer: asyncio.StreamWriter) -> int | None:
    """Identify the local RPC client without relying on its envelope."""
    sock = writer.get_extra_info("socket")
    if sock is None:
        return None
    try:
        if sys.platform == "darwin":
            # macOS sys/un.h: SOL_LOCAL=0, LOCAL_PEERPID=0x002.
            return struct.unpack("i", sock.getsockopt(0, 0x002, 4))[0]
        if hasattr(socket, "SO_PEERCRED"):
            return struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[0]
    except (OSError, struct.error):
        return None
    return None


class Daemon:
    def __init__(self, registry: DaemonRegistry, socket: Path) -> None:
        self.registry = registry
        self.socket = socket
        self.boot_id = uuid.uuid4().hex
        self.started = time.monotonic()
        self.server: asyncio.Server | None = None
        self.requests: set[asyncio.Task] = set()
        self.connections: set[asyncio.StreamWriter] = set()
        self._lock_file: TextIO | None = None
        self._flush: asyncio.Task | None = None
        self._probe_lock = asyncio.Lock()
        self._probe_cache: tuple[float, Any] | None = None
        self.draining = False

    async def start(self) -> None:
        if len(os.fsencode(self.socket)) > 100:
            raise ValueError("Unix socket path too long; choose a shorter runtime home")
        self.socket.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.socket.parent.stat().st_uid != os.getuid():
            raise PermissionError("socket directory owned by another user")
        self.socket.parent.chmod(0o700)
        self.registry.home.chmod(0o700)
        self._lock_file = (self.registry.home / "daemon.lock").open("a")
        try:
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock_file.close()
            self._lock_file = None
            raise RuntimeError("another daemon owns this runtime home") from None
        try:
            await self.registry.start()
            self.socket.unlink(missing_ok=True)
            self.server = await asyncio.start_unix_server(self._handle, path=self.socket, limit=MAX_FRAME)
            self.socket.chmod(0o600)
            self._flush = asyncio.create_task(self._flush_periodically())
            log.info("daemon_started boot=%s pid=%s socket=%s", self.boot_id, os.getpid(), self.socket)
        except BaseException:
            try:
                await self.registry.stop()
            finally:
                self._lock_file.close()
                self._lock_file = None
            raise

    async def _flush_periodically(self) -> None:
        while True:
            await asyncio.sleep(2)
            try:
                await asyncio.to_thread(flush_pending, self.registry.home)
            except OSError:
                log.exception("transcript_flush_failed")
            for task in tuple(self.registry.tasks.values()):
                if task.status not in TERMINAL_STATUSES:
                    try:
                        await self.registry.refresh_usage(task, self.registry.sessions[task.session_id])
                    except (OSError, ValueError, KeyError):
                        log.exception("usage_refresh_failed task=%s", task.task_id)

    def active_count(self) -> int:
        return sum(t.status not in TERMINAL_STATUSES for t in self.registry.tasks.values())

    async def drain(self) -> None:
        self.draining = True
        # Once dispatch is refused, current request/task sets only shrink.
        if self.requests:
            await asyncio.gather(*tuple(self.requests), return_exceptions=True)
        if self.registry._bg:
            await asyncio.gather(*tuple(self.registry._bg.values()), return_exceptions=True)

    async def close(self) -> None:
        self.draining = True
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        # Explicit runtime shutdown only. Client EOF never calls this method.
        for writer in tuple(self.connections):
            writer.close()
        if self.requests:
            await asyncio.gather(*self.requests, return_exceptions=True)
        if self._flush:
            self._flush.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush
        try:
            await self.registry.stop()
        finally:
            self.socket.unlink(missing_ok=True)
            if self._lock_file:
                self._lock_file.close()
                self._lock_file = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if len(self.connections) >= 128:
            writer.close()
            return
        self.connections.add(writer)
        request: asyncio.Task | None = None
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=10)
            payload = json.loads(raw)
            # Retain execution even if the client vanishes before the reply.
            request = asyncio.create_task(self.call(payload, caller_pid=peer_pid(writer)))
            self.requests.add(request)
            request.add_done_callback(self.requests.discard)
            result = await asyncio.shield(request)
            reply = {"ok": True, "result": result}
        except Exception as exc:
            reply = {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}
        try:
            encoded = json.dumps(reply, ensure_ascii=False).encode() + b"\n"
            if len(encoded) > MAX_FRAME:
                encoded = b'{"ok":false,"error":"response too large; request a smaller page"}\n'
            writer.write(encoded)
            await asyncio.wait_for(writer.drain(), timeout=5)
        except (OSError, TimeoutError):
            pass
        finally:
            self.connections.discard(writer)
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    async def call(self, payload: dict, *, caller_pid: int | None = None) -> Any:
        if not isinstance(payload, dict):
            raise ValueError("request must be an object")
        method = payload.get("method")
        if method not in METHODS:
            raise ValueError("unsupported method")
        params = dict(payload.get("params") or {})
        if "conversation_id" in params:
            raise ValueError("conversation scope belongs in the RPC envelope")
        nested = payload.get("runtime_context") == "worker"
        if nested and method in MUTATIONS:
            raise PermissionError("nested dispatch and mutations are disabled")
        scope = None
        if method not in UNSCOPED:
            # Prefer the live host binding: a resumed or cleared Claude session
            # may have a newer UUID than an MCP process's cached environment.
            scope = bound_conversation(caller_pid)
            if scope is None:
                scope = bound_conversation(payload.get("client_pid"))
            if scope is None and payload.get("conversation_id") is not None:
                scope = conversation_id(payload["conversation_id"])
            if scope is None and payload.get("runtime_context") != "worker":
                scope = create_conversation_binding(caller_pid or payload.get("client_pid"))
            if scope is None:
                scope = conversation_id(payload.get("conversation_id"))
        if self.draining and method == "dispatch_task":
            raise RuntimeError("daemon draining; new dispatch disabled")
        for key, rows in (("task_id", self.registry.tasks), ("session_id", self.registry.sessions)):
            if params.get(key):
                row = rows.get(params[key])
                if row is None or row.conversation_id != scope:
                    raise PermissionError("task/session unavailable in this conversation")
        if method == "health":
            return {"boot_id": self.boot_id, "pid": os.getpid(), "mode": "shared-daemon",
                    "uptime_sec": int(time.monotonic()-self.started), "connections": len(self.connections),
                    "active_tasks": self.active_count(), "draining": self.draining,
                    "persistence_healthy": self.registry._state_error is None}
        if method == "coordinator_status":
            return {**self.registry.coordinator_status(), "ownership": "shared-daemon",
                    "runtime_context": "worker" if nested else "coordinator", "dispatch_enabled": not nested}
        if method == "list_agents":
            async with self._probe_lock:
                if self._probe_cache is None or time.monotonic()-self._probe_cache[0] > 30:
                    self._probe_cache = (time.monotonic(), await self.registry.list_agents())
                return self._probe_cache[1]
        if method == "session_scope":
            return {"scope": "conversation", "conversation_id": scope, "ownership": "shared-daemon"}
        if method == "list_sessions":
            session_rows = self.registry.list_sessions(**params)
            return [r for r in session_rows if self.registry.sessions[r["session_id"]].conversation_id == scope]
        if method == "events":
            return await asyncio.to_thread(self.registry.store.events, scope, **params)
        if method == "dispatch_task":
            if not params.get("request_id"):
                raise ValueError("request_id UUID required in daemon mode for retry safety")
            params["conversation_id"] = scope
        if method == "wait_task":
            params["timeout_sec"] = min(max(float(params.get("timeout_sec", 30)), 0), 180)
        result = getattr(self.registry, method)(**params)
        return await result if inspect.isawaitable(result) else result
