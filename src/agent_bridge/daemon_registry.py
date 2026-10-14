from __future__ import annotations

import asyncio
import time
from datetime import datetime

from agent_bridge.daemon_store import DaemonStore
from agent_bridge.diagnostics import redact_diagnostic
from agent_bridge.kimi_observe import kimi_wire_path
from agent_bridge.models import TERMINAL_STATUSES, Session, Task, TaskStatus
from agent_bridge.native_usage import KimiUsageReader
from agent_bridge.registry import Registry


class DaemonRegistry(Registry):
    """Single writer runtime; legacy processes must use a different home."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store = DaemonStore(self.home)
        self.require_durable_dispatch = True
        self.config.server.idle_exit_sec = 0
        self._usage_readers: dict[str, KimiUsageReader] = {}
        self._usage_lookup: dict[str, float] = {}
        self._usage_lock = asyncio.Lock()

    async def refresh_usage(self, task: Task, session: Session) -> None:
        async with self._usage_lock:
            try:
                await self._refresh_usage(task, session)
            finally:
                if task.status in TERMINAL_STATUSES:
                    self._usage_readers.pop(task.task_id, None)
                    self._usage_lookup.pop(task.task_id, None)

    async def _refresh_usage(self, task: Task, session: Session) -> None:
        if task.agent != "kimi" or not session.native_session_id or not task.started_at:
            return
        reader = self._usage_readers.get(task.task_id)
        if reader is None:
            if task.status == TaskStatus.running and time.monotonic() - self._usage_lookup.get(task.task_id, -30) < 30:
                return
            self._usage_lookup[task.task_id] = time.monotonic()
            path = await asyncio.to_thread(kimi_wire_path, session.native_session_id)
            if path is None:
                return
            reader = KimiUsageReader(path, datetime.fromisoformat(task.started_at).timestamp()*1000)
            self._usage_readers[task.task_id] = reader
        usage = await asyncio.to_thread(reader.read)
        if usage and task.usage != usage:
            task.usage = usage
            self.save()
        if reader.failure and task.status == TaskStatus.completed:
            task.status = TaskStatus.failed
            task.error = redact_diagnostic(reader.failure)
            task.stop_reason = "provider_error"
            self.save()
    def _load_state(self) -> dict:
        return self.store.load()

    def _write_state(self, own: dict) -> None:
        self.store.save(own)

    async def start(self) -> None:
        interrupted = {row["task_id"] for row in self.store.load()["tasks"]
                       if row["status"] in {"queued", "running"}}
        await super().start()
        for task_id in interrupted:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.status = TaskStatus.failed
                task.error = "daemon_interrupted: worker side effects uncertain; inspect before any retry"
                task.stop_reason = "interrupted"
        if interrupted:
            self.save()
            if not await self.flush_state():
                raise RuntimeError("could not persist interrupted tasks")
