"""Daemon-only durable state. JSON is a read-only compatibility projection."""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from agent_bridge.persist import atomic_write_json


class DaemonStore:
    def __init__(self, home: Path) -> None:
        self.home = home
        self.path = home / "daemon.sqlite3"
        self.lock = threading.Lock()
        self._projection_cache: dict[Path, str] = {}
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS records (kind TEXT, id TEXT, payload TEXT NOT NULL, PRIMARY KEY(kind,id))")
            db.execute("CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, task_id TEXT, conversation_id TEXT, status TEXT, UNIQUE(task_id,status))")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            db.execute("PRAGMA synchronous=FULL")
            with db:
                yield db
        finally:
            db.close()

    def load(self) -> dict:
        with self.lock, self.connect() as db:
            result: dict[str, list[dict]] = {"sessions": [], "tasks": [], "requests": []}
            for kind, payload in db.execute("SELECT kind,payload FROM records"):
                result[kind].append(json.loads(payload))
            return result

    def save(self, state: dict) -> None:
        with self.lock, self.connect() as db:
            for kind, key in (("sessions", "session_id"), ("tasks", "task_id"), ("requests", "request_id")):
                keep = set()
                for row in state[kind]:
                    record_id = row[key]
                    keep.add(record_id)
                    payload = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                    db.execute(
                        "INSERT INTO records VALUES(?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload WHERE payload<>excluded.payload",
                        (kind, record_id, payload),
                    )
                    if kind == "tasks":
                        db.execute("INSERT OR IGNORE INTO events(task_id,conversation_id,status) VALUES(?,?,?)",
                                   (record_id, row.get("conversation_id"), row["status"]))
                for (record_id,) in db.execute("SELECT id FROM records WHERE kind=?", (kind,)).fetchall():
                    if record_id not in keep:
                        db.execute("DELETE FROM records WHERE kind=? AND id=?", (kind, record_id))
            db.execute("DELETE FROM events WHERE seq < (SELECT COALESCE(MAX(seq),0)-10000 FROM events)")
        # A projection failure must not roll back accepted, durable execution.
        try:
            atomic_write_json(self.home / "state.json", state)
            self._project_tasks(state)
        except OSError:
            import logging
            logging.getLogger(__name__).exception("compatibility projection failed; SQLite remains authoritative")

    def _project_tasks(self, state: dict) -> None:
        """Small watchable task files, created before a worker can be launched."""
        grouped: dict[str, list[dict]] = {}
        sessions = {s["session_id"]: s for s in state["sessions"]}
        for row in state["tasks"]:
            scope = row.get("conversation_id")
            if not scope:
                continue
            from agent_bridge.daemon_protocol import conversation_id
            scope = conversation_id(scope)
            projection = {k: row.get(k) for k in (
                "task_id", "session_id", "conversation_id", "agent", "status", "stop_reason",
                "created_at", "started_at", "finished_at", "usage", "error",
            )}
            projection["title"] = sessions.get(row["session_id"], {}).get("title")
            root = self.home / "conversations" / scope
            root.mkdir(parents=True, exist_ok=True, mode=0o700)
            target = root / f'{row["task_id"]}.json'
            # No rewrites for untouched rows when another task changes.
            encoded = json.dumps(projection, sort_keys=True)
            if self._projection_cache.get(target) != encoded:
                atomic_write_json(target, projection)
                self._projection_cache[target] = encoded
            grouped.setdefault(scope, []).append({"task_id": row["task_id"], "status": row["status"]})
        for scope, tasks in grouped.items():
            target = self.home / "conversations" / scope / "index.json"
            index = {"conversation_id": scope, "tasks": tasks}
            encoded = json.dumps(index, sort_keys=True)
            if self._projection_cache.get(target) != encoded:
                atomic_write_json(target, index)
                self._projection_cache[target] = encoded

    def events(self, conversation: str, after: int, limit: int = 100) -> list[dict]:
        with self.lock, self.connect() as db:
            return [dict(zip(("seq", "task_id", "conversation_id", "status"), row, strict=True))
                    for row in db.execute("SELECT seq,task_id,conversation_id,status FROM events WHERE conversation_id=? AND seq>? ORDER BY seq LIMIT ?",
                                          (conversation, after, min(max(limit, 1), 100)))]
