"""Bounded, cursor-based task activity. No full transcript or workspace scans."""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from agent_bridge.diagnostics import redact_diagnostic
from agent_bridge.models import Task
from agent_bridge.paths import transcript_path
from agent_bridge.transcript import _buffers, _buffers_lock

READ_LIMIT = 128 * 1024
OUTPUT_LIMIT = 16000


def _preview(value: Any, limit: int = 1200) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    return redact_diagnostic(text).encode("utf-8")[:limit].decode("utf-8", errors="ignore")


def _visible(row: dict) -> dict | None:
    kind, data = row.get("type"), row.get("data")
    if not isinstance(data, dict):
        return None
    event = {"time": row.get("ts"), "type": kind}
    # Codex command/tool events are wrapped JSONL. Never expose reasoning items.
    payload = data.get("payload")
    if isinstance(payload, dict) and isinstance(payload.get("item"), dict):
        item = payload["item"]
        item_type = item.get("type")
        if item_type not in {"command_execution", "mcp_tool_call", "file_change", "agent_message"}:
            return None
        event.update(type=item_type, phase=payload.get("type"))
        for key in ("id", "command", "tool", "server", "status", "exit_code", "text", "aggregated_output", "changes"):
            if item.get(key) is not None:
                event[key] = _preview(item[key])
        return event
    if kind not in {"tool_call", "tool_call_update", "message_chunk", "error", "permission", "turn_end"}:
        return None
    for key in ("tool_call_id", "title", "kind", "status", "input", "output", "locations", "text",
                "error", "message", "decision", "stop_reason"):
        if data.get(key) is not None:
            event[key] = _preview(data[key])
    return event


def task_activity(task: Task, home: Path, cursor: str | None = None, limit: int = 12) -> dict:
    if not 1 <= limit <= 50:
        raise ValueError("limit must be between 1 and 50")
    previous_inode, requested = 0, None
    if cursor is not None:
        try:
            task_id, inode_text, offset = cursor.split(":")
            previous_inode, requested = int(inode_text), int(offset)
            if task_id != task.task_id or requested < 0 or previous_inode < 0:
                raise ValueError
        except (ValueError, AttributeError):
            raise ValueError("cursor must be an inspect_task cursor for this task") from None
    path = transcript_path(task.session_id, home)
    # Capture file size + pending buffer atomically with respect to Bridge flushes.
    with _buffers_lock:
        pending = "".join(_buffers[path].lines).encode() if path in _buffers else b""
        try:
            stream = path.open("rb")
        except FileNotFoundError:
            stream = None
        stat = os.fstat(stream.fileno()) if stream else None
    persisted, inode = (stat.st_size, stat.st_ino) if stat else (0, 0)
    total = persisted + len(pending)
    reset = requested is not None and (requested > total or previous_inode not in {0, inode})
    start = max(0, total-READ_LIMIT) if requested is None or reset else requested
    end = min(total, start+READ_LIMIT)
    read_start = max(0, start-1)
    try:
        disk = b""
        if stream and read_start < persisted:
            stream.seek(read_start)
            disk = stream.read(min(end, persisted)-read_start)
        blob = disk + pending[max(0, read_start-persisted):max(0, end-persisted)]
    finally:
        if stream:
            stream.close()
    cut = start > 0 and blob[:1] != b"\n"
    if start > 0:
        blob = blob[1:]
    position, scanned, skipped = start, start, 0
    events: list[dict] = []
    output_size = 0
    tail = requested is None or reset
    dropped = 0
    lower = datetime.fromisoformat(task.started_at or task.created_at)
    upper = datetime.fromisoformat(task.finished_at) if task.finished_at else None
    for line in blob.splitlines(keepends=True):
        line_end = position + len(line)
        if not line.endswith(b"\n"):
            # Leave an incomplete tail for the next read; oversized lines advance.
            if len(line) == READ_LIMIT:
                scanned, skipped = line_end, skipped+1
            break
        if cut:
            cut = False
            position = scanned = line_end
            skipped += 1
            continue
        event = None
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                stamp = datetime.fromisoformat(row["ts"])
                if stamp >= lower and (upper is None or stamp <= upper):
                    event = _visible(row)
        except (ValueError, KeyError, TypeError, UnicodeDecodeError):
            skipped += 1
        if event is not None:
            size = len(json.dumps(event, ensure_ascii=False).encode())
            if not tail and (len(events) >= limit or output_size+size > OUTPUT_LIMIT):
                break
            output_size += size
            events.append(event)
            while len(events) > limit or output_size > OUTPUT_LIMIT:
                output_size -= len(json.dumps(events.pop(0), ensure_ascii=False).encode())
                dropped += 1
        position = scanned = line_end
    return {"events": events, "next_cursor": f"{task.task_id}:{inode}:{scanned}",
            "has_more": scanned < total, "cursor_reset": reset,
            "history_truncated": tail and (start > 0 or dropped > 0),
            "skipped_records": skipped, "bytes_read": len(blob),
            "source": "bridge_transcript", "observed_at": datetime.now().astimezone().isoformat()}
