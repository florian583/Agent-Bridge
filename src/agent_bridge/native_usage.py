"""Incremental Kimi per-response counters. Context occupancy is never included."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path

KEYS = ("inputOther", "inputCacheRead", "inputCacheCreation", "output")
READ_BUDGET = 4 * 1024 * 1024
LINE_BUDGET = 2 * 1024 * 1024


class KimiUsageReader:
    def __init__(self, path: Path, started_ms: float) -> None:
        self.path = path
        self.started_ms = started_ms
        self.inode: int | None = None
        self.offset = 0
        self.pending = b""
        self.seen: set[str] = set()
        self.totals = dict.fromkeys(KEYS, 0)
        self.partial = False
        self.discard_line = False
        self.failure: str | None = None

    def read(self) -> dict:
        with self.path.open("rb") as stream:
            stat = os.fstat(stream.fileno())
            if self.inode != stat.st_ino or stat.st_size < self.offset:
                self.inode, self.offset, self.pending = stat.st_ino, 0, b""
                self.discard_line = False
            stream.seek(self.offset)
            chunk = stream.read(READ_BUDGET)
            self.offset += len(chunk)
        lines = (self.pending + chunk).split(b"\n")
        self.pending = lines.pop()
        for line in lines:
            if self.discard_line:
                self.discard_line = False
                continue
            if len(line) > LINE_BUDGET:
                self.partial = True
                continue
            try:
                row = json.loads(line)
            except (ValueError, UnicodeDecodeError):
                self.partial = True
                continue
            if not isinstance(row, dict):
                continue
            stamp = row.get("time")
            if (not isinstance(stamp, (int, float)) or isinstance(stamp, bool)
                    or not math.isfinite(stamp) or stamp < self.started_ms):
                continue
            if row.get("type") == "agent.turn.ended":
                outcome = row.get("outcome")
                self.failure = str(row.get("errorMessage") or outcome) if outcome in {"failed", "blocked"} else None
            if row.get("type") != "usage.record" or row.get("agentId") != "main":
                continue
            usage = row.get("usage")
            if not isinstance(usage, dict) or any(
                not isinstance(usage.get(k), int) or isinstance(usage[k], bool) or usage[k] < 0
                for k in KEYS
            ):
                self.partial = True
                continue
            # Current wire has no request ID: identical event replays share a key.
            # Retain keys across truncation/rotation. Never also add message.meta.usage.
            key = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
            if key in self.seen:
                continue
            if len(self.seen) >= 50000:
                self.partial = True
                continue
            self.seen.add(key)
            for name in KEYS:
                self.totals[name] += usage[name]
        if len(self.pending) > LINE_BUDGET:
            self.pending = b""
            self.discard_line = True
            self.partial = True
        if not self.seen:
            return {}
        return {"schema": "agent-bridge.tokens.v1", "source": "kimi.wire.usage.record",
                "scope": "task", "input_tokens": sum(self.totals[k] for k in KEYS[:3]),
                "output_tokens": self.totals["output"], "total_tokens": sum(self.totals.values()),
                "cache_read_tokens": self.totals["inputCacheRead"],
                "cache_write_tokens": self.totals["inputCacheCreation"],
                "samples": len(self.seen), "partial": self.partial or self.offset < stat.st_size}
