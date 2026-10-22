"""Bounded, shared usage-limit observations; engine ranking belongs to the skill."""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

from agent_bridge.persist import atomic_write_json, file_lock

REASONS = {'monthly_limit', 'usage_limit', 'rate_limit', 'operator_hold'}
MODEL_WINDOWS = {
    'opus': 'weekly:opus', 'claude-opus-5-5': 'weekly:opus',
    'sonnet': 'weekly:sonnet', 'claude-sonnet-5-5': 'weekly:sonnet',
}


def reset_timestamp(value: str | None) -> float | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('reset date must include a timezone')
    return parsed.timestamp()


class UsageLimits:
    def __init__(self, home: Path):
        self.path = home / 'usage-limits.json'

    def read(self) -> list[dict]:
        if self.path.is_symlink():
            raise ValueError('unsafe usage limit file')
        if not self.path.exists():
            return []
        if self.path.stat().st_size > 256_000:
            raise ValueError('oversized usage limit file')
        data = json.loads(self.path.read_text())
        if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('limits'), list):
            raise ValueError('invalid usage limit file')
        rows = data['limits']
        if len(rows) > 256:
            raise ValueError('too many usage limits')
        for row in rows:
            if (not isinstance(row, dict) or row.get('reason') not in REASONS
                    or not isinstance(row.get('agent'), str)
                    or (row.get('model') is not None and not isinstance(row['model'], str))
                    or row.get('source') not in {'operator', 'observed_error'}):
                raise ValueError('invalid usage limit record')
            reset_timestamp(row.get('resets_at'))
        return rows

    def active(self, agent: str, model: str | None, *, now: float | None = None, include_models: bool = False) -> list[dict]:
        now = time.time() if now is None else now
        return [r for r in self.read() if r['agent'] == agent
                and (include_models or r.get('model') is None or r['model'] == model)
                and (r.get('resets_at') is None or reset_timestamp(r['resets_at']) > now)]

    def set(self, agent: str, model: str | None, reason: str, resets_at: str | None,
            *, source: str = 'operator') -> dict:
        if reason not in REASONS or source not in {'operator', 'observed_error'}:
            raise ValueError('invalid usage limit reason/source')
        if not agent or len(agent) > 128 or (model is not None and (not model or len(model) > 256)):
            raise ValueError('invalid usage limit identity')
        reset = reset_timestamp(resets_at)
        now = time.time()
        if reset is not None and not now < reset <= now + 366 * 86400:
            raise ValueError('reset must be in the next 366 days')
        row = dict(agent=agent, model=model, reason=reason, source=source,
                   observed_at=datetime.fromtimestamp(now, UTC).isoformat(),
                   resets_at=datetime.fromtimestamp(reset, UTC).isoformat() if reset else None)
        with file_lock(self.path.with_suffix('.lock'), timeout_sec=2):
            rows = self.read()
            existing = next((r for r in rows if r['agent'] == agent and r.get('model') == model), None)
            if source == 'observed_error' and existing and existing['source'] == 'operator':
                expiry = reset_timestamp(existing.get('resets_at'))
                if expiry is None or expiry > now:
                    return existing
            rows = [r for r in rows if (r['agent'], r.get('model')) != (agent, model)
                    and (r.get('resets_at') is None or reset_timestamp(r['resets_at']) > now)]
            if len(rows) >= 256:
                raise ValueError('too many usage limits')
            atomic_write_json(self.path, dict(version=1, limits=[*rows, row]))
        return row

    def clear(self, agent: str, model: str | None) -> dict:
        with file_lock(self.path.with_suffix('.lock'), timeout_sec=2):
            rows = self.read()
            kept = [r for r in rows if (r['agent'], r.get('model')) != (agent, model)]
            atomic_write_json(self.path, dict(version=1, limits=kept))
        return dict(agent=agent, model=model, cleared=len(rows) - len(kept))


def quota_blocked(quota: dict, model: str | None) -> bool:
    if quota.get('stale'):
        return False
    if quota.get('status') == 'exhausted':
        return True
    window = MODEL_WINDOWS.get(model)
    return bool(window and any(w.get('name') == window and w.get('remaining_percent') == 0
                               for w in quota.get('windows', [])))
