"""Select consumed-token usage without treating context occupancy as spend.

ACP ``UsageUpdate`` reports how full the context window is (``used`` / ``size``)
plus optional session ``cost``. That is not tokens consumed by the turn.
``PromptResponse.usage`` (and any payload that actually carries token counters)
is the consumed-token source. Callers must not sum occupancy into live totals
or merge both sources into one dict.
"""

from __future__ import annotations

from typing import Any

CONSUMED_TOKEN_KEYS = frozenset(
    {
        "inputTokens",
        "outputTokens",
        "totalTokens",
        "thoughtTokens",
        "cachedReadTokens",
        "cachedWriteTokens",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "thought_tokens",
        "cached_read_tokens",
        "cached_write_tokens",
    }
)


def consumed_token_usage(payload: Any) -> dict[str, Any] | None:
    """Return ``payload`` when it carries consumed-token counters.

    Context-only occupancy (``used`` / ``size``) is not consumed-token usage.
    """
    if not isinstance(payload, dict):
        return None
    if any(payload.get(key) is not None for key in CONSUMED_TOKEN_KEYS):
        return payload
    return None


def select_turn_usage(*candidates: Any) -> dict[str, Any]:
    """Prefer the first consumed-token payload. Skip context-only updates."""
    for candidate in candidates:
        usage = consumed_token_usage(candidate)
        if usage is not None:
            return usage
    return {}
