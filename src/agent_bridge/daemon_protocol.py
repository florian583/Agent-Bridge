from __future__ import annotations

import re
from typing import Any

MAX_FRAME = 2 * 1024 * 1024


def conversation_id(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", value):
        raise ValueError("stable conversation_id required; bind the host session UUID, not cwd or tmux")
    return value
