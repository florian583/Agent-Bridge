"""Bind Claude's real SessionStart UUID to its process incarnation, never cwd."""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import psutil

from agent_bridge.persist import atomic_write_json


def binding_root() -> Path:
    return Path(os.environ.get("AGENT_BRIDGE_BINDINGS", str(Path.home()/".local/state/agent-bridge-bindings")))


def ancestors() -> list[psutil.Process]:
    return psutil.Process().parents()[:16]


def binding_path(process: psutil.Process) -> Path:
    return binding_root()/f"{process.pid}-{process.create_time():.6f}.json"


def is_host_process(process: psutil.Process) -> bool:
    executable = Path(process.exe())
    names = {Path(process.name()).name, executable.name}
    native_claude = (executable.parent.name == "versions"
                     and executable.parent.parent.name == "claude"
                     and re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][\w.-]+)?", executable.name))
    return bool(names.intersection({"claude", "codex", "codex-cli"}) or native_claude)


def current_conversation() -> str | None:
    explicit = os.environ.get("AGENT_BRIDGE_CONVERSATION_ID") or os.environ.get("CODEX_THREAD_ID")
    if explicit:
        return explicit
    for process in ancestors():
        try:
            row = json.loads(binding_path(process).read_text())
            return str(row["session_id"])
        except (OSError, KeyError, ValueError, psutil.Error):
            continue
    return None


def record_session(payload: dict) -> bool:
    from agent_bridge.daemon_protocol import conversation_id

    if payload.get("agent_id"):
        return False
    session = conversation_id(payload.get("session_id"))
    for process in ancestors():
        try:
            # Hook shells/python are intermediaries. Bind nearest actual host only.
            if not is_host_process(process):
                continue
            binding_root().mkdir(parents=True, exist_ok=True, mode=0o700)
            binding_root().chmod(0o700)
            atomic_write_json(binding_path(process), {"session_id": session})
            return True
        except (OSError, psutil.Error):
            continue
    return False


def main() -> None:
    try:
        if record_session(json.loads(sys.stdin.read(1024*1024))):
            return
    except Exception:
        pass
    # Hook must never block the user's turn. The relay can request a strict
    # result when using this command as its independent recovery path.
    print("agent-bridge: session binding unavailable", file=sys.stderr)
    if os.environ.get("AGENT_BRIDGE_BINDING_STRICT") == "1":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
