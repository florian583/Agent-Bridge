from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from agent_bridge.paths import log_path


def setup_logging(home: Path | None = None, *, daemon: bool = False) -> None:
    root = logging.getLogger()
    if getattr(root, "_agent_bridge_configured", False):
        return
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(formatter)
    destination = log_path(home)
    if daemon:
        destination = destination.with_name("daemon.log")
    file_handler = RotatingFileHandler(destination, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.setLevel(logging.INFO)
    if not daemon:
        root.addHandler(stderr)
    root.addHandler(file_handler)
    root._agent_bridge_configured = True  # type: ignore[attr-defined]
