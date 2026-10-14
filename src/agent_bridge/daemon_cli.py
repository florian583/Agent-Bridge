from __future__ import annotations

import argparse
import asyncio
import json
import signal
from pathlib import Path

from agent_bridge.daemon import Daemon
from agent_bridge.daemon_client import DaemonClient
from agent_bridge.daemon_registry import DaemonRegistry
from agent_bridge.logging_setup import setup_logging
from agent_bridge.paths import ensure_home


async def serve(home: Path) -> None:
    ensure_home(home)
    home.chmod(0o700)
    setup_logging(home, daemon=True)
    registry = DaemonRegistry.create(home)
    assert isinstance(registry, DaemonRegistry)
    daemon = Daemon(registry, home/"daemon.sock")
    await daemon.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    # Ordinary termination drains accepted work. Never cancel active workers to upgrade.
    await daemon.drain()
    await daemon.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Shared Agent Bridge runtime (POSIX)")
    parser.add_argument("command", choices=["serve", "status"])
    parser.add_argument("--home", type=Path, default=Path.home()/".agent-bridge-daemon")
    args = parser.parse_args()
    if args.command == "serve":
        asyncio.run(serve(args.home.expanduser().resolve()))
    else:
        result = asyncio.run(DaemonClient(args.home.expanduser()/"daemon.sock").call("health"))
        print(json.dumps(result))


if __name__ == "__main__":
    main()
