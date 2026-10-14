"""Regression: a live MCP client follows a resumed Claude host UUID."""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import psutil

from agent_bridge.binding import binding_path
from agent_bridge.daemon import Daemon
from agent_bridge.daemon_client import DaemonClient


class ScopeReconnectTest(unittest.TestCase):
    def test_missing_binding_creates_and_reuses_scope(self) -> None:
        async def scenario(root: Path) -> None:
            socket_path = root / "daemon.sock"
            daemon = Daemon(SimpleNamespace(tasks={}, sessions={}), socket_path)
            server = await asyncio.start_unix_server(daemon._handle, path=socket_path)
            try:
                client = DaemonClient(socket_path)
                first = await client.call("session_scope")
                second = await client.call("session_scope")
                self.assertEqual(first["conversation_id"], second["conversation_id"])
                self.assertEqual(len(first["conversation_id"]), 32)
                self.assertEqual(json.loads(binding_path(psutil.Process()).read_text())["session_id"], first["conversation_id"])
            finally:
                server.close()
                await server.wait_closed()

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {"AGENT_BRIDGE_BINDINGS": directory, "AGENT_BRIDGE_CONVERSATION_ID": "", "CODEX_THREAD_ID": ""}),
            patch("agent_bridge.daemon.is_host_process", side_effect=lambda p: p.pid == os.getpid()),
        ):
            asyncio.run(scenario(Path(directory)))

    def test_missing_and_stale_client_scope_follow_host_binding(self) -> None:
        async def scenario(root: Path) -> None:
            binding = binding_path(psutil.Process())
            binding.write_text(json.dumps({"session_id": "session_before_resume"}))
            socket_path = root / "daemon.sock"
            daemon = Daemon(SimpleNamespace(tasks={}, sessions={}), socket_path)
            server = await asyncio.start_unix_server(daemon._handle, path=socket_path)
            try:
                client = DaemonClient(socket_path)
                first = await client.call("session_scope")
                self.assertEqual(first["conversation_id"], "session_before_resume")

                binding.write_text(json.dumps({"session_id": "session_after_resume"}))
                second = await client.call("session_scope")
                self.assertEqual(second["conversation_id"], "session_after_resume")

                stale_client = DaemonClient(socket_path, conversation="session_before_resume")
                third = await stale_client.call("session_scope")
                self.assertEqual(third["conversation_id"], "session_after_resume")
            finally:
                server.close()
                await server.wait_closed()

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {"AGENT_BRIDGE_BINDINGS": directory}),
        ):
            asyncio.run(scenario(Path(directory)))


if __name__ == "__main__":
    unittest.main()
