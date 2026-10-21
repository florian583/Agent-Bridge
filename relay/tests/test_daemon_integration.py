"""Exercise real daemon ownership with its in-process fake adapter only."""

import asyncio
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import psutil
from test_protocol import Client, content

from agent_bridge.daemon import Daemon
from agent_bridge.daemon_registry import DaemonRegistry


class DaemonIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="relay-real-", dir="/tmp")
        self.root = Path(self.temp.name)
        self.env = patch.dict(
            os.environ,
            {
                "AGENT_BRIDGE_ENABLE_FAKE": "1",
                "AGENT_BRIDGE_BINDINGS": str(self.root / "bindings"),
                "AGENT_BRIDGE_FAKE_DELAY": "0.15",
            },
        )
        self.env.start()
        home = self.root / "runtime"
        self.daemon = Daemon(DaemonRegistry.create(home), home / "s")
        await self.daemon.start()
        self.daemon.path = str(self.daemon.socket)
        self.clients = []

    async def asyncTearDown(self):
        for client in self.clients:
            await asyncio.to_thread(client.close)
        await self.daemon.close()
        self.env.stop()
        self.temp.cleanup()

    async def client(self, scope):
        client = await asyncio.to_thread(Client, self.daemon, extra_env={"AGENT_BRIDGE_CONVERSATION_ID": scope})
        self.clients.append(client)
        return client

    async def call(self, client, name, args, request_id=2):
        return content(await asyncio.to_thread(client.call, name, args, request_id))

    async def test_scope_isolation_reconnect_and_deduplication(self):
        a, b = await self.client("scope-a"), await self.client("scope-b")
        args = {"agent": "fake", "message": "test", "cwd": str(self.root), "request_id": str(uuid.uuid4())}
        first = await self.call(a, "dispatch_task", args)
        self.assertTrue(first["ok"], first)
        task = first["task_id"]
        await asyncio.to_thread(a.close)
        self.clients.remove(a)
        a = await self.client("scope-a")
        denied = await self.call(b, "get_result", {"task_id": task})
        self.assertFalse(denied["ok"])
        repeated = await self.call(a, "dispatch_task", args)
        self.assertEqual(repeated["task_id"], task)
        finished = await self.call(a, "wait_task", {"task_id": task, "timeout_sec": 3}, 3)
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(self.daemon.registry.tasks), 1)
        self.assertEqual((await self.call(b, "list_sessions", {}, 3))["sessions"], [])

    async def test_live_birth_bound_identity_overrides_stale_environment(self):
        client = await self.client("stale-environment")
        process = psutil.Process(client.process.pid)
        binding = self.root / "bindings" / f"{process.pid}-{process.create_time():.6f}.json"
        binding.parent.mkdir()
        binding.write_text(json.dumps({"session_id": "live-a"}))
        first = await self.call(client, "list_sessions", {})
        self.assertEqual(first["conversation_id"], "live-a")
        binding.write_text(json.dumps({"session_id": "live-b"}))
        second = await self.call(client, "list_sessions", {}, 3)
        self.assertEqual(second["conversation_id"], "live-b")
        binding.unlink()
        stale = binding.with_name(f"{process.pid}-1.000000.json")
        stale.write_text(json.dumps({"session_id": "wrong-process-generation"}))
        third = await self.call(client, "list_sessions", {}, 4)
        self.assertEqual(third["conversation_id"], "stale-environment")


if __name__ == "__main__":
    unittest.main()
