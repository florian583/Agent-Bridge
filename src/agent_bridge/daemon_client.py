"""Stateless RPC transport. No implicit retry, fallback, or worker shutdown."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from agent_bridge.daemon_protocol import MAX_FRAME


class DaemonClient:
    def __init__(self, socket: Path, conversation: str | None = None, *, nested: bool = False) -> None:
        self.socket = socket
        self.conversation = conversation
        self.nested = nested

    async def call(self, method: str, **params: Any) -> Any:
        from agent_bridge.binding import current_conversation

        scope = self.conversation or current_conversation()
        payload = {"method": method, "params": params, "conversation_id": scope,
                   "client_pid": os.getpid(),
                   "runtime_context": "worker" if self.nested else "coordinator"}
        encoded = json.dumps(payload).encode() + b"\n"
        if len(encoded) > MAX_FRAME:
            raise ValueError("request too large")
        async with asyncio.timeout(195):
            reader, writer = await asyncio.open_unix_connection(str(self.socket), limit=MAX_FRAME)
            try:
                writer.write(encoded)
                await writer.drain()
                response = json.loads(await reader.readline())
            finally:
                writer.close()
                await writer.wait_closed()
        if not response.get("ok"):
            raise RuntimeError(response.get("error", "daemon request failed"))
        return response["result"]
