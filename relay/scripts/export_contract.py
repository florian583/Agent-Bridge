"""Export the Python MCP interface without starting a daemon or worker."""

import asyncio
import json
from pathlib import Path

from agent_bridge.server import INSTRUCTIONS, mcp


async def main():
    tools = await mcp.list_tools()
    contract = {
        "instructions": INSTRUCTIONS,
        "tools": [t.model_dump(mode="json", by_alias=True, exclude_none=True) for t in tools],
    }
    destination = Path(__file__).resolve().parents[1] / "contract.json"
    destination.write_text(json.dumps(contract, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
