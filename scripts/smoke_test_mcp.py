"""Launch the MCP server over stdio like Claude would, list its tools, and call a few."""

import asyncio
import json
import sys

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


async def main() -> None:
    params = StdioServerParameters(command=sys.executable, args=["-m", "dora_mcp", "serve"])
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = await session.list_tools()
        print("tools:", [t.name for t in tools.tools])
        for name, args in [
            ("pr_cycle_time", {"start": "2026-10-01", "end": "2026-10-12"}),
            ("change_failure_rate", {"repo": "uv", "start": "2026-09-01"}),
            ("run_sql", {"sql": "DROP TABLE dim_repo"}),
            ("lead_time_for_changes", {"repo": "vscode"}),
        ]:
            result = await session.call_tool(name, args)
            text = result.content[0].text
            print(f"\n{name}({args}) error={result.is_error}\n{text[:600]}")


asyncio.run(main())
