# MCP: start `memd serve --mcp` over stdio and call its four tools the way Claude Code / Claude Desktop does.
# Run: python examples/03_mcp_server.py [DATA_DIR]   (needs: pip install "memd-engine[mcp]"; host configs in examples/mcp/)
import asyncio
import json
import os
import sys
import tempfile

from mcp import ClientSession, StdioServerParameters, stdio_client

data = sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="memd-mcp-")

# exactly what examples/mcp/*.json tell a host to run: `memd serve --mcp`
# (here via `python -m memd.cli`, the same entry point without a PATH lookup)
server = StdioServerParameters(
    command=sys.executable,
    args=["-m", "memd.cli", "serve", "--mcp"],
    env={**os.environ, "MEMD_DATA": os.path.abspath(data), "MEMD_NS": "claude"},
)


def payload(result) -> dict:
    return json.loads(result.content[0].text)


async def main() -> None:
    async with stdio_client(server) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        tools = [t.name for t in (await session.list_tools()).tools]
        print("tools:", tools)
        assert tools == ["memory_search", "memory_save", "memory_forget", "memory_status"]

        saved = payload(await session.call_tool("memory_save", {
            "content": "The staging database is postgres 16 on db-stage-2",
            "entity_keys": ["infra.staging_db"]}))
        print("saved:", saved)

        found = payload(await session.call_tool("memory_search", {"query": "which database does staging run?"}))
        print(found["packed_context"])
        assert found["items"][0]["id"] == saved["saved"]

        # forget is two-step: a preview first, then confirm=true deletes
        preview = payload(await session.call_tool("memory_forget", {"query_or_id": "staging database"}))
        print("forget preview:", preview["will_delete"])
        done = payload(await session.call_tool("memory_forget", {"query_or_id": "staging database",
                                                                 "confirm": True}))
        assert done["deleted"] == [saved["saved"]]

        status = payload(await session.call_tool("memory_status", {}))
        print("status:", {k: status[k] for k in ("namespace", "records", "tombstones")})


asyncio.run(main())
