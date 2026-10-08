"""MCP stdio transport test: spawn `memd serve --mcp` as a subprocess and
speak JSON-RPC to it like a real MCP host."""
import json
import subprocess
import sys
import os

import pytest

pytest.importorskip("mcp")


def _rpc(proc, payload: dict) -> dict:
    proc.stdin.write((json.dumps(payload) + "\n").encode())
    proc.stdin.flush()
    line = proc.stdout.readline()
    assert line, "no response from MCP server"
    return json.loads(line)


@pytest.fixture()
def server(tmp_path):
    env = dict(os.environ)
    env["MEMD_DATA"] = str(tmp_path / "data")
    env["MEMD_NS"] = "mcptest"
    p = subprocess.Popen(
        [sys.executable, "-m", "memd.server.mcp_server"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
    )
    yield p
    p.kill()


def test_mcp_stdio_handshake_and_tools(server):
    init = _rpc(server, {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                         "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                                    "clientInfo": {"name": "test-host", "version": "0"}}})
    assert init["result"]["serverInfo"]["name"] == "memd"
    server.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
    server.stdin.flush()

    tools = _rpc(server, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    names = [t["name"] for t in tools["result"]["tools"]]
    assert names == ["memory_search", "memory_save", "memory_forget", "memory_status"]

    saved = _rpc(server, {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                          "params": {"name": "memory_save",
                                     "arguments": {"content": "stdio mcp works", "entity_keys": ["x.y"]}}})
    assert "saved" in saved["result"]["content"][0]["text"]

    found = _rpc(server, {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                          "params": {"name": "memory_search", "arguments": {"query": "stdio mcp"}}})
    assert "stdio mcp works" in found["result"]["content"][0]["text"]

    status = _rpc(server, {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                           "params": {"name": "memory_status", "arguments": {}}})
    assert '"records": 1' in status["result"]["content"][0]["text"]


def test_mcp_persistence_across_restart(tmp_path):
    data_dir = str(tmp_path / "data")
    env = dict(os.environ)
    env["MEMD_DATA"] = data_dir
    env["MEMD_NS"] = "persist"

    def start():
        return subprocess.Popen(
            [sys.executable, "-m", "memd.server.mcp_server"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        )

    p1 = start()
    _rpc(p1, {"jsonrpc": "2.0", "id": 1, "method": "initialize",
              "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}})
    _rpc(p1, {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
              "params": {"name": "memory_save", "arguments": {"content": "survives restart"}}})
    p1.kill()

    p2 = start()  # minute-10 story: kill process, restart, recall
    _rpc(p2, {"jsonrpc": "2.0", "id": 1, "method": "initialize",
              "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}})
    res = _rpc(p2, {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": {"name": "memory_search", "arguments": {"query": "survives restart"}}})
    assert "survives restart" in res["result"]["content"][0]["text"]
    p2.kill()


def test_mcp_search_defaults_to_a_12k_session_pack(server):
    _rpc(server, {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                  "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                             "clientInfo": {"name": "t", "version": "0"}}})
    server.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
    server.stdin.flush()
    tools = _rpc(server, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    search = next(t for t in tools["result"]["tools"] if t["name"] == "memory_search")
    assert search["inputSchema"]["properties"]["budget_tokens"]["default"] == 12000
    assert "packing" in search["inputSchema"]["properties"]
    _rpc(server, {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                  "params": {"name": "memory_save", "arguments": {"content": "the deploy freeze starts monday"}}})
    found = _rpc(server, {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                          "params": {"name": "memory_search", "arguments": {"query": "deploy freeze"}}})
    body = json.loads(found["result"]["content"][0]["text"])
    assert body["packed_context"].startswith("Relevant excerpts from past conversations")
    assert "the deploy freeze starts monday" in body["packed_context"]

    flat = _rpc(server, {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                         "params": {"name": "memory_search",
                                    "arguments": {"query": "deploy freeze", "packing": "flat"}}})
    body = json.loads(flat["result"]["content"][0]["text"])
    assert body["packed_context"].startswith("Relevant memories (provenance-tagged")
