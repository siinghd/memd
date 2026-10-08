# memd as an MCP server

`memd serve --mcp` speaks MCP (Model Context Protocol) over stdio. It gives
exactly four tools: `memory_search`, `memory_save`, `memory_forget` (first a
preview, then `confirm: true`) and `memory_status`. It also gives the
`memory://recent` and `memory://profile` resources.

```bash
pip install "memd-engine[mcp]"
```

| variable | meaning | default |
|---|---|---|
| `MEMD_DATA` | data directory (or an `s3://bucket/prefix` root) | `./memd-data` |
| `MEMD_NS` | namespace that the server reads and writes | `default` |
| `MEMD_PACKING` | layout of the `memory_search` text: `sessions` or `flat` | `sessions` |

`memory_search` takes `query`, `budget_tokens` (default 12,000) and
`packing` (`sessions` or `flat`; the default is `MEMD_PACKING`). It returns
the packed text in `packed_context`, and the metadata of the hits in
`items` (id, kind, source, time, validity), without their text.

Use an **absolute** `MEMD_DATA`. Hosts start the server from a working
directory that you do not control.

Several hosts can use the same data directory. Each host starts its own
`memd serve --mcp` process. The first process that opens the namespace
writes it. The other processes send their calls to that process (write
forwarding). If that process stops, another process becomes the writer.
With `MEMD_FORWARDING=off`, the second process gets `NamespaceBusyError`.

## Claude Code

```bash
claude mcp add memd -e MEMD_DATA="$HOME/.memd/data" -- memd serve --mcp
claude mcp list        # memd should show as connected
```

To write the configuration to the `.mcp.json` of the repository instead, add
`-s project`. Then everyone on the project gets it. The file
[`.mcp.json`](.mcp.json) in this directory shows that format. To make the
server available in every project, use `-s user`.

## Claude Desktop

1. Merge [`claude_desktop_config.json`](claude_desktop_config.json) into the
   configuration file of the app (Settings → Developer → Edit Config):
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`
2. If `memd` is in a virtualenv, put the absolute path of the executable in
   `command` (`which memd` shows it). Claude Desktop does not inherit the
   `PATH` of your shell.
3. Restart the app.

## Check it without a host

```bash
python examples/03_mcp_server.py
```

This script starts the server exactly as a host does. Then it calls every
tool.
