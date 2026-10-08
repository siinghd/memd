# memd as an MCP server

`memd serve --mcp` speaks MCP over stdio and exposes exactly four tools:
`memory_search`, `memory_save`, `memory_forget` (a preview first, then
`confirm: true`) and `memory_status`, plus the `memory://recent` and
`memory://profile` resources.

```bash
pip install "memd-engine[mcp]"
```

| variable | meaning | default |
|---|---|---|
| `MEMD_DATA` | data directory (or an `s3://bucket/prefix` root) | `./memd-data` |
| `MEMD_NS` | namespace the server reads and writes | `default` |

Use an **absolute** `MEMD_DATA`: hosts start the server from a working
directory you do not control. One process per data root: two hosts pointed
at the same directory get `NamespaceBusyError` in the second one.

## Claude Code

```bash
claude mcp add memd -e MEMD_DATA="$HOME/.memd/data" -- memd serve --mcp
claude mcp list        # memd should show as connected
```

Add `-s project` to write it to the repository's `.mcp.json` instead (the
file [`.mcp.json`](.mcp.json) here is that shape) so everyone on the
project gets it; `-s user` makes it available in every project.

## Claude Desktop

Merge [`claude_desktop_config.json`](claude_desktop_config.json) into the
app's config file (Settings → Developer → Edit Config):

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

then restart the app. Claude Desktop does not inherit your shell's `PATH`:
if `memd` lives in a virtualenv, put the absolute path of the executable in
`command` (`which memd` prints it).

## Check it without a host

```bash
python examples/03_mcp_server.py
```

starts the server exactly as a host would and calls every tool.
