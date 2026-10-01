"""The scripts in examples/ run, end to end, as a user would run them.

Each example asserts its own results, so a zero exit is the check. The S3
one needs an endpoint (MEMD_TEST_S3_ENDPOINT, as the other s3 tests); the
TypeScript one runs in the sdk-ts workflow, which has Node and the build.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"


def _run(script: str, *args: str, env: dict | None = None, timeout: int = 180) -> str:
    full = dict(os.environ)
    full.pop("MEMD_URL", None)       # 04: start its own server, never a real one
    full.pop("MEMD_API_KEY", None)
    full["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT / "src"), full.get("PYTHONPATH")]))
    full["MEMD_EMBEDDER"] = "hash"   # deterministic and light: no model download
    full.update(env or {})
    p = subprocess.run([sys.executable, str(EXAMPLES / script), *args], env=full, cwd=ROOT,
                       capture_output=True, text=True, timeout=timeout)
    assert p.returncode == 0, f"{script} exited {p.returncode}\n--- stdout\n{p.stdout}\n--- stderr\n{p.stderr[-4000:]}"
    return p.stdout


def test_quickstart(tmp_path):
    out = _run("01_quickstart.py", str(tmp_path / "data"))
    assert "hard-deleted text is in no file" in out


def test_sessions_and_facts(tmp_path):
    out = _run("02_sessions_and_facts.py", str(tmp_path / "data"))
    assert "after restart: The user's editor is Zed" in out


def test_mcp_server(tmp_path):
    pytest.importorskip("mcp")
    out = _run("03_mcp_server.py", str(tmp_path / "data"))
    assert "'memory_forget'" in out


def test_http_server_sdk():
    out = _run("04_http_server_sdk.py")
    assert out.rstrip().endswith("ok")


@pytest.mark.s3
def test_s3_minio():
    endpoint = os.environ.get("MEMD_TEST_S3_ENDPOINT")
    if not endpoint:
        pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)")
    pytest.importorskip("boto3")
    out = _run("06_s3_minio.py", env={
        "MEMD_S3_ENDPOINT": endpoint,
        "MEMD_S3_ACCESS_KEY": os.environ.get("MEMD_TEST_S3_KEY", "minioadmin"),
        "MEMD_S3_SECRET_KEY": os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin"),
    })
    assert "cold recall: We deploy" in out


def test_mcp_host_configs_launch_the_mcp_server():
    for name in ("claude_desktop_config.json", ".mcp.json"):
        server = json.loads((EXAMPLES / "mcp" / name).read_text())["mcpServers"]["memd"]
        assert [server["command"], *server["args"]] == ["memd", "serve", "--mcp"], name
        assert "MEMD_DATA" in server["env"], name


@pytest.mark.parametrize("path", sorted(
    [p for p in EXAMPLES.glob("*.py")] + [EXAMPLES / "05_ts" / "quickstart.mjs"]), ids=lambda p: p.name)
def test_every_example_opens_with_a_one_line_summary(path):
    first = path.read_text().splitlines()[0]
    assert first.startswith(("# ", "// ")) and len(first) > 20, first
