"""The scripts in examples/ run, end to end, as a user would run them.

Each example asserts its own results, so a zero exit is the check. The S3
one needs an endpoint (MEMD_TEST_S3_ENDPOINT, as the other s3 tests). The
TypeScript ones (examples/ts) need Node, the sdk-ts build and an
`npm install` in examples/ts; without them they skip, unless
MEMD_REQUIRE_TS_EXAMPLES is set (the sdk-ts workflow sets it).
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"
TS = EXAMPLES / "ts"


def _env(extra: dict | None = None) -> dict:
    full = dict(os.environ)
    for name in ("MEMD_URL", "MEMD_API_KEY", "MEMD_NAMESPACE", "MEMD_ADMIN_KEY"):
        full.pop(name, None)         # start a throwaway server, never use a real one
    full["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT / "src"), full.get("PYTHONPATH")]))
    full["MEMD_EMBEDDER"] = "hash"   # deterministic and light: no model download
    full.update(extra or {})
    return full


def _run(script: str, *args: str, env: dict | None = None, timeout: int = 180) -> str:
    p = subprocess.run([sys.executable, str(EXAMPLES / script), *args], env=_env(env), cwd=ROOT,
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


def _ts_examples() -> list[Path]:
    return sorted(TS.glob("[0-9][0-9]_*.ts")) + [TS / "quickstart.mjs"]


def _node() -> str:
    """The node binary, once examples/ts is installed against the sdk-ts build."""
    node = shutil.which("node")
    if node is None:
        missing = "node is not on PATH"
    elif not (ROOT / "sdk-ts" / "dist" / "index.js").is_file():
        missing = "sdk-ts is not built (cd sdk-ts && npm ci && npm run build)"
    elif not (TS / "node_modules" / "tsx").is_dir():
        missing = "examples/ts is not installed (cd examples/ts && npm install)"
    else:
        return node
    if os.environ.get("MEMD_REQUIRE_TS_EXAMPLES"):
        pytest.fail(missing)  # a skipped run would pass green having proved nothing
    pytest.skip(missing)


def test_ts_examples_typecheck():
    node = _node()
    p = subprocess.run([node, str(TS / "node_modules" / "typescript" / "bin" / "tsc"), "--noEmit", "-p", str(TS)],
                       cwd=TS, capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, f"tsc --noEmit failed\n{p.stdout}{p.stderr}"


@pytest.mark.parametrize("path", _ts_examples(), ids=lambda p: p.name)
def test_ts_example(path):
    node = _node()
    # tsx runs the .ts files on any Node >= 20; the .mjs one needs nothing
    run = [node, path.name] if path.suffix == ".mjs" else [node, "--import", "tsx", path.name]
    # run as its header says: one spanning namespaces asks local_server.py for the operator key
    admin = ["--admin"] if "--admin" in path.read_text().splitlines()[1] else []
    p = subprocess.run([sys.executable, str(EXAMPLES / "local_server.py"), *admin, "--", *run],
                       env=_env(), cwd=TS, capture_output=True, text=True, timeout=180)
    assert p.returncode == 0, f"{path.name} exited {p.returncode}\n--- stdout\n{p.stdout}\n--- stderr\n{p.stderr[-4000:]}"
    assert p.stdout.rstrip().endswith("ok"), p.stdout


@pytest.mark.parametrize("path", sorted(
    [p for p in EXAMPLES.glob("*.py")] + _ts_examples() + list((TS / "nextjs").rglob("*.ts"))),
    ids=lambda p: p.name)
def test_every_example_opens_with_a_one_line_summary(path):
    first = path.read_text().splitlines()[0]
    assert first.startswith(("# ", "// ")) and len(first) > 20, first
