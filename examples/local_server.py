# Helper for the server examples: start a throwaway `memd serve --http`, mint a namespace key, stop it after.
# Run: python examples/local_server.py [--ns NAME] -- <command ...>   (the command gets MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE)
"""Not an API demo itself: 04_http_server_sdk.py imports it, and the
TypeScript example runs under it. Against a server you already run, set
MEMD_URL and MEMD_API_KEY instead and skip this helper."""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _memd(*args: str) -> list[str]:
    # `python -m memd.cli` is the `memd` console script, minus the PATH lookup
    return [sys.executable, "-m", "memd.cli", *args]


@contextlib.contextmanager
def local_server(namespace: str = "demo"):
    """Yield (base_url, api_key, namespace) for a fresh server on a temp data dir."""
    data = tempfile.mkdtemp(prefix="memd-example-server-")
    port = _free_port()
    env = {**os.environ, "MEMD_DATA": data,
           # the operator key: spans every namespace, never handed to apps
           "MEMD_ADMIN_KEY": secrets.token_urlsafe(32)}
    proc = subprocess.Popen(_memd("serve", "--http", "--host", "127.0.0.1", "--port", str(port)),
                            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 60
        while True:
            if proc.poll() is not None:
                raise RuntimeError(f"memd exited: {proc.stderr.read().decode()[-2000:]}")
            try:
                with urllib.request.urlopen(f"{base_url}/health", timeout=1) as r:
                    if json.load(r)["ok"]:
                        break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        # a per-namespace key, exactly what you would give an application
        out = subprocess.run(_memd("key", "create", "--ns", namespace, "--data", data),
                             env=env, check=True, capture_output=True, text=True).stdout
        yield base_url, json.loads(out)["key"], namespace
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(data, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ns", default="demo")
    ap.add_argument("command", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    cmd = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not cmd:
        ap.error("give a command to run, after --")
    with local_server(args.ns) as (url, key, ns):
        env = {**os.environ, "MEMD_URL": url, "MEMD_API_KEY": key, "MEMD_NAMESPACE": ns}
        return subprocess.run(cmd, env=env).returncode


if __name__ == "__main__":
    sys.exit(main())
