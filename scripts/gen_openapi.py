"""Print the REST API's OpenAPI schema (JSON), generated from the server itself.

    python scripts/gen_openapi.py [--out openapi.json]

The docs build calls build_schema() (scripts/docs_hooks.py) to render the
HTTP reference and publish reference/openapi.json, so the docs can never
drift from the routes. The schema is the one a self-hosted
`memd serve --http` serves at /openapi.json with MEMD_ENABLE_DOCS=1; the
hosted-mode billing routes are documented in README-engine.md ("Hosted
mode & billing").
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

# the schema route is off by default; the hash embedder skips any model load
_ENV = {"MEMD_ENABLE_DOCS": "1", "MEMD_EMBEDDER": "hash"}


def build_schema() -> dict:
    from memd.server.http import create_app

    saved = {k: os.environ.get(k) for k in _ENV}
    os.environ.update(_ENV)
    try:
        with tempfile.TemporaryDirectory() as data:
            app = create_app(data_dir=data, hosted=False)
            try:
                return app.openapi()
            finally:
                app.state.engine.close()
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def render(schema: dict) -> str:
    return json.dumps(schema, indent=2, ensure_ascii=False) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, help="write here instead of stdout")
    args = ap.parse_args()
    text = render(build_schema())
    if args.out:
        args.out.write_text(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
