"""A search forwarded to the namespace's holder (write forwarding) keeps
every search parameter: the layout and the budget the caller asked for,
and the caller's own `packing` default rather than the holder's. (The
holder's retrieval settings - reranker, pack_mode, pack_resolve_dates -
apply, as they do to every forwarded read.)"""
import json
import os
import subprocess
import sys

import pytest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC)

from memd.engine.memory import Memory  # noqa: E402
from memd.query.packing import DEFAULT_HEADER, SESSIONS_HEADER  # noqa: E402

CHILD = r'''
import json, os, sys
sys.path.insert(0, sys.argv[2])
os.environ.pop("MEMD_PACKING", None)
from memd.engine.memory import Memory
b = Memory(sys.argv[1], namespace="shared", encrypt=False,
           config={"embedder": "hash", "packing": "flat", "pack_resolve_dates": True})
assert b.ns is None or b.ns.namespace != "shared" or True
q = "how do we deploy"
out = {}
r = b.search(q, user_id="u1", packing="flat", budget_tokens=300)
out["flat_per_call"] = [r.packed_context.splitlines()[0], r.budget]
out["config_default"] = b.search(q, user_id="u1").packed_context.splitlines()[0]
r = b.search(q, user_id="u1", packing="sessions")
out["sessions_per_call"] = r.packed_context.splitlines()[0]
out["sessions_budget"] = r.budget
out["pack"] = b.pack([{"role": "user", "content": q}], user_id="u1")[0]["content"].splitlines()[0]
b.close()
print(json.dumps(out))
'''


@pytest.fixture()
def holder(tmp_path, monkeypatch):
    monkeypatch.delenv("MEMD_PACKING", raising=False)
    root = str(tmp_path / "root")
    a = Memory(root, namespace="shared", encrypt=False, config={"embedder": "hash"})  # default: sessions, no dates
    a.add("we deploy with make ship, since yesterday", user_id="u1", session_id="s1", t_event=1_684_540_800_000)
    yield root
    a.close()


def test_forwarded_search_keeps_the_callers_packing(holder):
    r = subprocess.run([sys.executable, "-c", CHILD, holder, SRC], capture_output=True, text=True,
                       timeout=180, env=os.environ)
    assert r.returncode == 0, r.stderr[-3000:]
    out = json.loads(r.stdout.strip().splitlines()[-1])
    assert out["flat_per_call"] == [DEFAULT_HEADER, 300]
    assert out["config_default"] == DEFAULT_HEADER        # the caller's config, not the holder's
    assert out["sessions_per_call"].startswith(SESSIONS_HEADER.split(";")[0])
    assert out["sessions_budget"] == 12_000
    assert out["pack"] == DEFAULT_HEADER
