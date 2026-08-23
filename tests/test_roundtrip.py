"""Anti-lock-in roundtrip: export -> fresh namespace -> import -> parity.

The promise (D4 §4.5): leaving must be as easy as arriving. A memd export
restores with full fidelity - kind, provenance tiers, bitemporal fields,
scopes, entity keys - and remains searchable with original event times."""
import json
import os
import tempfile
import time

import pytest

from memd.engine.memory import Memory


def _export(d):
    m = Memory(d)
    blob = m.export_jsonl(namespace="default")
    m.close()
    return blob


def test_native_export_import_full_fidelity(tmp_path):
    d1 = str(tmp_path / "d1")
    m = Memory(d1)
    rid_raw = m.add("roundtrip raw event alpha", user_id="u1", session_id="s1")[0]
    rid_fact = m.remember("roundtrip explicit fact beta", entity_keys=["rt.k"],
                          user_id="u1", t_event=1700000000000)
    rid_future = m.remember("scheduled future fact", entity_keys=["rt.f"],
                            user_id="u1", valid_from=time.time() * 1000 + 90 * 24 * 3600 * 1000)
    blob = m.export_jsonl()
    m.close()
    lines = [json.loads(l) for l in blob.decode().splitlines() if l.strip()]
    assert len(lines) == 3

    # restore into a FRESH directory via the native importer
    from memd.cli import _cmd_import

    export_path = str(tmp_path / "rt.json")
    with open(export_path, "w") as f:
        for l in lines:
            f.write(json.dumps(l) + "\n")

    d2 = str(tmp_path / "d2")
    rc = _cmd_import(type("A", (), {"source": "memd", "file": export_path,
                                    "namespace": "default", "data": d2})())
    assert rc == 0

    m2 = Memory(d2)
    try:
        st = m2.stats()
        assert st["records"] == 3 and st["facts"] >= 1
        # kind + provenance fidelity: the raw stayed RAW at user tier
        got_raw = m2.get(rid_raw)
        assert got_raw["kind"] == "raw_event"
        assert got_raw["provenance"]["source"] == "user"
        # fact kept its explicit t_event
        got_fact = m2.get(rid_fact)
        assert got_fact["time"]["t_event"] == 1700000000000
        assert got_fact["entity_keys"] == ["rt.k"]
        # future-valid fact still hidden now...
        now_view = m2.search("scheduled future fact", user_id="u1")
        assert not any(i.id == rid_future for i in now_view.items)
        # ...and revealed past its window
        later = m2.search("scheduled future fact", user_id="u1",
                          as_of=int(time.time() * 1000) + 120 * 24 * 3600 * 1000)
        assert any(i.id == rid_future for i in later.items)
        # scope isolation survived: cross-user still isolated
        other = m2.search("roundtrip", user_id="someone_else")
        assert not other.items
    finally:
        m2.close()


def test_import_mem0_format_still_works(tmp_path):
    """Foreign-format mapping (mem0 shape) unchanged by native detection."""
    from memd.cli import _cmd_import

    export_path = str(tmp_path / "m0.json")
    with open(export_path, "w") as f:
        json.dump({"results": [{"memory": "User likes dark mode",
                                "user_id": "u42", "created_at": "2026-01-15T10:00:00Z"}]}, f)
    d = str(tmp_path / "dd")
    rc = _cmd_import(type("A", (), {"source": "mem0", "file": export_path,
                                    "namespace": "default", "data": d})())
    assert rc == 0
    m = Memory(d)
    try:
        res = m.search("dark mode", user_id="u42")
        assert res.items and "dark mode" in res.items[0].content
    finally:
        m.close()


def test_cli_import_memd_alias(tmp_path):
    """`memd import memd --export file` routes through native restore."""
    from memd.cli import main

    d1 = str(tmp_path / "d1")
    m = Memory(d1)
    m.add("cli alias probe", user_id="u7")
    m.close()

    export_path = str(tmp_path / "e.json")
    rc = main(["export", "--data", d1, "--out", export_path])
    assert rc == 0

    d2 = str(tmp_path / "d2")
    rc = main(["import", "memd", "--export", export_path, "--data", d2])
    assert rc == 0

    m2 = Memory(d2)
    try:
        res = m2.search("cli alias probe", user_id="u7")
        assert res.items and "cli alias probe" in res.items[0].content
    finally:
        m2.close()
