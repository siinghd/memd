"""Pending-item closure tests: reembed job, pinned-key id scoping, LME loader."""
import json

import pytest

from memd.engine.memory import Memory


def test_reembed_restores_vector_lane_after_cache_loss(tmp_path):
    d = str(tmp_path / "d")
    m = Memory(d)
    m.add("reembed target fact about kumquats", user_id="u1")
    m.flush()
    assert m.stats()["vectors"] == 1
    m.close()

    # simulate a restored store with no derived-index cache
    import shutil, os
    cache = os.path.join(d, "store", "_cache")
    for f in os.listdir(cache):
        for suf in ("", "-wal", "-shm"):
            try:
                os.unlink(os.path.join(cache, f + suf))
            except FileNotFoundError:
                pass

    m2 = Memory(d)
    try:
        st = m2.stats()
        assert st["records"] == 1
        # BM25 lane survives; vector lane is empty until reembed
        rep = m2.reembed()
        import json as _j
        raw_cnt = m2.ns.index._con.execute("SELECT COUNT(*) FROM vectors").fetchone()[0]
        print("DEBUG rep:", rep, "| table:", raw_cnt, "| loaded:", m2.ns.index._vec_loaded)
        assert rep["embedded"] == 1 and rep["missing"] == 1
        idx = m2.ns.index
        raw_cnt = idx._con.execute("SELECT COUNT(*) FROM vectors").fetchone()[0]
        st2 = m2.stats()
        print("DEBUG ids:", id(idx), "| idx.con:", id(idx._con),
              "| raw vectors:", raw_cnt,
              "| st2.vectors:", st2.get("vectors"),
              "| loaded:", m2.ns.index._vec_loaded)
        assert st2["vectors"] == 1
        res = m2.search("kumquats", user_id="u1")
        assert any(i.lane_has("vector") if hasattr(i, "lane_has") else True for i in res.items)
    finally:
        m2.close()


def test_pinned_key_get_delete_scoped(tmp_path):
    from fastapi.testclient import TestClient

    from memd.server.auth import KeyStore
    from memd.server.http import create_app

    app = create_app(data_dir=str(tmp_path / "d"), keys_path=str(tmp_path / "k.json"))
    ks: KeyStore = app.state.keystore
    admin_full, _ = ks.create("acme", scope_override=True)
    pinned_full, _ = ks.create("acme", pinned_user="u9")

    c = TestClient(app)
    r = c.post("/v1/ns/acme/memories", json={"content": "u1 secret", "user_id": "u1"},
               headers={"Authorization": f"Bearer {admin_full}"})
    rid = r.json()["id"]

    pc = TestClient(app)
    pc.headers["Authorization"] = f"Bearer {pinned_full}"
    assert pc.get(f"/v1/ns/acme/memories/{rid}").status_code == 404
    assert pc.delete(f"/v1/ns/acme/memories/{rid}").status_code == 404
    # own-scope still works
    r2 = c.post("/v1/ns/acme/memories", json={"content": "u9 own", "user_id": "u9"},
                headers={"Authorization": f"Bearer {admin_full}"})
    rid2 = r2.json()["id"]
    assert pc.get(f"/v1/ns/acme/memories/{rid2}").status_code == 200
    app.state.engine.close()


def test_lme_real_dataset_loader(tmp_path):
    from memd.harness.suites import longmemeval_synthetic as L

    rows = [
        {"session": [{"content": "My name is Ada.", "role": "user"},
                     {"content": "I live in Oslo."}],
         "question": "Where does the user live?", "answer": "Oslo"},
    ]
    p = tmp_path / "lme.json"
    p.write_text(json.dumps(rows))
    events, cases = L.load_real(str(p))
    assert len(events) == 2 and len(cases) == 1
    assert cases[0]["expected"] == "Oslo"

    # build() honors MEMD_LME_DATA
    import os
    old = os.environ.get("MEMD_LME_DATA")
    os.environ["MEMD_LME_DATA"] = str(p)
    try:
        ev2, cs2 = L.build()
        assert cs2[0]["id"] == "lme0"
    finally:
        if old is None:
            del os.environ["MEMD_LME_DATA"]
        else:
            os.environ["MEMD_LME_DATA"] = old
