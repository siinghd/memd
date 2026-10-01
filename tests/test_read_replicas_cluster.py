"""Read replicas end to end, on real node processes (MinIO +
moto KMS), through the router.

  - an eventual read reaching a node that does not hold the namespace's
    lease is served by that node's replica (X-Memd-Served-By: replica, with
    its seq and age); a strong read is proxied to the writer as before;
  - the replica follows the writer: a new write becomes visible, and a hard
    delete stops being served, within the staleness bound;
  - a replica that cannot serve falls back to the writer, invisibly.

Needs MEMD_TEST_S3_ENDPOINT and moto[server]; skipped otherwise.
"""
import time

import pytest

from tests.test_cluster_router import H, _remember, fleet_factory, kms, s3  # noqa: F401

httpx = pytest.importorskip("httpx")
pytestmark = pytest.mark.s3

EVENTUAL = dict(H, **{"X-Memd-Read-Consistency": "eventual"})
BOUND_S = 8.0     # the default staleness bound is 3 x the 0.5 s refresh here; CI stalls


def _get(url, ns, rid, headers=EVENTUAL):
    return httpx.get(f"{url}/v1/ns/{ns}/memories/{rid}", headers=headers, timeout=30)


def _search(url, ns, query, headers=EVENTUAL):
    return httpx.post(f"{url}/v1/ns/{ns}/search", json={"query": query}, headers=headers, timeout=30)


def _until(cond, timeout=BOUND_S):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        got = cond()
        if got:
            return time.monotonic() - t0
        time.sleep(0.05)
    return None


def test_eventual_reads_are_served_by_the_replicas_of_non_owners(fleet_factory):  # noqa: F811
    f = fleet_factory(extra_env={"MEMD_REPLICA_REFRESH_S": "0.5"})
    nodes = ["n1", "n2", "n3"]
    for n in nodes:
        f.start(n)
    ns = "hot"
    r = _remember(f.urls["n1"], ns, "the hot namespace deploys with make ship")
    assert r.status_code == 201, r.text
    rid = r.json()["id"]
    owner = f.owner(ns)
    others = [n for n in nodes if n != owner]
    for n in others:
        g = _get(f.urls[n], ns, rid)
        assert g.status_code == 200, (n, g.status_code, g.text, f.log_tail())
        assert g.headers["X-Memd-Served-By"] == "replica", (n, dict(g.headers))
        assert int(g.headers["X-Memd-Replica-Seq"]) >= 1
        assert 0 <= int(g.headers["X-Memd-Replica-Age-Ms"]) < BOUND_S * 1000
        s = _search(f.urls[n], ns, "how do we deploy")
        assert s.status_code == 200 and "make ship" in s.json()["packed_context"]
        assert s.headers["X-Memd-Served-By"] == "replica"
        # a strong read still goes to the writer
        g = _get(f.urls[n], ns, rid, headers=H)
        assert g.status_code == 200 and g.headers["X-Memd-Served-By"] == "leader"
    # the owner serves eventual reads from the writer itself
    g = _get(f.urls[owner], ns, rid)
    assert g.status_code == 200 and g.headers["X-Memd-Served-By"] == "leader"
    assert f.owner(ns) == owner, "a replica read moved the namespace"

    # the replicas follow: a new write ...
    new = _remember(f.urls[owner], ns, "written after the replicas opened kilo").json()["id"]
    for n in others:
        took = _until(lambda n=n: _get(f.urls[n], ns, new).status_code == 200)
        assert took is not None, f"{n}'s replica never saw the write\n{f.log_tail()}"
    # ... and a hard delete stops being served within the bound
    d = httpx.delete(f"{f.urls[owner]}/v1/ns/{ns}/memories/{rid}?hard=true", headers=H, timeout=30)
    assert d.status_code == 200
    for n in others:
        took = _until(lambda n=n: _get(f.urls[n], ns, rid).status_code == 404)
        assert took is not None, f"{n}'s replica still serves a hard-deleted record"
        s = _search(f.urls[n], ns, "how do we deploy")
        assert "make ship" not in s.json()["packed_context"]
    # nothing a replica did shows up as a writer: the lease never moved
    assert f.owner(ns) == owner


def test_a_replica_that_cannot_serve_falls_back_to_the_writer(fleet_factory):  # noqa: F811
    f = fleet_factory()
    f.start("n1")
    f.start("n2", MEMD_TEST_REPLICA_FAIL="1")
    ns = "fallback"
    r = _remember(f.urls["n1"], ns, "served by the writer when replicas fail")
    assert r.status_code == 201
    rid = r.json()["id"]
    owner = f.owner(ns)
    if owner == "n2":
        # the failing node holds the lease: hand the namespace to n1 by a
        # graceful stop (its lease is released), then restart it
        f.stop("n2")
        assert _remember(f.urls["n1"], ns, "moved").status_code == 201
        f.start("n2", MEMD_TEST_REPLICA_FAIL="1")
        owner = f.owner(ns)
    assert owner == "n1"
    g = _get(f.urls["n2"], ns, rid)
    assert g.status_code == 200, (g.status_code, g.text)
    assert g.headers["X-Memd-Served-By"] == "leader", "a failing replica's read was not sent to the writer"
    s = _search(f.urls["n2"], ns, "served by the writer")
    assert s.status_code == 200 and s.headers["X-Memd-Served-By"] == "leader"
