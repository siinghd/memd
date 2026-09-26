"""Pass 27 regression: defects found auditing the one-commit-old S3 backend.

Every one of these is in code written earlier in this same session, which is
the point the audit log keeps making: pass-N's fix carries a bug found at N+1.

 1. CRIT  remove_prefix() matched a raw STRING prefix, so destroying namespace
          'acme' also crypto-shredded 'acme-eu'. The local backend was never
          exposed to this because a filesystem prefix IS a directory.
 2. CRIT  The single-writer lease was written once and never renewed, so a
          LIVE writer lost its namespace one TTL after opening it and a second
          process took over - the exact two-writers CRIT the lease exists to
          prevent, delayed by 60 seconds.
 3. CRIT  remove_prefix() was the one mutating operation with no fence check,
          so a writer that had already lost its lease could still delete the
          new owner's data.
 4. HIGH  Cold open read the whole WAL, and the WAL only rotated on BYTES -
          but on an object store every frame is an OBJECT, so a 2000-record
          namespace meant 2002 serial GETs and 8.4s against a 1.5s SLO.
 5. MED   delete() issued one DeleteObject per part while remove_prefix() in
          the same file already batched.
 6. MED   The '.__seq' bookkeeping object was counted as a logical object.
 7. MED   truncate(key, 0) removed the key on S3 while local left it empty.
"""
import os
import sys
import time
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.storage.objectstore import LocalObjectStore, count_io  # noqa: E402

ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")


@pytest.fixture(autouse=True)
def _require_s3(request):
    """Make the `s3` marker actually gate the test.

    A marker is only metadata: two tests here build Memory("s3://...")
    directly rather than through _s3(), so without this they tried to reach a
    None endpoint and FAILED instead of skipping when no server was
    configured.
    """
    if request.node.get_closest_marker("s3") and not ENDPOINT:
        pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)")


def _s3(**kw):
    if not ENDPOINT:
        pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)")
    pytest.importorskip("boto3")
    import boto3

    from memd.storage.s3store import S3ObjectStore

    c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET, region_name="us-east-1")
    try:
        c.create_bucket(Bucket=BUCKET)
    except Exception:
        pass
    return S3ObjectStore(bucket=BUCKET, prefix=f"h-{uuid.uuid4().hex[:10]}",
                         endpoint_url=ENDPOINT, access_key=KEY, secret_key=SECRET,
                         region="us-east-1", **kw)


# ------------------------------------------------- CRIT: prefix is a boundary

class TestPrefixIsABoundary:
    """destroy_namespace('acme') must not touch 'acme-eu'."""

    def _check(self, store):
        store.put("ns/acme/manifest.json", b"tenant A")
        store.put("ns/acme/wal", b"tenant A wal")
        store.put("ns/acme-eu/manifest.json", b"TENANT B - a different tenant")
        store.append("ns/acme-eu/wal", b"tenant B frames")
        removed = store.remove_prefix("ns/acme")
        assert store.get("ns/acme/manifest.json") is None, "target survived"
        assert store.get("ns/acme-eu/manifest.json") == b"TENANT B - a different tenant", \
            "a namespace whose name merely STARTS with the target was destroyed"
        assert store.get("ns/acme-eu/wal") == b"tenant B frames"
        return removed

    @pytest.mark.s3
    def test_s3(self):
        assert self._check(_s3()) == 2

    def test_local_oracle(self, tmp_path):
        assert self._check(LocalObjectStore(str(tmp_path / "o"))) == 2

    @pytest.mark.s3
    def test_seq_hint_is_not_a_logical_object(self):
        """The '.__seq' bookkeeping object is internal: counting it made
        remove_prefix report 3 where the local oracle reports 2."""
        s = _s3()
        s.append("p/log", b"frames")
        s.put("p/obj", b"x")
        assert s.remove_prefix("p") == 2
        assert "p/log.__seq" not in s.list("p/")


# ----------------------------------------------------- CRIT: the lease holds

@pytest.mark.s3
class TestSingleWriterLease:
    def test_a_live_writer_keeps_its_namespace_past_the_ttl(self):
        """The lease was written once and never renewed."""
        a = _s3(lease_ttl_s=3.0)
        b = _s3(lease_ttl_s=3.0)
        b.prefix = a.prefix                       # same data root
        assert a.try_acquire_owner("ns1", "node-A:1") is True
        assert b.try_acquire_owner("ns1", "node-B:2") is False
        for i in range(4):                        # outlive the TTL, still alive
            a.append("ns/ns1/wal", f"frame {i}".encode())
            time.sleep(1.0)
        assert b.try_acquire_owner("ns1", "node-B:2") is False, \
            "a LIVE writer was evicted - two writers on one data root"
        a.release_owner("ns1")

    def test_a_crashed_holder_does_not_wedge_the_namespace_forever(self):
        dead = _s3(lease_ttl_s=2.0)
        rescue = _s3(lease_ttl_s=2.0)
        rescue.prefix = dead.prefix
        assert dead.try_acquire_owner("ns1", "crashed:1") is True
        dead._lease_stop.set()                    # the process dies: no heartbeat
        dead._leases.clear()
        time.sleep(3.0)
        assert rescue.try_acquire_owner("ns1", "new:2") is True, \
            "a crashed holder must not hold a namespace hostage"
        rescue.release_owner("ns1")

    def test_a_writer_that_loses_its_lease_is_fenced(self):
        a = _s3(lease_ttl_s=2.0)
        assert a.try_acquire_owner("ns1", "node-A:1") is True
        a.append("ns/ns1/wal", b"mine")
        a._raw_put(a._owner_key("ns1"), f"node-B:2\n{time.time()}".encode())
        for _ in range(12):
            if "ns1" in a._fenced:
                break
            time.sleep(0.5)
        assert "ns1" in a._fenced, "the heartbeat did not notice the lost lease"
        with pytest.raises(RuntimeError, match="lease"):
            a.append("ns/ns1/wal", b"still writing after losing it")

    def test_a_fenced_writer_cannot_delete_the_new_owners_data(self):
        """remove_prefix was the one mutating call with no fence check."""
        a = _s3(lease_ttl_s=2.0)
        a.try_acquire_owner("ns1", "node-A:1")
        a.put("ns/ns1/manifest.json", b"data the new owner now owns")
        a._raw_put(a._owner_key("ns1"), f"node-B:2\n{time.time()}".encode())
        for _ in range(12):
            if "ns1" in a._fenced:
                break
            time.sleep(0.5)
        assert "ns1" in a._fenced
        with pytest.raises(RuntimeError, match="lease"):
            a.remove_prefix("ns/ns1")
        assert a._raw_get(a._full("ns/ns1/manifest.json")) is not None

    def test_release_does_not_delete_someone_elses_lease(self):
        a = _s3(lease_ttl_s=2.0)
        a.try_acquire_owner("ns1", "node-A:1")
        a._raw_put(a._owner_key("ns1"), f"node-B:2\n{time.time()}".encode())
        a.release_owner("ns1")
        who = (a._raw_get(a._owner_key("ns1")) or b"").decode().split("\n")[0]
        assert who == "node-B:2", "released a lease this process no longer held"


# ------------------------------------------------------ round trips & bytes

@pytest.mark.s3
class TestRoundTrips:
    def test_delete_of_an_append_log_batches_and_actually_deletes(self):
        """delete() looped one DeleteObject per part; a mutation removing the
        reaping loop entirely survived both suites, so this asserts the BYTES
        are gone as well as the request count."""
        s = _s3()
        for i in range(120):
            s.append("l/log", b"frame")
        with count_io() as io:
            s.delete("l/log")
        assert s.get("l/log") is None, "deleted log still readable"
        assert s.exists("l/log") is False
        assert io.get("delete", 0) < 20, f"unbatched delete: {dict(io)}"

    def test_put_over_an_appended_key_drops_the_stale_tail(self):
        """append-then-put was untested; without the reaping loop get()
        returned new body + stale tail."""
        s = _s3()
        s.append("l/log", b"OLD-TAIL-BYTES")
        s.put("l/log", b"NEW")
        assert s.get("l/log") == b"NEW"
        assert s.size("l/log") == 3

    def test_the_io_meter_sees_per_part_reads(self):
        """A get() fanning out across a thread pool was invisible to
        count_io(): pool threads start with a fresh context."""
        s = _s3()
        for i in range(40):
            s.append("l/log", b"x" * 10)
        with count_io() as io:
            s.get("l/log")
        assert io.get("get_object", 0) >= 40, f"per-part reads uncounted: {dict(io)}"

    def test_copy_of_a_whole_object_is_server_side(self):
        s = _s3()
        s.put("big", b"y" * (1024 * 64))
        with count_io() as io:
            s.copy("big", "big2")
        assert s.get("big2") == b"y" * (1024 * 64)
        assert io.get("get_object", 0) == 0, \
            f"a whole-object copy pulled bytes through the client: {dict(io)}"


# ------------------------------------------------ truncate parity with local

class TestTruncateParity:
    """Local is the oracle: truncate(k, 0) leaves a zero-byte object."""

    def _check(self, s):
        s.append("l/log", b"hello")
        s.truncate("l/log", 0)
        return s.get("l/log"), s.exists("l/log"), s.size("l/log")

    def test_local_oracle(self, tmp_path):
        assert self._check(LocalObjectStore(str(tmp_path / "o"))) == (b"", True, 0)

    @pytest.mark.s3
    def test_s3_agrees(self):
        assert self._check(_s3()) == (b"", True, 0)


# ------------------------------------------------------- WAL frame bounding

@pytest.mark.s3
def test_cold_open_is_bounded_by_wal_frames_not_record_count(tmp_path):
    """On an object store every WAL frame is an OBJECT, so rotation must be
    bounded by frame COUNT. Rotating on bytes alone meant cold open issued one
    GET per record."""
    from memd.engine.memory import Memory

    cfg = {"s3_endpoint_url": ENDPOINT, "s3_access_key": KEY, "s3_secret_key": SECRET,
           "s3_region": "us-east-1", "rate_max_writes": 10 ** 9}
    pfx = f"cold-{uuid.uuid4().hex[:8]}"
    m = Memory(f"s3://{BUCKET}/{pfx}", encrypt=False,
               config=dict(cfg, local_dir=str(tmp_path / "a")))
    try:
        for i in range(1500):
            m.add(f"record {i} about deployment", user_id="u")
        m.flush()
        assert m.ns._wal_frames <= m.ns.wal_rotate_frames, "the WAL never rotated on frames"
    finally:
        m.close()

    with count_io() as io:
        m2 = Memory(f"s3://{BUCKET}/{pfx}", encrypt=False,
                    config=dict(cfg, local_dir=str(tmp_path / "b")))
    try:
        assert m2.ns.index.stats()["records"] == 1500
        gets = io.get("get_object", 0)
        assert gets < 1500, f"cold open read one object per record ({gets} GETs)"
    finally:
        m2.close()


@pytest.mark.s3
def test_a_snapshot_makes_cold_open_flat(tmp_path):
    """Compaction publishes the folded index; a cold node should then not
    replay the namespace at all."""
    from memd.engine.memory import Memory

    cfg = {"s3_endpoint_url": ENDPOINT, "s3_access_key": KEY, "s3_secret_key": SECRET,
           "s3_region": "us-east-1", "rate_max_writes": 10 ** 9}
    pfx = f"snap-{uuid.uuid4().hex[:8]}"
    m = Memory(f"s3://{BUCKET}/{pfx}", encrypt=False,
               config=dict(cfg, local_dir=str(tmp_path / "a")))
    try:
        for i in range(2500):
            m.add(f"record {i} about deployment", user_id="u")
        m.flush()
        m.compact(force=True)
        assert m.ns.manifest.snapshot_name
    finally:
        m.close()

    with count_io() as io:
        m2 = Memory(f"s3://{BUCKET}/{pfx}", encrypt=False,
                    config=dict(cfg, local_dir=str(tmp_path / "b")))
    try:
        assert m2.ns.index.stats()["records"] == 2500
        assert io.get("get_object", 0) <= 12, \
            f"the snapshot was not used: {dict(io)}"
    finally:
        m2.close()


# ------------------------------------ pass 28: the pass-27 fixes, re-audited

@pytest.mark.s3
def test_the_wal_frame_bound_survives_restarts(tmp_path):
    """The frame counter started at 0 on every open, so the bound held only
    within one process: six cycles of 400 writes left 2,400 WAL parts in the
    bucket while the counter read 400 each time. Cold open was O(records)
    again for any workload that restarts."""
    from memd.engine.memory import Memory

    cfg = {"s3_endpoint_url": ENDPOINT, "s3_access_key": KEY, "s3_secret_key": SECRET,
           "s3_region": "us-east-1", "rate_max_writes": 10 ** 9}
    pfx = f"reopen-{uuid.uuid4().hex[:8]}"
    local = str(tmp_path / "node")
    for cycle in range(5):
        m = Memory(f"s3://{BUCKET}/{pfx}", encrypt=False, config=dict(cfg, local_dir=local))
        try:
            for i in range(300):
                m.add(f"cycle {cycle} record {i}", user_id="u")
            m.flush()
            parts = sum(1 for k, _ in m.ns.store._iter_keys(f"{pfx}/ns/default/wal.__part-"))
            assert parts <= m.ns.wal_rotate_frames, (
                f"cycle {cycle}: {parts} WAL parts exceeds the {m.ns.wal_rotate_frames}-frame "
                "bound - the counter is not seeded from the log at open")
        finally:
            m.close()


@pytest.mark.s3
def test_copy_is_fenced_like_every_other_mutating_path():
    """copy() writes dst and reaps its parts, so a fenced writer could
    overwrite the new owner's data through it - the same gap remove_prefix
    had."""
    a = _s3(lease_ttl_s=2.0)
    a.try_acquire_owner("ns1", "node-A:1")
    a.put("ns/ns1/keep", b"the new owner's data")
    a.put("src", b"payload")
    a._raw_put(a._owner_key("ns1"), f"node-B:2\n{time.time()}".encode())
    for _ in range(12):
        if "ns1" in a._fenced:
            break
        time.sleep(0.5)
    assert "ns1" in a._fenced
    with pytest.raises(RuntimeError, match="lease"):
        a.copy("src", "ns/ns1/keep")
    assert a._raw_get(a._full("ns/ns1/keep")) == b"the new owner's data"


@pytest.mark.s3
def test_every_mutating_method_fences():
    """Enumerated statically so a NEW mutating method cannot be added without
    a fence check - two have already shipped without one."""
    import ast

    src = open(os.path.join(os.path.dirname(__file__), "..",
                            "src", "memd", "storage", "s3store.py")).read()
    cls = next(n for n in ast.parse(src).body
               if isinstance(n, ast.ClassDef) and n.name == "S3ObjectStore")
    # _renew_one is the heartbeat's body (split out so a stale writer can
    # renew synchronously from _check_fence); it writes only the lease object
    lease_internals = {"try_acquire_owner", "_try_acquire_owner", "release_owner", "_renew_leases",
                       "_renew_one", "_start_lease_thread", "_delete_batch"}
    unfenced = []
    for fn in cls.body:
        if not isinstance(fn, ast.FunctionDef) or fn.name.startswith("_raw"):
            continue
        if fn.name in lease_internals:
            continue
        body = ast.unparse(fn)
        mutates = any(w in body for w in ("put_object", "delete_object", "delete_objects",
                                          "_raw_put", "_raw_delete", "upload_part",
                                          "copy_object", "_delete_batch"))
        if mutates and "_check_fence" not in body:
            unfenced.append(fn.name)
    assert not unfenced, f"mutating methods without a fence check: {unfenced}"


class TestAdversariallyNamedNamespaces:
    """`_validate_ns` permits dots, hyphens and underscores, so
    'acme.__part-000000000000' and 'acme.__seq' are LEGAL namespace names that
    collide with this backend's internal key scheme. Pass 27's boundary fix
    closed the obvious sibling case ('acme-eu') and left these open: a LIST
    prefix of '<key>.__part-' still matched 'ns/acme.__part-000000000000/...'.
    """

    EVIL = "acme.__part-000000000000"

    def _check(self, s):
        s.put("ns/acme/manifest.json", b"tenant A")
        s.append("ns/acme/wal", b"tenant A frames")
        s.put(f"ns/{self.EVIL}/manifest.json", b"tenant C")
        s.put("ns/acme.__seq/manifest.json", b"tenant D")
        s.put("ns/acme-eu/manifest.json", b"tenant B")
        removed = s.remove_prefix("ns/acme")
        assert s.get("ns/acme/manifest.json") is None, "target survived"
        assert s.get(f"ns/{self.EVIL}/manifest.json") == b"tenant C"
        assert s.get("ns/acme.__seq/manifest.json") == b"tenant D"
        assert s.get("ns/acme-eu/manifest.json") == b"tenant B"
        return removed

    def test_local_oracle(self, tmp_path):
        assert self._check(LocalObjectStore(str(tmp_path / "o"))) == 2

    @pytest.mark.s3
    def test_s3_agrees(self):
        assert self._check(_s3()) == 2

    @pytest.mark.s3
    def test_namespace_validation_still_permits_these_names(self):
        """If this ever stops being true the tests above lose their point."""
        from memd.storage.engine import _validate_ns

        assert _validate_ns(self.EVIL) == self.EVIL
        assert _validate_ns("acme.__seq") == "acme.__seq"


@pytest.mark.s3
def test_copy_over_a_log_clears_its_stale_seq_hint():
    """A leftover hint made the first append after a copy-over resume from a
    stale {seq, bytes} and return a wrong size - and that value becomes
    manifest.wal_size, which replay trusts."""
    s = _s3()
    for _ in range(200):
        s.append("dst", b"x" * 50)
    s.append("src", b"SRC")
    s.copy("src", "dst")
    returned = s.append("dst", b"Y")
    assert returned == s.size("dst") == 4, f"append returned {returned}, real size {s.size('dst')}"


# ------------------------------ pass 29: findings from re-auditing pass 28

@pytest.mark.s3
class TestLeaseClockAndStaleWriters:
    def test_a_future_dated_lease_does_not_wedge_a_namespace_forever(self):
        """`age = now - ts` goes NEGATIVE when the holder's clock is ahead
        (NTP step, VM drift, bad RTC), so the lease was never stale and a crash
        behind it made the namespace un-openable for the length of the skew -
        defeating the point of having a TTL at all."""
        s = _s3(lease_ttl_s=2.0)
        rescuer = _s3(lease_ttl_s=2.0)
        rescuer.prefix = s.prefix
        s._raw_put(s._owner_key("future"), f"skewed:1\n{time.time() + 3600}".encode())
        s._raw_put(s._owner_key("past"), f"dead:2\n{time.time() - 3600}".encode())
        time.sleep(2.5)
        assert rescuer.try_acquire_owner("past", "new:9") is True, "control: sane stamp"
        assert rescuer.try_acquire_owner("future", "new:9") is True, \
            "a future-dated lease is permanently un-reclaimable"
        rescuer.release_owner("past")
        rescuer.release_owner("future")

    def test_a_stalled_writer_cannot_overwrite_the_new_owner(self):
        """Only append() was hardened (part creation is a conditional PUT);
        put/delete/truncate are unconditional overwrites. A writer that stalled
        long enough for its lease to be reclaimed kept believing it was the
        owner until its next heartbeat, and its put() silently replaced the new
        owner's manifest."""
        a = _s3(lease_ttl_s=2.0)
        b = _s3(lease_ttl_s=2.0)
        b.prefix = a.prefix
        assert a.try_acquire_owner("ns1", "node-A:1") is True
        a._lease_stop.set()                      # A stalls: heartbeat stops
        time.sleep(2.5)
        assert b.try_acquire_owner("ns1", "node-B:2") is True
        b.put("ns/ns1/manifest.json", b'{"owner":"B"}')
        with pytest.raises(RuntimeError, match="lease"):
            a.put("ns/ns1/manifest.json", b'{"owner":"A","STALE":true}')
        assert a._raw_get(a._full("ns/ns1/manifest.json")) == b'{"owner":"B"}'

    def test_a_healthy_heartbeat_costs_no_extra_round_trip(self):
        """The ownership re-check must fire only when our own beat has gone
        stale, or it would add a GET to every single mutation."""
        s = _s3(lease_ttl_s=60.0)
        s.try_acquire_owner("ns1", "node-A:1")
        with count_io() as io:
            for i in range(20):
                s.put(f"ns/ns1/k{i}", b"x")
        assert io.get("get_object", 0) == 0, \
            f"the fence check is doing I/O on a healthy heartbeat: {dict(io)}"
        s.release_owner("ns1")
