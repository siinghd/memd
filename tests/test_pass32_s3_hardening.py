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
