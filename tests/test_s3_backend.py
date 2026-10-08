"""The engine on a real S3 API (MinIO), not a mock.

memd's design has always made object storage the source of truth, but until
now LocalObjectStore was the only implementation, so every hosted SLO was
unevidenced by construction. These tests run the actual engine
paths - write/recall, restart durability, compaction, cold start from the
index snapshot, crypto-shred, and the single-writer lease - against a real
S3 server.

Skipped unless MEMD_TEST_S3_ENDPOINT is set. CI runs MinIO as a service
container; locally:

    docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
      -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
    MEMD_TEST_S3_ENDPOINT=http://127.0.0.1:9000 python -m pytest tests/test_s3_backend.py
"""
import os
import subprocess
import sys
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

pytestmark = pytest.mark.s3

ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")

if not ENDPOINT:
    pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)", allow_module_level=True)

boto3 = pytest.importorskip("boto3")

from memd.engine.memory import Memory  # noqa: E402
from memd.storage.engine import NamespaceBusyError  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _bucket():
    c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET, region_name="us-east-1")
    try:
        c.create_bucket(Bucket=BUCKET)
    except Exception:
        pass
    return c


def _mem(tmp_path, prefix, **cfg):
    base = {"s3_endpoint_url": ENDPOINT, "s3_access_key": KEY,
            "s3_secret_key": SECRET, "s3_region": "us-east-1",
            "local_dir": str(tmp_path / f"local-{uuid.uuid4().hex[:6]}"),
            "rate_max_writes": 10 ** 9}
    base.update(cfg)
    return Memory(f"s3://{BUCKET}/{prefix}", encrypt=False, config=base)


@pytest.fixture()
def prefix():
    return f"eng-{uuid.uuid4().hex[:10]}"


class TestRoundTrip:
    def test_write_and_recall(self, tmp_path, prefix):
        m = _mem(tmp_path, prefix)
        try:
            m.add("We deploy with make ship, never CI", session_id="s1", user_id="u1")
            m.close_session("s1")
            res = m.search("how do we deploy?", user_id="u1")
            assert res.items, "no recall from an S3-backed namespace"
        finally:
            m.close()

    def test_data_actually_lives_in_the_bucket(self, tmp_path, prefix, _bucket):
        m = _mem(tmp_path, prefix)
        try:
            m.add("durable in object storage", user_id="u1")
            m.flush()
        finally:
            m.close()
        keys = [o["Key"] for o in
                _bucket.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])]
        assert any(k.endswith("manifest.json") for k in keys), keys[:10]
        assert any("/wal" in k for k in keys), keys[:10]


class TestDurability:
    def test_acked_writes_survive_a_restart(self, tmp_path, prefix):
        """The whole point of a remote source of truth."""
        local = str(tmp_path / "shared-local")
        m = _mem(tmp_path, prefix, local_dir=local)
        acked = []
        try:
            for i in range(60):
                acked += m.add(f"record {i} about deployment staging", user_id="u1")
            m.flush()
        finally:
            m.close()

        m2 = _mem(tmp_path, prefix, local_dir=local)
        try:
            missing = [i for i in acked if m2.get(i) is None]
            assert not missing, f"{len(missing)} of {len(acked)} acked writes lost"
        finally:
            m2.close()

    def test_a_second_node_rebuilds_from_the_bucket_alone(self, tmp_path, prefix):
        """Wipe the local derived index entirely: a fresh node must recover
        every record from object storage."""
        m = _mem(tmp_path, prefix, local_dir=str(tmp_path / "node-a"))
        try:
            for i in range(60):
                m.add(f"record {i} about deployment staging", user_id="u1")
            m.flush()
            n = m.ns.index.stats()["records"]
        finally:
            m.close()

        # a DIFFERENT local dir = a different machine, same bucket
        m2 = _mem(tmp_path, prefix, local_dir=str(tmp_path / "node-b"))
        try:
            assert m2.ns.index.stats()["records"] == n
            assert m2.search("deployment staging", user_id="u1").items
        finally:
            m2.close()

    def test_writes_after_compaction_survive(self, tmp_path, prefix):
        """The pass-22 CRIT, re-checked on the backend where append is a PUT."""
        local = str(tmp_path / "shared-local")
        m = _mem(tmp_path, prefix, local_dir=local)
        try:
            for i in range(40):
                m.add(f"record {i}", user_id="u1")
            m.flush()
            m.compact(force=True)
            rid = m.add("zanzibar written after compaction", user_id="u1")[0]
            m.flush()
        finally:
            m.close()
        m2 = _mem(tmp_path, prefix, local_dir=local)
        try:
            assert m2.get(rid) is not None, "post-compaction write lost on S3"
        finally:
            m2.close()

    def test_acked_deletes_stay_deleted_on_a_second_node(self, tmp_path, prefix):
        """Replay applied the ops log before the WAL records, so a node that
        rebuilt from the bucket re-created every record whose delete was still
        behind it in the log - on S3 as on local disk (same replay path)."""
        m = _mem(tmp_path, prefix, local_dir=str(tmp_path / "node-a"))
        try:
            soft = m.add("second node soft victim", user_id="u1")[0]
            hard = m.add_events([{"content": f"second node hard victim {i}", "user_id": "u1"}
                                 for i in range(3)])
            kept = m.add("second node survivor", user_id="u1")[0]
            m.delete(soft)
            m.delete_many(hard, hard=True)
            m.add("written after the deletes", user_id="u1")
        finally:
            m.close()
        m2 = _mem(tmp_path, prefix, local_dir=str(tmp_path / "node-b"))
        try:
            back = [rid for rid in [soft, *hard] if m2.get(rid) is not None]
            assert not back, f"acked deletes resurrected on a second node: {back}"
            assert m2.get(kept) is not None
            assert not any("victim" in i.content for i in m2.search("second node victim", user_id="u1").items)
        finally:
            m2.close()


class TestSingleWriter:
    def test_a_second_process_is_refused(self, tmp_path, prefix):
        """flock cannot see another machine; the lease can. (With forwarding
        off: on, the second process forwards to the first -
        tests/test_write_forwarding.py.)"""
        m = _mem(tmp_path, prefix)
        try:
            m.add("first writer owns this namespace", user_id="u1")
            m.flush()
            code = (
                "import sys; sys.path.insert(0, %r)\n"
                "from memd.engine.memory import Memory\n"
                "cfg = dict(s3_endpoint_url=%r, s3_access_key=%r, s3_secret_key=%r,"
                "           s3_region='us-east-1', local_dir=%r)\n"
                "try:\n"
                "    Memory('s3://%s/%s', encrypt=False, config=cfg, forwarding='off')\n"
                "    print('OPENED')\n"
                "except Exception as e:\n"
                "    print(type(e).__name__)\n"
            ) % (os.path.join(os.path.dirname(__file__), "..", "src"),
                 ENDPOINT, KEY, SECRET, str(tmp_path / "other-node"), BUCKET, prefix)
            out = subprocess.run([sys.executable, "-c", code], capture_output=True,
                                 text=True, timeout=120).stdout.strip()
            assert "NamespaceBusyError" in out, out
        finally:
            m.close()

    def test_the_lease_is_released_on_close(self, tmp_path, prefix, _bucket):
        """Asserts the lease is RELEASED (a released tombstone,
        compare-and-swapped onto the lease so a stale holder cannot delete a
        successor's lease) and a DIFFERENT holder can take it.

        The first version of this test only reopened in the same process,
        which can never fail: the holder string is host:pid, and
        try_acquire_owner refreshes when who == holder. It stayed green with
        release_owner() replaced by a bare `return`.
        """
        m = _mem(tmp_path, prefix)
        m.add("x", user_id="u1")
        m.close()

        owner_key = f"{prefix}/ns/default/.owner"
        try:
            body = _bucket.get_object(Bucket=BUCKET, Key=owner_key)["Body"].read()
        except Exception:
            body = None
        assert body in (None, b"\n0"), f"close() left a live lease behind: {body!r}"

        # a genuinely different holder must be able to claim it
        from memd.storage.s3store import S3ObjectStore
        other = S3ObjectStore(bucket=BUCKET, prefix=prefix, endpoint_url=ENDPOINT,
                              access_key=KEY, secret_key=SECRET, region="us-east-1")
        assert other.try_acquire_owner("default", "some-other-host:9999") is True
        other.release_owner("default")

        m2 = _mem(tmp_path, prefix)     # and we can still take it back
        m2.close()


class TestCryptoShred:
    def test_destroy_removes_the_objects_from_the_bucket(self, tmp_path, prefix, _bucket):
        m = _mem(tmp_path, prefix)
        try:
            for i in range(30):
                m.add(f"secret {i}", user_id="u1", namespace="doomed")
            m.flush()
            before = [o["Key"] for o in _bucket.list_objects_v2(
                Bucket=BUCKET, Prefix=f"{prefix}/ns/doomed").get("Contents", [])]
            assert before, "expected objects for the namespace"
            m.destroy_namespace("doomed", actor="admin")
            after = [o["Key"] for o in _bucket.list_objects_v2(
                Bucket=BUCKET, Prefix=f"{prefix}/ns/doomed").get("Contents", [])]
            assert not after, f"shred left {len(after)} objects: {after[:4]}"
        finally:
            m.close()


class TestIoBudget:
    def test_a_warm_search_costs_no_object_store_round_trips(self, tmp_path, prefix):
        """The complexity budget's I/O axis, finally enforced where round trips
        cost money: retrieval is served by the local derived index."""
        from memd.storage.objectstore import count_io

        m = _mem(tmp_path, prefix)
        try:
            for i in range(40):
                m.add(f"note {i} about deployment staging", user_id="u1")
            m.flush()
            with count_io() as tally:
                m.search("deployment staging", user_id="u1")
            gets = tally.get("get", 0) + tally.get("list", 0)
            assert gets == 0, f"a warm search made {gets} object-store reads: {tally}"
        finally:
            m.close()

    def test_a_write_costs_a_bounded_number_of_puts(self, tmp_path, prefix):
        from memd.storage.objectstore import count_io

        m = _mem(tmp_path, prefix)
        try:
            m.add("warm up", user_id="u1")
            with count_io() as tally:
                m.add("one ordinary write", user_id="u1")
            puts = tally.get("append", 0) + tally.get("put", 0)
            assert puts <= 4, f"one write cost {puts} object-store writes: {tally}"
        finally:
            m.close()
