"""The ObjectStore contract, run against every backend.

This suite exists because the interface had exactly one implementation and
therefore no specification: several behaviours below (what `size()` does on a
missing key, whether `get()` on an append-log returns the concatenation,
whether `truncate` to a nonzero offset is required) were only ever defined by
what LocalObjectStore happened to do, and engine code was written against those
accidents. LocalObjectStore is the oracle here; a second backend is only
correct if it agrees on all of it.

Run against S3/MinIO with:
    MEMD_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
    MEMD_TEST_S3_KEY=minioadmin MEMD_TEST_S3_SECRET=minioadmin \
    python -m pytest tests/test_objectstore_contract.py
"""
import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.storage.objectstore import LocalObjectStore  # noqa: E402


def _local(tmp_path):
    return LocalObjectStore(str(tmp_path / f"obj-{uuid.uuid4().hex[:8]}"))


def _s3(tmp_path):
    endpoint = os.environ.get("MEMD_TEST_S3_ENDPOINT")
    if not endpoint:
        pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT to run)")
    boto3 = pytest.importorskip("boto3")
    from memd.storage.s3store import S3ObjectStore

    bucket = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-contract")
    client = boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("MEMD_TEST_S3_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin"),
        region_name="us-east-1",
    )
    try:
        client.create_bucket(Bucket=bucket)
    except Exception:
        pass
    # a fresh prefix per test keeps cases independent
    return S3ObjectStore(bucket=bucket, prefix=f"t-{uuid.uuid4().hex[:10]}",
                         endpoint_url=endpoint,
                         access_key=os.environ.get("MEMD_TEST_S3_KEY", "minioadmin"),
                         secret_key=os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin"),
                         region="us-east-1")


BACKENDS = [pytest.param(_local, id="local"),
            pytest.param(_s3, id="s3", marks=pytest.mark.s3)]


@pytest.fixture(params=BACKENDS)
def store(request, tmp_path):
    return request.param(tmp_path)


# ------------------------------------------------------------ absent objects

class TestAbsent:
    def test_get_returns_none(self, store):
        assert store.get("nope") is None

    def test_size_is_zero_not_an_error(self, store):
        # engine code calls size() on keys that may not exist yet
        assert store.size("nope") == 0

    def test_exists_is_false(self, store):
        assert store.exists("nope") is False

    def test_delete_is_a_noop(self, store):
        store.delete("nope")          # must not raise

    def test_list_is_empty(self, store):
        assert store.list("nope/") == []

    def test_remove_prefix_returns_zero(self, store):
        assert store.remove_prefix("nope") == 0


# ------------------------------------------------------------- put/get/copy

class TestWholeObjects:
    def test_roundtrip(self, store):
        store.put("a/x", b"data")
        assert store.get("a/x") == b"data"
        assert store.exists("a/x") is True
        assert store.size("a/x") == 4

    def test_overwrite(self, store):
        store.put("a/x", b"first")
        store.put("a/x", b"second")
        assert store.get("a/x") == b"second"
        assert store.size("a/x") == 6

    def test_empty_object_is_not_absent(self, store):
        store.put("a/e", b"")
        assert store.get("a/e") == b""
        assert store.exists("a/e") is True
        assert store.size("a/e") == 0

    def test_binary_safe(self, store):
        blob = bytes(range(256)) * 8
        store.put("a/b", blob)
        assert store.get("a/b") == blob

    def test_copy(self, store):
        store.put("a/x", b"data")
        store.copy("a/x", "a/y")
        assert store.get("a/y") == b"data"
        assert store.get("a/x") == b"data"

    def test_delete(self, store):
        store.put("a/x", b"data")
        store.delete("a/x")
        assert store.get("a/x") is None
        assert store.exists("a/x") is False

    def test_put_hint_is_readable(self, store):
        # hints are rebuildable and may skip durability, but must read back
        store.put_hint("a/h", b"hint")
        assert store.get("a/h") == b"hint"


# ----------------------------------------------------------------- appending

class TestAppendLog:
    def test_append_returns_running_total(self, store):
        assert store.append("l/log", b"hello") == 5
        assert store.append("l/log", b"world") == 10

    def test_get_returns_the_concatenation(self, store):
        store.append("l/log", b"hello")
        store.append("l/log", b"world")
        assert store.get("l/log") == b"helloworld"
        assert store.size("l/log") == 10

    def test_append_to_a_fresh_key(self, store):
        assert store.append("l/new", b"abc") == 3
        assert store.get("l/new") == b"abc"

    def test_many_small_appends_preserve_order(self, store):
        for i in range(50):
            store.append("l/many", f"{i:03d}".encode())
        assert store.get("l/many") == b"".join(f"{i:03d}".encode() for i in range(50))
        assert store.size("l/many") == 150

    def test_binary_frames(self, store):
        frames = [bytes([i]) * (i + 1) for i in range(20)]
        for f in frames:
            store.append("l/bin", f)
        assert store.get("l/bin") == b"".join(frames)

    def test_truncate_to_zero_empties_the_log(self, store):
        store.append("l/log", b"hello")
        store.truncate("l/log", 0)
        assert store.size("l/log") == 0
        assert store.get("l/log") in (b"", None)

    def test_truncate_cuts_at_a_byte_offset(self, store):
        """Torn-tail repair: the engine cuts a partial frame off the end."""
        store.append("l/log", b"hello")
        store.append("l/log", b"world")
        store.truncate("l/log", 7)
        assert store.size("l/log") == 7
        assert store.get("l/log") == b"hellowo"

    def test_append_after_truncate_continues_from_there(self, store):
        store.append("l/log", b"hello")
        store.truncate("l/log", 3)
        assert store.append("l/log", b"XY") == 5
        assert store.get("l/log") == b"helXY"

    def test_put_then_append(self, store):
        store.put("l/mixed", b"seed")
        assert store.append("l/mixed", b"more") == 8
        assert store.get("l/mixed") == b"seedmore"


# --------------------------------------------------------------- prefix ops

class TestPrefixes:
    def test_list_under_a_prefix(self, store):
        for k in ("p/a", "p/b", "p/sub/c", "q/d"):
            store.put(k, b"x")
        got = set(store.list("p/"))
        assert {"p/a", "p/b", "p/sub/c"} <= got
        assert "q/d" not in got

    def test_remove_prefix_deletes_and_counts(self, store):
        for k in ("p/a", "p/b", "p/sub/c"):
            store.put(k, b"x")
        store.put("q/d", b"x")
        assert store.remove_prefix("p") == 3
        assert store.list("p/") == []
        assert store.get("q/d") == b"x"

    def test_remove_prefix_takes_append_logs_too(self, store):
        store.append("p/log", b"hello")
        store.put("p/obj", b"x")
        assert store.remove_prefix("p") >= 1
        assert store.get("p/log") in (b"", None)
        assert store.get("p/obj") is None


# ------------------------------------------------------------------ hygiene

class TestKeyHygiene:
    @pytest.mark.parametrize("key", ["../evil", "a/../../evil", "/absolute"])
    def test_traversal_keys_are_rejected(self, store, key):
        with pytest.raises(ValueError):
            store.put(key, b"x")
