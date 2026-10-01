"""Pass 46: concurrent first-key creation never reads a partial key file (K5).

The `local` key provider created root.key (and each namespace's wrapped
key file) with O_CREAT|O_EXCL and wrote the key into it afterwards. A
process creating the first key at the same moment saw the file in between
- empty - and failed ("AESGCM key must be 128, 192, or 256 bits"; 17 of
1200 processes in a race of first-key creations).

Now a key file is written and fsynced under a temporary name and linked
into place (which, like O_EXCL, fails when it exists): it is never visible
without all of its bytes. A key file that is empty or short - one an older
build is still writing in place - is read again for a moment before it is
used. A temporary file a crash left behind is shredded by a later envelope.
"""
import os
import threading
import time

import pytest

from memd.storage import crypto
from memd.storage.crypto import KEY_LEN, LocalKeyEnvelope


def _stall_first_write_of(monkeypatch, thread_name: str):
    """os.write blocks, the first time `thread_name` calls it, until
    released: the key file being created is then as a concurrent process
    would see it mid-creation."""
    stalled, release = threading.Event(), threading.Event()
    real_write = os.write
    seen = []

    def write(fd, data):
        if threading.current_thread().name == thread_name and not seen:
            seen.append(fd)
            stalled.set()
            release.wait(10)
        return real_write(fd, data)

    monkeypatch.setattr(crypto.os, "write", write)
    return stalled, release


def _run(name: str, fn, out: dict) -> threading.Thread:
    def body():
        try:
            out[name] = fn()
        except Exception as ex:  # noqa: BLE001 - reported by the test
            out[name] = ex
    t = threading.Thread(target=body, name=name, daemon=True)
    t.start()
    return t


def test_a_concurrent_first_root_key_is_never_read_partial(tmp_path, monkeypatch):
    d = str(tmp_path / "keys")
    a, b = LocalKeyEnvelope(d), LocalKeyEnvelope(d)   # two processes, one keys directory
    stalled, release = _stall_first_write_of(monkeypatch, "creator-a")
    out: dict = {}
    t = _run("creator-a", lambda: a.encrypt("n1", b"from a"), out)
    try:
        assert stalled.wait(10)
        try:
            out["b"] = b.encrypt("n2", b"from b")      # a's root key is mid-creation
        except Exception as ex:  # noqa: BLE001
            out["b"] = ex
    finally:
        release.set()
        t.join(10)
    assert not isinstance(out["b"], Exception), f"read a partial root key: {out['b']!r}"
    assert not isinstance(out["creator-a"], Exception), out["creator-a"]
    fresh = LocalKeyEnvelope(d)
    assert fresh.decrypt("n1", out["creator-a"]) == b"from a"
    assert fresh.decrypt("n2", out["b"]) == b"from b"
    assert sorted(os.listdir(d)) == ["ns-n1.key", "ns-n2.key", "root.key"], "a temporary file was left"
    with open(os.path.join(d, "root.key"), "rb") as f:
        assert len(f.read()) == KEY_LEN


def test_a_concurrent_first_data_key_is_never_read_partial(tmp_path, monkeypatch):
    d = str(tmp_path / "keys")
    LocalKeyEnvelope(d).encrypt("other", b"x")         # the root key exists
    a, b = LocalKeyEnvelope(d), LocalKeyEnvelope(d)
    stalled, release = _stall_first_write_of(monkeypatch, "creator-a")
    out: dict = {}
    t = _run("creator-a", lambda: a.data_key("n1"), out)
    try:
        assert stalled.wait(10)
        try:
            out["b"] = b.data_key("n1")                 # a's wrapped key is mid-creation
        except Exception as ex:  # noqa: BLE001
            out["b"] = ex
    finally:
        release.set()
        t.join(10)
    assert not isinstance(out["b"], Exception), f"read a partial wrapped key: {out['b']!r}"
    assert out["creator-a"] == out["b"], "the two creators disagree on the namespace's key"
    assert LocalKeyEnvelope(d).data_key("n1") == out["b"]


@pytest.mark.parametrize("which", ["root", "wrapped"])
def test_a_key_file_an_older_build_is_still_writing_is_read_again(tmp_path, which):
    d = str(tmp_path / "keys")
    LocalKeyEnvelope(d).data_key("n1")
    path = os.path.join(d, "root.key" if which == "root" else "ns-n1.key")
    with open(path, "rb") as f:
        content = f.read()
    with open(path, "wb"):
        pass                                           # created in place, not written yet

    def land():
        time.sleep(0.2)
        with open(path, "r+b") as f:
            f.write(content)

    t = threading.Thread(target=land)
    t.start()
    try:
        env = LocalKeyEnvelope(d)
        if which == "root":
            assert len(env._root) == KEY_LEN
        else:
            assert len(env.data_key("n1")) == KEY_LEN
    finally:
        t.join()


def test_a_temporary_key_file_a_crash_left_is_shredded(tmp_path):
    d = str(tmp_path / "keys")
    LocalKeyEnvelope(d).data_key("n1")
    LocalKeyEnvelope(d).data_key("a.tmp-0123456789ab")    # a namespace named like one
    old = os.path.join(d, "root.key.tmp-dead01dead01")
    new = os.path.join(d, "ns-n2.key.tmp-0123456789ab")
    for p in (old, new):
        with open(p, "wb") as f:
            f.write(os.urandom(KEY_LEN))
    hour_ago = time.time() - 3600
    for p in [old] + [os.path.join(d, fn) for fn in os.listdir(d) if fn.endswith(".key")]:
        os.utime(p, (hour_ago, hour_ago))
    LocalKeyEnvelope(d)
    assert not os.path.exists(old), "a crash's temporary key file was left"
    assert os.path.exists(new), "a creation in progress lost its temporary file"
    assert sorted(fn for fn in os.listdir(d) if fn.endswith(".key")) == [
        "ns-a.tmp-0123456789ab.key", "ns-n1.key", "root.key"], "a key file was swept"
    assert LocalKeyEnvelope(d).data_key("a.tmp-0123456789ab")
