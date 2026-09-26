"""A crash inside a namespace key shred (between the overwrite and the
unlink) must not leave random bytes under the live key name: that bricked
every later open of the namespace name with a misleading 'wrong root key'
error (crash oracle seed 15)."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.storage import crypto  # noqa: E402
from memd.storage.crypto import LocalKeyEnvelope  # noqa: E402


class _Crash(BaseException):
    pass


def _crash_on_unlink(monkeypatch):
    def boom(path, *a, **k):
        raise _Crash(path)
    monkeypatch.setattr(crypto.os, "unlink", boom)


def test_a_crash_between_overwrite_and_unlink_leaves_the_name_usable(tmp_path, monkeypatch):
    kd = str(tmp_path / "keys")
    env = LocalKeyEnvelope(kd)
    env.data_key("side")
    env.data_key("keep")
    keep_before = open(os.path.join(kd, "ns-keep.key"), "rb").read()
    with monkeypatch.context() as m:
        _crash_on_unlink(m)
        with pytest.raises(_Crash):
            env.destroy("side")
    # the process "restarts": a new envelope over the same directory
    env2 = LocalKeyEnvelope(kd)
    assert env2.has_key("side") is False
    # the name is reusable: a fresh key is minted and round-trips
    blob = env2.encrypt("side", b"hello")
    assert env2.decrypt("side", blob) == b"hello"
    # nothing else was touched, and the interrupted shred was finished
    assert open(os.path.join(kd, "ns-keep.key"), "rb").read() == keep_before
    assert not [f for f in os.listdir(kd) if ".shred-" in f]
    assert sorted(f for f in os.listdir(kd) if f.startswith("ns-")) == ["ns-keep.key", "ns-side.key"]


def test_a_crash_before_the_overwrite_is_swept_too(tmp_path, monkeypatch):
    kd = str(tmp_path / "keys")
    env = LocalKeyEnvelope(kd)
    env.data_key("side")
    with monkeypatch.context() as m:
        m.setattr(crypto, "_shred_file", lambda p: (_ for _ in ()).throw(_Crash(p)))
        with pytest.raises(_Crash):
            env.destroy("side")
    leftovers = [f for f in os.listdir(kd) if ".shred-" in f]
    assert len(leftovers) == 1     # the key bytes, out of the live name
    LocalKeyEnvelope(kd)
    assert not [f for f in os.listdir(kd) if ".shred-" in f]
    assert not os.path.exists(os.path.join(kd, "ns-side.key"))


def test_a_plain_destroy_still_shreds(tmp_path):
    kd = str(tmp_path / "keys")
    env = LocalKeyEnvelope(kd)
    env.data_key("side")
    assert env.destroy("side") is True
    assert os.listdir(kd) == ["root.key"]
