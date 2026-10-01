"""Pass 49: a key file a crash left empty is reported, then replaced (K5).

On a filesystem without hard links the `local` provider creates a key file
in place - O_CREAT|O_EXCL, then the write. A crash in between leaves the
file EMPTY. Reading it then failed far from the cause: an empty wrapped
data key raised a bare ValueError ("Nonce must be between 8 and 128
bytes"), and an empty root.key ("local root key must be 32 bytes") made
every later key creation fail, for good.

A key file that is empty or short past the read retry window is now a
KeyCustodyError naming the file. An EMPTY one older than the stale
threshold is what such a crash left - it holds no key material: a key is
used only once its file is written - and the next creation of that key
replaces it. A file with bytes in it is never replaced. A write that fails
(rather than a crash) removes the file it created.
"""
import errno
import os
import subprocess
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.storage import crypto  # noqa: E402
from memd.storage.crypto import KEY_LEN, KeyCustodyError, LocalKeyEnvelope  # noqa: E402

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")

# a child that creates a key on a filesystem without hard links and is
# killed right after the key file is created, before anything is written
CRASH_CHILD = r"""
import errno, os, sys
sys.path.insert(0, {src!r})
from memd.storage import crypto
d, ns = sys.argv[1], sys.argv[2]
def no_link(a, b, *k, **kw):
    raise PermissionError(errno.EPERM, "Operation not permitted")
real_open = os.open
def die_after_create(p, flags, *a, **kw):
    fd = real_open(p, flags, *a, **kw)
    if str(p).endswith(".key") and flags & os.O_CREAT:
        os._exit(9)
    return fd
os.link = no_link
os.open = die_after_create
crypto.LocalKeyEnvelope(d).data_key(ns)
os._exit(0)
"""


@pytest.fixture(autouse=True)
def _short_settle(monkeypatch):
    monkeypatch.setattr(crypto, "_SECRET_SETTLE_S", 0.05)


def _no_hard_links(monkeypatch):
    def no_link(a, b, *k, **kw):
        raise PermissionError(errno.EPERM, "Operation not permitted")
    monkeypatch.setattr(crypto.os, "link", no_link)


def _crash_creating(d: str, ns: str) -> None:
    child = subprocess.run([sys.executable, "-c", CRASH_CHILD.format(src=SRC), d, ns],
                           capture_output=True, timeout=60)
    assert child.returncode == 9, child.stderr.decode()[-2000:]


def _age(path: str, seconds: float = 3600) -> None:
    t = time.time() - seconds
    os.utime(path, (t, t))


def _size(path: str) -> int:
    return os.path.getsize(path)


@pytest.mark.parametrize("which", ["root", "wrapped"])
def test_a_crash_after_creating_a_key_file_is_reported_then_recovered(tmp_path, which):
    d = str(tmp_path / "keys")
    if which == "wrapped":
        LocalKeyEnvelope(d).data_key("warmup")          # the root key exists
    _crash_creating(d, "alpha")
    path = os.path.join(d, "root.key" if which == "root" else "ns-alpha.key")
    assert os.path.exists(path) and _size(path) == 0, "precondition: the crash left it empty"
    # young: it may be a creation in progress - reported, never replaced
    with pytest.raises(KeyCustodyError, match=path.replace(".", r"\.")):
        LocalKeyEnvelope(d).data_key("alpha")
    assert _size(path) == 0
    # stale: what a crash left - it holds no key, and is replaced
    _age(path)
    dk = LocalKeyEnvelope(d).data_key("alpha")
    assert len(dk) == KEY_LEN
    assert _size(path) == (KEY_LEN if which == "root" else crypto._WRAPPED_KEY_LEN)
    fresh = LocalKeyEnvelope(d)
    assert fresh.data_key("alpha") == dk
    assert fresh.decrypt("alpha", LocalKeyEnvelope(d).encrypt("alpha", b"payload")) == b"payload"
    if which == "wrapped":
        assert len(fresh.data_key("warmup")) == KEY_LEN, "the other namespace's key is untouched"
    for f in os.listdir(d):
        if ".tmp-" in f:
            _age(os.path.join(d, f))                   # the crash's temporary file: swept
    LocalKeyEnvelope(d)
    assert not [f for f in os.listdir(d) if ".tmp-" in f or ".shred-" in f]
    assert LocalKeyEnvelope(d).data_key("alpha") == dk


@pytest.mark.parametrize("which", ["root", "wrapped"])
@pytest.mark.parametrize("length", [0, 7])
def test_a_short_key_file_is_a_custody_error_naming_it(tmp_path, which, length):
    d = str(tmp_path / "keys")
    LocalKeyEnvelope(d).data_key("n1")
    path = os.path.join(d, "root.key" if which == "root" else "ns-n1.key")
    with open(path, "r+b") as f:
        f.truncate(length)
    for read in (lambda e: e.data_key("n1"), lambda e: e.peek_data_key("n1"),
                 lambda e: e.decrypt("n1", b"\0" * 40)):
        with pytest.raises(KeyCustodyError, match=path.replace(".", r"\.")):
            read(LocalKeyEnvelope(d))
    if which == "root":
        with pytest.raises(KeyCustodyError, match=path.replace(".", r"\.")):
            LocalKeyEnvelope(d).data_key("n2")              # a new key needs the root key
    assert _size(path) == length


@pytest.mark.parametrize("which", ["root", "wrapped"])
def test_a_key_file_with_bytes_in_it_is_never_replaced(tmp_path, which):
    d = str(tmp_path / "keys")
    LocalKeyEnvelope(d).data_key("n1")
    path = os.path.join(d, "root.key" if which == "root" else "ns-n1.key")
    with open(path, "r+b") as f:
        f.truncate(5)
    with open(path, "rb") as f:
        before = f.read()
    _age(path)
    with pytest.raises(KeyCustodyError):
        LocalKeyEnvelope(d).data_key("n1")
    if which == "root":
        with pytest.raises(KeyCustodyError):
            LocalKeyEnvelope(d).data_key("n2")   # a mint reads the root key
    with open(path, "rb") as f:
        assert f.read() == before, "a key file with bytes in it was replaced"


@pytest.mark.parametrize("which", ["root", "wrapped"])
def test_a_failed_in_place_write_leaves_no_empty_key_file(tmp_path, monkeypatch, which):
    d = str(tmp_path / "keys")
    if which == "wrapped":
        LocalKeyEnvelope(d).data_key("warmup")
    path = os.path.join(d, "root.key" if which == "root" else "ns-alpha.key")
    _no_hard_links(monkeypatch)
    real_write = os.write

    def write(fd, data):
        if os.path.realpath(f"/proc/self/fd/{fd}") == os.path.realpath(path):
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_write(fd, data)

    monkeypatch.setattr(crypto.os, "write", write)
    with pytest.raises(OSError, match="No space"):
        LocalKeyEnvelope(d).data_key("alpha")
    assert not os.path.exists(path), "the failed creation left its key file behind"
    monkeypatch.setattr(crypto.os, "write", real_write)
    assert len(LocalKeyEnvelope(d).data_key("alpha")) == KEY_LEN


@pytest.mark.parametrize("which", ["root", "wrapped"])
def test_concurrent_creators_replace_a_crashed_key_file_once(tmp_path, monkeypatch, which):
    d = str(tmp_path / "keys")
    if which == "wrapped":
        LocalKeyEnvelope(d).data_key("warmup")
    path = os.path.join(d, "root.key" if which == "root" else "ns-alpha.key")
    os.makedirs(d, exist_ok=True)
    with open(path, "wb"):
        pass                                           # what a crash left: created, never written
    _age(path)
    _no_hard_links(monkeypatch)
    n = 8
    gate = threading.Barrier(n)
    out: list = [None] * n

    def body(i):
        env = LocalKeyEnvelope(d)
        gate.wait(10)
        try:
            out[i] = env.encrypt("alpha", f"from {i}".encode())
        except Exception as ex:  # noqa: BLE001 - reported below
            out[i] = ex

    threads = [threading.Thread(target=body, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    errors = [o for o in out if isinstance(o, Exception)]
    assert not errors, errors
    fresh = LocalKeyEnvelope(d)
    for i, blob in enumerate(out):
        assert fresh.decrypt("alpha", blob) == f"from {i}".encode(), "creators disagree on the key"


@pytest.mark.parametrize("which", ["root", "wrapped"])
def test_a_creator_stalled_past_the_threshold_uses_the_key_that_replaced_its_file(
        tmp_path, monkeypatch, which):
    """A creator paused between creating the file and writing it for longer
    than the stale threshold: its empty file is taken for a crashed creation
    and replaced. Its own key must then not be used - nothing could read it."""
    d = str(tmp_path / "keys")
    if which == "wrapped":
        LocalKeyEnvelope(d).data_key("warmup")
    path = os.path.join(d, "root.key" if which == "root" else "ns-alpha.key")
    _no_hard_links(monkeypatch)
    real_write = os.write
    other: dict = {}

    def write(fd, data):
        if not other and os.path.realpath(f"/proc/self/fd/{fd}") == os.path.realpath(path):
            other["stalled"] = True
            _age(path)                                 # the stall, past the threshold
            other["dk"] = LocalKeyEnvelope(d).data_key("alpha")   # replaces the empty file
        return real_write(fd, data)

    monkeypatch.setattr(crypto.os, "write", write)
    stalled = LocalKeyEnvelope(d).data_key("alpha")
    monkeypatch.setattr(crypto.os, "write", real_write)
    assert "dk" in other, "precondition: the stalled creator's file was replaced"
    assert stalled == other["dk"] == LocalKeyEnvelope(d).data_key("alpha"), \
        "the stalled creator used a key no file holds"
