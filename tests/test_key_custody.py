"""Key custody: a wrong key - valid, just not this data's - never destroys data.

A restore mix-up (another deployment's `keys/root.key` + `keys/ns-t.key`
copied in) opened a namespace whose data lived only in compacted segments as
an EMPTY namespace: every decrypt failure was swallowed and read as damage,
the segments were skipped as corrupt, and the next compaction deleted them.
Putting the right key back then served nothing - permanent loss. The WAL
fared no better (its complete frames were cut off as a "torn tail"), nor
did the ops log, nor a restore without the keys directory (a fresh key was
minted over the existing data).

A complete frame or segment that fails authentication under the key in hand
is a key-custody failure, not damage: the open raises KeyCustodyError and
nothing is truncated, rewritten or deleted. The manifest carries a
fingerprint of the data key, so even a warm open that reads nothing refuses
a wrong key. A frame cut short by its length prefix is still a torn tail and
still repaired. Once the key is proven, a segment that still does not
authenticate or parse is damage: skipped by reads as before - and now kept by
compaction instead of deleted. A complete WAL or ops frame is never cut or
folded away when it does not decrypt.
"""
import hashlib
import json
import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.storage.crypto import KeyCustodyError, LocalKeyEnvelope  # noqa: E402
from memd.storage.engine import _frame_encode, _frames_with_offsets  # noqa: E402

CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9}
NS = "t"


def _open(root: str, **kw) -> Memory:
    return Memory(root, namespace=NS, config=CFG, **kw)


def _count(m: Memory) -> int:
    return len([ln for ln in m.export_jsonl().splitlines() if ln.strip()])


def _ns_dir(root: str) -> str:
    return os.path.join(root, "store", "ns", NS)


def _path(root: str, name: str) -> str:
    return os.path.join(_ns_dir(root), name)


def _files(root: str) -> dict[str, str]:
    """Every durable object of the namespace -> sha256 (the lock file aside)."""
    out = {}
    for fn in sorted(os.listdir(_ns_dir(root))):
        if fn == ".owner":
            continue
        with open(_path(root, fn), "rb") as f:
            out[fn] = hashlib.sha256(f.read()).hexdigest()
    return out


def _segments(root: str) -> list[str]:
    return sorted(fn for fn in os.listdir(_ns_dir(root)) if fn.startswith("seg-"))


def _manifest(root: str) -> dict:
    with open(_path(root, "manifest.json")) as f:
        return json.load(f)


def _unstamp(root: str) -> None:
    """The manifest as the previous release wrote it: no key check."""
    m = _manifest(root)
    m.pop("key_check", None)
    with open(_path(root, "manifest.json"), "w") as f:
        json.dump(m, f)


def _foreign_keys(root: str, tmp_path) -> str:
    """A restore mix-up: another deployment's keys - valid keys for a
    namespace called `t`, just not the ones this data was written with -
    copied over this deployment's. Returns where the right ones were saved."""
    kd = os.path.join(root, "keys")
    saved = str(tmp_path / "keys.saved")
    shutil.copytree(kd, saved)
    other = str(tmp_path / "otherdeploy" / "keys")
    LocalKeyEnvelope(other).data_key(NS)
    for fn in ("root.key", f"ns-{NS}.key"):
        shutil.copy(os.path.join(other, fn), os.path.join(kd, fn))
    return saved


def _restore_keys(root: str, saved: str) -> None:
    kd = os.path.join(root, "keys")
    shutil.rmtree(kd, ignore_errors=True)   # (a refused open creates no keys directory)
    shutil.copytree(saved, kd)


def _write(root: str, layout: str) -> int:
    """A namespace whose data lives only where `layout` says. Returns how
    many records it serves."""
    m = _open(root)
    try:
        ids = [m.remember(f"precious fact number {i}") for i in range(6)]
        m.flush()
        if layout in ("segments", "ops"):
            m.compact(force=True)
        if layout == "ops":
            m.delete(ids[0])
            m.delete(ids[1])
        m.flush()
        n = _count(m)
    finally:
        m.close()
    def used(log: str) -> bool:
        return os.path.exists(_path(root, log)) and os.path.getsize(_path(root, log)) > 0

    assert (bool(_segments(root)), used("wal"), used("ops")) == {
        "segments": (True, False, False), "wal": (False, True, False), "ops": (True, False, True)}[layout]
    return n


# ----------------------------------------------------------------- wrong key


@pytest.mark.parametrize("stamped", [True, False], ids=["stamped", "unstamped"])
@pytest.mark.parametrize("cache", ["warm", "cold"])
@pytest.mark.parametrize("layout", ["segments", "wal", "ops"])
def test_a_wrong_key_is_refused_and_nothing_is_touched(tmp_path, layout, cache, stamped):
    root = str(tmp_path / "d")
    n = _write(root, layout)
    assert n == (4 if layout == "ops" else 6)
    if not stamped:
        _unstamp(root)
    saved = _foreign_keys(root, tmp_path)
    if cache == "cold":
        shutil.rmtree(os.path.join(root, "store", "_cache"))
    before = _files(root)
    for _attempt in range(2):   # a retry changes nothing either
        with pytest.raises(KeyCustodyError, match="key"):
            _open(root)
        assert _files(root) == before, "a refused open must not truncate, rewrite or delete anything"
    _restore_keys(root, saved)
    m = _open(root)
    try:
        assert _count(m) == n, "the right key must serve every record again"
        assert m.search("precious fact number").items
        m.compact(force=True)
        assert _count(m) == n
    finally:
        m.close()


def test_the_verifiers_repro_segments_survive_and_the_right_key_serves_them(tmp_path):
    root = str(tmp_path / "d")
    assert _write(root, "segments") == 6
    segs = _segments(root)
    saved = _foreign_keys(root, tmp_path)
    with pytest.raises(KeyCustodyError):
        m = _open(root)
        # (what used to happen: it opened empty, and a compaction deleted segs)
        m.remember("written under the wrong key")
        m.compact(force=True)
        m.close()
    assert _segments(root) == segs
    _restore_keys(root, saved)
    m = _open(root)
    try:
        assert _count(m) == 6
    finally:
        m.close()


def test_a_wrong_root_key_alone_is_refused(tmp_path):
    root = str(tmp_path / "d")
    _write(root, "segments")
    kd = os.path.join(root, "keys")
    shutil.copy(os.path.join(kd, "root.key"), str(tmp_path / "root.key.saved"))
    with open(os.path.join(kd, "root.key"), "wb") as f:
        f.write(os.urandom(32))
    before = _files(root)
    with pytest.raises(KeyCustodyError):
        _open(root)
    assert _files(root) == before
    shutil.copy(str(tmp_path / "root.key.saved"), os.path.join(kd, "root.key"))
    m = _open(root)
    try:
        assert _count(m) == 6
    finally:
        m.close()


@pytest.mark.parametrize("stamped", [True, False], ids=["stamped", "unstamped"])
@pytest.mark.parametrize("layout", ["segments", "wal"])
def test_a_restore_without_the_keys_directory_mints_no_key_over_the_data(tmp_path, layout, stamped):
    root = str(tmp_path / "d")
    _write(root, layout)
    if not stamped:
        _unstamp(root)
    kd = os.path.join(root, "keys")
    saved = str(tmp_path / "keys.saved")
    shutil.move(kd, saved)
    before = _files(root)
    with pytest.raises(KeyCustodyError):
        _open(root)
    assert _files(root) == before
    assert not os.path.exists(os.path.join(kd, f"ns-{NS}.key")), \
        "a data key must never be minted for a namespace that already has encrypted data"
    _restore_keys(root, saved)
    m = _open(root)
    try:
        assert _count(m) == 6
    finally:
        m.close()


def test_an_encrypted_namespace_opened_with_encryption_off_is_refused(tmp_path):
    root = str(tmp_path / "d")
    _write(root, "wal")
    before = _files(root)
    with pytest.raises(KeyCustodyError):
        _open(root, encrypt=False)   # its frames would read as garbage: a "torn tail"
    assert _files(root) == before
    m = _open(root)
    try:
        assert _count(m) == 6
    finally:
        m.close()


def test_a_wrong_key_is_a_key_custody_error_not_invalid_tag(tmp_path):
    a = LocalKeyEnvelope(str(tmp_path / "a"))
    b = LocalKeyEnvelope(str(tmp_path / "b"))
    blob = a.encrypt(NS, b"secret")
    with pytest.raises(KeyCustodyError):
        b.decrypt(NS, blob)
    # a wrapped data key under another root key
    c_dir = tmp_path / "c"
    c_dir.mkdir()
    shutil.copy(str(tmp_path / "b" / "root.key"), str(c_dir / "root.key"))
    shutil.copy(str(tmp_path / "a" / f"ns-{NS}.key"), str(c_dir / f"ns-{NS}.key"))
    with pytest.raises(KeyCustodyError):
        LocalKeyEnvelope(str(c_dir)).data_key(NS)
    assert a.decrypt(NS, blob) == b"secret"


# ------------------------------------------------------- damage, not custody


def _flip(path: str, frame_no: int) -> bytes:
    """Flip one ciphertext byte inside complete frame `frame_no` of a log."""
    with open(path, "rb") as f:
        data = bytearray(f.read())
    ends = [end for end, _ in _frames_with_offsets(bytes(data))]
    start = ends[frame_no - 1] if frame_no else 0
    data[start + 4 + 20] ^= 0x01
    with open(path, "wb") as f:
        f.write(bytes(data))
    return bytes(data)


def test_a_complete_wal_frame_that_fails_authentication_is_not_a_torn_tail(tmp_path):
    root = str(tmp_path / "d")
    _write(root, "wal")
    _flip(_path(root, "wal"), 0)
    before = _files(root)
    # warm: the open's bisection never reads frame 0, so it opens - but the
    # rotate that would fold (and delete) the log refuses
    m = _open(root)
    try:
        with pytest.raises(KeyCustodyError):
            m.ns.rotate("probe")
        with pytest.raises(KeyCustodyError):
            m.compact(force=True)
    finally:
        m.close()
    assert {k: v for k, v in _files(root).items() if k.startswith(("wal", "seg-"))} == \
        {k: v for k, v in before.items() if k.startswith(("wal", "seg-"))}
    shutil.rmtree(os.path.join(root, "store", "_cache"))
    with pytest.raises(KeyCustodyError):   # cold: the replay reads it
        _open(root)
    assert _files(root)["wal"] == before["wal"]


def test_a_complete_ops_frame_that_fails_authentication_is_not_cut_off(tmp_path):
    root = str(tmp_path / "d")
    _write(root, "ops")
    _flip(_path(root, "ops"), 1)   # the last op: what a repair would cut
    before = _files(root)
    with pytest.raises(KeyCustodyError):
        _open(root)
    assert _files(root) == before


def test_a_torn_wal_tail_is_still_repaired_under_encryption(tmp_path):
    root = str(tmp_path / "d")
    _write(root, "wal")
    wal = _path(root, "wal")
    good = os.path.getsize(wal)
    with open(wal, "ab") as f:   # a frame cut short by its length prefix
        f.write((500).to_bytes(4, "big") + os.urandom(40))
    m = _open(root)
    try:
        assert _count(m) == 6
    finally:
        m.close()
    assert os.path.getsize(wal) == good


def test_ops_acked_behind_a_torn_record_are_still_salvaged_under_encryption(tmp_path):
    root = str(tmp_path / "d")
    m = _open(root)
    try:
        ids = [m.remember(f"precious fact number {i}") for i in range(3)]
        m.delete(ids[0])
        seq = m.ns.manifest.seq
    finally:
        m.close()
    env = LocalKeyEnvelope(os.path.join(root, "keys"))
    op = json.dumps({"op": "tombstone", "id": ids[1], "at": 1, "seq": seq + 1},
                    separators=(",", ":")).encode()
    with open(_path(root, "ops"), "ab") as f:
        f.write((500).to_bytes(4, "big") + os.urandom(20))   # torn...
        f.write(_frame_encode(env.encrypt(NS, op)))          # ...and acked behind it
    size = os.path.getsize(_path(root, "ops"))
    shutil.rmtree(os.path.join(root, "store", "_cache"))
    m = _open(root)
    try:
        assert _count(m) == 1
    finally:
        m.close()
    assert os.path.getsize(_path(root, "ops")) == size, "a salvaged op must not be cut off"


def _two_segments(root: str, **kw) -> tuple[str, str]:
    m = _open(root, **kw)
    try:
        for i in range(3):
            m.remember(f"first batch fact {i}")
        a = m.ns.rotate("probe")
        for i in range(3):
            m.remember(f"second batch fact {i}")
        b = m.ns.rotate("probe")
    finally:
        m.close()
    return a, b


@pytest.mark.parametrize("damage", ["plaintext-random", "encrypted-unparseable",
                                    "encrypted-flipped", "encrypted-random"])
def test_compaction_keeps_a_damaged_segment_it_cannot_read(tmp_path, damage):
    """Once the key is proven (the manifest's key check), a segment that
    still does not authenticate or parse is damage, not custody: reads skip
    it, and compaction keeps it - quarantined in place - instead of deleting
    it. (Under a key that is not proven the open refuses: see above.)"""
    root = str(tmp_path / "d")
    enc = damage.startswith("encrypted")
    a, _b = _two_segments(root, encrypt=enc)
    with open(_path(root, a), "rb") as f:
        blob = f.read()
    if damage == "encrypted-unparseable":   # authenticates, but is not a segment
        blob = LocalKeyEnvelope(os.path.join(root, "keys")).encrypt(NS, b"\x00not a segment\xff")
    elif damage == "encrypted-flipped":     # bit rot
        blob = blob[:40] + bytes([blob[40] ^ 0x01]) + blob[41:]
    else:
        blob = os.urandom(300)
    with open(_path(root, a), "wb") as f:
        f.write(blob)
    before = _files(root)
    for rnd in ("warm", "cold", "warm"):
        if rnd == "cold":
            shutil.rmtree(os.path.join(root, "store", "_cache"))
        m = _open(root, encrypt=enc)
        try:
            assert _count(m) == 3, "a damaged segment is skipped by reads"
            m.compact(force=True)
            assert _count(m) == 3
        finally:
            m.close()
        with open(_path(root, a), "rb") as f:
            assert f.read() == blob, "compaction must never delete what it could not read"
        entry = next(s for s in _manifest(root)["segments"] if s["name"] == a)
        assert entry.get("unreadable") is True, "kept referenced, marked"
    assert _files(root)[a] == before[a]


# ------------------------------------------------------------------- legacy


def test_a_namespace_written_before_encryption_still_loads_with_it_on(tmp_path):
    root = str(tmp_path / "d")
    m = _open(root, encrypt=False)
    try:
        ids = [m.remember(f"plain fact {i}") for i in range(3)]
        m.ns.rotate("probe")                             # a plaintext segment
        ids += [m.remember(f"plain fact {i}") for i in range(3, 5)]   # plaintext WAL frames
        m.delete(ids[0])                                 # a plaintext op
    finally:
        m.close()
    assert not _manifest(root).get("key_check"), "no key, no stamp"
    for _round in range(2):
        m = _open(root)
        try:
            assert _count(m) == 4
        finally:
            m.close()
    m = _open(root)
    try:
        m.remember("an encrypted fact")
        m.compact(force=True)
        assert _count(m) == 5
    finally:
        m.close()
    assert _manifest(root).get("key_check"), "stamped once a key exists"
    shutil.rmtree(os.path.join(root, "store", "_cache"))
    m = _open(root)
    try:
        assert _count(m) == 5
    finally:
        m.close()


def test_a_stamp_is_written_once_the_key_is_proven(tmp_path):
    root = str(tmp_path / "d")
    _write(root, "segments")
    _unstamp(root)
    m = _open(root)
    m.close()
    assert _manifest(root).get("key_check"), "an open that proved the key stamps it"


# ------------------------------------------ unstamped stores (every 0.2.0 one)


def _write_mixed(root: str) -> int:
    """Data everywhere: a compacted segment (5), a rotated one (3), two
    records in the WAL and an acked soft delete in the ops log -> 9 served."""
    m = _open(root)
    try:
        ids = [m.remember(f"alpha fact {i}") for i in range(5)]
        m.flush()
        m.compact(force=True)
        for i in range(3):
            m.remember(f"beta fact {i}")
        m.flush()
        m.ns.rotate("probe")
        for i in range(2):
            m.remember(f"gamma fact {i}")
        m.delete(ids[0])
        m.flush()
        n = _count(m)
    finally:
        m.close()
    assert len(_segments(root)) == 2 and os.path.getsize(_path(root, "wal"))
    assert os.path.getsize(_path(root, "ops"))
    return n


def _keyfiles(root: str) -> dict[str, str]:
    kd = os.path.join(root, "keys")
    if not os.path.isdir(kd):
        return {}
    return {fn: hashlib.sha256(open(os.path.join(kd, fn), "rb").read()).hexdigest()
            for fn in sorted(os.listdir(kd))}


@pytest.mark.parametrize("layout", ["mixed", "wal", "ops", "segments"])
def test_an_unstamped_encrypted_namespace_opened_with_encryption_off_is_refused(tmp_path, layout):
    """The stamp only exists on stores this release wrote. On an unstamped
    one (every store 0.2.0 wrote) one open + close with encryption off, no
    write, cut the WAL off as a torn tail and emptied the ops log: with the
    key back, two acked records were gone and an acked delete undone."""
    root = str(tmp_path / "d")
    n = _write_mixed(root) if layout == "mixed" else _write(root, layout)
    _unstamp(root)
    before, keys = _files(root), _keyfiles(root)
    for _attempt in range(2):
        with pytest.raises(KeyCustodyError, match="encrypt"):
            _open(root, encrypt=False)
        assert _files(root) == before, "a refused open must not truncate, rewrite or delete anything"
    assert _keyfiles(root) == keys
    m = _open(root)
    try:
        assert _count(m) == n, "every acked record, every acked delete"
        m.compact(force=True)
        assert _count(m) == n
    finally:
        m.close()


@pytest.mark.parametrize("enc", [True, False], ids=["encrypted", "plaintext"])
def test_a_complete_frame_that_does_not_parse_is_never_cut_under_any_envelope(tmp_path, enc):
    """Only a frame cut short by its own length prefix is a torn tail. A
    COMPLETE frame that does not parse is refused - encrypted or not - and
    the refusal says what it is: under a key proven to be this data's
    (another frame decrypts) it is damage, not a wrong key."""
    root = str(tmp_path / "d")
    m = _open(root, encrypt=enc)
    try:
        for i in range(4):
            m.remember(f"precious fact number {i}")
    finally:
        m.close()
    wal = _path(root, "wal")
    good = os.path.getsize(wal)
    with open(wal, "ab") as f:   # a torn tail: still repaired
        f.write((500).to_bytes(4, "big") + os.urandom(40))
    m = _open(root, encrypt=enc)
    try:
        assert _count(m) == 4
    finally:
        m.close()
    assert os.path.getsize(wal) == good
    for junk in (os.urandom(64), b"\x07garbage"):   # complete frames, long and short
        with open(wal, "ab") as f:
            f.write(_frame_encode(junk))
        size = os.path.getsize(wal)
        shutil.rmtree(os.path.join(root, "store", "_cache"))
        with pytest.raises(KeyCustodyError) as ei:
            _open(root, encrypt=enc)
        assert os.path.getsize(wal) == size, "a complete frame must never be cut off"
        msg = str(ei.value)
        assert "damaged" in msg
        if enc:
            assert "not the one" not in msg, "the key is proven: do not blame it"
        with open(wal, "r+b") as f:   # (the operator's repair: cut the frame by hand)
            f.truncate(good)


def test_an_unstamped_namespace_whose_first_probed_segment_is_damaged_opens_with_the_right_key(tmp_path):
    """The unstamped probe used to try ONE candidate - the smallest segment
    - and blame the key when it did not decrypt. It is "wrong key" only if
    NOTHING decrypts; the right key proves itself on another segment, and
    the damaged one is damage: skipped by reads, kept by compaction."""
    root = str(tmp_path / "d")
    m = _open(root)
    try:
        for i in range(5):
            m.remember(f"alpha fact {i}")
        m.compact(force=True)
        for i in range(3):
            m.remember(f"beta fact {i}")
        small = m.ns.rotate("probe")
    finally:
        m.close()
    _unstamp(root)
    with open(_path(root, small), "rb") as f:
        size = len(f.read())
    with open(_path(root, small), "wb") as f:
        f.write(os.urandom(size))
    saved = _foreign_keys(root, tmp_path)
    before = _files(root)
    with pytest.raises(KeyCustodyError, match="key"):   # nothing decrypts: a wrong key
        _open(root)
    assert _files(root) == before
    _restore_keys(root, saved)
    m = _open(root)
    try:
        assert _count(m) == 5
        m.compact(force=True)
        assert _count(m) == 5
    finally:
        m.close()
    assert _files(root)[small] == before[small], "never deleted"
    assert _manifest(root).get("key_check"), "the right key proved itself: stamped"


def test_an_unstamped_damaged_first_wal_frame_is_damage_not_a_wrong_key(tmp_path):
    root = str(tmp_path / "d")
    _write(root, "wal")
    _unstamp(root)
    _flip(_path(root, "wal"), 0)
    shutil.rmtree(os.path.join(root, "store", "_cache"))
    before = _files(root)
    with pytest.raises(KeyCustodyError) as ei:
        _open(root)
    assert _files(root) == before
    assert "damaged" in str(ei.value) and "not the one" not in str(ei.value)
    assert "SECURITY.md" in str(ei.value), "the refusal points at the recovery procedure"


@pytest.mark.parametrize("enc", [True, False], ids=["encrypted", "plaintext"])
def test_a_hard_delete_held_in_a_quarantined_segment_stays_pending_and_is_never_served_again(
        tmp_path, enc):
    """A hard delete of a record only a damaged (quarantined) segment holds
    was counted as purged, and retired: once the segment was repaired the
    purged record came back in export. While a segment is quarantined, the
    deletes a compaction retires ride on in its output's header (as a rotate
    carries them past segments it cannot see) and a hard delete stays
    pending; the fold that can read the segment again purges it."""
    root = str(tmp_path / "d")
    cfg = dict(CFG, hard_delete_deadline_ms=0)
    m = Memory(root, namespace=NS, config=cfg, encrypt=enc)
    try:
        for i in range(5):
            m.remember(f"alpha fact {i}")
        m.compact(force=True)
        b = [m.remember(f"beta secret {i} zqx") for i in range(3)]
        victim = m.ns.rotate("probe")
    finally:
        m.close()
    with open(_path(root, victim), "rb") as f:
        orig = f.read()
    with open(_path(root, victim), "wb") as f:
        f.write(os.urandom(len(orig)))
    m = Memory(root, namespace=NS, config=cfg, encrypt=enc)
    try:
        assert m.delete(b[0], hard=True)
        assert m.delete(b[1])
        m.compact(force=True)
        m.compact(force=True)
        assert m.ns.pending_hard_deletes == 1, "its bytes may still sit in the quarantined segment"
    finally:
        m.close()
    assert victim in _segments(root)
    with open(_path(root, victim), "wb") as f:   # repaired (restored from a backup)
        f.write(orig)
    for cache in ("warm", "cold"):
        if cache == "cold":
            shutil.rmtree(os.path.join(root, "store", "_cache"))
        m = Memory(root, namespace=NS, config=cfg, encrypt=enc)
        try:
            ids = {json.loads(ln)["id"] for ln in m.export_jsonl().splitlines() if ln.strip()}
            assert b[0] not in ids, f"a hard-deleted record served again ({cache})"
            assert b[1] not in ids, f"a deleted record served again ({cache})"
            assert b[2] in ids, "the repaired segment's other records are back"
            assert not m.get(b[0]) and not m.get(b[1])
            assert all(it.id not in (b[0], b[1]) for it in m.search("beta secret zqx").items)
        finally:
            m.close()
    m = Memory(root, namespace=NS, config=cfg, encrypt=enc)
    try:
        m.compact(force=True)
        assert m.ns.pending_hard_deletes == 0, "readable again: purged"
    finally:
        m.close()
    assert victim not in _segments(root)
    if not enc:
        for fn in os.listdir(_ns_dir(root)):
            with open(_path(root, fn), "rb") as f:
                assert b"beta secret 0" not in f.read(), f"purged text left in {fn}"


def test_a_wrong_key_never_stamps_a_namespace_it_could_not_read(tmp_path):
    """A manifest whose checkpoint is missing from its segment list (an
    older crash state) looked empty to the unstamped probe: a wrong key
    opened it empty and stamped ITS fingerprint, and the right key was then
    refused. The checkpoint is probed too, and a stamp is written only once
    the key decrypted something (or there is nothing encrypted at all)."""
    root = str(tmp_path / "d")
    n = _write(root, "segments")
    man = _manifest(root)
    man.pop("key_check", None)
    assert man["checkpoint"] and len(man["segments"]) == 1
    man["segments"] = []
    with open(_path(root, "manifest.json"), "w") as f:
        json.dump(man, f)
    saved = _foreign_keys(root, tmp_path)
    before = _files(root)
    with pytest.raises(KeyCustodyError, match="key"):
        _open(root)
    assert _files(root) == before
    assert not _manifest(root).get("key_check")
    _restore_keys(root, saved)
    m = _open(root)
    try:
        assert _count(m) == n
    finally:
        m.close()


def test_a_refused_open_creates_no_key_material(tmp_path):
    root = str(tmp_path / "d")
    _write(root, "segments")
    kd = os.path.join(root, "keys")
    saved = str(tmp_path / "keys.saved")
    shutil.move(kd, saved)
    with pytest.raises(KeyCustodyError):
        _open(root)
    assert not os.path.exists(os.path.join(kd, "root.key")), "no root key minted by a refused open"
    assert _keyfiles(root) == {}
    _restore_keys(root, saved)
    m = _open(root)
    try:
        assert _count(m) == 6
    finally:
        m.close()


# ----------------------------------------------------------------------- S3


def test_a_wrong_key_on_s3_is_refused_and_no_object_is_touched(tmp_path):
    endpoint = os.environ.get("MEMD_TEST_S3_ENDPOINT")
    if not endpoint:
        pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)")
    boto3 = pytest.importorskip("boto3")
    import uuid

    bucket = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
    ak = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
    sk = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=ak, aws_secret_access_key=sk,
                      region_name="us-east-1")
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass
    prefix = f"custody-{uuid.uuid4().hex[:10]}"
    local = str(tmp_path / "local")
    cfg = dict(CFG, s3_endpoint_url=endpoint, s3_access_key=ak, s3_secret_key=sk,
               s3_region="us-east-1", local_dir=local)

    def objects() -> dict[str, str]:
        out = {}
        for o in s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/ns/{NS}/").get("Contents", []):
            name = o["Key"].rsplit("/", 1)[-1]
            if name.startswith(("seg-", "manifest", "wal", "ops")):
                out[o["Key"]] = o["ETag"]
        return out

    m = Memory(f"s3://{bucket}/{prefix}", namespace=NS, config=cfg)
    try:
        for i in range(6):
            m.remember(f"precious fact number {i}")
        m.flush()
        m.compact(force=True)
        for i in range(2):
            m.remember(f"after the compaction {i}")   # and some in the WAL
    finally:
        m.close()
    saved = _foreign_keys(local, tmp_path)
    before = objects()
    assert any(k.rsplit("/", 1)[-1].startswith("seg-") for k in before)
    with pytest.raises(KeyCustodyError):
        Memory(f"s3://{bucket}/{prefix}", namespace=NS, config=cfg)
    assert objects() == before
    _restore_keys(local, saved)
    m = Memory(f"s3://{bucket}/{prefix}", namespace=NS, config=cfg)
    try:
        assert _count(m) == 8
    finally:
        m.close()
