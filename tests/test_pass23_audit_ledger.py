"""Pass 16 regression: the audit ledger's ownership, open cost and chain.

Three defects, all in the hash-chained, SIEM-exportable audit log, all
confirmed by reproduction before the fix:

 1. TENANCY. `self.audit` was bound to the facade's DEFAULT namespace, so a
    facade serving many namespaces (exactly what the REST door does - every
    handler passes namespace=ns into one shared engine) filed every tenant's
    adds/searches/deletes into the default namespace's ledger. One tenant's
    exportable trail carried another tenant's record ids; the namespace that
    actually served the request had no trail at all.
 2. OPEN COST. __init__ read the WHOLE ledger just to learn the tail hash -
    O(total audit history) bytes and RAM on every namespace open, unbounded
    in operations served, paid against the cold-open SLO (p90 <= 1.5s).
 3. CHAIN INTEGRITY. `_last_hash` parsed the object as NDJSON, but an
    encrypted ledger is [len][ciphertext] frames. The parse always failed and
    the tail silently reset to 0*64, so with encryption ON (the default) every
    reopen forked the chain and verify() returned False forever after - a real
    tamper became indistinguishable from a routine restart.
"""
import json

from memd.engine.memory import Memory
from memd.storage.audit import AuditLog, BufferedAuditLog
from memd.storage.crypto import LocalKeyEnvelope
from memd.storage.objectstore import LocalObjectStore


def _entries(mem, ns):
    return AuditLog(mem.engine.store, f"ns/{ns}/audit", mem.engine.envelope).read()


def test_audit_entries_land_in_the_namespace_that_served_the_request(tmp_path):
    m = Memory(str(tmp_path / "d"), namespace="tenant_a")
    m.add("alice likes espresso", user_id="alice")
    m.search("espresso", user_id="alice")
    m.add("bob likes tea", user_id="bob", namespace="tenant_b")
    m.search("tea", user_id="bob", namespace="tenant_b")
    m.flush()

    a, b = _entries(m, "tenant_a"), _entries(m, "tenant_b")
    a_targets = {e["target"] for e in a}
    b_targets = {e["target"] for e in b}

    assert any(e["action"] == "add" for e in b), "tenant_b must have its own trail"
    assert any(e["action"] == "search" for e in b)
    # and nothing of tenant_b's may appear in tenant_a's exportable ledger
    assert not (a_targets & b_targets), a_targets & b_targets
    for e in a:
        assert e["action"] != "add" or e["target"] not in b_targets
    m.close()


def test_every_namespace_ledger_verifies_independently(tmp_path):
    m = Memory(str(tmp_path / "d"), namespace="t1")
    for ns in ("t1", "t2", "t3"):
        for i in range(5):
            m.add(f"record {i} for {ns}", user_id="u", namespace=ns)
    m.flush()
    for ns in ("t1", "t2", "t3"):
        log = AuditLog(m.engine.store, f"ns/{ns}/audit", m.engine.envelope)
        assert log.verify(), f"{ns} chain broken"
        assert len(log.read()) >= 5
    m.close()


def test_hash_chain_survives_reopen_with_encryption_on(tmp_path):
    root = str(tmp_path / "r")
    env = LocalKeyEnvelope(root + "/keys")
    store = LocalObjectStore(root + "/store")
    KEY = "ns/t/audit"

    a = BufferedAuditLog(store, KEY, env, flush_every=1)
    for i in range(3):
        a.append(actor="u", action="add", target=f"r{i}")
    a.flush()
    assert a.verify()

    b = BufferedAuditLog(store, KEY, env, flush_every=1)  # process restart
    assert b._tail_hash != "0" * 64, "reopen must recover the real tail, not reset the chain"
    for i in range(3, 6):
        b.append(actor="u", action="add", target=f"r{i}")
    b.flush()
    assert b.verify(), "chain must still verify across a restart"
    assert len(b.read()) == 6


def test_open_is_o1_and_the_checkpoint_agrees_with_a_full_read(tmp_path):
    root = str(tmp_path / "r")
    env = LocalKeyEnvelope(root + "/keys")
    store = LocalObjectStore(root + "/store")
    KEY = "ns/t/audit"
    a = BufferedAuditLog(store, KEY, env, flush_every=200)
    for i in range(2_000):
        a.append(actor="search", action="search", target=f"q:{i:08x}")
    a.flush()

    # instrument the store: a checkpointed open must not read the ledger body
    reads = []
    real_get = store.get
    store.get = lambda k: (reads.append(k), real_get(k))[1]
    fast = BufferedAuditLog(store, KEY, env, flush_every=200)
    assert KEY not in reads, f"open read the whole ledger: {reads}"
    store.get = real_get

    # and the cheap answer must equal the expensive one
    store.delete(KEY + ".state")
    slow = BufferedAuditLog(store, KEY, env, flush_every=200)
    assert fast._tail_hash == slow._tail_hash
    assert slow.verify()


def test_cold_open_self_heals_the_checkpoint(tmp_path):
    root = str(tmp_path / "r")
    store = LocalObjectStore(root + "/store")
    KEY = "ns/t/audit"
    a = BufferedAuditLog(store, KEY, None, flush_every=1)
    a.append(actor="u", action="add", target="x")
    store.delete(KEY + ".state")

    BufferedAuditLog(store, KEY, None, flush_every=1)  # cold path
    state = store.get(KEY + ".state")
    assert state, "cold open must rewrite the checkpoint so the next open is O(1)"
    assert json.loads(state.decode())["h"] == a._tail_hash


def test_checkpoint_failure_never_breaks_a_write():
    """The checkpoint is a read-side optimization; a store that cannot put it
    must degrade to the slow open, never propagate into the caller."""
    class NoPutStore:
        def __init__(self):
            self.data = b""
        def get(self, key):
            return self.data
        def append(self, key, payload):
            self.data += payload
        def size(self, key):
            return len(self.data)

    log = BufferedAuditLog(NoPutStore(), "a", flush_every=1)
    log.append(actor="x", action="add", target="t1")
    log.append(actor="x", action="add", target="t2")
    assert len(log.read()) == 2


def test_destroyed_namespace_ledger_is_dropped_not_resurrected(tmp_path):
    """Appending to a shredded namespace's ledger would recreate both the
    object and a fresh data key beneath it, defeating crypto-shred."""
    m = Memory(str(tmp_path / "d"), namespace="keep")
    m.add("gone soon", user_id="u", namespace="doomed")
    m.flush()
    assert m.engine.store.exists("ns/doomed/audit")
    m.destroy_namespace("doomed", actor="admin")
    m.flush()
    assert not m.engine.store.exists("ns/doomed/audit")
    assert not m.engine.store.exists("ns/doomed/audit.state")
    # the administrative record lives in the facade's own ledger
    assert any(e["action"] == "destroy_namespace" for e in _entries(m, "keep"))
    m.close()


def test_junk_appended_to_encrypted_ledger_fails_verify_but_torn_tail_does_not(tmp_path):
    """Release-verification finding: a COMPLETE frame that fails to decrypt was
    skipped silently, so junk appended to an encrypted ledger left verify() True
    (no chain gap at the end). A torn tail (a crash mid-append) must still be
    tolerated."""
    import os as _os
    root = str(tmp_path / "r")
    env = LocalKeyEnvelope(root + "/keys")
    store = LocalObjectStore(root + "/store")
    KEY = "ns/t/audit"
    a = BufferedAuditLog(store, KEY, env, flush_every=1)
    for i in range(3):
        a.append(actor="u", action="add", target=f"r{i}")
    a.flush()
    assert a.verify()
    clean = store.get(KEY)

    junk = _os.urandom(48)
    store.put(KEY, clean + len(junk).to_bytes(4, "big") + junk)  # complete, bogus frame
    assert not BufferedAuditLog(store, KEY, env, flush_every=1).verify(), \
        "an undecryptable complete frame is tampering and must fail verification"

    store.put(KEY, clean + (200).to_bytes(4, "big") + b"partial")  # torn tail: shorter than its length
    assert BufferedAuditLog(store, KEY, env, flush_every=1).verify(), \
        "a torn final frame (crash mid-append) is not tampering"
