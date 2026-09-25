"""Crash-oracle child: one SESSION of a random mixed workload on a Memory,
with an fsync'd intent/ack journal, killed (os._exit) at a chosen point.
Sessions after the first reopen the crashed store, so kills also land in
open-time repair, migration, garbage collection, scrub and snapshot drop.

argv: src root seed journal mode k how sess
  mode = legacy | timer | inject | focus | scrub | openk | segdel | none
    legacy: write a format-1 namespace (the pre-0.2 layout) and the old
            binary's warm index, journaling its acked history; no Memory.
            Some deletes/supersedes live only in that index, as a format-1
            rotate dropped them. The next session's open migrates it.
    inject: die at the K-th store mutation (any thread), armed after open
    focus : die at the K-th store mutation made inside rotate/compact
    scrub : die inside NamespaceIndex.scrub (how=before|mid|after)
    openk : die at the K-th store mutation counted FROM Memory() construction
            (migration, torn-tail repair, GC, snapshot drop, scrub), then the workload
    segdel: die at the K-th delete of a segment or snapshot (compaction, GC)
    timer : SIGKILLed by the parent after a random delay
    none  : run to the end (then os._exit - never a clean close)
"""
import json
import os
import random
import sys
import threading

src, root, seed, journal, mode, K, how, sess = sys.argv[1:9]
seed, K, sess = int(seed), int(K), int(sess)
sys.path.insert(0, src)
from memd.engine.memory import Memory  # noqa: E402
from memd.storage import objectstore as osmod  # noqa: E402

rng = random.Random(seed * 1000 + sess)
P = json.load(open(os.path.join(root, "params.json")))
jf = open(journal, "a")


def J(**kw):
    jf.write(json.dumps(kw) + "\n")
    jf.flush()
    os.fsync(jf.fileno())


def tok_factory():
    n = [0]

    def tok():
        n[0] += 1
        return f"t{seed}s{sess}x{n[0]}"
    return tok


tok = tok_factory()


def write_legacy() -> None:
    """A consistent format-1 history, byte for byte as the old layout: frames
    unstamped (seq inferred), a seq per frame and per op, rotates that fold
    the WAL + ops log and DROP ops whose target lives in an older segment -
    the old binary's live index had applied them, and is written as the
    warm cache the migration reads its evidence from. Hard deletes remove the
    index row, as the old builds did; a fold re-appends the pending ones
    after deleting the ops log, and is "killed" there half of the time, so
    the delete then survives only in the namespace's audit ledger (written
    like the old facade's: an entry per acked delete, synchronously)."""
    from memd.core.schema import MemoryRecord, Scope, now_ms, records_to_jsonl
    from memd.index.sqlite_index import NamespaceIndex
    from memd.storage.audit import AuditLog
    from memd.storage.engine import _frame_encode
    from memd.storage.objectstore import LocalObjectStore

    store = os.path.join(root, "d", "store")
    nsdir = os.path.join(store, "ns", "default")
    os.makedirs(nsdir, exist_ok=True)
    seq = base = 0
    segments: list[dict] = []
    wal: list[list] = []
    ops: list[dict] = []
    live: list[str] = []
    recs: dict[str, MemoryRecord] = {}
    sup_of: dict[str, str] = {}
    idx_ops: list[dict] = []
    ledger = AuditLog(LocalObjectStore(store), "ns/default/audit")

    def rotate() -> None:
        nonlocal wal, ops, base, seq
        fold = {r.id: r for frame in wal for r in frame}
        pending = []
        for op in ops:  # ops on ids outside this WAL are dropped (the format-1 bug)
            if op["op"] == "hard_delete" and op["deadline"] > now_ms():
                pending.append(dict(op))  # its bytes stay until the deadline
                continue
            r = fold.get(op.get("id") or op.get("old"))
            if r is None:
                continue
            if op["op"] in ("tombstone", "hard_delete"):
                fold.pop(r.id)
            elif op["op"] == "supersede":
                r.time.invalidated_at, r.time.superseded_by = op["at"], op["new"]
        name = f"seg-{rng.getrandbits(64):016x}LEGACY{len(segments):04d}"
        body = records_to_jsonl(list(fold.values()))
        with open(os.path.join(nsdir, name), "wb") as f:
            f.write(json.dumps({"_seg": {"fold_seq": seq}}).encode() + b"\n" + body)
        segments.append({"name": name, "records": len(fold), "fold_seq": seq, "reason": "size"})
        wal, ops, base = [], [], seq
        if pending and rng.random() < 0.5:  # re-appended at new seqs, unless killed
            for op in pending:
                seq += 1
                ops.append(dict(op, seq=seq))

    for _ in range(rng.randint(8, 40)):
        k = rng.choices(["add", "del", "hdel", "sup", "rotate"], [5, 2, 1, 1, 1])[0]
        if k == "add":
            ts = [tok() for _ in range(rng.randint(1, 3))]
            J(t="i", op="add_events", toks=ts)
            batch = [MemoryRecord.create(namespace="default", kind="raw_event",
                                         content=f"{t} alpha note", scope=Scope(user="u"))
                     for t in ts]
            seq += 1
            wal.append([MemoryRecord.from_dict(r.to_dict()) for r in batch])
            for r in batch:
                recs[r.id] = r
            ledger.append(actor="batch", action="add_events", target=f"{len(batch)} events")
            J(t="a", op="add_events", toks=ts, ids=[r.id for r in batch])
            live += [r.id for r in batch]
        elif k in ("del", "hdel") and live:
            rid = rng.choice(live)
            J(t="i", op=k, ids=[rid])
            seq += 1
            at = now_ms()
            op = {"op": "tombstone", "id": rid, "at": at, "seq": seq}
            ops.append(op)
            idx_ops.append(op)
            if k == "hdel":
                seq += 1
                op = {"op": "hard_delete", "id": rid, "deadline": at + int(P["deadline"]), "seq": seq}
                ops.append(op)
                idx_ops.append(op)
            ledger.append(actor="api", action="hard_delete" if k == "hdel" else "delete", target=rid)
            J(t="a", op=k, ids=[rid])
            live.remove(rid)
            sup_of.pop(rid, None)
        elif k == "sup" and len(live) >= 2:
            old, new = rng.sample(live, 2)
            if old in sup_of or new in sup_of or old in sup_of.values():
                continue
            seq += 1
            op = {"op": "supersede", "old": old, "new": new, "at": now_ms(), "seq": seq}
            ops.append(op)
            idx_ops.append(op)
            sup_of[old] = new
            J(t="a", op="legacy_sup", sup={old: new})
        elif k == "rotate" and wal:
            rotate()
    wal_bytes = b"".join(_frame_encode(records_to_jsonl(frame)) for frame in wal)
    ops_bytes = b"".join(_frame_encode(json.dumps(o, separators=(",", ":")).encode()) for o in ops)
    for key, data in (("wal", wal_bytes), ("ops", ops_bytes)):
        if data:
            with open(os.path.join(nsdir, key), "wb") as f:
                f.write(data)
    with open(os.path.join(nsdir, "manifest.json"), "w") as f:
        json.dump({"version": 3 + len(segments), "seq": seq, "segments": segments,
                   "wal_size": len(wal_bytes), "ops_size": len(ops_bytes), "wal_base_seq": base,
                   "snapshot_seq": 0, "snapshot_name": ""}, f)
    idx = NamespaceIndex(os.path.join(store, "_cache", "default.sqlite"))
    idx.upsert_batch([(r, None, "") for r in recs.values()])
    for op in idx_ops:  # the old binary's index applied every op live
        if op["op"] == "tombstone":
            idx.tombstone(op["id"], op["at"])
        elif op["op"] == "hard_delete":
            idx.hard_delete(op["id"])   # the row goes: no marker is left
        else:
            idx.mark_superseded(op["old"], op["new"], op["at"])
    idx.flush()
    idx.set_meta("applied_seq", str(seq))
    idx.close()
    J(t="done", calls=0)


if mode == "legacy":
    J(t="sess", sess=sess)
    write_legacy()
    os._exit(0)

calls = [0]
armed = [False]
_fold = threading.local()


def die_point(name):
    if not armed[0] or mode in ("scrub", "none", "timer", "segdel"):
        return False
    if mode == "focus" and not getattr(_fold, "on", False):
        return False
    calls[0] += 1
    return calls[0] == K


def site(tag):
    import traceback
    fr = [f.name for f in traceback.extract_stack()[:-2] if "/memd/" in f.filename]
    with open(os.path.join(root, f"kill.{sess}"), "w") as f:
        f.write(json.dumps({"tag": tag, "stack": fr[-14:]}))
        f.flush()
        os.fsync(f.fileno())


def wrap(cls, meth):
    orig = getattr(cls, meth)

    def w(self, *a, **kw):
        hit = die_point(meth)
        if hit:
            site(f"{meth}:{how}:{(a[0] if a else '')!s}"[-80:])
        if hit and how == "before":
            os._exit(9)
        r = orig(self, *a, **kw)
        if hit:
            os._exit(9)
        return r
    setattr(cls, meth, w)


if mode == "focus":
    from memd.storage.engine import NamespaceStore as _NS
    for _mn in ("_rotate_locked", "compact"):
        _o = getattr(_NS, _mn)

        def _fw(self, *a, _o=_o, **kw):
            _fold.on = True
            try:
                return _o(self, *a, **kw)
            finally:
                _fold.on = False
        setattr(_NS, _mn, _fw)
if mode in ("inject", "focus", "openk"):
    for m_ in ("put", "append", "delete", "truncate", "remove_prefix"):
        wrap(osmod.LocalObjectStore, m_)
    wrap(osmod.LocalLogWriter, "write")
    wrap(osmod.LocalLogWriter, "sync")
if P.get("snap"):
    from memd.storage.engine import NamespaceStore as _NS2
    _NS2.SNAPSHOT_MIN_RECORDS = 5
if mode == "segdel":
    _od = osmod.LocalObjectStore.delete
    _nd = [0]

    def _sd(self, key, *a, **kw):
        k = str(key)
        if armed[0] and ("/seg-" in k or k.endswith(".snap")):
            _nd[0] += 1
            if _nd[0] == K:
                site(f"delete:{how}:{k[-40:]}")
                if how == "before":
                    os._exit(9)
                _od(self, key, *a, **kw)
                os._exit(9)
        return _od(self, key, *a, **kw)
    osmod.LocalObjectStore.delete = _sd
if mode == "scrub":
    from memd.index.sqlite_index import NamespaceIndex as _NI
    _os = _NI.scrub

    def _scrub(self, *a, **kw):
        if not armed[0]:
            return _os(self, *a, **kw)
        site(f"scrub:{how}")
        if how == "before":
            os._exit(9)
        if how == "mid":
            threading.Timer(rng.random() * 0.03, lambda: os._exit(9)).start()
        _os(self, *a, **kw)
        os._exit(9)
    _NI.scrub = _scrub

cfg = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9,
       "lexical_backend": P["lex"], "hard_delete_deadline_ms": P["deadline"]}
if mode in ("openk", "segdel"):
    armed[0] = True
J(t="sess", sess=sess)
m = Memory(os.path.join(root, "d"), encrypt=P["enc"], config=cfg)
m.engine.wal_rotate_bytes = P["rot"]
m.ns.wal_rotate_bytes = P["rot"]
armed[0] = True

# rebuild the acked model of earlier sessions (ids whose fate is certain only)
live: list[str] = []
facts: dict[str, list[str]] = {}
inflight_ids: set = set()
for line in open(journal):
    try:
        e = json.loads(line)
    except ValueError:
        continue
    if e["t"] == "i":
        inflight_ids = set(e.get("ids", []))
    elif e["t"] in ("sess", "done"):
        continue
    elif e["t"] == "e":
        for i in inflight_ids:
            if i in live:
                live.remove(i)
    elif e["t"] == "a":
        op = e["op"]
        if op in ("add", "add_events", "remember"):
            live += e["ids"]
        elif op in ("del", "hdel", "delm", "hdelm", "forget"):
            for i in e["ids"]:
                if i in live:
                    live.remove(i)
live = [i for i in live if i not in inflight_ids]


def pick(k):
    k = min(k, len(live))
    return rng.sample(live, k) if k else []


weights = [("add", 14), ("add_events", 10), ("del", 8), ("hdel", 6), ("delm", 5), ("hdelm", 3),
           ("remember", 8), ("forget", 3), ("compact", 2), ("fcompact", 1), ("rotate", 3),
           ("side_add", 4), ("side_destroy", 1)]
names = [w[0] for w in weights]
ws = [w[1] for w in weights]
for step in range(int(P["steps"])):
    op = rng.choices(names, ws)[0]
    try:
        if op == "add":
            t = tok()
            J(t="i", op=op, toks=[t])
            ids = m.add(f"{t} alpha note", user_id="u")
            J(t="a", op=op, toks=[t], ids=ids)
            live += ids
        elif op == "add_events":
            ts = [tok() for _ in range(rng.randint(2, 5))]
            J(t="i", op=op, toks=ts)
            ids = m.add_events([{"content": f"{t} beta event", "user_id": "u"} for t in ts])
            J(t="a", op=op, toks=ts, ids=ids)
            live += ids
        elif op in ("del", "hdel"):
            ids = pick(1)
            if not ids:
                continue
            J(t="i", op=op, ids=ids)
            m.delete(ids[0], hard=(op == "hdel"))
            J(t="a", op=op, ids=ids)
            live.remove(ids[0])
        elif op in ("delm", "hdelm"):
            ids = pick(rng.randint(2, 4))
            if not ids:
                continue
            J(t="i", op=op, ids=ids)
            m.delete_many(ids, hard=(op == "hdelm"))
            J(t="a", op=op, ids=ids)
            for i in ids:
                live.remove(i)
        elif op == "remember":
            ek = f"ent{rng.randint(0, 3)}s{seed}p{sess}"
            t = tok()
            J(t="i", op=op, toks=[t], ek=ek)
            rid = m.remember(f"{t} gamma fact about {ek}", entity_keys=[ek], user_id="u")
            sup = {}
            for old in facts.get(ek, []):
                r = m.ns.index.get_by_id(old, include_deleted=True)
                if r is not None and not r.deleted and r.time.superseded_by:
                    sup[old] = r.time.superseded_by
            J(t="a", op=op, toks=[t], ids=[rid], sup=sup)
            facts.setdefault(ek, []).append(rid)
            live.append(rid)
        elif op == "forget":
            ids = pick(1)
            if not ids:
                continue
            r = m.ns.index.get_by_id(ids[0])
            if r is None:
                continue
            q = r.content.split()[0]
            J(t="i", op=op, ids=ids, q=q)
            got = m.forget(q, user_id="u")
            J(t="a", op=op, ids=list(got), q=q)
            for i in got:
                if i in live:
                    live.remove(i)
        elif op in ("compact", "fcompact"):
            J(t="i", op=op)
            m.compact(force=(op == "fcompact"))
            J(t="a", op=op)
        elif op == "rotate":
            J(t="i", op=op)
            m.ns.rotate("oracle")
            J(t="a", op=op)
        elif op == "side_add":
            t = tok()
            J(t="i", op=op, toks=[t])
            ids = m.add(f"{t} side delta", user_id="u", namespace="side")
            J(t="a", op=op, toks=[t], ids=ids)
        elif op == "side_destroy":
            J(t="i", op=op)
            m.destroy_namespace("side")
            J(t="a", op=op)
    except Exception as ex:  # noqa: BLE001
        J(t="e", op=op, err=f"{type(ex).__name__}: {ex}"[:300])
J(t="done", calls=calls[0])
os._exit(9)  # never close cleanly: the parent always verifies a crashed store
