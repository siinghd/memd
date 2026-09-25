"""Crash-oracle verifier: check a crashed store (possibly several sessions)
against its journal. Prints one JSON result line.

argv: src root

Checks: acked writes survive; acked deletes stay deleted; no unknown rows;
atomic batches; supersede chains; side-namespace destroy; warm, warm after a
compaction, cold, rebuilt and compacted opens all agree; EXPORT holds exactly
what reads serve at every stage that exports; and D7: no due, acked
hard-deleted record's text remains in any file under the data root after a
compaction (with a positive control on live records' text).
"""
import gzip
import json
import os
import shutil
import sqlite3
import sys
import traceback

src, root = sys.argv[1], sys.argv[2]
sys.path.insert(0, src)
from memd.engine.memory import Memory  # noqa: E402

P = json.load(open(os.path.join(root, "params.json")))
cfg = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9,
       "lexical_backend": P["lex"], "hard_delete_deadline_ms": P["deadline"]}
data = os.path.join(root, "d")
cache = os.path.join(data, "store", "_cache")
if P.get("snap"):
    from memd.storage.engine import NamespaceStore as _NS

    _NS.SNAPSHOT_MIN_RECORDS = 5

lines = []
for raw in open(os.path.join(root, "journal")):
    try:
        lines.append(json.loads(raw))
    except ValueError:
        pass
acked_live: dict[str, str] = {}
dead: set[str] = set()
hard_dead: set[str] = set()
all_toks: set[str] = set()
uncertain_ids: set[str] = set()
sup_expect: dict[str, set] = {}
side_live: dict[str, str] = {}
side_dead: set[str] = set()
errors = []
inflight = None
interrupted: list[dict] = []
side_uncertain = False
tok_of: dict[str, str] = {}


def interrupt(e):
    global side_uncertain
    interrupted.append(e)
    uncertain_ids.update(e.get("ids", []))
    if e["op"] == "remember":
        for old in list(sup_expect) + list(acked_live):
            sup_expect.setdefault(old, set()).add("*inflight*")
    if e["op"] == "side_destroy":
        side_uncertain = True


for e in lines:
    if e["t"] == "i":
        if inflight is not None:
            interrupt(inflight)
        inflight = e
        all_toks.update(e.get("toks", []))
        continue
    if e["t"] == "sess":
        if inflight is not None:
            interrupt(inflight)
        inflight = None
        continue
    if e["t"] == "done":
        inflight = None
        continue
    op = e["op"]
    if e["t"] == "e":
        errors.append(e)
        if inflight is not None:
            interrupt(inflight)
        inflight = None
        continue
    inflight = None
    if op in ("add", "add_events", "remember"):
        for t, rid in zip(e["toks"], e["ids"]):
            acked_live[rid] = t
            tok_of[rid] = t
        for old, new in (e.get("sup") or {}).items():
            sup_expect[old] = {new}
    elif op == "legacy_sup":
        for old, new in e["sup"].items():
            sup_expect[old] = {new}
    elif op in ("del", "hdel", "delm", "hdelm", "forget"):
        for rid in e["ids"]:
            acked_live.pop(rid, None)
            dead.add(rid)
            if op in ("hdel", "hdelm"):
                hard_dead.add(rid)
    elif op == "side_add":
        for t, rid in zip(e["toks"], e["ids"]):
            side_live[rid] = t
    elif op == "side_destroy":
        side_dead.update(side_live)
        side_live = {}
if inflight is not None:
    interrupt(inflight)


def rows(path):
    if not os.path.exists(path):
        return {}
    con = sqlite3.connect(path)
    try:
        r = con.execute("SELECT id, content, superseded_by FROM records WHERE deleted=0").fetchall()
    finally:
        con.close()
    return {i: (c.split()[0], s) for i, c, s in r}


def snapshot(pre=None, export=True):
    m = Memory(data, encrypt=P["enc"], config=cfg)
    try:
        if pre == "rebuild":
            m.ns.rebuild_index()
        elif pre == "compact":
            m.compact()
        m.ns.index.flush()
        main = rows(m.ns.index.path)
        side_ns = m.engine.namespace("side")
        side_ns.index.flush()
        side = rows(side_ns.index.path)
        exp = None
        if export:
            exp = {json.loads(x)["id"] for x in m.export_jsonl().splitlines() if x.strip()}
    finally:
        m.close()
    return {"main": main, "side": side, "export": exp}


def wipe():
    shutil.rmtree(cache, ignore_errors=True)


def d7_scan():
    """due, acked hard-deleted records whose text is still in a file under the data root"""
    if P["deadline"] >= 10**8 or P["enc"]:
        return None
    pats = {}
    for rid in hard_dead - uncertain_ids:
        t = tok_of.get(rid)
        if t:
            pats[rid] = [f"{t} alpha note".encode(), f"{t} beta event".encode(),
                         f"{t} gamma fact".encode()]
    blobs = []
    for dp, _dn, fn in os.walk(data):
        for f in fn:
            p = os.path.join(dp, f)
            try:
                b = open(p, "rb").read()
            except OSError:
                continue
            if b[:2] == b"\x1f\x8b":
                try:
                    b = b + gzip.decompress(b)
                except Exception:  # noqa: BLE001
                    pass
            blobs.append((os.path.relpath(p, root), b))
    left = []
    for rid, ps in pats.items():
        where = [n for n, b in blobs if any(p in b for p in ps)]
        if where:
            left.append(f"{rid}({tok_of[rid]}) in {where[:4]}")
    # positive control: the same scan must find live acked records' text
    ctl = [rid for rid in acked_live if rid not in uncertain_ids][:20]
    found = 0
    for rid in ctl:
        t = tok_of[rid]
        ps = [f"{t} alpha note".encode(), f"{t} beta event".encode(), f"{t} gamma fact".encode()]
        if any(any(p in b for p in ps) for n, b in blobs if "/seg-" in n):
            found += 1
    return {"checked": len(pats), "left": left, "ctl": f"{found}/{len(ctl)}"}


res = {"seed": P["seed"], "plan": P["mode"], "n_lines": len(lines),
       "errors": [f"{e['op']}: {e['err']}" for e in errors][:5],
       "inflight": [e["op"] for e in interrupted], "problems": []}
snaps = {}
d7 = {}
try:
    snaps["warm"] = snapshot()
    snaps["warm_compact"] = snapshot(pre="compact")   # crashed cache kept
    d7["after_warm_compact"] = d7_scan()
    snaps["warm2"] = snapshot(export=False)
    wipe()
    snaps["cold"] = snapshot(export=False)
    snaps["rebuild"] = snapshot(pre="rebuild", export=False)
    snaps["compact"] = snapshot(pre="compact")
    wipe()
    snaps["compact_cold"] = snapshot()
    d7["final"] = d7_scan()
except Exception as ex:  # noqa: BLE001
    res["problems"].append(f"VERIFY-CRASH {type(ex).__name__}: {ex} {traceback.format_exc()[-600:]}")


def check(label, s):
    st = s["main"]
    probs = []
    for rid, t in acked_live.items():
        if rid in uncertain_ids:
            continue
        if rid not in st:
            probs.append(f"{label}: LOST acked {rid} ({t})")
        elif st[rid][0] != t:
            probs.append(f"{label}: WRONG-CONTENT {rid}")
    for rid in dead:
        if rid in st and rid not in uncertain_ids:
            probs.append(f"{label}: RESURRECTED acked-deleted {rid} ({st[rid][0]})")
    for rid, (t, _) in st.items():
        if t not in all_toks:
            probs.append(f"{label}: UNKNOWN row {rid} {t}")
    for e in interrupted:
        if e["op"] == "add_events":
            present = {t for (t, _) in st.values()} & set(e["toks"])
            if present and len(present) != len(e["toks"]):
                probs.append(f"{label}: HALF-APPLIED add_events {len(present)}/{len(e['toks'])}")
        if e["op"] in ("delm", "hdelm"):
            n_live = sum(1 for i in e["ids"] if i in st)
            # a gone id another (acked) delete explains is no evidence
            gone = [i for i in e["ids"] if i not in st and i not in dead]
            if n_live and gone:
                probs.append(f"{label}: HALF-APPLIED {e['op']} {n_live}/{len(e['ids'])} still live")
    for old, acc in sup_expect.items():
        if old in st and old not in uncertain_ids:
            got = st[old][1]
            if got not in acc and "*inflight*" not in acc:
                probs.append(f"{label}: SUPERSEDE {old} expected {acc} got {got}")
    sd = s["side"]
    for rid in side_dead:
        if rid in sd:
            probs.append(f"{label}: SIDE-RESURRECTED {rid}")
    if not side_uncertain:
        for rid in side_live:
            if rid not in sd:
                probs.append(f"{label}: SIDE-LOST {rid}")
    if s["export"] is not None:
        exp = s["export"]
        bad_dead = sorted((exp & dead) - uncertain_ids)
        if bad_dead:
            probs.append(f"{label}: EXPORT-HAS-DELETED {bad_dead[:4]} (n={len(bad_dead)})")
        extra = exp - set(st)
        if extra:
            probs.append(f"{label}: EXPORT-EXTRA (not visible) {sorted(extra)[:4]}")
        missing = set(st) - exp
        if missing:
            probs.append(f"{label}: EXPORT-MISSING {sorted(missing)[:4]}")
    return probs


for k, s in snaps.items():
    res["problems"] += check(k, s)
base = snaps.get("warm")
if base:
    for k, s in snaps.items():
        if s["main"] != base["main"]:
            a, b = set(base["main"]), set(s["main"])
            res["problems"].append(
                f"DIVERGE warm vs {k}: only-warm={sorted(a - b)[:4]} only-{k}={sorted(b - a)[:4]} "
                f"sup-diff={[i for i in a & b if base['main'][i] != s['main'][i]][:4]}")
        if s["side"] != base["side"]:
            diff = sorted(set(base["side"]) ^ set(s["side"]))[:4]
            res["problems"].append(f"DIVERGE side warm vs {k}: {diff}")
for k, v in d7.items():
    if v and v["left"]:
        res["problems"].append(
            f"D7 {k}: hard-deleted text remains: {v['left'][:3]} (n={len(v['left'])}/{v['checked']})")
for k, v in d7.items():
    if v and v["ctl"].split("/")[0] != v["ctl"].split("/")[1]:
        res["problems"].append(f"D7-CONTROL {k}: live text not found in segments {v['ctl']}")
res["d7_checked"] = {k: (f"{v['checked']} ctl={v['ctl']}" if v else None) for k, v in d7.items()}
res["sessions"] = sum(1 for e in lines if e["t"] == "sess")
res["n_live"] = len(base["main"]) if base else None
res["n_dead"] = len(dead)
res["n_hard"] = len(hard_dead)
print(json.dumps(res))
