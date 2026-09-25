"""Crash oracle: randomized mixed workloads killed at random store calls,
verified against an fsync'd journal of what was acknowledged.

Each seed runs one to three sessions of a random workload (adds, batch adds,
soft and hard deletes, batch deletes, remember/supersede, forget, rotates,
compactions, a side namespace created and crypto-shredded) in child
processes, each killed at a chosen point: the K-th store mutation anywhere,
the K-th inside rotate/compact, the K-th counted from Memory() construction
(migration, torn-tail repair, GC, snapshot drop, scrub), the K-th segment or
snapshot delete (compaction, GC), inside the index scrub, or by SIGKILL on a
timer. A third of the seeds start from a format-1 namespace (the pre-0.2
layout, with deletes only the old binary's warm index still knows), so the
first open migrates it - and is killed in the middle.

crash_verify.py then opens the store warm, warm after a compaction, cold,
rebuilt and compacted-cold, and checks each against the journal: acked
writes survive, acked deletes stay deleted, batches are atomic, supersedes
hold, reads agree with each other and with export, and no due hard-deleted
record's text remains in any file under the data root.

Slow (a few seconds per seed): skipped unless MEMD_RUN_SLOW=1 or `-m slow`.
MEMD_CRASH_SEEDS picks the seeds ("0-30", "7", "3,9,12"); CI runs 30 nightly.
A failing seed keeps its store and journal under pytest's tmp_path.
"""
import json
import os
import random
import signal
import subprocess
import sys
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "..", "src")
CHILD = os.path.join(HERE, "crash_child.py")
VERIFY = os.path.join(HERE, "crash_verify.py")

pytestmark = pytest.mark.slow


def _seeds() -> list[int]:
    spec = os.environ.get("MEMD_CRASH_SEEDS", "0-30")
    out: list[int] = []
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            out += list(range(int(lo), int(hi)))
        elif part:
            out.append(int(part))
    return out


def _tantivy() -> bool:
    try:
        import tantivy  # noqa: F401
    except ImportError:
        return False
    return True


def _plan(seed: int) -> dict:
    """Seeds cycle through three plans: a mixed crash history, the
    GC/snapshot plan (kills at segment and snapshot deletes, a snapshot
    floor low enough that every compaction publishes one) and an upgrade
    from format 1 killed inside its migration."""
    rng = random.Random(seed * 7919 + 17)
    P = {"seed": seed, "enc": rng.random() < 0.4, "lex": "tantivy" if rng.random() < 0.25 else "fts5",
         "deadline": rng.choice([0, 30, 30, 10**9]), "rot": rng.choice([3000, 12000, 1 << 20]),
         "steps": rng.randint(40, 200)}
    if P["lex"] == "tantivy" and not _tantivy():
        P["lex"] = "fts5"  # without memd[fast] an explicit tantivy refuses to open
    kind = ("mixed", "gc", "legacy")[seed % 3]
    plan = []
    if kind == "gc":
        P["snap"] = rng.random() < 0.5
        P["deadline"] = rng.choice([0, 30, 10**9])
        P["rot"] = rng.choice([3000, 12000])
        plan = [(1, "segdel", rng.randint(1, 3), rng.choice(["before", "after"])),
                (2, "segdel", rng.randint(1, 3), rng.choice(["before", "after"])),
                (3, rng.choice(["none", "openk", "segdel"]), rng.randint(1, 4),
                 rng.choice(["before", "after"]))]
    elif kind == "legacy":
        P["enc"] = False  # the format-1 store is written in plaintext
        plan = [(0, "legacy", 0, "none"),
                # the migrating open's writes: segment, manifest (the commit),
                # the old logs' deletes, open's own manifest put
                (1, "openk", rng.randint(1, 5), rng.choice(["before", "after"]))]
        if rng.random() < 0.7:
            plan.append((2, rng.choice(["inject", "focus", "openk", "scrub", "none"]),
                         rng.randint(1, 40), rng.choice(["before", "after"])))
    else:
        nsess = 1 if rng.random() < 0.55 else rng.choice([2, 3])
        for s in range(1, nsess + 1):
            if s == 1:
                mode = rng.choices(["inject", "focus", "timer", "scrub"], [4, 3, 1, 2])[0]
            elif s == 2:
                mode = rng.choices(["openk", "scrub", "inject", "focus"], [5, 2, 2, 1])[0]
            else:
                mode = rng.choice(["none", "openk", "inject"])
            plan.append((s, mode, 0, ""))
    # per-session kill point
    fixed = []
    for (s, mode, k, how) in plan:
        if mode == "scrub":
            how = rng.choice(["before", "mid", "after"])
        elif not how:
            how = rng.choice(["before", "after"])
        if not k:
            k = {"inject": rng.randint(1, max(2, P["steps"] * 3)), "focus": rng.randint(1, 40),
                 "openk": rng.randint(1, 8)}.get(mode, 0)
        fixed.append((s, mode, k, how))
    P["plan"] = fixed
    P["mode"] = kind + ":" + "+".join(p[1] for p in fixed) + ("/snap" if P.get("snap") else "")
    if "scrub" in P["mode"] and P["deadline"] >= 10**8:
        P["deadline"] = rng.choice([0, 30])  # scrub only runs when a purge happens
    P["timer"] = 0.4 + rng.random() * 1.6
    return P


def _kill_sites(root: str, plan) -> list[str]:
    tags = []
    for (s, *_rest) in plan:
        kp = os.path.join(root, f"kill.{s}")
        if not os.path.exists(kp):
            continue
        try:
            k = json.load(open(kp))
        except ValueError:
            continue
        st = set(k["stack"])
        hit = [t for t, names in (("rotate", {"_rotate_locked", "rotate"}), ("compact", {"compact"}),
                                  ("gc", {"_collect_garbage"}), ("scrub", {"scrub", "_scrub_caches"}),
                                  ("snapdrop", {"_drop_snapshot"}), ("snapshot", {"write_index_snapshot"}),
                                  ("open", {"_open"}), ("migrate", {"_migrate_legacy"}),
                                  ("opsrepair", {"_read_ops"}), ("destroy", {"destroy_namespace"}))
               if st & names]
        if k["tag"].startswith("scrub:"):
            hit.append("scrub")
        tags.append(f"s{s}:" + "/".join(sorted(set(hit)) or ["other"]) + f"@{k['tag'][:40]}")
    return tags


@pytest.mark.parametrize("seed", _seeds())
def test_crash_oracle(seed, tmp_path):
    P = _plan(seed)
    root = str(tmp_path)
    with open(os.path.join(root, "params.json"), "w") as f:
        json.dump(P, f)
    journal = os.path.join(root, "journal")
    for (s, mode, k, how) in P["plan"]:
        with open(os.path.join(root, f"child{s}.err"), "w") as err:
            p = subprocess.Popen([sys.executable, CHILD, SRC, root, str(seed), journal, mode,
                                  str(k), how, str(s)], stdout=subprocess.DEVNULL, stderr=err)
            if mode == "timer":
                try:
                    p.wait(timeout=P["timer"])
                except subprocess.TimeoutExpired:
                    p.send_signal(signal.SIGKILL)
            try:
                p.wait(timeout=300)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
                pytest.fail(f"seed {seed}: session {s} ({mode}) hung")
        if mode == "legacy":
            assert p.returncode == 0, open(os.path.join(root, f"child{s}.err")).read()[-2000:]
    t0 = time.time()
    out = subprocess.run([sys.executable, VERIFY, SRC, root], capture_output=True, text=True, timeout=900)
    try:
        r = json.loads(out.stdout.strip().splitlines()[-1])
    except Exception:  # noqa: BLE001
        r = {"problems": ["VERIFIER-FAILED " + out.stderr[-2000:]]}
    sites = _kill_sites(root, P["plan"])
    assert not r["problems"], (
        f"seed={seed} plan={P['mode']} enc={P['enc']} lex={P['lex']} dl={P['deadline']} "
        f"rot={P['rot']} kills={sites} verify={time.time() - t0:.1f}s store kept in {root}\n  "
        + "\n  ".join(r["problems"][:12]))
