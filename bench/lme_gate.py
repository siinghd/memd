#!/usr/bin/env python
"""Nightly real-data regression gate: LongMemEval_S (cleaned), 60 questions.

The synthetic harness (`python -m memd.harness.run`) cannot see regressions
that only real conversations expose - the bm25 lane that ranked by distinct
query-term coverage scored fine there and 0.739 ndcg@5 on LongMemEval. This
gate runs memd's public path on real data every night:

  - downloads LongMemEval_S cleaned (HF xiaowu0162/longmemeval-cleaned) into
    a cache dir if absent, and SKIPS (exit 0) when it cannot (offline);
  - takes the 60 fixed questions in bench/lme_gate_qids.json: 10 per
    question type, drawn with a fixed seed from the dev split (index % 5 != 0,
    abstention variants excluded; index % 5 == 0 is the lab's held-out set
    and is never used here);
  - ingests each question's haystack into a fresh Memory (one user, one
    add_events batch per session, t_event = the session date), searches the
    question, ranks sessions by first appearance in the result;
  - reports session recall_any@5 / ndcg_any@5 and FAILS (exit 1) when
    ndcg_any@5 < 0.80 with the hash embedder and no reranker.

Run:  python bench/lme_gate.py [--data path/to/longmemeval_s_cleaned.json]
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import shutil
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

HF_URL = ("https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/"
          "resolve/main/longmemeval_s_cleaned.json")
QIDS_PATH = os.path.join(HERE, "lme_gate_qids.json")
THRESHOLD = 0.80
SELECT_SEED = 20260924
PER_TYPE = 10


def default_cache_dir() -> str:
    return os.environ.get("MEMD_LME_CACHE") or os.path.join(
        os.path.expanduser("~"), ".cache", "memd", "longmemeval")


def fetch(cache_dir: str, timeout_s: float = 60.0) -> str | None:
    """Path to the dataset, downloading it once; None when unavailable."""
    path = os.path.join(cache_dir, "longmemeval_s_cleaned.json")
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path
    os.makedirs(cache_dir, exist_ok=True)
    tmp = path + ".part"
    try:
        with urllib.request.urlopen(HF_URL, timeout=timeout_s) as resp, open(tmp, "wb") as f:  # nosec B310
            shutil.copyfileobj(resp, f, length=1 << 20)
        os.replace(tmp, path)
        return path
    except Exception as e:  # noqa: BLE001 - offline is a skip, not a failure
        print(f"lme_gate: could not download {HF_URL}: {type(e).__name__}: {e}", file=sys.stderr)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None


def select_qids(data: list[dict], seed: int = SELECT_SEED, per_type: int = PER_TYPE) -> list[str]:
    """How bench/lme_gate_qids.json was drawn (kept for reproducibility; the
    checked-in list is authoritative)."""
    by_type: dict[str, list[str]] = {}
    for i, q in enumerate(data):
        if i % 5 == 0 or q["question_id"].endswith("_abs"):
            continue
        by_type.setdefault(q["question_type"], []).append(q["question_id"])
    rng = random.Random(seed)
    out: list[str] = []
    for qtype in sorted(by_type):
        out += sorted(rng.sample(sorted(by_type[qtype]), per_type))
    return out


def date_ms(s: str) -> int | None:
    from memd.harness.suites.longmemeval_synthetic import _lme_date_ms

    return _lme_date_ms(s)


def metrics_for(ranked: list[str], gold: list[str]) -> dict[str, float]:
    """LongMemEval's session-level conventions (binary relevance)."""
    g = set(gold)
    out = {}
    for k in (5, 10):
        top = ranked[:k]
        hits = [1 if s in g else 0 for s in top]
        out[f"recall_any@{k}"] = float(any(hits))
        out[f"recall_all@{k}"] = float(g.issubset(top))
        dcg = sum(h / math.log2(i + 2) for i, h in enumerate(hits))
        idcg = sum(1 / math.log2(i + 2) for i in range(min(len(g), k)))
        out[f"ndcg_any@{k}"] = dcg / idcg if idcg else 0.0
    return out


def rank_question(q: dict, config: dict, workdir: str) -> list[str]:
    from memd.engine.memory import Memory

    d = os.path.join(workdir, q["question_id"])
    shutil.rmtree(d, ignore_errors=True)
    mem = Memory(d, encrypt=False, config=config)
    try:
        rid2sess: dict[str, str] = {}
        for sid, date, sess in zip(q["haystack_session_ids"], q["haystack_dates"],
                                   q["haystack_sessions"]):
            ts = date_ms(date)
            ev = [{"content": t["content"], "role": t["role"], "user_id": "u",
                   "session_id": sid, "t_event": ts} for t in sess if t["content"].strip()]
            for i in range(0, len(ev), 200):
                for rid in mem.add_events(ev[i:i + 200]):
                    rid2sess[rid] = sid
        mem.flush()
        res = mem.search(q["question"], user_id="u", budget_tokens=200_000)
        ranked: list[str] = []
        for it in res.items:
            s = rid2sess.get(it.id)
            if s and s not in ranked:
                ranked.append(s)
    finally:
        mem.close()
        shutil.rmtree(d, ignore_errors=True)
    return ranked + [s for s in q["haystack_session_ids"] if s not in ranked]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", help="path to longmemeval_s_cleaned.json (else download to the cache)")
    ap.add_argument("--cache-dir", default=default_cache_dir())
    ap.add_argument("--threshold", type=float, default=THRESHOLD)
    ap.add_argument("--embedder", default="hash")
    ap.add_argument("--reranker", default="none")
    ap.add_argument("--lexical", default="auto", help="lexical_backend: auto|fts5|tantivy")
    ap.add_argument("--limit", type=int, default=0, help="first N questions only (smoke)")
    ap.add_argument("--out", help="write the JSON report here")
    ap.add_argument("--print-selection", action="store_true",
                    help="print select_qids() for the dataset and exit")
    args = ap.parse_args()

    path = args.data or fetch(args.cache_dir)
    if not path or not os.path.exists(path):
        print(json.dumps({"gate": "lme_s", "status": "skipped", "reason": "dataset unavailable"}))
        return 0
    with open(path) as f:
        data = json.load(f)
    if args.print_selection:
        print(json.dumps(select_qids(data), indent=1))
        return 0
    with open(QIDS_PATH) as f:
        qids = json.load(f)["qids"]
    by_id = {q["question_id"]: q for q in data}
    missing = [x for x in qids if x not in by_id]
    if missing:
        print(f"lme_gate: {len(missing)} gate qids not in the dataset: {missing[:5]}", file=sys.stderr)
        return 1
    qs = [by_id[x] for x in qids][: args.limit or None]
    config = {"embedder": args.embedder, "reranker": args.reranker,
              "lexical_backend": args.lexical, "rate_max_writes": 10 ** 9,
              "dup_max_repeats": 10 ** 9, "vector_selfheal": False}
    workdir = tempfile.mkdtemp(prefix="memd-lme-gate-")
    rows, per_type = [], {}
    t0 = time.time()
    try:
        for n, q in enumerate(qs, 1):
            m = metrics_for(rank_question(q, config, workdir), q["answer_session_ids"])
            rows.append({"qid": q["question_id"], "type": q["question_type"], **m})
            per_type.setdefault(q["question_type"], []).append(m["ndcg_any@5"])
            print(f"[{n}/{len(qs)}] {q['question_id']} ndcg@5={m['ndcg_any@5']:.3f}", file=sys.stderr)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    mean = {k: round(sum(r[k] for r in rows) / len(rows), 4)
            for k in ("ndcg_any@5", "recall_any@5", "recall_all@5", "ndcg_any@10")}
    report = {
        "gate": "lme_s", "n": len(rows), "config": {k: config[k] for k in ("embedder", "reranker", "lexical_backend")},
        "threshold_ndcg_any@5": args.threshold, **mean,
        "ndcg_any@5_by_type": {k: round(sum(v) / len(v), 4) for k, v in sorted(per_type.items())},
        "seconds": round(time.time() - t0, 1),
    }
    report["status"] = "pass" if mean["ndcg_any@5"] >= args.threshold else "fail"
    print(json.dumps(report, indent=1))
    if args.out:
        with open(args.out, "w") as f:
            json.dump({**report, "per_question": rows}, f, indent=1)
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
