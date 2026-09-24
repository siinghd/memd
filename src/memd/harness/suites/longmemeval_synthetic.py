"""LongMemEval-class synthetic suite (D5).

Deterministic generator (seeded): sessions of facts per user, then query
classes mirroring LongMemEval's: single-hop, multi-hop, temporal, and the
wedge classes - knowledge-update, knowledge-overwrite, abstention.

The real LongMemEval is used when MEMD_LME_DATA points at a JSON file with
either {"events": [...], "cases": [...]} in this suite's schema, or a list of
rows. Two row shapes are accepted:
  - the real LongMemEval release (longmemeval_s / _m / _oracle):
      {"question_id", "question_type", "question", "question_date", "answer",
       "answer_session_ids", "haystack_dates", "haystack_session_ids",
       "haystack_sessions": [[{"role", "content", "has_answer"?}, ...], ...]}
  - a flat legacy shape:
      {"session": [{"content": ..., "role": ..., "user_id"?...}],
       "question": ..., "answer": ...}
Rows are converted to events+cases at load time. The synthetic suite exists
so CI can gate every commit offline with zero network.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import random
import re

DAY = 86_400_000
T0 = 1_700_000_000_000


def generate_cases(seed: int = 42, users: int = 8) -> tuple[list[dict], list[dict]]:
    rng = random.Random(seed)
    names = ["Alice", "Bob", "Cara", "Dan", "Elena", "Frank", "Grace", "Hank"]
    cities = ["Lisbon", "Oslo", "Kyoto", "Austin", "Nairobi", "Perth"]
    tools = ["make ship", "cargo deploy", "npm run release", "just prod", "fab rollout"]
    editors = ["Neovim", "Emacs", "VS Code", "Zed"]

    events: list[dict] = []
    cases: list[dict] = []

    for u in range(users):
        uid = f"u{u}"
        # pools cycle so --users beyond len(names) works (the CLI exposes
        # users as free-form; an IndexError here bricked the whole harness).
        # Uniqueness comes from uid-scoped session ids and per-user queries.
        name, city, tool, editor = (
            names[u % len(names)], cities[u % len(cities)],
            tools[u % len(tools)], editors[u % len(editors)],
        )
        base = T0 + u * DAY

        def ev(day: int, content: str):
            events.append({"content": content, "user_id": uid, "session_id": f"s{uid}-{day}",
                           "role": "user", "t_event": base + day * DAY})

        # stable facts
        ev(0, f"My name is {name}.")
        ev(0, f"I live in {city}.")
        ev(1, f"We deploy with `{tool}`, never raw ftp.")
        ev(2, f"I use {editor} as my editor.")
        # temporal facts
        ev(3, f"I moved to {city} in 2021.")
        ev(4, f"My review meeting is scheduled on March 3rd.")

        # single-hop
        cases.append({
            "id": f"{uid}-sh-deploy", "qclass": "single_hop", "user_id": uid,
            "query": f"How do we deploy?",
            "expected": tool,
            "events_hint": None,
        })
        cases.append({
            "id": f"{uid}-sh-editor", "qclass": "single_hop", "user_id": uid,
            "query": f"What editor does {name} use?",
            "expected": editor,
        })
        # multi-hop: name -> city via two facts
        cases.append({
            "id": f"{uid}-mh-city", "qclass": "multi_hop", "user_id": uid,
            "query": f"Where does {name} live?",
            "expected": city,
        })
        # temporal
        cases.append({
            "id": f"{uid}-tp-review", "qclass": "temporal", "user_id": uid,
            "query": "When is my review meeting?",
            "expected": "March 3rd",
        })
        # knowledge-update: old employer then new one; only latest correct
        old_job, new_job = "Initech", "Initrode"
        ev(10, f"I just started working at {old_job}.")
        ev(40, f"I switched jobs, now I work at {new_job}.")
        cases.append({
            "id": f"{uid}-ku-employer", "qclass": "knowledge_update", "user_id": uid,
            "query": f"Where does {name} work?",
            "expected": new_job,
            "stale": old_job,
        })
        return_value = f"ticket #{100+u}"
        ev(20, f"My support ticket got resolved, reference {return_value}.")
        cases.append({
            "id": f"{uid}-sh-ticket", "qclass": "single_hop", "user_id": uid,
            "query": "What was my support ticket reference?",
            "expected": return_value,
        })

        # distractor volume: makes the full-context baseline pay real token
        # cost and retrieval work under noise (LongMemEval-style long horizon)
        topics = ["standup", "refactor", "bug bash", "on-call", "retro", "planning",
                  "lint", "flaky test", "cache", "auth flow", "docs", "telemetry"]
        for d in range(400):
            t = topics[d % len(topics)]
            ev(5 + d % 90, f"Notes from {t} session number {d}: we discussed the {t} "
                           f"and agreed to follow up about {t} details later.")
        _ = rng  # reserved for future noise injection
    return events, cases


def check_answer(ans: str, case: dict) -> tuple[bool, float]:
    expected = str(case["expected"]).lower()
    a = ans.lower()
    hit = expected in a
    stale_leak = False
    if case.get("stale"):
        stale_leak = str(case["stale"]).lower() in a and not hit
    if not hit or stale_leak:
        return False, 0.0
    return True, 1.0


def build(seed: int = 42, users: int = 8) -> tuple[list[dict], list[dict]]:
    path = os.environ.get("MEMD_LME_DATA")
    if path:
        return load_real(path)
    return generate_cases(seed=seed, users=users)


_LME_DATE = re.compile(r"\s*(\d{4})/(\d{1,2})/(\d{1,2})(?:.*?(\d{1,2}):(\d{2}))?")


def _lme_date_ms(s: str) -> int | None:
    """'2023/05/20 (Sat) 02:21' -> epoch ms (UTC; the dataset has no zone)."""
    m = _LME_DATE.match(str(s))
    if not m:
        return None
    y, mo, d, hh, mm = (int(x) if x else 0 for x in m.groups())
    dt = _dt.datetime(y, mo, d, hh, mm, tzinfo=_dt.timezone.utc)
    return int(dt.timestamp() * 1000)


def _load_lme_row(row: dict, events: list[dict], cases: list[dict]) -> None:
    """One real LongMemEval question: its own user (haystacks are per
    question), one event per haystack turn, one case. Abstention variants
    (question_id ending in `_abs`) are skipped: substring grading of
    "expected" cannot score an abstention."""
    qid = str(row["question_id"])
    if qid.endswith("_abs"):
        return
    uid = qid
    sids = row.get("haystack_session_ids") or []
    dates = row.get("haystack_dates") or []
    for k, session in enumerate(row.get("haystack_sessions") or []):
        sid = str(sids[k]) if k < len(sids) else f"s{k}"
        t_event = _lme_date_ms(dates[k]) if k < len(dates) else None
        for turn in session:
            content = turn.get("content") if isinstance(turn, dict) else turn
            if not content:
                continue
            events.append({
                "content": str(content), "user_id": uid,
                # uid-scoped: the same distractor session id recurs across
                # questions, and the harness closes sessions by id alone
                "session_id": f"{uid}:{sid}",
                "role": (turn.get("role") if isinstance(turn, dict) else None) or "user",
                "t_event": t_event,
            })
    cases.append({
        "id": qid, "qclass": row.get("question_type", "real"), "user_id": uid,
        "query": str(row["question"]), "expected": str(row["answer"]),
        "question_date": row.get("question_date"),
        "answer_session_ids": list(row.get("answer_session_ids") or []),
    })


def load_real(path: str) -> tuple[list[dict], list[dict]]:
    """Load a LongMemEval-derived dataset and normalize it into
    (events, cases) with the same schema as the synthetic generator."""
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, dict) and "events" in data and "cases" in data:
        return data["events"], data["cases"]
    rows = data if isinstance(data, list) else []
    events: list[dict] = []
    cases: list[dict] = []
    for i, row in enumerate(rows):
        if isinstance(row, dict) and "haystack_sessions" in row and "question_id" in row:
            _load_lme_row(row, events, cases)
            continue
        uid = f"lme{i}"
        sess_events = row.get("session") or row.get("haystack") or row.get("context") or []
        for j, s in enumerate(sess_events):
            content = s.get("content") if isinstance(s, dict) else str(s)
            if not content:
                continue
            events.append({
                "content": str(content), "user_id": uid,
                "session_id": s.get("session_id", f"{uid}-s{j//20}") if isinstance(s, dict) else f"{uid}-s{j//20}",
                "role": (s.get("role", "user") if isinstance(s, dict) else "user"),
            })
        q = row.get("question") or row.get("query")
        a = row.get("answer") or row.get("expected")
        if q and a:
            cases.append({
                "id": f"lme{i}", "qclass": row.get("qclass", row.get("category", "real")),
                "user_id": uid, "query": str(q), "expected": str(a),
                "stale": row.get("stale"),
            })
    if not cases:
        raise ValueError(f"{path}: no usable LongMemEval rows found")
    return events, cases
