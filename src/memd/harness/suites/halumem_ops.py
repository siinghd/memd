"""HaluMem-style operation-level suite: extraction/update correctness.

Gates the biggest quality wedge: incumbents' update ops are <50% correct.
Target: latest-fact correctness >=0.95, stale-answer rate <=2%.
"""
from __future__ import annotations

DAY = 86_400_000
T0 = 1_710_000_000_000


def generate_ops(seed: int = 7, subjects: int = 10) -> tuple[list[dict], list[dict]]:
    """Each subject gets an ADD then an UPDATE on the same entity key;
    the system must surface only the latest value after session close."""
    events, cases = [], []
    props = [
        ("employer", "I started working at {}", "I switched jobs, now I work at {}",
         ["Initech", "Umbrella", "Aperture", "Tyrell", "Wayne"],),
        ("city", "I moved to {}.", "I relocated to {}.",
         ["Oslo", "Lima", "Hanoi", "Cairo", "Perth"]),
        ("editor", "My editor is {}.", "I switched my editor to {}.",
         ["Neovim", "Emacs", "Helix", "Zed"]),
    ]
    for i in range(subjects):
        uid = f"op{i}"
        prop_idx = i % len(props)
        key, add_tpl, upd_tpl, values = props[prop_idx]
        v1, v2 = values[i % len(values)], values[(i + 1) % len(values)]
        events.append({"content": add_tpl.format(v1), "user_id": uid, "session_id": f"{uid}-s1",
                       "role": "user", "t_event": T0 + i * DAY})
        events.append({"content": upd_tpl.format(v2), "user_id": uid, "session_id": f"{uid}-s2",
                       "role": "user", "t_event": T0 + (i + 30) * DAY})
        q = {
            "employer": f"Where does {uid} work?",
            "city": f"Where does {uid} live?",
            "editor": f"What editor does {uid} use?",
        }[key]
        cases.append({
            "id": f"{uid}-{key}-update", "qclass": "knowledge_update", "user_id": uid,
            "query": q,
            "expected": v2,
            "stale": v1,
            "close_sessions": [f"{uid}-s1", f"{uid}-s2"],
        })
    return events, cases


def check_answer(ans: str, case: dict) -> tuple[bool, float]:
    a = ans.lower()
    hit = str(case["expected"]).lower() in a
    stale = str(case.get("stale", "")).lower() in a
    if hit and not stale:
        return True, 1.0
    return False, 0.0
