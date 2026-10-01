"""Plans and entitlements: configuration, not code.

Defaults: extraction on our key is the dominant COGS line, so it is capped
(free) or metered (paid); Jev reranking (~$0.0004 per reranked search) gets
its own meter. Operators override any of it with a JSON file at
MEMD_PLANS_PATH, merged per plan and per meter over these defaults:

    {"dev": {"meters": {"searches": {"limit": 300000, "hard": true}}}}

Per meter:
  limit   the included quantity per period (None = unlimited)
  hard    True: over the limit -> 402 quota_exceeded. False: allowed, and the
          quantity above `limit` is overage (billed when the meter is listed
          in the plan's `metered`)
`metered`: the meters whose billable quantity (overage above `limit`, or all
usage when there is no limit) is pushed to Stripe as meter events.
"""
from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass, field

# counters accumulate per period; gauges are point-in-time snapshots
MEMORIES_STORED = "memories_stored"
STORED_GB = "stored_gb"
SEARCHES = "searches"
RERANKED_SEARCHES = "reranked_searches"
EXTRACTIONS_OUR_KEY = "extractions_our_key"
WRITES = "writes"

GAUGES = frozenset({MEMORIES_STORED, STORED_GB})
COUNTERS = frozenset({SEARCHES, RERANKED_SEARCHES, EXTRACTIONS_OUR_KEY, WRITES})
METERS = tuple(sorted(GAUGES | COUNTERS))

# Stripe meter values are sent as whole numbers. A GB gauge rounded to whole
# GB bills a 30 MB tenant for a full GB, so stored_gb goes to Stripe in
# milli-GB: price the Stripe meter per 1/1000 GB ($0.0001 for $0.10/GB-mo).
STRIPE_UNIT_SCALE = {STORED_GB: 1000}

DEFAULT_PLANS: dict[str, dict] = {
    "free": {
        "paid": False,
        "meters": {
            MEMORIES_STORED: {"limit": 50_000, "hard": True},
            SEARCHES: {"limit": 10_000, "hard": True},
            EXTRACTIONS_OUR_KEY: {"limit": 10_000, "hard": True},
            RERANKED_SEARCHES: {"limit": 1_000, "hard": True},
        },
        "metered": [],
    },
    "dev": {
        "paid": True,
        "price_usd_month": 29,
        "meters": {
            MEMORIES_STORED: {"limit": 500_000, "hard": True},
            SEARCHES: {"limit": 250_000, "hard": True},
            # overage: $8/100K on our key (or bring your own key)
            EXTRACTIONS_OUR_KEY: {"limit": 100_000, "hard": False},
            RERANKED_SEARCHES: {"limit": 10_000, "hard": False},
        },
        "metered": [EXTRACTIONS_OUR_KEY, RERANKED_SEARCHES],
    },
    "scale": {
        "paid": True,
        # all usage-based: $0.10/GB-mo stored, $5/1M searches, writes
        # $10/1M (BYO key) + our-key extraction, reranked searches
        "meters": {},
        "metered": [STORED_GB, SEARCHES, WRITES, EXTRACTIONS_OUR_KEY, RERANKED_SEARCHES],
    },
}


@dataclass(frozen=True)
class Entitlement:
    limit: float | None = None
    hard: bool = False


@dataclass(frozen=True)
class Plan:
    name: str
    paid: bool = False
    meters: dict[str, Entitlement] = field(default_factory=dict)
    metered: frozenset[str] = frozenset()

    def entitlement(self, meter: str) -> Entitlement:
        return self.meters.get(meter, Entitlement())


class Plans:
    def __init__(self, raw: dict[str, dict] | None = None):
        merged = copy.deepcopy(DEFAULT_PLANS)
        for name, over in (raw or {}).items():
            base = merged.setdefault(name, {"paid": True, "meters": {}, "metered": []})
            for k, v in over.items():
                if k == "meters":
                    for m, ent in v.items():
                        base["meters"][m] = {**base["meters"].get(m, {}), **ent}
                else:
                    base[k] = v
        self._plans: dict[str, Plan] = {}
        for name, spec in merged.items():
            for m in list(spec.get("meters", {})) + list(spec.get("metered", [])):
                if m not in METERS:
                    raise ValueError(f"plan {name!r}: unknown meter {m!r}; expected one of {list(METERS)}")
            self._plans[name] = Plan(
                name=name,
                paid=bool(spec.get("paid", name != "free")),
                meters={m: Entitlement(limit=e.get("limit"), hard=bool(e.get("hard", False)))
                        for m, e in spec.get("meters", {}).items()},
                metered=frozenset(spec.get("metered", [])),
            )
        if "free" not in self._plans:
            raise ValueError("plans must define 'free' (the fallback plan)")

    @classmethod
    def from_env(cls) -> "Plans":
        path = os.environ.get("MEMD_PLANS_PATH")
        if not path:
            return cls()
        with open(path) as f:
            return cls(json.load(f))

    def get(self, name: str | None) -> Plan:
        """Unknown plan names fall back to free: an entitlement typo must
        never mean 'unlimited'."""
        return self._plans.get(name or "free") or self._plans["free"]

    def __contains__(self, name: str) -> bool:
        return name in self._plans

    def names(self) -> list[str]:
        return list(self._plans)

    def paid_names(self) -> list[str]:
        return [n for n, p in self._plans.items() if p.paid]
