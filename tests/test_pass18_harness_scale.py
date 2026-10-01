"""Pass 18: harness must support user counts beyond the name-pool size.

--users is free-form CLI input; the generator indexed names[u] directly and
IndexError'd the entire harness for users > 8.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from memd.harness.suites.longmemeval_synthetic import generate_cases


class TestGeneratorScale:
    def test_users_beyond_name_pool(self):
        events, cases = generate_cases(seed=123, users=12)
        uids = {e["user_id"] for e in events}
        assert len(uids) == 12
        assert len(cases) >= 12 * 5

    def test_deterministic_for_same_seed(self):
        e1, c1 = generate_cases(seed=42, users=8)
        e2, c2 = generate_cases(seed=42, users=8)
        assert e1 == e2 and c1 == c2

    def test_single_user_smoke(self):
        events, cases = generate_cases(seed=1, users=1)
        assert events and cases

    @pytest.mark.parametrize("seed", [0, 7, 123, 2**31 - 1])
    def test_extreme_seeds(self, seed):
        events, _ = generate_cases(seed=seed, users=3)
        assert events
