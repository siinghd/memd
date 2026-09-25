"""Slow tests (the crash oracle) run only when asked for: MEMD_RUN_SLOW=1,
or a -m expression that names `slow`."""
import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.environ.get("MEMD_RUN_SLOW") or "slow" in (config.getoption("markexpr") or ""):
        return
    skip = pytest.mark.skip(reason="slow: set MEMD_RUN_SLOW=1 or pass -m slow")
    for item in items:
        if item.get_closest_marker("slow"):
            item.add_marker(skip)
