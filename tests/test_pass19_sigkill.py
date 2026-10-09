"""Pass 19: real crash-consistency evidence - SIGKILL mid-write-stream.

The core contract: write ack = durable append (fsync); any
process can reopen any namespace by replaying the object-store log.
Prior durability tests SIMULATED crash states (torn frames, orphan
segments). This module kills a live writer with SIGKILL at arbitrary
moments and audits every acknowledged id against physical reality.
"""
import os
import signal
import subprocess
import sys
import time

import pytest

CHILD = r"""
import os, sys, time
sys.path.insert(0, {src!r})
from memd.engine.memory import Memory

root, ack_path = sys.argv[1], sys.argv[2]
mem = Memory(root)
ack_fd = os.open(ack_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
i = 0
while True:
    ids = mem.add(f"crash probe record {{i}} durable-payload marker", user_id="u1")
    for rid in ids:
        os.write(ack_fd, (rid + "\n").encode())
        os.fsync(ack_fd)
    i += 1
"""

# The longest time to wait for the writer's first ack. A loaded host can be
# slow to start the child, so this is large; a healthy run uses less than 2 s.
FIRST_ACK_TIMEOUT_S = 120.0


def _start_writer(root: str, ack_path: str) -> subprocess.Popen:
    src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
    return subprocess.Popen(
        [sys.executable, "-c", CHILD.format(src=src), root, ack_path],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )


def _read_acks(ack_path: str) -> list[str]:
    if not os.path.exists(ack_path):
        return []
    with open(ack_path) as f:
        return [line.strip() for line in f if line.strip()]


def _wait_for_first_ack(child: subprocess.Popen, ack_path: str) -> None:
    """Wait until the writer has acked at least one write. A fixed sleep
    before the kill is not enough: on a loaded host the child can need
    more time to start, and then the kill comes before the first ack."""
    deadline = time.monotonic() + FIRST_ACK_TIMEOUT_S
    while not _read_acks(ack_path):
        if child.poll() is not None:
            err = child.stderr.read().decode(errors="replace") if child.stderr else ""
            pytest.fail(f"the writer stopped before its first ack (rc={child.returncode}): "
                        f"{err[-2000:]}")
        if time.monotonic() > deadline:
            pytest.fail(f"no ack from the writer in {FIRST_ACK_TIMEOUT_S:.0f}s")
        time.sleep(0.02)


def _kill_after_first_ack(child: subprocess.Popen, ack_path: str, extra_s: float) -> None:
    """Kill the writer `extra_s` seconds after its first ack, at a point
    in its write stream that the test does not control."""
    _wait_for_first_ack(child, ack_path)
    time.sleep(extra_s)
    if child.poll() is None:
        os.kill(child.pid, signal.SIGKILL)
    child.wait(timeout=15)


class TestSigkillDurability:
    # the time between the first ack and the kill
    @pytest.mark.parametrize("delay_s", [0.0, 0.15, 0.6])
    def test_sigkilled_writer_loses_no_acked_record(self, tmp_path, delay_s):
        root = str(tmp_path / "data")
        ack_path = str(tmp_path / "acks.log")
        child = _start_writer(root, ack_path)
        try:
            _kill_after_first_ack(child, ack_path, delay_s)
            acked = _read_acks(ack_path)
            assert acked, "no acked writes to audit"

            # reopen WITHOUT any cleanup: replay must recover every ack
            from memd.engine.memory import Memory

            mem2 = Memory(root)
            try:
                missing = [rid for rid in acked if mem2.get(rid) is None]
                assert not missing, (
                    f"{len(missing)}/{len(acked)} ACKED records lost after "
                    f"SIGKILL {delay_s}s after the first ack: {missing[:5]}")
                # and they are retrievable through the normal search path
                res = mem2.search("durable-payload marker", user_id="u1")
                found = {h.id for h in res.items}
                assert set(acked) & found, "acked records invisible to search"
            finally:
                mem2.close()
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)

    def test_reopen_after_kill_is_fast_and_repeatable(self, tmp_path):
        """Two consecutive kill/reopen cycles: replay cost stays bounded
        (tail-only), and the second cycle inherits all prior acks."""
        root = str(tmp_path / "data")
        ack_path = str(tmp_path / "acks.log")
        all_acked: list[str] = []
        for round_no in range(2):
            path = ack_path if round_no == 0 else ack_path + ".2"
            child = _start_writer(root, path)
            try:
                _kill_after_first_ack(child, path, 0.3)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
            acked = _read_acks(path)
            assert acked, f"no acked writes in round {round_no}"
            all_acked.extend(acked)

        from memd.engine.memory import Memory

        # The first Memory in a process pays one-time costs (imports, the
        # embedder). Pay them on a different root before the timed reopen,
        # so that the time limit measures only the replay.
        warm = Memory(str(tmp_path / "warm"))
        warm.add("warm-up record", user_id="u1")
        warm.close()

        t0 = time.monotonic()
        mem2 = Memory(root)
        open_ms = (time.monotonic() - t0) * 1000
        try:
            missing = [rid for rid in all_acked if mem2.get(rid) is None]
            assert not missing, f"lost across two crash cycles: {missing[:5]}"
            assert open_ms < 5000, f"replay took {open_ms:.0f}ms (unbounded tail?)"
        finally:
            mem2.close()
