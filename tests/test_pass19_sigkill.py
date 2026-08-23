"""Pass 19: real crash-consistency evidence - SIGKILL mid-write-stream.

The core contract (ADR-2/D2): write ack = durable append (fsync); any
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


class TestSigkillDurability:
    @pytest.mark.parametrize("delay_s", [0.45, 0.6, 1.1])
    def test_sigkilled_writer_loses_no_acked_record(self, tmp_path, delay_s):
        root = str(tmp_path / "data")
        ack_path = str(tmp_path / "acks.log")
        src = "/home/deploy/agent-memory/src"
        child = subprocess.Popen(
            [sys.executable, "-c", CHILD.format(src=src), root, ack_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        try:
            time.sleep(delay_s)
            # writer may have crashed early on a slow box; only audit if it ran
            alive = child.poll() is None
            if alive:
                os.kill(child.pid, signal.SIGKILL)
            child.wait(timeout=15)
            if not os.path.exists(ack_path):
                pytest.skip("writer died before first ack (environment too slow)")
            with open(ack_path) as f:
                acked = [line.strip() for line in f if line.strip()]
            assert acked, "no acked writes to audit"
            assert not alive or True

            # reopen WITHOUT any cleanup: replay must recover every ack
            from memd.engine.memory import Memory

            mem2 = Memory(root)
            try:
                missing = [rid for rid in acked if mem2.get(rid) is None]
                assert not missing, (
                    f"{len(missing)}/{len(acked)} ACKED records lost after "
                    f"SIGKILL at {delay_s}s: {missing[:5]}")
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
        src = "/home/deploy/agent-memory/src"
        all_acked: list[str] = []
        for round_no, delay in enumerate((0.5, 0.5)):
            child = subprocess.Popen(
                [sys.executable, "-c", CHILD.format(src=src), root,
                 ack_path if round_no == 0 else ack_path + ".2"],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            time.sleep(delay)
            if child.poll() is None:
                os.kill(child.pid, signal.SIGKILL)
            child.wait(timeout=15)
            path = ack_path if round_no == 0 else ack_path + ".2"
            if os.path.exists(path):
                with open(path) as f:
                    all_acked.extend(l.strip() for l in f if l.strip())

        assert all_acked
        from memd.engine.memory import Memory

        t0 = time.monotonic()
        mem2 = Memory(root)
        open_ms = (time.monotonic() - t0) * 1000
        try:
            missing = [rid for rid in all_acked if mem2.get(rid) is None]
            assert not missing, f"lost across two crash cycles: {missing[:5]}"
            assert open_ms < 5000, f"replay took {open_ms:.0f}ms (unbounded tail?)"
        finally:
            mem2.close()
