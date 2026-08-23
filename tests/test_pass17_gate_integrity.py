"""Pass 16b: gate_check must grade the RIGHT suite's metrics.

Both longmemeval-synthetic and halumem-ops emit system='memd'. A bare
system-keyed dict let halumem's summary silently replace longmemeval's, so
the token-ratio check graded ~51 tokens against a bar computed from
longmemeval's full-context size and could never fail."""
import sys

sys.path.insert(0, "/home/deploy/agent-memory/src")

from memd.harness.core import CaseResult, SuiteReport
from memd.harness.run import gate_check


def _report(suite: str, system: str, tokens: float, qclass: str = "factual") -> SuiteReport:
    r = SuiteReport(suite=suite, system=system, harness_version="test")
    r.add(CaseResult(case_id="c1", qclass=qclass, correct=True, score=1.0,
                     tokens_injected=tokens, latency_ms=1.0))
    return r


def _adv(passed=True):
    return {"passed": passed, "score": 1.0 if passed else 0.0, "probes": []}


class TestGateCheckKeying:
    def test_token_ratio_uses_longmemeval_not_halumem(self):
        lm_memd = _report("longmemeval-synthetic", "memd", tokens=1900)
        lm_fc = _report("longmemeval-synthetic", "full-context", tokens=10_000)
        lm_rag = _report("longmemeval-synthetic", "plain-rag", tokens=100)
        # halumem's tiny token count must NOT mask longmemeval's fat one
        halumem = _report("halumem-ops", "memd", tokens=51)

        ok, failures = gate_check([lm_memd, lm_rag, lm_fc, halumem], _adv())
        assert not ok, "19% token ratio must fail the 10% bar despite halumem masking"
        assert any("token ratio" in f for f in failures)

    def test_within_bar_passes(self):
        lm_memd = _report("longmemeval-synthetic", "memd", tokens=800)
        lm_fc = _report("longmemeval-synthetic", "full-context", tokens=10_000)
        lm_rag = _report("longmemeval-synthetic", "plain-rag", tokens=100)
        halumem = _report("halumem-ops", "memd", tokens=51)
        ok, failures = gate_check([lm_memd, lm_rag, lm_fc, halumem], _adv())
        assert ok, f"8% ratio should pass: {failures}"

    def test_missing_longmemeval_report_fails_closed(self):
        ok, failures = gate_check([_report("halumem-ops", "memd", 51)], _adv())
        assert not ok and "no longmemeval memd report" in failures[0]

    def test_adversarial_failure_blocks_gate(self):
        lm_memd = _report("longmemeval-synthetic", "memd", tokens=800)
        lm_fc = _report("longmemeval-synthetic", "full-context", tokens=10_000)
        ok, failures = gate_check([lm_memd, lm_fc], _adv(False))
        assert not ok and any("adversarial" in f for f in failures)
