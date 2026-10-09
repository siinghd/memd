"""scripts/ste_check.py: the report of text in the docs that is probably not
Simplified Technical English."""
import os
import sys

import pytest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import ste_check  # noqa: E402


@pytest.fixture()
def check(tmp_path, monkeypatch):
    monkeypatch.setattr(ste_check, "ROOT", str(tmp_path))

    def run(text: str, name: str = "doc.md", rules=("length", "passive", "ing", "word")):
        (tmp_path / name).write_text(text)
        return [(f.line, f.rule, f.message) for f in ste_check.check_file(name, set(rules))]
    return run


def test_each_rule_finds_its_problem(check):
    long = " ".join(["word"] * 26) + "."
    out = check(f"Intro line.\n\n{long}\n\nThe file is written at once.\n\nIt keeps running.\n\n"
                "We utilize the cache.\n")
    assert (3, "length", "26 words (max 25)") in out
    assert any(r == "passive" and "is written" in m for _l, r, m in out)
    assert (7, "ing", "'running'") in out
    assert (9, "word", "'utilize': use use") in out


def test_an_instruction_has_a_lower_limit(check):
    assert [r for _l, r, _m in check("Run " + " ".join(["it"] * 21) + ".", rules=("length",))] == ["length"]
    assert check("Run " + " ".join(["it"] * 19) + ".", rules=("length",)) == []


def test_code_urls_includes_and_headings_are_not_read(check):
    text = ("# Running the utilized thing\n\n"
            "```bash\nwe utilize running code\n```\n\n"
            "Use `utilize_running()` and <https://example.com/running>.\n\n"
            "See [the docs](https://example.com/utilize-running).\n\n"
            '{% include-markdown "x.md" start="running" %}\n\n'
            "<!-- we utilize this -->\n")
    assert check(text) == []


def test_a_sentence_split_keeps_abbreviations_and_numbers(check):
    text = "Use a value, e.g. 0.5 s, then stop. It works."
    assert check(text, rules=("length",)) == []
    assert len(list(ste_check._sentences(text))) == 2


def test_only_the_unreleased_changelog_entry_is_read(check):
    text = ("# Changelog\n\n## [Unreleased]\n\n- We utilize it.\n\n"
            "## [0.1.0] - 2026-01-01\n\n- We utilize it too.\n")
    assert check(text, name="CHANGELOG.md", rules=("word",)) == [(5, "word", "'utilize': use use")]


def test_without_strict_the_exit_code_is_zero(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(ste_check, "ROOT", str(tmp_path))
    (tmp_path / "a.md").write_text("We utilize it.\n")
    assert ste_check.main(["a.md"]) == 0
    assert ste_check.main(["a.md", "--strict"]) == 1
    assert "1 findings in 1 of 1 files: word 1" in capsys.readouterr().out
