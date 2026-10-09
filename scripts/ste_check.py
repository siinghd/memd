#!/usr/bin/env python3
"""A small checker for ASD-STE100 Simplified Technical English in the docs.

The project's documentation is written in STE. This script reports text that
is probably not STE. It does not replace a review: it finds candidates, and
a person decides.

What it reports, for each sentence of prose:

  length      a sentence of more than 25 words, or more than 20 words in an
              instruction (a sentence that starts with a verb such as "Run"
              or "Set", or a numbered step)
  passive     a form of "to be" and a past participle ("is written"): a
              passive-voice candidate. STE prefers the active voice.
  ing         a word that ends in "-ing". STE does not use the "-ing" form
              of a verb. A technical name ("embedding", "billing") is not
              reported (see ING_NAMES).
  word        a word from WORDS, with the approved word to use.

WORDS is a small subset, chosen for this project. The official ASD-STE100
dictionary is not in this repository, so a word that is not in WORDS can
also be not approved.

What it does not read: fenced code blocks, inline code, URLs and link
targets, HTML tags and comments, include markers ({% ... %}), headings
(their anchors are links), and in CHANGELOG.md all the released entries
(only [Unreleased] is read).

Usage:
    python scripts/ste_check.py                  # all the tracked *.md files
    python scripts/ste_check.py README.md docs/  # these files or directories
    python scripts/ste_check.py --rules word,length
    python scripts/ste_check.py --strict         # exit 1 if it finds a problem

Without --strict the exit code is 0: the report is for information.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MAX_WORDS = 25
MAX_WORDS_INSTRUCTION = 20

# Not approved, and the approved word to use. A subset only (see above).
WORDS = {
    "utilize": "use", "utilise": "use",
    "ensure": "make sure",
    "allow": "let, or make possible",
    "enable": "let, or make possible",
    "launch": "start",
    "terminate": "stop",
    "commence": "start",
    "obtain": "get",
    "perform": "do",
    "require": "need, or must",
    "sufficient": "enough",
    "numerous": "many",
    "additional": "more",
    "subsequently": "then, or after",
    "assist": "help",
    "attempt": "try",
    "demonstrate": "show",
    "however": "but",
    "whether": "if",
    "via": "through, or with",
    "prior to": "before",
    "in order to": "to",
    "due to": "because of",
}
# the forms of each word that are reported
_FORMS = {
    "utilize": "utiliz(?:e|es|ed|ing)", "utilise": "utilis(?:e|es|ed|ing)",
    "ensure": "ensur(?:e|es|ed|ing)", "allow": "allow(?:s|ed|ing)?", "enable": "enabl(?:e|es|ed|ing)",
    "launch": "launch(?:es|ed|ing)?", "terminate": "terminat(?:e|es|ed|ing)",
    "commence": "commenc(?:e|es|ed|ing)", "obtain": "obtain(?:s|ed|ing)?",
    "perform": "perform(?:s|ed|ing)?", "require": "requir(?:e|es|ed|ing)",
    "assist": "assist(?:s|ed|ing)?", "attempt": "attempt(?:s|ed|ing)?",
    "demonstrate": "demonstrat(?:e|es|ed|ing)",
}
_WORD_RES = [(w, re.compile(r"\b(?:" + _FORMS.get(w, re.escape(w).replace(r"\ ", r"\s+")) + r")\b", re.I))
             for w in WORDS]

# "-ing" words that are technical names in this project, or nouns that are
# not a verb form
ING_NAMES = {
    "billing", "metering", "embedding", "embeddings", "packing", "reranking", "forwarding",
    "routing", "logging", "encoding", "padding", "pricing", "setting", "settings", "heading",
    "headings", "string", "strings", "thing", "things", "something", "nothing", "anything",
    "everything", "during", "morning", "evening", "ring", "spring", "ceiling", "king", "sterling",
    "warning", "warnings", "ping", "listing", "staging", "tracing", "mapping", "mappings",
    "hosting", "sharding", "caching", "chunking", "streaming", "versioning", "rating", "ratings",
    "bring", "sing", "wing", "swing", "sling", "reasoning", "fencing", "hashing", "bookkeeping",
    "tooling", "meaning", "sampling", "signing",
}

_BE = r"(?:am|is|are|was|were|be|been|being|isn't|aren't|wasn't|weren't)"
_IRREGULAR = (
    "done|made|given|taken|written|rewritten|overwritten|sent|kept|held|built|rebuilt|shown|known|seen|"
    "found|lost|left|bound|chosen|driven|broken|hidden|drawn|grown|thrown|shut|paid|said|sold|told|"
    "brought|bought|caught|taught|thought|sought|meant|spent|dealt|felt|heard|led|fed|met|won|begun|"
    "stuck|struck|hung|frozen|forgotten|got|gotten|eaten|fallen|beaten|shaken|undone|forbidden|"
    "withdrawn|mistaken|understood|withheld|split|set|put|run|read|cut|sent|spun|torn|worn|woken"
)
_PASSIVE = re.compile(r"\b" + _BE + r"\s+(?:not\s+|never\s+|also\s+|only\s+|\w+ly\s+)?"
                      r"((?:\w+ed)|(?:" + _IRREGULAR + r"))\b", re.I)
# "-ed" words after "to be" that are adjectives here, not a passive
_NOT_PASSIVE = {"need", "speed", "seed", "feed", "bed", "red", "shed", "proceed", "exceed", "succeed",
                "embed", "indeed", "hundred", "naked", "wicked", "sacred", "kindred"}

# verbs that start an instruction ("Run the tests.")
_INSTRUCTION = re.compile(
    r"^(?:run|set|use|install|add|start|open|make|do|read|write|give|put|send|keep|call|pass|check|build|"
    r"create|delete|remove|stop|restart|change|copy|move|get|see|turn|point|pin|rotate|export|import|"
    r"replace|configure|upgrade|download|upload|enter|type|select|close|wait|test|try|look|find|show|"
    r"tell|ask|report|include|put|leave|let|push|pull|tag|merge|publish|release|deploy|mount|bind|"
    r"choose|edit|update|apply|attach|record|measure|compare|do not|don't|never|always)\b", re.I)

_ABBREV = re.compile(r"\b(?:e\.g|i\.e|etc|vs|cf|approx|fig|incl)\.", re.I)
# a sentence can start with a lowercase name ("memd", "usearch")
_SENTENCE_END = re.compile(r"(?<=[.!?])[\"')\]”’]*\s+(?=[\"'(\[“‘]?[A-Za-z0-9`*_])")


@dataclass
class Finding:
    path: str
    line: int
    rule: str
    message: str
    text: str

    def __str__(self) -> str:
        excerpt = self.text if len(self.text) <= 110 else self.text[:107] + "..."
        return f"{self.path}:{self.line}: [{self.rule}] {self.message}: {excerpt}"


def _tracked_md() -> list[str]:
    try:
        out = subprocess.run(["git", "ls-files", "*.md"], cwd=ROOT, capture_output=True, text=True,
                             check=True).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        out = []
        for d, dirs, files in os.walk(ROOT):
            dirs[:] = [x for x in dirs if not x.startswith(".") and x != "node_modules"]
            out += [os.path.relpath(os.path.join(d, f), ROOT) for f in files if f.endswith(".md")]
    return sorted(p for p in out if "node_modules" not in p)


def _expand(paths: list[str]) -> list[str]:
    out = []
    for p in paths:
        full = p if os.path.isabs(p) else os.path.join(ROOT, p)
        if os.path.isdir(full):
            for d, _dirs, files in os.walk(full):
                out += [os.path.relpath(os.path.join(d, f), ROOT) for f in sorted(files) if f.endswith(".md")]
        else:
            out.append(os.path.relpath(full, ROOT))
    return out


def _clean_inline(text: str) -> str:
    """The prose of a block: code, URLs, link targets, HTML and emphasis
    markers removed. A removed piece of code becomes the word CODE, so that
    it still counts as one word. The line breaks stay."""
    text = re.sub(r"<!--.*?-->", " ", text)
    text = re.sub(r"\{%.*?%\}", " ", text)
    # inline code can continue on the next line: keep its line breaks
    text = re.sub(r"(`+)(?:(?!\1).)+?\1", lambda m: "CODE" + "\n" * m.group(0).count("\n"), text,
                  flags=re.S)
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", " ", text)            # images
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)          # links: keep the text
    text = re.sub(r"\[([^\]]*)\]\[[^\]]*\]", r"\1", text)
    text = re.sub(r"<https?://[^>]+>", "URL", text)
    text = re.sub(r"https?://\S+", "URL", text)
    text = re.sub(r"</?[A-Za-z][^>]*>", " ", text)                # HTML tags
    text = re.sub(r"(\*\*|__|\*|~~)", "", text)
    return text


def _paragraphs(path: str, lines: list[str]):
    """(first line number, text) for each block of prose."""
    in_code = False
    fence = ""
    in_comment = False
    buf: list[str] = []
    start = 0
    changelog = os.path.basename(path).lower() == "changelog.md"
    seen_unreleased = False

    def flush():
        nonlocal buf
        if buf:
            yield start, "\n".join(buf)
        buf = []

    for no, raw in enumerate(lines, 1):
        line = raw.rstrip("\n")
        s = line.strip()
        if changelog and re.match(r"##\s+\[", s):
            if seen_unreleased and "unreleased" not in s.lower():
                break                                   # a released entry: stop
            seen_unreleased = seen_unreleased or "unreleased" in s.lower()
        m = re.match(r"(`{3,}|~{3,})", s)
        if m:
            if not in_code:
                yield from flush()
                in_code, fence = True, m.group(1)[0] * 3
            elif s.startswith(fence):
                in_code = False
            continue
        if in_code:
            continue
        if in_comment:
            if "-->" in s:
                in_comment = False
            continue
        if s.startswith("<!--") and "-->" not in s:
            in_comment = True
            continue
        if (not s or s.startswith("#") or s.startswith("{%") or s.startswith("--8<--")
                or re.match(r"^\|?\s*:?-{3,}", s) or re.match(r"<(?!https?:)[A-Za-z/!]", s)
                or s == "---"):
            yield from flush()
            continue
        if s.startswith("|"):                           # a table row: each cell is a block
            yield from flush()
            for cell in s.strip("|").split("|"):
                if cell.strip():
                    yield no, cell.strip()
            continue
        item = re.match(r"^(?:[-*+]|\d+[.)])\s+", s)
        if item or s.startswith(">"):
            yield from flush()
            s = s[item.end():] if item else s.lstrip("> ")
            if item and item.group(0)[0].isdigit():
                s = "STEP " + s                       # a numbered step: an instruction
        if not buf:
            start = no
        buf.append(s)
    yield from flush()


def _sentences(text: str):
    """(offset, sentence) for each sentence of a block."""
    protected = _ABBREV.sub(lambda m: m.group(0).replace(".", "\x00"), text)
    pos = 0
    for m in _SENTENCE_END.finditer(protected):
        yield pos, text[pos:m.start()]
        pos = m.end()
    if text[pos:].strip():
        yield pos, text[pos:]


def check_file(path: str, rules: set[str]) -> list[Finding]:
    with open(os.path.join(ROOT, path), encoding="utf-8") as f:
        lines = f.readlines()
    out: list[Finding] = []
    for start, block in _paragraphs(path, lines):
        prose = _clean_inline(block)
        for off, sent in _sentences(prose):
            line = start + prose[:off].count("\n")
            flat = " ".join(sent.split())
            step = flat.startswith("STEP ")
            if step:
                flat = flat[5:]
            words = re.findall(r"[A-Za-z0-9][\w'’./-]*", flat)
            if not words:
                continue
            if "length" in rules:
                limit = MAX_WORDS_INSTRUCTION if (step or _INSTRUCTION.match(flat)) else MAX_WORDS
                if len(words) > limit:
                    out.append(Finding(path, line, "length", f"{len(words)} words (max {limit})", flat))
            if "passive" in rules:
                for m in _PASSIVE.finditer(flat):
                    if m.group(1).lower() not in _NOT_PASSIVE:
                        out.append(Finding(path, line, "passive", f"'{m.group(0)}'", flat))
            if "ing" in rules:
                for w in re.findall(r"\b[A-Za-z][a-z]+ing\b", flat):
                    if w.lower() not in ING_NAMES and len(w) > 5:
                        out.append(Finding(path, line, "ing", f"'{w}'", flat))
            if "word" in rules:
                for w, rx in _WORD_RES:
                    for m in rx.finditer(flat):
                        out.append(Finding(path, line, "word", f"'{m.group(0)}': use {WORDS[w]}", flat))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("paths", nargs="*", help="files or directories (default: all tracked *.md files)")
    ap.add_argument("--rules", default="length,passive,ing,word",
                    help="comma-separated: length, passive, ing, word")
    ap.add_argument("--strict", action="store_true", help="exit 1 if there is a finding")
    ap.add_argument("--summary", action="store_true", help="only the counts for each file and rule")
    args = ap.parse_args(argv)
    rules = {r.strip() for r in args.rules.split(",") if r.strip()}
    files = _expand(args.paths) if args.paths else _tracked_md()
    findings: list[Finding] = []
    for p in files:
        findings += check_file(p, rules)
    if not args.summary:
        for f in findings:
            print(f)
    counts = Counter((f.path, f.rule) for f in findings)
    by_rule = Counter(f.rule for f in findings)
    print(f"\n{len(findings)} findings in {len({f.path for f in findings})} of {len(files)} files: "
          + ", ".join(f"{r} {n}" for r, n in sorted(by_rule.items())))
    if args.summary:
        for (p, r), n in sorted(counts.items()):
            print(f"  {p}: {r} {n}")
    return 1 if (args.strict and findings) else 0


if __name__ == "__main__":
    sys.exit(main())
