"""Extractor contract: raw segments -> candidate facts.

Contract rules:
  - Extraction is a *derived index build*: re-runnable, versioned, never the
    only copy of information (raw lane is retained).
  - Every fact carries `entity_keys` (normalized cluster keys) and `lineage`
    (the raw record ids it came from).
  - Trust-tier propagation: facts inherit the *cap* of their sources'
    tiers - extraction from untrusted raw can never mint higher-trust facts.
  - Conservative by default: precision over recall; consolidation resolves
    conflicts cluster-locally via supersedence, never globally.

Providers: LLMExtractor (BYO OpenAI-compatible key) and HeuristicExtractor
(high-precision patterns; keeps the fact lane functional with zero keys).
"""
from __future__ import annotations

import json
import math
import os
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from memd.core.schema import MemoryRecord, Source

PROMPT_VERSION = "v2"
DEFAULT_MAX_TOKENS = 4096   # output tokens per extraction call
DEFAULT_TIMEOUT_S = 120.0   # the longest one extraction call may take


@dataclass
class ExtractedFact:
    content: str
    entity_keys: list[str] = field(default_factory=list)
    lineage: list[str] = field(default_factory=list)


def normalize_entity_key(key: str) -> str:
    k = key.strip().lower()
    k = re.sub(r"[^a-z0-9._-]+", "_", k)
    k = re.sub(r"_+", "_", k).strip("_")
    return k[:120]


def slug(s: str, max_len: int = 40) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", s.strip().lower()).strip("_")
    return s[:max_len]


class Extractor(ABC):
    name: str = "base"

    @abstractmethod
    def extract(self, records: list[MemoryRecord]) -> list[ExtractedFact]: ...

    def cap_source_tier(self, facts: list[ExtractedFact], sources: list[MemoryRecord]) -> list[ExtractedFact]:
        return facts


def min_source_tier(records: list[MemoryRecord]) -> Source:
    if not records:
        return Source.IMPORT
    return min((r.provenance.source for r in records), key=lambda s: int(s))


class HeuristicExtractor(Extractor):
    """High-precision pattern extraction. Zero keys, zero network.

    Facts are rewritten as standalone third-person statements (same contract
    as the LLM prompt) keyed to canonical entity keys so supersedence works
    out of the box. Original wording kept in meta.verbatim."""

    name = "heuristic"

    _PATTERNS: list[tuple[re.Pattern, str]] = [
        # identity
        (re.compile(r"\bmy name is\s+([A-Z][\w'-]*(?:\s[A-Z][\w'-]*)?)", re.I), "user.name"),
        (re.compile(r"\b(?:call me|i(?:'m| am) called)\s+([A-Z][\w'-]*)", re.I), "user.name"),
        # employment
        (re.compile(r"\bi\s+(?:\w+\s+){0,3}?work(?:ed|ing)?\s+(?:at|for)\s+([A-Za-z0-9&.-]+)", re.I), "user.employer"),
        (re.compile(r"\bmy employer is\s+([A-Za-z0-9&.-]+)", re.I), "user.employer"),
        # location
        (re.compile(r"\bi\s+(?:live|moved|relocated)\s+(?:to|in)\s+([A-Za-z .'-]+?)(?:[.,;!]|$)", re.I), "user.city"),
        # deployment / tooling
        (re.compile(r"\bwe\s+(?:deploy|ship|release)\s+(?:with|using|via)\s+(.+?)(?:[.,;!]|$)", re.I), "team.deploy_cmd"),
        (re.compile(r"\bthe\s+(?:deploy|deployment)\s+command\s+is\s+(.+?)(?:[.,;!]|$)", re.I), "team.deploy_cmd"),
        (re.compile(r"\bthe\s+(?:build|test|run)\s+command\s+is\s+(.+?)(?:[.,;!]|$)", re.I), "repo.build_cmd"),
        # editors
        (re.compile(r"\bmy editor is\s+([\w.-]+)", re.I), "user.editor"),
        (re.compile(r"\b(?:i|we)\s+(?:switched|moved)\s+(?:my |our )?editor\s+to\s+([\w.-]+)", re.I), "user.editor"),
        (re.compile(r"\bi\s+use\s+([\w.-]+)\s+as\s+my\s+editor", re.I), "user.editor"),
        # preferences
        (re.compile(r"\bwe\s+use\s+(.+?)\s+(?:for|to)\s+(\w+)", re.I), None),
        (re.compile(r"\b(?:i|we)\s+(?:prefer|like)\s+(.+?)(?:[.,;!]|$)", re.I), None),
        (re.compile(r"\b(?:i|we)\s+(?:hate|dislike|avoid|don't like|do not like)\s+(.+?)(?:[.,;!]|$)", re.I), None),
        (re.compile(r"\b(?:always|never)\s+([a-z].*?)(?:[.,;!]|$)", re.I), None),
        # contact / environment
        (re.compile(r"\bmy\s+(?:email|e-mail)\s+is\s+(\S+@\S+)", re.I), "user.email"),
        (re.compile(r"\bmy\s+(?:timezone|tz)\s+is\s+([\w/+_-]+)", re.I), "user.timezone"),
        (re.compile(r"\bi\s+use\s+(?:a\s+)?([\w-]+)\s+(?:laptop|machine|computer)", re.I), "user.machine"),
    ]

    def extract(self, records: list[MemoryRecord]) -> list[ExtractedFact]:
        facts: list[ExtractedFact] = []
        # resolve display names within the batch so facts read as standalone
        # third-person statements about the person, not opaque user ids
        name_map = self._batch_names(records)
        for rec in records:
            text = rec.content.strip()
            if not text or rec.kind not in ("raw_event", "procedure"):
                continue
            subject_id = rec.scope.user or rec.scope.agent or rec.scope.org or "the user"
            subject = name_map.get(subject_id, subject_id)
            for pat, key in self._PATTERNS:
                m = pat.search(text)
                if not m:
                    continue
                groups = [g.strip() for g in m.groups() if g and g.strip()]
                if not groups:
                    continue
                if key is None:
                    key = self._dynamic_key(pat, groups)
                ekeys = [normalize_entity_key(key)] if key else []
                content = self._third_person(normalize_entity_key(key), subject, groups)
                facts.append(
                    ExtractedFact(content=content, entity_keys=ekeys, lineage=[rec.id])
                )
        seen: set[str] = set()
        out = []
        for f in facts:
            k = f.content.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(f)
        return out

    @staticmethod
    def _batch_names(records: list[MemoryRecord]) -> dict[str, str]:
        names: dict[str, str] = {}
        for rec in records:
            sid = rec.scope.user or rec.scope.agent
            if not sid:
                continue
            m = re.search(r"\bmy name is\s+([A-Z][\w'-]*(?:\s[A-Z][\w'-]*)?)", rec.content, re.I)
            if not m:
                m = re.search(r"\b(?:call me|i(?:'m| am) called)\s+([A-Z][\w'-]*)", rec.content, re.I)
            if m:
                names.setdefault(sid, m.group(1).strip())
        return names

    @staticmethod
    def _third_person(key: str, subject: str, groups: list[str]) -> str:
        x = groups[0].strip().rstrip(".,;!")
        y = groups[1].strip() if len(groups) > 1 else ""
        if key == "user.name":
            return f"{subject} is called {x}"
        if key == "user.employer":
            return f"{subject} works at {x}"
        if key == "user.city":
            return f"{subject} lives in {x}"
        if key == "team.deploy_cmd":
            return f"the team deploys with {x}"
        if key == "repo.build_cmd":
            return f"the build command is {x}"
        if key == "user.editor":
            return f"{subject}'s editor is {x}"
        if key == "user.email":
            return f"{subject}'s email is {x}"
        if key == "user.timezone":
            return f"{subject}'s timezone is {x}"
        if key == "user.machine":
            return f"{subject} uses a {x} machine"
        if key.startswith("tool."):
            return f"{subject} uses {x} for {y}" if y else f"{subject} uses {x}"
        if key.startswith("user.pref."):
            return f"{subject} prefers {x}"
        if key.startswith("user.avoid."):
            return f"{subject} avoids {x}"
        if key.startswith("policy."):
            return f"policy: {x}"
        return f"{subject}: {x}"

    @staticmethod
    def _dynamic_key(pat: re.Pattern, groups: list[str]) -> str:
        pats = pat.pattern
        if "for|to" in pats:
            return f"tool.{slug(groups[-1])}"
        if "prefer|like" in pats:
            return f"user.pref.{slug(groups[0], 24)}"
        if "hate|dislike" in pats:
            return f"user.avoid.{slug(groups[0], 24)}"
        if "always|never" in pats:
            head = slug(groups[0].split()[0] if groups[0].split() else "rule", 16)
            return f"policy.{head}"
        return "fact.general"


class LLMExtractor(Extractor):
    """Cheap-model extraction over OpenAI-compatible chat completions.
    Batched per segment; prompt pinned by PROMPT_VERSION."""

    name = "llm"
    prompt_version = PROMPT_VERSION

    # v2: each turn says who spoke and when, and facts name the turns they
    # came from (lineage) - v1 sent bare text, so an assistant's suggestion
    # read like the user's statement and every fact was attributed to the
    # session's first turn
    _SYSTEM_PROMPT = f"""You extract durable memories from agent conversation segments.
Each turn is one line: [<turn id>] <time, UTC> <speaker>: <text>. The speaker is user (the person), assistant (the AI agent), or system/tool.
Return ONLY a JSON array. Each item: {{"content": "<standalone third-person fact>", "entity_keys": ["<dot.separated.key>"], "lineage": ["<turn id>"]}}.
Rules:
- Only stable, reusable facts (identity, preferences, procedures, environment, decisions). No chit-chat.
- Attribute each fact to who said it. What the user says about themselves is a fact about the user; what the assistant says or suggests is not, unless the user confirms it.
- Resolve relative dates ("yesterday", "next week") against the turn's time.
- entity_keys are short normalized keys like user.employer, repo.build_cmd, user.pref.editor.
- lineage lists the ids of the turns the fact comes from.
- One fact per distinct assertion; keep the original wording when possible.
- If nothing qualifies, return [].
prompt_version={PROMPT_VERSION}"""

    def __init__(self, model: str, api_key: str, base_url: str = "https://api.openai.com/v1",
                 chunk_records: int = 40, chunk_chars: int = 24_000,
                 max_tokens: int = DEFAULT_MAX_TOKENS, timeout_s: float = DEFAULT_TIMEOUT_S,
                 request_options: dict | None = None):
        import httpx

        self.model = model
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.chunk_records = max(1, int(chunk_records))
        self.chunk_chars = max(1000, int(chunk_chars))
        # output cap per call (0 = none): one uncapped call to a model that
        # looped ran to 131,072 output tokens and 413 s
        self.max_tokens = int(max_tokens)
        self.timeout_s = float(timeout_s)
        # merged into every request body last; a None value removes a field
        self.request_options = _request_options(request_options or {})
        self.chunks_sent = 0
        self._client = httpx.Client(timeout=self.timeout_s)

    def _chunks(self, records: list[MemoryRecord]):
        """Bounded request chunks: a session can carry up to 1000 rows x
        multi-KB contents - sending that as ONE completion explodes token
        cost and gets rejected by every provider. Chunks stop growing at
        whichever bound hits first (rows or serialized chars)."""
        buf: list[MemoryRecord] = []
        chars = 0
        for r in records:
            n = len(r.content) + len(r.id) + 4
            if buf and (len(buf) >= self.chunk_records or chars + n > self.chunk_chars):
                yield buf
                buf, chars = [], 0
            buf.append(r)
            chars += n
        if buf:
            yield buf

    def extract(self, records: list[MemoryRecord]) -> list[ExtractedFact]:
        """Chunked extraction with per-chunk failure isolation: one bad
        chunk (provider hiccup, malformed output) degrades THIS chunk only -
        previously a single failure discarded every fact of the session."""
        from memd.metrics import METRICS

        if not records:
            return []
        out: list[ExtractedFact] = []
        for chunk in self._chunks(records):
            self.chunks_sent += 1
            try:
                out.extend(self._extract_chunk(chunk))
            except Exception:
                METRICS.inc("memd_extraction_chunks_failed_total", model=self.model,
                            help="extraction chunks lost to provider errors")
        return out

    def _extract_chunk(self, records: list[MemoryRecord]) -> list[ExtractedFact]:
        lines = [self._turn_line(r) for r in records]
        user_msg = "Segment:\n" + "\n".join(lines)
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self._SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            "temperature": 0,
        }
        if self.max_tokens:
            body["max_tokens"] = self.max_tokens
        for k, v in self.request_options.items():
            if v is None:
                body.pop(k, None)
            else:
                body[k] = v
        text = self._complete(body)["choices"][0]["message"]["content"]
        return self._parse(text, records)

    @staticmethod
    def _turn_line(rec: MemoryRecord) -> str:
        """[id] time speaker: text - when and who as the reranker sees a
        record (role from the writer's actor id, else the source tier)."""
        from memd.query.rerank import candidate_from_record

        c = candidate_from_record(rec)
        return f"[{rec.id}] {c['date']} {c['role']}: {rec.content}"

    def _complete(self, body: dict) -> dict:
        """One provider call, at most timeout_s long. The client's timeout
        cuts off a provider silent for that long; the deadline cuts off one
        that keeps the connection alive with whitespace while the model
        generates (OpenRouter does), since every byte resets a read timeout."""
        deadline = time.monotonic() + self.timeout_s
        buf = bytearray()
        with self._client.stream("POST", f"{self.base_url}/chat/completions",
                                 headers={"Authorization": f"Bearer {self.api_key}"},
                                 json=body, timeout=self.timeout_s) as resp:
            resp.raise_for_status()
            for part in resp.iter_bytes():
                buf += part
                if time.monotonic() > deadline:
                    raise TimeoutError(f"extraction call exceeded {self.timeout_s:g}s")
        return json.loads(bytes(buf))

    def _parse(self, text: str, records: list[MemoryRecord]) -> list[ExtractedFact]:
        try:
            start, end = text.find("["), text.rfind("]")
            arr = json.loads(text[start : end + 1])
        except Exception:
            return []
        id_set = {r.id for r in records}
        out = []
        for item in arr:
            if not isinstance(item, dict):
                continue
            content = str(item.get("content", "")).strip()
            if not content:
                continue
            ekeys = [normalize_entity_key(k) for k in item.get("entity_keys", []) if k]
            lin = [x for x in item.get("lineage", []) if x in id_set]
            out.append(ExtractedFact(content=content, entity_keys=ekeys, lineage=lin))
        return out


def extraction_setting(cfg: dict, key: str, default=None):
    """config[key], else env MEMD_<KEY>, else `default`. A config value that
    is not None wins - so config {"extraction_api_key": ""} turns an env key
    off."""
    if cfg.get(key) is not None:
        return cfg[key]
    env = os.environ.get(f"MEMD_{key.upper()}")
    return env if env is not None else default


def resolve_extractor(config: dict | None = None) -> Extractor:
    cfg = config or {}
    api_key = extraction_setting(cfg, "extraction_api_key")
    if api_key:
        return LLMExtractor(
            model=extraction_setting(cfg, "extraction_model", "gpt-4o-mini"),
            api_key=api_key,
            base_url=extraction_setting(cfg, "extraction_base_url", "https://api.openai.com/v1"),
            max_tokens=_max_tokens(extraction_setting(cfg, "extraction_max_tokens", DEFAULT_MAX_TOKENS)),
            timeout_s=_timeout_s(extraction_setting(cfg, "extraction_timeout_s", DEFAULT_TIMEOUT_S)),
            request_options=_request_options(extraction_setting(cfg, "extraction_request_options", {})),
        )
    return HeuristicExtractor()


def _max_tokens(v) -> int:
    try:
        n = int(str(v).strip())
    except ValueError:
        n = -1
    if n < 0:
        raise ValueError(f"extraction_max_tokens must be a whole number >= 0 (0 = no cap), got {v!r}")
    return n


# the extractor's own fields: its prompt, its model setting, and a whole
# JSON reply it can parse
_RESERVED_OPTIONS = ("model", "messages", "stream")


def _request_options(v) -> dict:
    """A dict, or (from the env) a JSON object."""
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError as ex:
            raise ValueError(f"extraction_request_options is not valid JSON: {ex}") from None
    if not isinstance(v, dict):
        raise ValueError(f"extraction_request_options must be a JSON object, got {type(v).__name__}")
    bad = [k for k in _RESERVED_OPTIONS if k in v]
    if bad:
        raise ValueError(f"extraction_request_options cannot set {bad} "
                         "(use extraction_model for the model)")
    return dict(v)


def _timeout_s(v) -> float:
    try:
        t = float(str(v).strip())
    except ValueError:
        t = 0.0
    if not (t > 0 and math.isfinite(t)):
        raise ValueError(f"extraction_timeout_s must be a number of seconds > 0, got {v!r}")
    return t
