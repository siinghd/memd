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
import re
import secrets
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from memd.core.schema import ExtractorInfo, MemoryRecord, Source

PROMPT_VERSION = "v2"
DEFAULT_MAX_TOKENS = 4096   # output tokens per extraction call
DEFAULT_TIMEOUT_S = 120.0   # an extraction call is cut off after this (LLMExtractor._complete)
DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024  # a reply larger than this is refused unread


@dataclass
class ExtractedFact:
    content: str
    entity_keys: list[str] = field(default_factory=list)
    lineage: list[str] = field(default_factory=list)
    # the extractor that made it, when not the configured one (a failed LLM
    # call's turns go through the pattern extractor)
    extractor: ExtractorInfo | None = None


class Extraction(list):
    """The facts of one extract() call, plus `errors`: the reason of each
    provider call that failed, and `failed_records`: the turns of those
    calls (they went through the pattern extractor instead). An extractor
    that returns a plain list had none."""

    def __init__(self, facts=(), errors=()):
        super().__init__(facts)
        self.errors: list[str] = list(errors)
        self.failed_records = 0


class ExtractionError(Exception):
    """A provider reply with no usable facts; `reason` is a metric label."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


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
    prompt_version: str = "v1"

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
Each turn is one line: [<turn id>] <time, UTC> <speaker>: <text>. A line break inside a turn's text is written as \\n.
The speaker is user (the person), assistant or agent (the AI agent), system (instructions to the agent) or tool (a tool's output).
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
                 request_options: dict | str | None = None,
                 max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES):
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
        # a 120 MB reply cost 360 MB of memory to read and parse
        self.max_response_bytes = int(max_response_bytes)
        # merged into every request body last; a None value removes a field
        self.request_options = _request_options(request_options or {})
        self.chunks_sent = 0
        # each call gets its own client (see _complete), sharing one TLS
        # context; tests swap the transport
        self._verify = httpx.create_ssl_context()
        self._transport = None
        self._fallback = HeuristicExtractor()

    def _chunks(self, records: list[MemoryRecord]):
        """Bounded request chunks: a session can carry up to 1000 rows x
        multi-KB contents - sending that as ONE completion explodes token
        cost and gets rejected by every provider. Chunks stop growing at
        whichever bound hits first (rows or serialized chars)."""
        buf: list[MemoryRecord] = []
        chars = 0
        for r in records:
            n = len(self._turn_text(r)) + _TURN_ID_CHARS
            if buf and (len(buf) >= self.chunk_records or chars + n > self.chunk_chars):
                yield buf
                buf, chars = [], 0
            buf.append(r)
            chars += n
        if buf:
            yield buf

    def extract(self, records: list[MemoryRecord]) -> Extraction:
        """Chunked extraction with per-chunk failure isolation: one bad
        chunk (provider error, timeout, an empty, cut-off or malformed
        reply) degrades THIS chunk only. Its turns go through the pattern
        extractor instead - previously its facts were dropped, a malformed
        reply without even a count - and the failure is counted by reason
        and listed in the result's `errors`. A call is never retried."""
        from memd.metrics import METRICS

        out = Extraction()
        for chunk in self._chunks(records):
            self.chunks_sent += 1
            try:
                out.extend(self._extract_chunk(chunk))
            except Exception as ex:
                reason = _failure_reason(ex)
                out.errors.append(reason)
                out.failed_records += len(chunk)
                METRICS.inc("memd_extraction_chunks_failed_total", model=self.model, reason=reason,
                            help="extraction calls that failed (their turns went through the pattern extractor)")
                made_by = ExtractorInfo(model=self._fallback.name, prompt_version=self._fallback.prompt_version)
                for f in self._fallback.extract(chunk):
                    f.extractor = made_by
                    out.append(f)
        return out

    def _extract_chunk(self, records: list[MemoryRecord]) -> list[ExtractedFact]:
        # turn ids are made up per call (unguessable): a turn's text cannot
        # name another turn, and record ids never leave the engine
        tag = secrets.token_hex(3)
        ids = {f"{tag}-{i}": r.id for i, r in enumerate(records, 1)}
        lines = [f"[{tid}] {self._turn_text(r)}" for tid, r in zip(ids, records)]
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
        reply = self._complete(body)
        try:
            choice = reply["choices"][0]
            text = choice["message"].get("content")
        except (KeyError, IndexError, TypeError, AttributeError):
            raise ExtractionError("malformed", "no choices[0].message in the reply") from None
        if choice.get("finish_reason") == "length":
            # the output cap hit mid-answer, or a reasoning model spent it
            # all on reasoning: whatever JSON there is is cut off
            raise ExtractionError("truncated", f"the reply reached max_tokens ({self.max_tokens})")
        if text is None or (isinstance(text, str) and not text.strip()):
            # a reasoning model can answer with its reasoning only
            raise ExtractionError("empty", "the reply has no content")
        if not isinstance(text, str):
            raise ExtractionError("malformed", f"content is a {type(text).__name__}")
        return self._parse(text, records, ids)

    @staticmethod
    def _speaker(rec: MemoryRecord) -> str:
        from memd.query.rerank import candidate_from_record

        return candidate_from_record(rec)["role"]

    @staticmethod
    def _turn_text(rec: MemoryRecord) -> str:
        """time speaker: text - when and who as the reranker sees a record
        (role from the writer's actor id, else the source tier), on ONE
        line: a line break in the text is written as \\n, so a turn cannot
        start a line of its own (a fake turn, another speaker)."""
        from memd.query.rerank import candidate_from_record

        c = candidate_from_record(rec)
        text = _LINE_BREAKS.sub(lambda _m: "\\n", rec.content)
        return f"{c['date']} {c['role']}: {text}"

    def _complete(self, body: dict) -> dict:
        """One provider call, bounded as a whole by timeout_s: connecting,
        sending, the response headers and the body. Read timeouts alone do
        not bound it - a provider that sends its headers or body a byte at a
        time (OpenRouter keeps a connection alive with whitespace while the
        model generates) resets them with every byte. So the call runs on its
        own thread with its own client: at the deadline the caller gets a
        timeout and the client is closed, which ends the thread at its next
        read (a provider silent that long, by the client's own timeout)."""
        import httpx

        client = httpx.Client(timeout=self.timeout_s, transport=self._transport, verify=self._verify)
        box: dict = {}

        def call() -> None:
            try:
                box["reply"] = self._post(client, body)
            except BaseException as ex:  # handed to the caller below
                box["error"] = ex

        t = threading.Thread(target=call, name="memd-extraction-call", daemon=True)
        t.start()
        t.join(self.timeout_s)
        late = t.is_alive()
        client.close()  # aborts a call still running
        if late:
            raise TimeoutError(f"extraction call exceeded {self.timeout_s:g}s")
        if "error" in box:
            raise box["error"]
        return box["reply"]

    def _post(self, client, body: dict) -> dict:
        buf = bytearray()
        with client.stream("POST", f"{self.base_url}/chat/completions",
                           headers={"Authorization": f"Bearer {self.api_key}"}, json=body) as resp:
            resp.raise_for_status()
            for part in resp.iter_bytes():
                buf += part
                if len(buf) > self.max_response_bytes:
                    raise ExtractionError("oversize", f"the reply exceeded {self.max_response_bytes} bytes")
        return json.loads(bytes(buf))

    def _parse(self, text: str, records: list[MemoryRecord],
               ids: dict[str, str] | None = None) -> list[ExtractedFact]:
        """`ids`: the turn ids the model was shown -> record ids."""
        ids = ids if ids is not None else {r.id: r.id for r in records}
        try:
            start, end = text.find("["), text.rfind("]")
            arr = json.loads(text[start : end + 1])
        except ValueError:
            raise ExtractionError("malformed", "no JSON array in the reply") from None
        # a fact naming no turn of this chunk is traced to the chunk's first
        # user turn (else its first turn) - never to a turn outside the
        # chunk, which is what the session-wide default would pick
        default_lineage = [next((r.id for r in records if self._speaker(r) == "user"), records[0].id)]
        out = []
        malformed = 0
        for item in arr:
            # one bad item is dropped and counted; the reply's other facts stay
            content = item.get("content") if isinstance(item, dict) else None
            ekeys = _str_list(item.get("entity_keys")) if isinstance(item, dict) else None
            lin = _str_list(item.get("lineage")) if isinstance(item, dict) else None
            if not isinstance(content, str) or not content.strip() or ekeys is None or lin is None:
                malformed += 1
                continue
            ekeys = [normalize_entity_key(k) for k in ekeys if k.strip()]
            lin = [ids[x] for x in lin if x in ids]
            out.append(ExtractedFact(content=content.strip(), entity_keys=ekeys, lineage=lin or default_lineage))
        if malformed:
            from memd.metrics import METRICS

            METRICS.inc("memd_extraction_items_malformed_total", malformed, model=self.model,
                        help="items of an extraction reply dropped as malformed (the reply's other facts kept)")
        return out


# every line boundary str.splitlines() knows (models split on them too)
_LINE_BREAKS = re.compile(r"\r\n|[\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029]")
_TURN_ID_CHARS = 16  # "[a1b2c3-40] " and the line break, as chunk_chars counts a line


def _str_list(v) -> list[str] | None:
    """A reply's list-of-strings field: null is empty, one string is one
    entry; anything else is None (malformed)."""
    if v is None:
        return []
    if isinstance(v, str):
        return [v]
    if isinstance(v, list) and all(isinstance(x, str) for x in v):
        return v
    return None


def _failure_reason(exc: BaseException) -> str:
    """Bounded label set for memd_extraction_chunks_failed_total{reason}."""
    import httpx

    if isinstance(exc, ExtractionError):
        return exc.reason
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return "timeout"
    if isinstance(exc, httpx.HTTPStatusError):
        return "http_status"
    if isinstance(exc, httpx.TransportError):
        return "transport"
    if isinstance(exc, ValueError):
        return "malformed"  # the reply body is not JSON
    return "error"


def resolve_extractor(config: dict | None = None) -> Extractor:
    """Settings come from config, else MEMD_<SETTING> in the environment (a
    config value that is not None wins: extraction_api_key="" turns an env
    key off)."""
    from memd.pipeline.embedder import config_or_env

    cfg = config or {}
    api_key = config_or_env(cfg, "extraction_api_key")
    if api_key:
        return LLMExtractor(
            model=config_or_env(cfg, "extraction_model", "gpt-4o-mini"),
            api_key=api_key,
            base_url=config_or_env(cfg, "extraction_base_url", "https://api.openai.com/v1"),
            max_tokens=_max_tokens(config_or_env(cfg, "extraction_max_tokens", DEFAULT_MAX_TOKENS)),
            timeout_s=_timeout_s(config_or_env(cfg, "extraction_timeout_s", DEFAULT_TIMEOUT_S)),
            request_options=config_or_env(cfg, "extraction_request_options"),
            max_response_bytes=_positive_int("extraction_max_response_bytes", config_or_env(
                cfg, "extraction_max_response_bytes", DEFAULT_MAX_RESPONSE_BYTES)),
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


def _positive_int(name: str, v) -> int:
    try:
        n = int(str(v).strip())
    except ValueError:
        n = 0
    if n <= 0:
        raise ValueError(f"{name} must be a whole number > 0, got {v!r}")
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
