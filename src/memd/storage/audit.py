"""Append-only, hash-chained audit log per namespace (D7 control #7).

Nearly free on immutable segments; exportable for SIEM. Each entry commits
to the previous entry's digest: tampering breaks the chain.
"""
from __future__ import annotations

import hashlib
import json
import threading

from memd.core.schema import now_ms


class AuditLog:
    # Ledgers grow ~200 bytes per operation and nothing ever reclaimed them:
    # a namespace serving 1000 req/s accrued ~17GB/day. Sealed segments keep
    # the live object bounded; read()/verify() walk segments then the tail.
    ROTATE_BYTES = 64 * 1024 * 1024
    KEEP_SEGMENTS = 16
    # class-level defaults so read()/verify() work on a hand-built instance
    # (tests construct one via __new__ to feed the chain a forged ledger)
    _segments = 0
    _pruned = 0
    # Hash the RETAINED window chains back to. Retention is bounded, so the
    # oldest surviving entry's `prev` points at a segment that no longer
    # exists; verifying from 0*64 therefore failed forever once the ring
    # engaged (4000 appended -> 800 readable -> verify() False, permanently).
    # Tamper-evidence is a property of the window you still hold, not of
    # history you deliberately discarded.
    _chain_start = "0" * 64
    rotate_bytes = ROTATE_BYTES

    def __init__(self, store, key: str, envelope=None, rotate_bytes: int | None = None):
        self.store = store
        self.key = key
        self.envelope = envelope
        self.rotate_bytes = int(rotate_bytes if rotate_bytes is not None else self.ROTATE_BYTES)
        self._segments = 0
        self._pruned = 0
        self._chain_start = "0" * 64
        self._lock = threading.Lock()
        self._tail_hash = "0" * 64
        self._load_tail()

    # ---------------------------------------------------------------- framing

    def _state_key(self) -> str:
        return self.key + ".state"

    def _seg_key(self, n: int) -> str:
        return f"{self.key}.{n:05d}"

    def _first_prev(self, seg_key: str) -> str | None:
        """`prev` digest of the first entry in a sealed segment."""
        try:
            data = self._decode(self.store.get(seg_key) or b"")
            for line in data.decode(errors="replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                e = json.loads(line)
                if isinstance(e, dict) and isinstance(e.get("prev"), str):
                    return e["prev"]
                return None
        except Exception:
            return None
        return None

    def _maybe_rotate(self, size: int | None = None) -> None:
        """Seal the live ledger into a numbered segment when it grows past
        `rotate_bytes`.

        Ordering is copy-then-truncate, so a crash in between leaves the tail
        present in BOTH the segment and the live object. That is deliberate:
        losing audit entries is worse than seeing one twice, and read()
        de-duplicates on the entry hash, which a hash chain gives us for free.
        """
        try:
            if (self._size() if size is None else size) < self.rotate_bytes:
                return
            self.store.copy(self.key, self._seg_key(self._segments))
            self.store.truncate(self.key, 0)
            self._segments += 1
            if self._segments > self.KEEP_SEGMENTS:
                oldest = self._segments - self.KEEP_SEGMENTS - 1
                # Re-anchor the chain BEFORE dropping the segment: the new
                # oldest surviving entry's `prev` is the digest of the last
                # entry we are about to discard, so it becomes the start of
                # the verifiable window.
                anchor = self._first_prev(self._seg_key(oldest + 1))
                try:
                    self.store.delete(self._seg_key(oldest))
                except Exception:
                    pass
                else:
                    self._pruned += 1
                    if anchor:
                        self._chain_start = anchor
                    from memd.metrics import METRICS

                    METRICS.inc("memd_audit_segments_pruned_total",
                                help="audit segments dropped past the retention bound")
            self._checkpoint(0)
            from memd.metrics import METRICS

            METRICS.inc("memd_audit_rotations_total", help="audit ledger segments sealed")
        except Exception:
            from memd.metrics import METRICS

            METRICS.inc("memd_audit_rotation_failures_total",
                        help="audit ledger rotations that failed (ledger keeps growing)")

    def _ns(self) -> str:
        return self.key.split("/")[1] if "/" in self.key else ""

    def _decode(self, data: bytes) -> bytes:
        """Envelope-framed `[len][ciphertext]...` -> plaintext NDJSON.

        Plaintext ledgers pass through untouched (they start with '{')."""
        if not (self.envelope is not None and self.envelope.enabled and data and data[:1] != b"{"):
            return data
        ns = self._ns()
        dec: list[bytes] = []
        i = 0
        while i + 4 <= len(data):
            ln = int.from_bytes(data[i : i + 4], "big")
            chunk = data[i + 4 : i + 4 + ln]
            try:
                dec.append(self.envelope.decrypt(ns, chunk))
            except Exception:
                pass  # torn/garbled frame: skip; chain verify will flag gaps
            i += 4 + ln
        return b"".join(dec)

    def _size(self) -> int:
        try:
            return int(self.store.size(self.key))
        except Exception:
            return 0

    def _checkpoint(self, size: int | None = None) -> None:
        """Persist the tail digest next to the ledger.

        Two defects motivate this sidecar, both fixed by it:
          1. O(1) open. __init__ used to `store.get(key)` the WHOLE ledger just
             to learn the last hash - 47.9MB / 143.7MB peak RSS at 200K
             entries, paid on every namespace open (cold-start SLO p90<=1.5s).
          2. Chain integrity across restarts. `_last_hash` parsed the object as
             NDJSON, but an ENCRYPTED ledger is `[len][ciphertext]` frames, so
             the parse always failed and the tail silently reset to 0*64. With
             encryption on (the default) every reopen forked the hash chain and
             verify() returned False forever after - a real tamper became
             indistinguishable from a routine restart.
        The checkpoint is written AFTER the data append: a crash in between
        leaves a stale `bytes` and the reader self-heals via the full-read path.
        """
        try:
            put = getattr(self.store, "put_hint", None) or self.store.put
            put(
                self._state_key(),
                json.dumps({"h": self._tail_hash,
                            "bytes": self._size() if size is None else size,
                            "segs": self._segments,
                            "pruned": self._pruned,
                            "chain_start": self._chain_start},
                           separators=(",", ":")).encode(),
            )
        except Exception:
            # Best-effort by construction: the checkpoint is a read-side
            # optimization and the full-read path reproduces it exactly, so a
            # store that cannot put (or an I/O error) must degrade to the slow
            # open - never propagate into the caller's write.
            from memd.metrics import METRICS

            METRICS.inc("memd_audit_checkpoint_failures_total")

    def _load_tail(self) -> str:
        """Sets self._tail_hash; returns it. Self-heals the sidecar on the
        cold path so the expensive full read is paid at most once per ledger."""
        size = self._size()
        raw = None
        try:
            raw = self.store.get(self._state_key())
        except Exception:
            raw = None
        if raw:
            try:
                st = json.loads(raw.decode())
                self._segments = int(st.get("segs", 0) or 0)
                self._pruned = int(st.get("pruned", 0) or 0)
                self._chain_start = str(st.get("chain_start") or "0" * 64)
                if int(st.get("bytes", -1)) == size and isinstance(st.get("h"), str) and st["h"]:
                    self._tail_hash = str(st["h"])  # O(1): no ledger read at all
                    return self._tail_hash
            except Exception:
                pass
        # cold path: sidecar absent (first open / upgrade) or stale (crash
        # between append and checkpoint). Decode properly so an encrypted
        # ledger yields its real tail instead of resetting the chain.
        data = self.store.get(self.key) or b""
        self._tail_hash = self._last_hash(self._decode(data))
        if data:
            self._checkpoint(size)  # heal: the next open is O(1)
        return self._tail_hash

    @staticmethod
    def _last_hash(data: bytes) -> str:
        if not data:
            return "0" * 64
        last_line = data.rstrip(b"\n").rsplit(b"\n", 1)[-1]
        try:
            return json.loads(last_line)["h"]
        except Exception:
            return "0" * 64

    def append(self, actor: str, action: str, target: str, detail: dict | None = None) -> None:
        with self._lock:
            entry = {
                "ts": now_ms(),
                "actor": actor,
                "action": action,
                "target": target,
                "detail": detail or {},
                "prev": self._tail_hash,
            }
            body = json.dumps({k: v for k, v in entry.items() if k != "h"}, sort_keys=True).encode()
            entry["h"] = hashlib.sha256(body).hexdigest()
            payload = json.dumps(entry, separators=(",", ":")).encode() + b"\n"
            if self.envelope is not None and self.envelope.enabled:
                ns = self.key.split("/")[1] if "/" in self.key else ""
                enc = self.envelope.encrypt(ns, payload)
                # frame the CIPHERTEXT length - read() walks [len][ciphertext]
                payload = len(enc).to_bytes(4, "big") + enc
            self.store.append(self.key, payload)
            self._tail_hash = entry["h"]
            self._checkpoint()
            self._maybe_rotate()

    def read(self) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        keys = [self._seg_key(n) for n in range(self._segments)] + [self.key]
        for k in keys:
            try:
                blob = self.store.get(k)
            except Exception:
                blob = None
            if not blob:
                continue
            data = self._decode(blob) if k != self.key else self._decode(blob)
            for line in data.decode(errors="replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not (isinstance(e, dict) and "action" in e):
                    continue
                h = e.get("h")
                if h in seen:
                    continue  # crash window between copy and truncate
                if isinstance(h, str):
                    seen.add(h)
                out.append(e)
        return out

    def verify(self) -> bool:
        """Tamper-evidence over the RETAINED window.

        Retention is bounded (KEEP_SEGMENTS), so verification anchors at
        `_chain_start` - the digest the oldest surviving entry chains back to -
        rather than at zero. Anchoring at zero made verify() permanently False
        the moment the ring engaged, which reads as "tampered" when the truth
        is "we pruned on purpose". `_pruned` records that history was dropped
        so a reader can tell the two apart.
        """
        prev = self._chain_start
        for e in self.read():
            body = json.dumps({k: v for k, v in e.items() if k != "h"}, sort_keys=True).encode()
            if e.get("prev") != prev or e.get("h") != hashlib.sha256(body).hexdigest():
                return False
            prev = e["h"]
        return True


class BufferedAuditLog(AuditLog):
    """Audit writer that batches fsyncs: entries chain in memory and flush
    every `flush_every` entries, on rotate/close/compact, or on read.

    Trade-off (documented honestly for the embedded threat model): a hard
    crash can lose the last few audit ENTRIES while losing no memory data.
    Hosted compliance deployments flip this to synchronous via flush_every=1.
    """

    def __init__(self, store, key: str, envelope=None, flush_every: int = 32):
        super().__init__(store, key, envelope)
        self.flush_every = flush_every
        self._lock = threading.RLock()  # append() nests flush()
        self._buffer: list[bytes] = []
        self._since = 0

    def append(self, actor: str, action: str, target: str, detail: dict | None = None) -> None:
        with self._lock:
            entry = {
                "ts": now_ms(),
                "actor": actor,
                "action": action,
                "target": target,
                "detail": detail or {},
                "prev": self._tail_hash,
            }
            body = json.dumps({k: v for k, v in entry.items() if k != "h"}, sort_keys=True).encode()
            entry["h"] = hashlib.sha256(body).hexdigest()
            payload = json.dumps(entry, separators=(",", ":")).encode() + b"\n"
            if self.envelope is not None and self.envelope.enabled:
                ns = self.key.split("/")[1] if "/" in self.key else ""
                enc = self.envelope.encrypt(ns, payload)
                # frame the CIPHERTEXT length - read() walks [len][ciphertext]
                payload = len(enc).to_bytes(4, "big") + enc
            self._buffer.append(payload)
            self._tail_hash = entry["h"]
            self._since += 1
            if self._since >= self.flush_every:
                self.flush()

    def flush(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            blob = b"".join(self._buffer)
            self._buffer.clear()
            self._since = 0
            try:
                # append() returns the new size, so neither the checkpoint nor
                # the rotate check needs to re-measure it. Letting each call
                # _size() cost two extra round trips per flush on a remote
                # store, for a number we were already handed.
                size = self.store.append(self.key, blob)
                self._checkpoint(size)
                self._maybe_rotate(size)
            except OSError:
                from memd.metrics import METRICS

                METRICS.inc("memd_audit_flush_failures_total")
                # availability over tail-durability in embedded mode (documented
                # trade-off); entries are dropped, chain stays verifiable

    def read(self) -> list[dict]:
        # flush first so buffered entries are included in the parsed result
        # (the previous formulation computed `store.get(...) or b"" + pending`,
        # which bound the pending bytes to the wrong branch and never used them)
        self.flush()
        return super().read()

    def verify(self) -> bool:
        self.flush()
        return super().verify()


def read_verified(store, key: str, envelope=None) -> tuple[list[dict], bool, int]:
    """The ledger at `key` up to the last valid link of its hash chain ->
    (entries, whole chain verified, entries read).

    Read-only, unlike AuditLog(): opening one heals the `.state` sidecar,
    which a reader that must not change what it reads (a migration before its
    commit) cannot afford. The window and its anchor come from the sidecar
    (segments, chain_start) exactly as AuditLog reads them, and an encrypted
    ledger is decoded with the namespace's envelope the same way. The walk
    stops at the first entry whose `prev` is not its predecessor's digest or
    whose own digest does not match its body: nothing past a break is
    returned."""
    log = AuditLog.__new__(AuditLog)
    log.store, log.key, log.envelope = store, key, envelope
    log._segments, log._pruned, log._chain_start = 0, 0, "0" * 64
    try:
        raw = store.get(key + ".state")
        if raw:
            st = json.loads(raw.decode())
            log._segments = int(st.get("segs", 0) or 0)
            log._chain_start = str(st.get("chain_start") or "0" * 64)
    except Exception:  # noqa: BLE001 - no usable sidecar: the live object alone, anchored at zero
        pass
    entries = log.read()
    prev = log._chain_start
    good: list[dict] = []
    for e in entries:
        body = json.dumps({k: v for k, v in e.items() if k != "h"}, sort_keys=True).encode()
        if e.get("prev") != prev or e.get("h") != hashlib.sha256(body).hexdigest():
            return good, False, len(entries)
        good.append(e)
        prev = e["h"]
    return good, True, len(entries)
