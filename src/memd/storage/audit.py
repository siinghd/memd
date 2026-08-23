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
    def __init__(self, store, key: str, envelope=None):
        self.store = store
        self.key = key
        self.envelope = envelope
        self._lock = threading.Lock()
        self._tail_hash = "0" * 64
        self._load_tail()

    # ---------------------------------------------------------------- framing

    def _state_key(self) -> str:
        return self.key + ".state"

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
                            "bytes": self._size() if size is None else size},
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

    def read(self) -> list[dict]:
        data = self._decode(self.store.get(self.key) or b"")
        out = []
        for line in data.decode(errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(e, dict) and "action" in e:
                out.append(e)
        return out

    def verify(self) -> bool:
        prev = "0" * 64
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
                self.store.append(self.key, blob)
                self._checkpoint()
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
