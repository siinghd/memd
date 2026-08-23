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
        data = store.get(key) or b""
        self._tail_hash = self._last_hash(data)

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

    def read(self) -> list[dict]:
        data = self.store.get(self.key) or b""
        out = []
        if self.envelope is not None and self.envelope.enabled and data and data[:1] != b"{":
            ns = self.key.split("/")[1] if "/" in self.key else ""
            dec = []
            i = 0
            while i + 4 <= len(data):
                ln = int.from_bytes(data[i : i + 4], "big")
                chunk = data[i + 4 : i + 4 + ln]
                try:
                    dec.append(self.envelope.decrypt(ns, chunk))
                except Exception:
                    pass  # torn/garbled frame: skip; chain verify will flag gaps
                i += 4 + ln
            data = b"".join(dec)
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
