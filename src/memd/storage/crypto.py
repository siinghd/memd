"""Per-namespace envelope encryption (D7 control #9).

Namespace = key scope. Every namespace has its own 256-bit data key (DEK);
the DEK is stored only WRAPPED by a root key the process does not keep on
disk next to the data. Deleting a namespace destroys its wrapped DEK:
crypto-shred.

Who holds the root key is a `KeyProvider` (ADR-12):

  local          a root key FILE beside the data (`<data>/keys/root.key`);
                 wrapped DEKs are files too. Today's behaviour and the
                 default - one machine can decrypt, nobody else.
  aws-kms        AWS KMS: the CMK never leaves KMS; DEKs are wrapped with
                 Encrypt/GenerateDataKey under an encryption context that
                 names the namespace.
  vault-transit  HashiCorp Vault's transit engine (plain HTTP via httpx),
                 the namespace bound as associated data.

With a remote provider the wrapped DEKs live as OBJECTS in the namespace's
own object store (`keys/<ns>.dek`), so ANY node that can reach the bucket and
is authorised on the provider can open ANY namespace - the precondition for
multi-node serving. See ObjectStoreKeyEnvelope.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_LEN = 32
_log = logging.getLogger(__name__)

PROVIDERS = ("local", "aws-kms", "vault-transit")
# object-store layout for wrapped keys (remote providers)
KEYS_PREFIX = "keys"
CUSTODY_KEY = f"{KEYS_PREFIX}/_custody.json"


class KeyCustodyError(RuntimeError):
    """A namespace's data key is held somewhere this process is not
    configured for (or not at all while the namespace has data). Minting a
    fresh key instead would make the existing ciphertext unreadable, so the
    open is refused with a message saying what to do."""


class KeyUnavailableError(RuntimeError):
    """The key provider refused or failed to wrap/unwrap (access denied,
    key disabled or pending deletion, provider unreachable)."""


# --------------------------------------------------------------- key providers


@dataclass(frozen=True)
class WrappedKey:
    """A data key as the provider returned it, plus what is needed to unwrap
    it again and to tell which root key (and version) it is under."""

    provider: str
    key_id: str          # the wrapping key: root fingerprint / KMS key ARN / Vault key name
    key_version: str     # "" when the provider does not expose one (KMS rotates transparently)
    ciphertext: bytes

    def to_record(self, namespace: str) -> dict:
        return {"v": 1, "namespace": namespace, "provider": self.provider,
                "key_id": self.key_id, "key_version": self.key_version,
                "wrapped": base64.b64encode(self.ciphertext).decode(),
                "created_ms": int(time.time() * 1000)}

    @classmethod
    def from_record(cls, rec: dict) -> "WrappedKey":
        return cls(provider=str(rec["provider"]), key_id=str(rec.get("key_id", "")),
                   key_version=str(rec.get("key_version", "")),
                   ciphertext=base64.b64decode(rec["wrapped"]))


class KeyProvider:
    """Wraps and unwraps per-namespace data keys under a root key the
    provider holds.

    `namespace` is passed to every call and is bound into the wrapping (AAD /
    encryption context), so a wrapped key copied under another namespace's
    name does not unwrap. `destroy()` is the provider-side half of a
    crypto-shred - what it can do depends on whether the root key is shared
    by every namespace (the usual case: then it does nothing, and the shred
    is the deletion of the wrapped key) or dedicated to one."""

    name = "abstract"

    def key_id(self, namespace: str) -> str:
        raise NotImplementedError

    def wrap(self, namespace: str, data_key: bytes) -> WrappedKey:
        raise NotImplementedError

    def unwrap(self, namespace: str, wrapped: WrappedKey) -> bytes:
        raise NotImplementedError

    def generate(self, namespace: str) -> tuple[bytes, WrappedKey]:
        """A fresh data key, plaintext and wrapped."""
        dk = secrets.token_bytes(KEY_LEN)
        return dk, self.wrap(namespace, dk)

    def rewrap(self, namespace: str, wrapped: WrappedKey) -> WrappedKey:
        """The same data key under the provider's CURRENT root key version
        (rotation). The data key itself never changes, so no data is
        re-encrypted."""
        return self.wrap(namespace, self.unwrap(namespace, wrapped))

    def destroy(self, namespace: str) -> dict:
        """Provider-side shred step for `namespace`; returns what was done,
        for the audit log. Nothing, unless the root key is per-namespace."""
        return {"provider_action": "none"}

    def describe(self) -> dict:
        return {"provider": self.name}


class LocalKeyProvider(KeyProvider):
    """Root key held in process memory (loaded from a file by the caller).
    Wrap format: 12-byte nonce || AES-256-GCM(root, dk, aad=namespace) -
    byte-identical to what LocalKeyEnvelope has always written."""

    name = "local"

    def __init__(self, root_key: bytes):
        if len(root_key) != KEY_LEN:
            raise ValueError("local root key must be 32 bytes")
        self._root = root_key
        self._fp = "local:" + hashlib.sha256(root_key).hexdigest()[:16]

    def key_id(self, namespace: str) -> str:
        return self._fp

    def wrap(self, namespace: str, data_key: bytes) -> WrappedKey:
        nonce = secrets.token_bytes(12)
        ct = nonce + AESGCM(self._root).encrypt(nonce, data_key, namespace.encode())
        return WrappedKey(self.name, self._fp, "1", ct)

    def unwrap(self, namespace: str, wrapped: WrappedKey) -> bytes:
        ct = wrapped.ciphertext
        return AESGCM(self._root).decrypt(ct[:12], ct[12:], namespace.encode())


def _per_namespace(template: str) -> bool:
    return "{namespace}" in template


class AwsKmsProvider(KeyProvider):
    """AWS KMS (or anything speaking its API: LocalStack, moto).

    `key_id` is a key ARN, key id or alias. It may contain `{namespace}`
    (e.g. `alias/memd-{namespace}`) to give every namespace its OWN CMK -
    operator-provisioned, at KMS's per-key price.

    Crypto-shred, honestly: with ONE shared CMK (the usual deployment) the
    CMK cannot be deleted for one tenant, so the shred is the deletion of
    that namespace's wrapped data key object. Anyone holding a COPY of it
    (a bucket backup, a replica) and kms:Decrypt on the CMK can still unwrap
    it - see SECURITY.md. `shred="disable"` / `"schedule-deletion"` act on
    the CMK itself and are therefore only accepted for a per-namespace
    template; they close that gap for tenants who pay for their own key.
    """

    name = "aws-kms"
    SHRED_ACTIONS = ("none", "disable", "schedule-deletion")

    def __init__(self, key_id: str, *, region: str | None = None, endpoint_url: str | None = None,
                 client: Any = None, shred: str = "none", pending_window_days: int = 7):
        if not key_id:
            raise ValueError("aws-kms needs a key id (MEMD_KMS_KEY_ID: ARN, key id or alias)")
        if shred not in self.SHRED_ACTIONS:
            raise ValueError(f"unknown kms shred action {shred!r}; expected one of {self.SHRED_ACTIONS}")
        if shred != "none" and not _per_namespace(key_id):
            raise ValueError(
                f"kms shred={shred!r} acts on the CMK itself, and {key_id!r} is SHARED by every "
                "namespace - disabling or deleting it would destroy all of them. Use a "
                "per-namespace key template (e.g. alias/memd-{namespace}) or shred='none'")
        self._template = key_id
        self.shred_action = shred
        self.pending_window_days = int(pending_window_days)
        if client is None:
            try:
                import boto3
                from botocore.config import Config
            except ImportError as ex:  # pragma: no cover - dependency guard
                raise RuntimeError("the aws-kms key provider needs boto3: pip install 'memd[s3]'") from ex
            client = boto3.client("kms", region_name=region, endpoint_url=endpoint_url,
                                  config=Config(retries={"max_attempts": 5, "mode": "standard"}))
        self._kms = client

    def key_id(self, namespace: str) -> str:
        return self._template.format(namespace=namespace) if _per_namespace(self._template) \
            else self._template

    @staticmethod
    def _ctx(namespace: str) -> dict:
        return {"memd:namespace": namespace}

    def _call(self, what: str, fn, **kw):
        try:
            return fn(**kw)
        except Exception as ex:
            code = getattr(ex, "response", {}).get("Error", {}).get("Code", "")
            raise KeyUnavailableError(f"aws-kms {what} failed ({code or type(ex).__name__}): {ex}") from ex

    def generate(self, namespace: str) -> tuple[bytes, WrappedKey]:
        r = self._call("GenerateDataKey", self._kms.generate_data_key, KeyId=self.key_id(namespace),
                       KeySpec="AES_256", EncryptionContext=self._ctx(namespace))
        return r["Plaintext"], WrappedKey(self.name, r["KeyId"], "", r["CiphertextBlob"])

    def wrap(self, namespace: str, data_key: bytes) -> WrappedKey:
        r = self._call("Encrypt", self._kms.encrypt, KeyId=self.key_id(namespace),
                       Plaintext=data_key, EncryptionContext=self._ctx(namespace))
        return WrappedKey(self.name, r["KeyId"], "", r["CiphertextBlob"])

    def unwrap(self, namespace: str, wrapped: WrappedKey) -> bytes:
        # KeyId pins the CMK: a blob wrapped under some OTHER key this
        # principal can use is refused instead of silently accepted
        r = self._call("Decrypt", self._kms.decrypt, CiphertextBlob=wrapped.ciphertext,
                       KeyId=wrapped.key_id or self.key_id(namespace),
                       EncryptionContext=self._ctx(namespace))
        return r["Plaintext"]

    def rewrap(self, namespace: str, wrapped: WrappedKey) -> WrappedKey:
        # server side: the plaintext key never reaches this process
        r = self._call("ReEncrypt", self._kms.re_encrypt, CiphertextBlob=wrapped.ciphertext,
                       SourceKeyId=wrapped.key_id or self.key_id(namespace),
                       SourceEncryptionContext=self._ctx(namespace),
                       DestinationKeyId=self.key_id(namespace),
                       DestinationEncryptionContext=self._ctx(namespace))
        return WrappedKey(self.name, r["KeyId"], "", r["CiphertextBlob"])

    def destroy(self, namespace: str) -> dict:
        if self.shred_action == "none":
            return {"provider_action": "none", "note": "shared CMK: the wrapped data key was the shred"}
        # DisableKey / ScheduleKeyDeletion take a key id or ARN, not an alias
        kid = self._call("DescribeKey", self._kms.describe_key,
                         KeyId=self.key_id(namespace))["KeyMetadata"]["KeyId"]
        if self.shred_action == "disable":
            self._call("DisableKey", self._kms.disable_key, KeyId=kid)
            return {"provider_action": "kms_disable_key", "kms_key": kid}
        r = self._call("ScheduleKeyDeletion", self._kms.schedule_key_deletion, KeyId=kid,
                       PendingWindowInDays=self.pending_window_days)
        return {"provider_action": "kms_schedule_key_deletion", "kms_key": kid,
                "deletion_date": str(r.get("DeletionDate", ""))}

    def describe(self) -> dict:
        return {"provider": self.name, "key_id": self._template, "shred": self.shred_action}


class VaultTransitProvider(KeyProvider):
    """HashiCorp Vault transit engine over its HTTP API (httpx; no hvac).

    The data key is generated locally and wrapped with `transit/encrypt`,
    the namespace passed as `associated_data` (Vault >= 1.14 for AES-GCM
    keys): a ciphertext moved under another namespace's name fails to
    decrypt. Ciphertexts carry the key version (`vault:v3:...`), so rotating
    the transit key and running `memd keys rotate` moves every DEK to the
    newest version (decrypt + encrypt: `transit/rewrap` does not take
    associated_data, so it cannot be used on bound ciphertexts).

    `key` may contain `{namespace}` for a key per namespace; only then may
    `shred="delete-key"` delete that transit key (which also needs
    `deletion_allowed`, set here right before the delete)."""

    name = "vault-transit"
    SHRED_ACTIONS = ("none", "delete-key")

    def __init__(self, addr: str, token: str, key: str = "memd", *, mount: str = "transit",
                 vault_namespace: str | None = None, shred: str = "none", verify: Any = True,
                 timeout_s: float = 10.0, client: Any = None):
        if not addr or not token:
            raise ValueError("vault-transit needs VAULT_ADDR and VAULT_TOKEN")
        if shred not in self.SHRED_ACTIONS:
            raise ValueError(f"unknown vault shred action {shred!r}; expected one of {self.SHRED_ACTIONS}")
        if shred != "none" and not _per_namespace(key):
            raise ValueError(f"vault shred={shred!r} deletes the transit key, and {key!r} is SHARED "
                             "by every namespace; use a per-namespace key name (memd-{namespace})")
        self._addr = addr.rstrip("/")
        self._token = token
        self._template = key
        self._mount = mount.strip("/")
        self.shred_action = shred
        headers = {"X-Vault-Token": token}
        if vault_namespace:
            headers["X-Vault-Namespace"] = vault_namespace
        import httpx

        self._http = client or httpx.Client(headers=headers, timeout=timeout_s, verify=verify)
        if client is not None:
            self._http.headers.update(headers)

    def key_id(self, namespace: str) -> str:
        return self._template.format(namespace=namespace) if _per_namespace(self._template) \
            else self._template

    def _req(self, method: str, path: str, body: dict | None = None) -> dict:
        url = f"{self._addr}/v1/{self._mount}/{path}"
        try:
            r = self._http.request(method, url, json=body)
        except Exception as ex:
            raise KeyUnavailableError(f"vault-transit {path.split('/')[0]} unreachable: "
                                      f"{type(ex).__name__}") from ex
        if r.status_code >= 400:
            try:
                errs = r.json().get("errors")
            except Exception:
                errs = None
            raise KeyUnavailableError(f"vault-transit {path.split('/')[0]} failed "
                                      f"(HTTP {r.status_code}): {errs or r.text[:200]}")
        if r.status_code == 204 or not r.content:
            return {}
        out = r.json()
        warns = out.get("warnings") or []
        if any("associated_data" in str(w) for w in warns):
            # an older Vault ignored the namespace binding: fail closed
            # rather than store keys that any namespace name would unwrap
            raise KeyUnavailableError("vault-transit ignored associated_data (Vault too old, or the "
                                      "transit key type is not AEAD); refusing unbound wrapping")
        return out.get("data") or {}

    @staticmethod
    def _ad(namespace: str) -> str:
        return base64.b64encode(namespace.encode()).decode()

    @staticmethod
    def _version_of(ct: str) -> str:
        parts = ct.split(":")
        return parts[1][1:] if len(parts) >= 3 and parts[1].startswith("v") else ""

    def wrap(self, namespace: str, data_key: bytes) -> WrappedKey:
        key = self.key_id(namespace)
        d = self._req("POST", f"encrypt/{key}", {
            "plaintext": base64.b64encode(data_key).decode(), "associated_data": self._ad(namespace)})
        ct = d["ciphertext"]
        return WrappedKey(self.name, key, self._version_of(ct), ct.encode())

    def unwrap(self, namespace: str, wrapped: WrappedKey) -> bytes:
        key = wrapped.key_id or self.key_id(namespace)
        d = self._req("POST", f"decrypt/{key}", {
            "ciphertext": wrapped.ciphertext.decode(), "associated_data": self._ad(namespace)})
        return base64.b64decode(d["plaintext"])

    def destroy(self, namespace: str) -> dict:
        if self.shred_action == "none":
            return {"provider_action": "none", "note": "shared transit key: the wrapped data key was the shred"}
        key = self.key_id(namespace)
        self._req("POST", f"keys/{key}/config", {"deletion_allowed": True})
        self._req("DELETE", f"keys/{key}")
        return {"provider_action": "vault_delete_key", "vault_key": key}

    def describe(self) -> dict:
        return {"provider": self.name, "key_id": self._template, "mount": self._mount,
                "shred": self.shred_action}


# ------------------------------------------------------------------ envelopes


class KeyEnvelope:
    """Provides 256-bit data keys per namespace, wrapped by a root key."""

    enabled = True
    provider_name = "abstract"
    # what the last destroy() did, per namespace (bounded): the audit detail
    # of a crypto-shred (see Memory.destroy_namespace)
    last_destroy: "dict[str, dict]"

    def __init__(self, root_key: bytes | None = None):
        self._root = root_key or self._load_or_create_root()

    @staticmethod
    def _load_or_create_root() -> bytes:
        raise NotImplementedError

    def data_key(self, namespace: str) -> bytes:
        raise NotImplementedError

    def has_key(self, namespace: str) -> bool:
        """True if `namespace` already has a data key. data_key() MINTS one
        otherwise - a reader that must not create keys asks this first."""
        return True

    def destroy(self, namespace: str) -> bool:
        raise NotImplementedError

    def destroy_report(self, namespace: str) -> dict:
        return dict(getattr(self, "last_destroy", {}).get(namespace) or {})

    def _note_destroy(self, namespace: str, detail: dict) -> None:
        if not hasattr(self, "last_destroy"):
            self.last_destroy = {}
        self.last_destroy[namespace] = detail
        while len(self.last_destroy) > 1024:
            self.last_destroy.pop(next(iter(self.last_destroy)))

    def encrypt(self, namespace: str, plaintext: bytes) -> bytes:
        key = self.data_key(namespace)
        nonce = secrets.token_bytes(12)
        return nonce + AESGCM(key).encrypt(nonce, plaintext, namespace.encode())

    def decrypt(self, namespace: str, blob: bytes) -> bytes:
        key = self.data_key(namespace)
        return AESGCM(key).decrypt(blob[:12], blob[12:], namespace.encode())


class LocalKeyEnvelope(KeyEnvelope):
    """Root key + per-namespace wrapped keys under {root}/keys/.

    Threat model (documented honestly per D7): protects data at rest on the
    volume and makes crypto-shred possible; not protection against a user
    with full filesystem read access on the same machine.
    """

    provider_name = "local"

    def __init__(self, dir_path: str, root_key: bytes | None = None):
        self.dir = dir_path
        os.makedirs(dir_path, exist_ok=True)
        # unwrapped per-namespace data keys, LRU-bounded. Caching is sound:
        # every encrypt/decrypt needs the raw key in process memory anyway.
        # Without this, each WAL append/replay frame paid a file read +
        # AESGCM unwrap for the SAME namespace key.
        self._cache: "OrderedDict[str, bytes]" = OrderedDict()
        self.CACHE_MAX = 1024  # namespaces; matches hosted per-node open set
        rk_path = os.path.join(dir_path, "root.key")
        if root_key is not None:
            self._root = root_key
            if not os.path.exists(rk_path):
                self._write_secret(rk_path, root_key)
        elif os.path.exists(rk_path):
            self._root = self._read_secret(rk_path)
        else:
            self._root = secrets.token_bytes(KEY_LEN)
            self._write_secret(rk_path, self._root)
        self.provider = LocalKeyProvider(self._root)

    @staticmethod
    def _read_secret(path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    @staticmethod
    def _write_secret(path: str, data: bytes) -> None:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)

    def _key_path(self, namespace: str) -> str:
        return legacy_key_path(self.dir, namespace)

    def _unwrap_file(self, namespace: str, path: str) -> bytes:
        return self.provider.unwrap(namespace, WrappedKey("local", "", "1", self._read_secret(path)))

    def data_key(self, namespace: str) -> bytes:
        cached = self._cache.get(namespace)
        if cached is not None:
            self._cache.move_to_end(namespace)
            return cached
        p = self._key_path(namespace)
        if os.path.exists(p):
            dk = self._unwrap_file(namespace, p)
        else:
            dk, wk = self.provider.generate(namespace)
            try:
                self._write_secret(p, wk.ciphertext)
            except FileExistsError:
                # concurrent creator won; use their key deterministically
                dk = self._unwrap_file(namespace, p)
        self._cache[namespace] = dk
        if len(self._cache) > self.CACHE_MAX:
            self._cache.popitem(last=False)
        return dk

    def peek_data_key(self, namespace: str) -> bytes | None:
        """The namespace's key if it has one - never mints (key migration)."""
        if namespace in self._cache:
            return self._cache[namespace]
        p = self._key_path(namespace)
        return self._unwrap_file(namespace, p) if os.path.exists(p) else None

    def namespaces(self) -> list[str]:
        """Namespaces with a wrapped key file here."""
        out = []
        for fn in sorted(os.listdir(self.dir)):
            if fn.startswith("ns-") and fn.endswith(".key"):
                out.append(fn[3:-4].replace("__", "/"))
        return out

    def has_key(self, namespace: str) -> bool:
        return namespace in self._cache or os.path.exists(self._key_path(namespace))

    def destroy(self, namespace: str) -> bool:
        # shred order matters: drop the cached copy BEFORE the file so no
        # encrypt() can re-adopt a destroyed namespace's key from RAM
        had_cached = self._cache.pop(namespace, None) is not None
        p = self._key_path(namespace)
        existed = os.path.exists(p)
        if existed:
            _overwrite_unlink(p)
        self._note_destroy(namespace, {"provider": "local", "wrapped_key_deleted": existed,
                                       "provider_action": "none"})
        return existed or had_cached


def legacy_key_path(dir_path: str, namespace: str) -> str:
    safe = namespace.replace("/", "__")
    return os.path.join(dir_path, f"ns-{safe}.key")


def _overwrite_unlink(p: str) -> None:
    # overwrite before unlink so shred survives lazy fs behavior
    size = os.path.getsize(p)
    with open(p, "wb") as f:
        f.write(secrets.token_bytes(size))
        f.flush()
        os.fsync(f.fileno())
    os.unlink(p)


def wrapped_key_object(namespace: str) -> str:
    return f"{KEYS_PREFIX}/{namespace}.dek"


class ObjectStoreKeyEnvelope(KeyEnvelope):
    """Data keys wrapped by a remote KeyProvider, stored as objects in the
    object store (`keys/<ns>.dek`, a small JSON record).

    The bucket then holds everything a node needs EXCEPT the root key: any
    node with bucket access AND provider authorisation unwraps the DEK and
    serves the namespace. Neither alone is enough - the bucket holds only
    ciphertext and wrapped keys, the provider holds no data.

    Minting is guarded. A missing wrapped key for a namespace that already
    HAS data means the key lives elsewhere (a `local` key file this node
    does not have, a different provider) - minting a fresh one would write
    new data under a key that can never read the old, and the replay would
    treat the old frames as garbage. So that open is refused
    (KeyCustodyError) and says to run `memd keys migrate`.
    """

    # NamespaceStore resolves the key at open, before it writes a manifest
    # (see _guard_mint: "has data" is judged by the manifest)
    prefetch_at_open = True

    def __init__(self, provider: KeyProvider, store: Any, *, legacy_dir: str | None = None,
                 allow_mint_existing: bool = False):
        self.provider = provider
        self.provider_name = provider.name
        self.store = store
        self.legacy_dir = legacy_dir
        self.allow_mint_existing = allow_mint_existing
        self._cache: "OrderedDict[str, bytes]" = OrderedDict()
        self.CACHE_MAX = 1024
        self._mint_lock = threading.Lock()
        self._custody_written = False

    # the plaintext DEK cache is as in LocalKeyEnvelope: a DEK has to be in
    # process memory to encrypt anyway

    def _remember(self, namespace: str, dk: bytes) -> bytes:
        self._cache[namespace] = dk
        self._cache.move_to_end(namespace)
        while len(self._cache) > self.CACHE_MAX:
            self._cache.popitem(last=False)
        return dk

    def read_record(self, namespace: str) -> dict | None:
        raw = self.store.get(wrapped_key_object(namespace))
        if not raw:
            return None
        rec = json.loads(raw.decode())
        if rec.get("namespace") != namespace:
            # a record copied in from another namespace (its wrapping is
            # bound to the OTHER name and would not unwrap anyway)
            raise KeyCustodyError(f"wrapped key object for {namespace!r} names "
                                  f"{rec.get('namespace')!r}; refusing it")
        return rec

    def _unwrap_record(self, namespace: str, rec: dict) -> bytes:
        if rec.get("provider") != self.provider.name:
            raise KeyCustodyError(
                f"namespace {namespace!r}'s data key is wrapped by {rec.get('provider')!r} but this "
                f"process is configured for {self.provider.name!r} (MEMD_KEY_PROVIDER)")
        return self.provider.unwrap(namespace, WrappedKey.from_record(rec))

    def data_key(self, namespace: str) -> bytes:
        cached = self._cache.get(namespace)
        if cached is not None:
            self._cache.move_to_end(namespace)
            return cached
        with self._mint_lock:
            cached = self._cache.get(namespace)
            if cached is not None:
                return cached
            rec = self.read_record(namespace)
            if rec is not None:
                return self._remember(namespace, self._unwrap_record(namespace, rec))
            self._guard_mint(namespace)
            dk, wk = self.provider.generate(namespace)
            body = json.dumps(wk.to_record(namespace), sort_keys=True).encode()
            if not self.store.put_if_absent(wrapped_key_object(namespace), body):
                # another node minted it first: theirs is THE key
                rec = self.read_record(namespace)
                if rec is None:
                    raise KeyCustodyError(f"wrapped key for {namespace!r} vanished while minting")
                dk = self._unwrap_record(namespace, rec)
            self._mark_custody()
            return self._remember(namespace, dk)

    def _guard_mint(self, namespace: str) -> None:
        if self.legacy_dir and os.path.exists(legacy_key_path(self.legacy_dir, namespace)):
            raise KeyCustodyError(
                f"namespace {namespace!r} has a LOCAL data key ({legacy_key_path(self.legacy_dir, namespace)}) "
                f"that is not wrapped under {self.provider.name!r} yet: run `memd keys migrate --to "
                f"{self.provider.name}` on this node")
        if not self.allow_mint_existing and self.store.exists(f"ns/{namespace}/manifest.json"):
            raise KeyCustodyError(
                f"namespace {namespace!r} has data but no data key under {self.provider.name!r} - its key "
                "is held elsewhere (a `local` key file on the node that created it?). Run `memd keys "
                "migrate` there; minting a new key here would make the existing data unreadable")

    def _mark_custody(self) -> None:
        if self._custody_written:
            return
        try:
            write_custody(self.store, self.provider)
        except Exception:  # noqa: BLE001 - the marker is a guard rail, not data
            _log.warning("memd: could not write the key-custody marker", exc_info=True)
        self._custody_written = True

    def peek_data_key(self, namespace: str) -> bytes | None:
        if namespace in self._cache:
            return self._cache[namespace]
        rec = self.read_record(namespace)
        return None if rec is None else self._unwrap_record(namespace, rec)

    def has_key(self, namespace: str) -> bool:
        return namespace in self._cache or self.store.exists(wrapped_key_object(namespace))

    def destroy(self, namespace: str) -> bool:
        # the RAM copy first, then every stored copy of the wrapped key, then
        # whatever the provider can do on its side
        had_cached = self._cache.pop(namespace, None) is not None
        key = wrapped_key_object(namespace)
        existed = self.store.exists(key)
        self.store.shred(key)
        detail: dict = {"provider": self.provider.name, "key_id": self.provider.key_id(namespace),
                        "wrapped_key_deleted": existed}
        try:
            detail.update(self.provider.destroy(namespace))
        except Exception as ex:  # noqa: BLE001 - the wrapped key is already gone
            from memd.metrics import METRICS

            METRICS.inc("memd_key_provider_destroy_failures_total",
                        help="provider-side crypto-shred steps that failed (the wrapped key was deleted)")
            detail["provider_error"] = f"{type(ex).__name__}: {ex}"[:300]
            _log.error("memd: provider-side shred of %r failed (wrapped key already deleted): %s",
                       namespace, ex)
        self._note_destroy(namespace, detail)
        return existed or had_cached

    def rewrap(self, namespace: str) -> dict | None:
        """Re-wrap this namespace's DEK under the provider's current root key
        version (`memd keys rotate`). The DEK is unchanged; overwriting the
        one record is atomic, and old and new record unwrap to the same key."""
        rec = self.read_record(namespace)
        if rec is None:
            return None
        if rec.get("provider") != self.provider.name:
            raise KeyCustodyError(f"{namespace!r} is wrapped by {rec.get('provider')!r}, not "
                                  f"{self.provider.name!r}")
        old = WrappedKey.from_record(rec)
        new = self.provider.rewrap(namespace, old)
        if self.provider.unwrap(namespace, new) != self.provider.unwrap(namespace, old):
            raise KeyCustodyError(f"rewrap of {namespace!r} did not round-trip; record left unchanged")
        self.store.put(wrapped_key_object(namespace),
                       json.dumps(new.to_record(namespace), sort_keys=True).encode())
        return {"namespace": namespace, "from": [old.key_id, old.key_version],
                "to": [new.key_id, new.key_version]}


class NullKeyEnvelope(KeyEnvelope):
    """No encryption at rest (testing / explicit opt-out)."""

    enabled = False
    provider_name = "none"

    def __init__(self) -> None:
        self._keys: dict[str, bytes] = {}

    def encrypt(self, namespace: str, plaintext: bytes) -> bytes:
        return plaintext

    def decrypt(self, namespace: str, blob: bytes) -> bytes:
        return blob

    def data_key(self, namespace: str) -> bytes:
        if namespace not in self._keys:
            self._keys[namespace] = b"\x00" * KEY_LEN
        return self._keys[namespace]

    def destroy(self, namespace: str) -> bool:
        return self._keys.pop(namespace, None) is not None


# ------------------------------------------------------------ configuration


def _opt(cfg: dict, key: str, env: str, default: Any = None) -> Any:
    v = cfg.get(key)
    if v is None or v == "":
        v = os.environ.get(env)
    return default if v is None or v == "" else v


def resolve_key_provider_name(cfg: dict | None = None) -> str:
    name = str(_opt(cfg or {}, "key_provider", "MEMD_KEY_PROVIDER", "local")).strip().lower()
    if name not in PROVIDERS:
        raise ValueError(f"unknown key provider {name!r}; expected one of {list(PROVIDERS)}")
    return name


def provider_from_config(name: str, cfg: dict | None = None) -> KeyProvider:
    """A remote KeyProvider from config keys / environment:

    aws-kms:       kms_key_id / MEMD_KMS_KEY_ID (ARN, id or alias; may hold
                   {namespace}), kms_region / MEMD_KMS_REGION / AWS_REGION,
                   kms_endpoint_url / MEMD_KMS_ENDPOINT, kms_shred /
                   MEMD_KMS_SHRED (none | disable | schedule-deletion)
    vault-transit: vault_addr / VAULT_ADDR, vault_token / VAULT_TOKEN,
                   vault_transit_key / MEMD_VAULT_TRANSIT_KEY (default memd),
                   vault_transit_mount / MEMD_VAULT_TRANSIT_MOUNT (transit),
                   vault_namespace / VAULT_NAMESPACE, vault_shred /
                   MEMD_VAULT_SHRED (none | delete-key), VAULT_CACERT
    """
    cfg = cfg or {}
    if name == "aws-kms":
        return AwsKmsProvider(
            str(_opt(cfg, "kms_key_id", "MEMD_KMS_KEY_ID", "")),
            region=_opt(cfg, "kms_region", "MEMD_KMS_REGION", os.environ.get("AWS_REGION")),
            endpoint_url=_opt(cfg, "kms_endpoint_url", "MEMD_KMS_ENDPOINT"),
            shred=str(_opt(cfg, "kms_shred", "MEMD_KMS_SHRED", "none")),
            pending_window_days=int(_opt(cfg, "kms_pending_window_days", "MEMD_KMS_PENDING_WINDOW_DAYS", 7)),
            client=cfg.get("kms_client"),
        )
    if name == "vault-transit":
        return VaultTransitProvider(
            str(_opt(cfg, "vault_addr", "VAULT_ADDR", "")),
            str(_opt(cfg, "vault_token", "VAULT_TOKEN", "")),
            str(_opt(cfg, "vault_transit_key", "MEMD_VAULT_TRANSIT_KEY", "memd")),
            mount=str(_opt(cfg, "vault_transit_mount", "MEMD_VAULT_TRANSIT_MOUNT", "transit")),
            vault_namespace=_opt(cfg, "vault_namespace", "VAULT_NAMESPACE"),
            shred=str(_opt(cfg, "vault_shred", "MEMD_VAULT_SHRED", "none")),
            verify=_opt(cfg, "vault_cacert", "VAULT_CACERT", True),
        )
    raise ValueError(f"{name!r} is not a remote key provider")


def read_custody(store: Any) -> dict | None:
    """The store's key-custody marker: which provider wraps its data keys.
    Absent for a store that has only ever used `local` keys."""
    try:
        raw = store.get(CUSTODY_KEY)
    except Exception:  # noqa: BLE001 - unreadable marker: treat as absent
        return None
    if not raw:
        return None
    try:
        return json.loads(raw.decode())
    except ValueError:
        return None


def write_custody(store: Any, provider: KeyProvider) -> None:
    body = json.dumps({"provider": provider.name, **provider.describe(),
                       "since_ms": int(time.time() * 1000)}, sort_keys=True).encode()
    if not store.put_if_absent(CUSTODY_KEY, body):
        cur = read_custody(store) or {}
        if cur.get("provider") != provider.name:
            store.put(CUSTODY_KEY, body)


def envelope_from_config(cfg: dict | None, local_dir: str, store: Any, *,
                         encrypt: bool = True) -> KeyEnvelope:
    """The envelope a Memory opens with.

    `local` (default): LocalKeyEnvelope under `<local_dir>/keys`, exactly as
    before - but refused when the store's custody marker says its keys have
    been moved to a remote provider (a node with the old config would
    otherwise mint fresh keys over data it can no longer read).
    Remote: ObjectStoreKeyEnvelope over `store`, with `<local_dir>/keys` as
    the legacy directory the mint guard checks."""
    if not encrypt:
        return NullKeyEnvelope()
    cfg = cfg or {}
    name = resolve_key_provider_name(cfg)
    keys_dir = os.path.join(local_dir, "keys")
    custody = read_custody(store) if store is not None else None
    if name == "local":
        if custody and custody.get("provider") not in (None, "local"):
            raise KeyCustodyError(
                f"this store's data keys are held by {custody.get('provider')!r} "
                f"(key {custody.get('key_id')!r}); set MEMD_KEY_PROVIDER={custody.get('provider')} "
                "and its settings - the local provider cannot read them")
        return LocalKeyEnvelope(keys_dir)
    if custody and custody.get("provider") != name:
        raise KeyCustodyError(
            f"this store's data keys are held by {custody.get('provider')!r}, not {name!r}")
    allow = str(_opt(cfg, "keys_allow_mint_existing", "MEMD_KEYS_ALLOW_MINT_EXISTING", "")) in ("1", "true")
    return ObjectStoreKeyEnvelope(provider_from_config(name, cfg), store,
                                  legacy_dir=keys_dir, allow_mint_existing=allow)
