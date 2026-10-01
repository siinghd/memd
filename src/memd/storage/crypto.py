"""Per-namespace envelope encryption.

Namespace = key scope. Every namespace has its own 256-bit data key (DEK);
the DEK is stored only WRAPPED by a root key the process does not keep on
disk next to the data. Deleting a namespace destroys its wrapped DEK:
crypto-shred.

Who holds the root key is a `KeyProvider`:

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
import contextlib
import errno
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_LEN = 32
# a `local` wrapped data key: 12-byte nonce || AES-GCM(32-byte key) || 16-byte tag
_WRAPPED_KEY_LEN = 12 + KEY_LEN + 16
# how long a key file read keeps retrying one that is still being written
_SECRET_SETTLE_S = 1.0
_log = logging.getLogger(__name__)
# the label a data key's public fingerprint is taken over (KeyEnvelope.key_check)
_KEY_CHECK_LABEL = b"memd/key-check/v1"

PROVIDERS = ("local", "aws-kms", "vault-transit")
# object-store layout for wrapped keys (remote providers)
KEYS_PREFIX = "keys"
CUSTODY_KEY = f"{KEYS_PREFIX}/_custody.json"


class KeyCustodyError(RuntimeError):
    """A namespace's data key is held somewhere this process is not
    configured for (or not at all while the namespace has data). Minting a
    fresh key instead would make the existing ciphertext unreadable, so the
    open is refused with a message saying what to do.

    Also what a ciphertext that does not authenticate under the key in hand
    raises (never cryptography's bare InvalidTag): a valid key that is not
    the one the data was written with - another deployment's keys directory
    restored over this one, a replaced root key, another deployment's
    wrapped key object under a remote provider - is a custody failure, not
    damage. Storage never skips, truncates or deletes such data."""


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
        try:
            return AESGCM(self._root).decrypt(ct[:12], ct[12:], namespace.encode())
        except InvalidTag:
            # not a torn or corrupt read to retry: the data key was wrapped by
            # another root key (a replaced or mixed-up keys directory)
            raise KeyCustodyError(
                f"the data key of namespace {namespace!r} does not unwrap under this "
                f"local root key ({self._fp}): it was wrapped by a different one. "
                "Restore the root key it was written with") from None


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
            # a transport error (a timeout) has no parsed reply: `response`
            # is missing or None, and must not mask the error itself
            reply = getattr(ex, "response", None)
            code = ((reply.get("Error") or {}).get("Code", "") if isinstance(reply, dict) else "")
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
        try:
            return AESGCM(key).decrypt(blob[:12], blob[12:], namespace.encode())
        except InvalidTag:
            raise KeyCustodyError(
                f"a ciphertext of namespace {namespace!r} does not authenticate under its data "
                "key: the key is not the one it was written with (lost, replaced, restored from "
                "another deployment) or the ciphertext is damaged") from None

    def key_check(self, namespace: str) -> str:
        """A public fingerprint of `namespace`'s data key: HMAC-SHA256 under
        the key of a fixed label, 128 bits. Kept beside the data (the
        namespace manifest), it lets an open tell whether the key in hand is
        the one the data was written with before it reads, repairs or
        rewrites anything. It reveals nothing about the key (a PRF output on
        a public input - the audit chain key is derived the same way, under
        another label). Resolves the key: callers ask has_key() first where
        minting one would be wrong.

        It fingerprints the DATA key, not its wrapping: `memd keys migrate`
        (local -> a remote provider) and `memd keys rotate` re-wrap the same
        data key, so a stamp stays valid across them. Which provider holds
        the wrapping is the store's custody marker (read_custody); the two
        checks are complementary - the marker refuses a node on the wrong
        provider before it can mint anything, the stamp refuses a data key
        that unwraps fine but is not this data's (another deployment's
        wrapped key object restored over this one's)."""
        return hmac.new(self.data_key(namespace), _KEY_CHECK_LABEL, hashlib.sha256).hexdigest()[:32]


class LocalKeyEnvelope(KeyEnvelope):
    """Root key + per-namespace wrapped keys under {root}/keys/ (the `local`
    KeyProvider: LocalKeyProvider over the root key file).

    Threat model (documented honestly): protects data at rest on the
    volume and makes crypto-shred possible; not protection against a user
    with full filesystem read access on the same machine.

    Nothing here guards against minting a key for a namespace that already
    has data under another one (a restore without the keys directory, a
    namespace another node created on a shared bucket): NamespaceStore
    settles key custody at open, before anything is read or written, from
    the manifest's key check or a probe of the data itself (see
    NamespaceStore._verify_key), and resolves a key only once the open is
    past that. A namespace written before encryption was on, with no key
    anywhere, is legitimately keyed on its first encrypted write.
    """

    provider_name = "local"

    def __init__(self, dir_path: str, root_key: bytes | None = None):
        self.dir = dir_path
        # unwrapped per-namespace data keys, LRU-bounded. Caching is sound:
        # every encrypt/decrypt needs the raw key in process memory anyway.
        # Without this, each WAL append/replay frame paid a file read +
        # AESGCM unwrap for the SAME namespace key.
        self._cache: "OrderedDict[str, bytes]" = OrderedDict()
        self.CACHE_MAX = 1024  # namespaces; matches hosted per-node open set
        self._rk_path = os.path.join(dir_path, "root.key")
        # Loaded - or created - on first use, not here: an open that refuses
        # (a restore without the keys directory: see KeyCustodyError) must
        # not leave a fresh root key behind, and neither may a tool that
        # only reads (`memd keys status|migrate`). One is minted only
        # together with the first data key it wraps (data_key).
        self._root_key: bytes | None = root_key
        self._provider: LocalKeyProvider | None = None
        _sweep_shreds(dir_path)
        if root_key is not None and not os.path.exists(self._rk_path):
            try:
                self._write_secret(self._rk_path, root_key)
            except FileExistsError:
                pass  # a concurrent creator won: the file decides (see _root)

    @property
    def _root(self) -> bytes:
        """The root key on disk. Raises KeyCustodyError when there is none:
        a wrapped data key without it cannot be unwrapped, and minting one
        here would not change that."""
        if self._root_key is None:
            if not os.path.exists(self._rk_path):
                raise KeyCustodyError(
                    f"no root key at {self._rk_path}: the data keys under {self.dir} cannot be "
                    "unwrapped without the root key they were wrapped with. Restore it")
            self._root_key = self._read_secret(self._rk_path, KEY_LEN)
        return self._root_key

    def _root_or_mint(self) -> bytes:
        """The root key, minted if this keys directory has none yet (only
        ever to wrap a data key being minted) - or has only the empty file a
        crash left: that is reported (_read_secret), never replaced)."""
        if self._root_key is None and not os.path.exists(self._rk_path):
            fresh = secrets.token_bytes(KEY_LEN)
            try:
                self._write_secret(self._rk_path, fresh)
                self._root_key = fresh
            except FileExistsError:
                pass  # a concurrent creator won; use theirs
        return self._root

    @property
    def provider(self) -> LocalKeyProvider:
        """The `local` KeyProvider over the root key on disk (KeyCustodyError
        without one - see _root). Never mints a root key."""
        if self._provider is None:
            self._provider = LocalKeyProvider(self._root)
        return self._provider

    @staticmethod
    def _read_secret(path: str, size: int = _WRAPPED_KEY_LEN) -> bytes:
        """A key file's bytes. One shorter than `size` - a key file created
        in place (an older build, or _write_secret on a filesystem without
        hard links) is created first and written after, so a concurrent
        reader could catch it empty - is read again for up to
        _SECRET_SETTLE_S. One still short then is a KeyCustodyError naming
        it: it used to be returned as it was, and failed far from the cause
        (an empty wrapped key as "Nonce must be between 8 and 128 bytes",
        an empty root.key as "local root key must be 32 bytes")."""
        deadline = time.monotonic() + _SECRET_SETTLE_S
        while True:
            with open(path, "rb") as f:
                data = f.read()
            if len(data) >= size:
                return data
            if time.monotonic() >= deadline:
                what = "empty" if not data else f"short ({len(data)} of {size} bytes)"
                raise KeyCustodyError(
                    f"key file {path} is {what}: a crash interrupted its creation (a key "
                    "file created in place, on a filesystem without hard links), or it was "
                    "truncated or damaged. memd never replaces a key file, because it cannot "
                    "tell those apart. Restore it from a backup of the keys directory; only "
                    "if you know no data was ever written under it (a creation a crash cut "
                    "short, and for root.key: no ns-*.key file exists), delete it and retry")
            time.sleep(0.01)

    @staticmethod
    def _write_secret(path: str, data: bytes) -> None:
        """Create the key file `path` holding `data`: FileExistsError when it
        exists (a concurrent creator won - its key is the one). It used to
        be created first and written after, and a process creating the
        first key at the same moment read it empty in between. Now it is
        written and fsynced under a temporary name and hard-linked into
        place, which does not replace an existing file: it is never
        visible without all of its bytes."""
        d = os.path.dirname(path) or "."
        os.makedirs(d, exist_ok=True)
        tmp = f"{path}.tmp-{secrets.token_hex(6)}"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            try:
                view = memoryview(data)
                while view:
                    view = view[os.write(fd, view):]
                os.fsync(fd)
            finally:
                os.close(fd)
            try:
                os.link(tmp, path)
            except FileExistsError:
                raise   # a concurrent creator won (not the fallback below)
            except OSError:
                # no hard links here: create it in place (a reader retries
                # one it catches short - see _read_secret). A crash before
                # the write lands leaves it empty: reported as a
                # KeyCustodyError naming it, never replaced. A write that
                # FAILS removes it - it is this call's (O_EXCL), and nothing
                # used the key it was to hold.
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    ours = os.fstat(fd)
                    try:
                        view = memoryview(data)
                        while view:
                            view = view[os.write(fd, view):]
                        os.fsync(fd)
                    except BaseException:
                        if _is_file(path, ours):
                            with contextlib.suppress(OSError):
                                os.unlink(path)
                        raise
                    if not _is_file(path, ours):
                        # this process stalled between the create and the
                        # write for so long that the empty file was taken
                        # for a crashed creation and replaced: the key now
                        # in the file is the one
                        raise FileExistsError(errno.EEXIST, "replaced while it was empty", path)
                finally:
                    os.close(fd)
            _fsync_dir(d)
        finally:
            try:
                os.unlink(tmp)
                _fsync_dir(d)
            except FileNotFoundError:
                pass

    def _key_path(self, namespace: str) -> str:
        return legacy_key_path(self.dir, namespace)

    def _unwrap_file(self, namespace: str, path: str) -> bytes:
        provider = self.provider   # (no root key at all: KeyCustodyError, as it says)
        wrapped = self._read_secret(path)   # (empty or short: KeyCustodyError naming it)
        try:
            return provider.unwrap(namespace, WrappedKey("local", "", "1", wrapped))
        except KeyCustodyError as ex:
            raise KeyCustodyError(
                f"{ex} (namespace {namespace!r}'s wrapped data key {path} does not unwrap under "
                f"the root key {self._rk_path}: the key files come from different deployments, "
                "or the root key was replaced)") from None

    def data_key(self, namespace: str) -> bytes:
        cached = self._cache.get(namespace)
        if cached is not None:
            self._cache.move_to_end(namespace)
            return cached
        p = self._key_path(namespace)
        if os.path.exists(p):
            dk = self._unwrap_file(namespace, p)
        else:
            self._root_or_mint()
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
        try:
            names = sorted(os.listdir(self.dir))
        except FileNotFoundError:
            return out   # no keys directory (yet): no keys
        for fn in names:
            # names are taken verbatim: a namespace never contains "/", but
            # may contain "__" (a legal name), which must not be rewritten
            if fn.startswith("ns-") and fn.endswith(".key"):
                out.append(fn[3:-4])
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


_SHRED_SUFFIX = ".shred-"
# a key file being created (LocalKeyEnvelope._write_secret): "<file>.tmp-"
# + 12 hex digits - never the name of a key file, which ends in ".key"
# whatever its namespace is called. One older than _TMP_STALE_S is what a
# crash left, and is shredded (_sweep_shreds).
_TMP_RE = re.compile(r"\.key\.tmp-[0-9a-f]{12}\Z")
# what _overwrite_unlink renames a key file to: "<file>.key.shred-" + 8 hex
# digits, at the END of the name. A substring test is not enough: a
# namespace may be called "a.shred-b", and its live key file
# ("ns-a.shred-b.key") must never be taken for a shred leftover.
_SHRED_RE = re.compile(r"\.key\.shred-[0-9a-f]{8}\Z")
_TMP_STALE_S = 300.0


def _fsync_dir(d: str) -> None:
    try:
        fd = os.open(d or ".", os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _overwrite_unlink(p: str) -> None:
    """Shred a key file, crash-safely.

    The file is first RENAMED out of its name (atomic), and only then
    overwritten and unlinked. Overwriting in place meant a crash between
    the overwrite and the unlink left random bytes under the live name:
    every later open of that namespace name then failed as if the root key
    were wrong. A crash now leaves at most a `*.shred-*` file, which no key
    lookup reads and the next LocalKeyEnvelope sweeps (_sweep_shreds)."""
    d = os.path.dirname(p)
    tmp = f"{p}{_SHRED_SUFFIX}{secrets.token_hex(4)}"
    os.replace(p, tmp)
    _fsync_dir(d)
    _shred_file(tmp)
    _fsync_dir(d)


def _shred_file(p: str) -> None:
    # overwrite before unlink so shred survives lazy fs behavior
    size = os.path.getsize(p)
    with open(p, "r+b") as f:
        f.write(secrets.token_bytes(size))
        f.flush()
        os.fsync(f.fileno())
    os.unlink(p)


def _sweep_shreds(d: str) -> None:
    """Finish shreds a crash interrupted (see _overwrite_unlink), and shred
    the temporary file of a key creation a crash interrupted - a key never
    linked into place, so never used, but key material all the same (one
    younger than _TMP_STALE_S may be a creation in progress: kept)."""
    try:
        names = os.listdir(d)
    except OSError:
        return
    for fn in names:
        p = os.path.join(d, fn)
        try:
            if _SHRED_RE.search(fn):
                _shred_file(p)
            elif _TMP_RE.search(fn) and time.time() - os.path.getmtime(p) > _TMP_STALE_S:
                if os.stat(p).st_nlink > 1:
                    # a crash between os.link(tmp, key) and os.unlink(tmp)
                    # (_write_secret): this name and the LIVE key file are
                    # one inode. Overwriting it would destroy the key; drop
                    # the extra name only.
                    os.unlink(p)
                    _fsync_dir(d)
                else:
                    _shred_file(p)
        except FileNotFoundError:
            pass  # a concurrent sweep (or its creator) finished it


def _is_file(path: str, st: os.stat_result) -> bool:
    """`path` names the file `st` was taken of."""
    try:
        cur = os.stat(path)
    except FileNotFoundError:
        return False
    return (cur.st_dev, cur.st_ino) == (st.st_dev, st.st_ino)


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
    new data under a key that can never read the old. So that open is
    refused (KeyCustodyError) and says to run `memd keys migrate`. A wrapped
    key that IS here and unwraps, but is not the one the data was written
    with (another deployment's `keys/<ns>.dek` restored over this one's),
    is the namespace store's to refuse: the manifest's key check, or a probe
    of the data (NamespaceStore._verify_key).
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
        # one lock per namespace being resolved: a KMS round trip for one
        # namespace must not serialize every other namespace's open
        self._locks_guard = threading.Lock()
        self._ns_locks: dict[str, list] = {}
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
        return self._read_record_versioned(namespace)[0]

    def _read_record_versioned(self, namespace: str) -> tuple[dict | None, str | None]:
        got = self.store.get_versioned(wrapped_key_object(namespace))
        if not got or not got[0]:
            return None, None
        raw, ver = got
        rec = json.loads(raw.decode())
        if rec.get("namespace") != namespace:
            # a record copied in from another namespace (its wrapping is
            # bound to the OTHER name and would not unwrap anyway)
            raise KeyCustodyError(f"wrapped key object for {namespace!r} names "
                                  f"{rec.get('namespace')!r}; refusing it")
        return rec, ver

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
        with self._ns_lock(namespace):
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

    @contextlib.contextmanager
    def _ns_lock(self, namespace: str):
        with self._locks_guard:
            ent = self._ns_locks.setdefault(namespace, [threading.Lock(), 0])
            ent[1] += 1
        try:
            with ent[0]:
                yield
        finally:
            with self._locks_guard:
                ent[1] -= 1
                if ent[1] == 0:
                    self._ns_locks.pop(namespace, None)

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
        # again: a racing data_key() may have re-read the record between the
        # first pop and the shred
        had_cached = (self._cache.pop(namespace, None) is not None) or had_cached
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
        rec, ver = self._read_record_versioned(namespace)
        if rec is None:
            return None
        if rec.get("provider") != self.provider.name:
            raise KeyCustodyError(f"{namespace!r} is wrapped by {rec.get('provider')!r}, not "
                                  f"{self.provider.name!r}")
        old = WrappedKey.from_record(rec)
        new = self.provider.rewrap(namespace, old)
        if self.provider.unwrap(namespace, new) != self.provider.unwrap(namespace, old):
            raise KeyCustodyError(f"rewrap of {namespace!r} did not round-trip; record left unchanged")
        # conditional on the version just unwrapped: a concurrent shred or
        # rotation wins, and this rewrap must not resurrect or clobber it
        # (fence=False: this is an admin tool, not the namespace's writer)
        self.store.put_if_match(wrapped_key_object(namespace),
                                json.dumps(new.to_record(namespace), sort_keys=True).encode(),
                                ver, fence=False)
        return {"namespace": namespace, "from": [old.key_id, old.key_version],
                "to": [new.key_id, new.key_version]}


class NullKeyEnvelope(KeyEnvelope):
    """No encryption at rest (testing / explicit opt-out).

    `keys_dir`, if given, is where this deployment keeps its `local` keys
    when encryption is on, and `store` the object store a remote provider
    keeps its wrapped keys in (keys/<ns>.dek): key_on_disk() tells an open
    that a namespace has a data key in either - it was written with
    encryption on at some point - so it looks for ciphertext before it
    reads anything as plaintext (see NamespaceStore._refuse_sealed_data)."""

    enabled = False
    provider_name = "none"

    def __init__(self, keys_dir: str | None = None, store: Any = None) -> None:
        self._keys: dict[str, bytes] = {}
        self._keys_dir = keys_dir
        self._store = store

    def key_on_disk(self, namespace: str) -> bool:
        if self._keys_dir and os.path.exists(legacy_key_path(self._keys_dir, namespace)):
            return True
        if self._store is None:
            return False
        try:
            return bool(self._store.exists(wrapped_key_object(namespace)))
        except Exception:  # noqa: BLE001 - unknown counts as keyed: look harder
            return True

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
    None only when it is ABSENT (a store that has only ever used `local`
    keys). Fails CLOSED: a marker that cannot be read or parsed - garbage,
    a truncated write, an outage - raises KeyCustodyError instead of reading
    as "absent", which would let a node on the wrong provider mint fresh
    keys over data it cannot decrypt."""
    try:
        raw = store.get(CUSTODY_KEY)
    except Exception as ex:  # noqa: BLE001
        raise KeyCustodyError(f"cannot read the key-custody marker {CUSTODY_KEY!r}: "
                              f"{type(ex).__name__}; refusing to open") from ex
    if raw is None:
        return None
    try:
        cur = json.loads(raw.decode())
    except (ValueError, UnicodeDecodeError):
        cur = None
    if not isinstance(cur, dict) or not isinstance(cur.get("provider"), str):
        raise KeyCustodyError(
            f"the key-custody marker {CUSTODY_KEY!r} is present but unreadable ({raw[:40]!r}); "
            "refusing to open: which provider holds this store's keys is unknown. Restore it "
            "(`memd keys status` on a node that knows) - never delete it to get past this")
    return cur


def write_custody(store: Any, provider: KeyProvider) -> None:
    """Record `provider` as the store's key custodian - conditionally, on
    the version read here, so two processes switching custody at once cannot
    both believe they won (the loser re-reads and refuses a different one)."""
    from memd.storage.objectstore import PreconditionFailed

    body = json.dumps({"provider": provider.name, **provider.describe(),
                       "since_ms": int(time.time() * 1000)}, sort_keys=True).encode()
    for _ in range(3):
        got = store.get_versioned(CUSTODY_KEY)
        if got is not None:
            try:
                cur = json.loads(got[0].decode())
            except ValueError:
                cur = {}
            if cur.get("provider") == provider.name:
                return
        try:
            store.put_if_match(CUSTODY_KEY, body, got[1] if got else None)
            return
        except PreconditionFailed:
            continue
    cur = read_custody(store) or {}
    if cur.get("provider") != provider.name:
        raise KeyCustodyError(f"key custody changed concurrently to {cur.get('provider')!r}")


def envelope_from_config(cfg: dict | None, local_dir: str, store: Any, *,
                         encrypt: bool = True) -> KeyEnvelope:
    """The envelope a Memory opens with.

    `local` (default): LocalKeyEnvelope under `<local_dir>/keys`, exactly as
    before - but refused when the store's custody marker says its keys have
    been moved to a remote provider (a node with the old config would
    otherwise mint fresh keys over data it can no longer read).
    Remote: ObjectStoreKeyEnvelope over `store`, with `<local_dir>/keys` as
    the legacy directory the mint guard checks."""
    keys_dir = os.path.join(local_dir, "keys")
    if not encrypt:
        # (the keys it would have: an open still tells an encrypted namespace
        # from a plaintext one - see NullKeyEnvelope.key_on_disk)
        return NullKeyEnvelope(keys_dir, store)
    cfg = cfg or {}
    name = resolve_key_provider_name(cfg)
    custody = read_custody(store) if store is not None else None
    if name == "local":
        if custody and custody.get("provider") not in (None, "local"):
            raise KeyCustodyError(
                f"this store's data keys are held by {custody.get('provider')!r} "
                f"(key {custody.get('key_id')!r}); set MEMD_KEY_PROVIDER={custody.get('provider')} "
                "and its settings - the local provider cannot read them")
        # A namespace that has encrypted data but no key file here (keys lost
        # or moved, a restore without the keys directory, another node's
        # namespace on a shared bucket) is refused at open, nothing minted:
        # NamespaceStore._verify_key, on every root. The key is resolved
        # lazily, only once that check passed (and the root key with it).
        return LocalKeyEnvelope(keys_dir)
    if custody and custody.get("provider") != name:
        raise KeyCustodyError(
            f"this store's data keys are held by {custody.get('provider')!r}, not {name!r}")
    allow = str(_opt(cfg, "keys_allow_mint_existing", "MEMD_KEYS_ALLOW_MINT_EXISTING", "")) in ("1", "true")
    return ObjectStoreKeyEnvelope(provider_from_config(name, cfg), store,
                                  legacy_dir=keys_dir, allow_mint_existing=allow)
