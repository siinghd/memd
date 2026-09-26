"""Per-namespace envelope encryption (D7 control #9).

Namespace = key scope. Embedded mode stores the namespace data-encryption
key locally (wrapped by a root key file); hosted mode swaps the root-key
provider for a KMS. Namespace deletion destroys the key: crypto-shred.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from collections import OrderedDict

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_LEN = 32
# the label a data key's public fingerprint is taken over (KeyEnvelope.key_check)
_KEY_CHECK_LABEL = b"memd/key-check/v1"


class KeyCustodyError(RuntimeError):
    """A namespace's data key is held somewhere this process is not
    configured for (or not at all while the namespace has data). Minting a
    fresh key instead would make the existing ciphertext unreadable, so the
    open is refused with a message saying what to do.

    Also what a ciphertext that does not authenticate under the key in hand
    raises (never cryptography's bare InvalidTag): a valid key that is not
    the one the data was written with - another deployment's keys directory
    restored over this one, a replaced root key - is a custody failure, not
    damage. Storage never skips, truncates or deletes such data."""


class KeyEnvelope:
    """Provides 256-bit data keys per namespace, wrapped by a root key."""

    enabled = True

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
        minting one would be wrong."""
        return hmac.new(self.data_key(namespace), _KEY_CHECK_LABEL, hashlib.sha256).hexdigest()[:32]


class LocalKeyEnvelope(KeyEnvelope):
    """Root key + per-namespace wrapped keys under {root}/keys/.

    Threat model (documented honestly per D7): protects data at rest on the
    volume and makes crypto-shred possible; not protection against a user
    with full filesystem read access on the same machine.
    """

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
        safe = namespace.replace("/", "__")
        return os.path.join(self.dir, f"ns-{safe}.key")

    def _unwrap(self, namespace: str, path: str) -> bytes:
        wrapped = self._read_secret(path)
        try:
            return AESGCM(self._root).decrypt(wrapped[:12], wrapped[12:], namespace.encode())
        except InvalidTag:
            raise KeyCustodyError(
                f"namespace {namespace!r}'s wrapped data key ({path}) does not unwrap under this "
                f"root key ({os.path.join(self.dir, 'root.key')}): the key files come from "
                "different deployments, or the root key was replaced. Restore the root key they "
                "were wrapped with") from None

    def data_key(self, namespace: str) -> bytes:
        cached = self._cache.get(namespace)
        if cached is not None:
            self._cache.move_to_end(namespace)
            return cached
        p = self._key_path(namespace)
        if os.path.exists(p):
            dk = self._unwrap(namespace, p)
        else:
            dk = secrets.token_bytes(KEY_LEN)
            nonce = secrets.token_bytes(12)
            wrapped = nonce + AESGCM(self._root).encrypt(nonce, dk, namespace.encode())
            try:
                self._write_secret(p, wrapped)
            except FileExistsError:
                # concurrent creator won; use their key deterministically
                dk = self._unwrap(namespace, p)
        self._cache[namespace] = dk
        if len(self._cache) > self.CACHE_MAX:
            self._cache.popitem(last=False)
        return dk

    def has_key(self, namespace: str) -> bool:
        return namespace in self._cache or os.path.exists(self._key_path(namespace))

    def destroy(self, namespace: str) -> bool:
        # shred order matters: drop the cached copy BEFORE the file so no
        # encrypt() can re-adopt a destroyed namespace's key from RAM
        had_cached = self._cache.pop(namespace, None) is not None
        p = self._key_path(namespace)
        if os.path.exists(p):
            # overwrite before unlink so shred survives lazy fs behavior
            size = os.path.getsize(p)
            with open(p, "wb") as f:
                f.write(secrets.token_bytes(size))
                f.flush()
                os.fsync(f.fileno())
            os.unlink(p)
            return True
        return had_cached


class NullKeyEnvelope(KeyEnvelope):
    """No encryption at rest (testing / explicit opt-out)."""

    enabled = False

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
