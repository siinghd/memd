"""Per-namespace envelope encryption (D7 control #9).

Namespace = key scope. Embedded mode stores the namespace data-encryption
key locally (wrapped by a root key file); hosted mode swaps the root-key
provider for a KMS. Namespace deletion destroys the key: crypto-shred.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
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


def _ns_key_path(keys_dir: str, namespace: str) -> str:
    """Where a namespace's wrapped data key lives under a keys directory."""
    safe = namespace.replace("/", "__")
    return os.path.join(keys_dir, f"ns-{safe}.key")


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
        # unwrapped per-namespace data keys, LRU-bounded. Caching is sound:
        # every encrypt/decrypt needs the raw key in process memory anyway.
        # Without this, each WAL append/replay frame paid a file read +
        # AESGCM unwrap for the SAME namespace key.
        self._cache: "OrderedDict[str, bytes]" = OrderedDict()
        self.CACHE_MAX = 1024  # namespaces; matches hosted per-node open set
        self._rk_path = os.path.join(dir_path, "root.key")
        # Loaded - or created - on first use, not here: an open that refuses
        # (a restore without the keys directory: see KeyCustodyError) must
        # not leave a fresh root key behind. One is minted only together
        # with the first data key it wraps (data_key).
        self._root_key: bytes | None = root_key
        _sweep_shreds(dir_path)
        if root_key is not None and not os.path.exists(self._rk_path):
            self._write_secret(self._rk_path, root_key)

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
            self._root_key = self._read_secret(self._rk_path)
        return self._root_key

    def _root_or_mint(self) -> bytes:
        """The root key, minted if this keys directory has none yet (only
        ever to wrap a data key being minted)."""
        if self._root_key is None and not os.path.exists(self._rk_path):
            fresh = secrets.token_bytes(KEY_LEN)
            try:
                self._write_secret(self._rk_path, fresh)
                self._root_key = fresh
            except FileExistsError:
                pass  # a concurrent creator won; use theirs
        return self._root

    @staticmethod
    def _read_secret(path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    @staticmethod
    def _write_secret(path: str, data: bytes) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)

    def _key_path(self, namespace: str) -> str:
        return _ns_key_path(self.dir, namespace)

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
            wrapped = nonce + AESGCM(self._root_or_mint()).encrypt(nonce, dk, namespace.encode())
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
            _overwrite_unlink(p)
            return True
        return had_cached


_SHRED_SUFFIX = ".shred-"
# what _overwrite_unlink renames a key file to: "<file>.key.shred-" + 8 hex
# digits, at the END of the name. A substring test is not enough: a
# namespace may be called "a.shred-b", and its live key file
# ("ns-a.shred-b.key") must never be taken for a shred leftover.
_SHRED_RE = re.compile(r"\.key\.shred-[0-9a-f]{8}\Z")


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
    """Finish shreds a crash interrupted (see _overwrite_unlink)."""
    try:
        names = os.listdir(d)
    except OSError:
        return
    for fn in names:
        if _SHRED_RE.search(fn):
            try:
                _shred_file(os.path.join(d, fn))
            except FileNotFoundError:
                pass  # a concurrent sweep finished it


class NullKeyEnvelope(KeyEnvelope):
    """No encryption at rest (testing / explicit opt-out).

    `keys_dir`, if given, is where this deployment keeps its keys when
    encryption is on: key_on_disk() tells an open that a namespace has a
    data key there - it was written with encryption on at some point - so
    it looks for ciphertext before it reads anything as plaintext."""

    enabled = False

    def __init__(self, keys_dir: str | None = None) -> None:
        self._keys: dict[str, bytes] = {}
        self._keys_dir = keys_dir

    def key_on_disk(self, namespace: str) -> bool:
        return bool(self._keys_dir) and os.path.exists(_ns_key_path(self._keys_dir, namespace))

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
