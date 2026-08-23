"""Per-namespace envelope encryption (D7 control #9).

Namespace = key scope. Embedded mode stores the namespace data-encryption
key locally (wrapped by a root key file); hosted mode swaps the root-key
provider for a KMS. Namespace deletion destroys the key: crypto-shred.
"""
from __future__ import annotations

import os
import secrets

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_LEN = 32


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

    def destroy(self, namespace: str) -> bool:
        raise NotImplementedError

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

    def __init__(self, dir_path: str, root_key: bytes | None = None):
        self.dir = dir_path
        os.makedirs(dir_path, exist_ok=True)
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

    def data_key(self, namespace: str) -> bytes:
        p = self._key_path(namespace)
        if os.path.exists(p):
            wrapped = self._read_secret(p)
            nonce, ct = wrapped[:12], wrapped[12:]
            return AESGCM(self._root).decrypt(nonce, ct, namespace.encode())
        dk = secrets.token_bytes(KEY_LEN)
        nonce = secrets.token_bytes(12)
        wrapped = nonce + AESGCM(self._root).encrypt(nonce, dk, namespace.encode())
        try:
            self._write_secret(p, wrapped)
            return dk
        except FileExistsError:
            # concurrent creator won; use their key deterministically
            wrapped2 = self._read_secret(p)
            n2, c2 = wrapped2[:12], wrapped2[12:]
            return AESGCM(self._root).decrypt(n2, c2, namespace.encode())

    def destroy(self, namespace: str) -> bool:
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
        return False


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
