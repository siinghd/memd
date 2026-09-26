"""ADR-12 item 1: pluggable key providers (local / aws-kms / vault-transit).

What must hold for every provider:
  - data at rest is ciphertext; a fresh process (another node) with the
    same provider config reads it back - the wrapped key lives in the store;
  - crypto-shred leaves no way to the data key: the wrapped key object is
    gone, a restored copy of the ciphertext cannot be opened, and minting a
    new key over it is refused;
  - a namespace with data but no wrapped key is never silently re-keyed;
  - `memd keys migrate` moves a local store under a KMS crash-safely.

aws-kms runs against moto (in-process); vault-transit against a Vault dev
server in docker - skipped cleanly when docker is unavailable.
"""
import base64
import json
import os
import shutil
import socket
import subprocess
import time
import uuid

import pytest

from memd.engine.memory import Memory
from memd.storage.crypto import (KeyCustodyError, LocalKeyEnvelope, LocalKeyProvider,
                                 ObjectStoreKeyEnvelope, WrappedKey, legacy_key_path,
                                 read_custody, wrapped_key_object)
from memd.storage.objectstore import LocalObjectStore

SECRET = "the launch code is 4242-ALPHA"


def _cfg(**kw):
    base = {"embedder": "hash", "rate_max_writes": 10 ** 9}
    base.update(kw)
    return base


def _write_secret(mem: Memory, ns: str) -> str:
    rid = mem.remember(SECRET, namespace=ns)
    mem.add("the deploy train leaves at noon", session_id="s1", namespace=ns)
    return rid


def _all_objects(root: str) -> dict[str, bytes]:
    """Every OBJECT under a local store root - not the derived index cache
    (`_cache/`, node-local and plaintext by design, as with every provider)."""
    out = {}
    for dp, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d != "_cache"]
        for fn in files:
            p = os.path.join(dp, fn)
            with open(p, "rb") as f:
                out[os.path.relpath(p, root)] = f.read()
    return out


# ------------------------------------------------------------------- local


class TestLocalProvider:
    def test_wrap_format_is_unchanged(self, tmp_path):
        """A key file written by the pre-ADR-12 code (nonce || AESGCM(root,
        dk, aad=ns)) must still unwrap, and new files keep that format."""
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        d = str(tmp_path / "keys")
        env = LocalKeyEnvelope(d)
        assert not os.path.exists(os.path.join(d, "root.key")), "the root key is minted lazily"
        fresh = env.data_key("new")   # ...together with the first data key it wraps
        root = open(os.path.join(d, "root.key"), "rb").read()
        dk = os.urandom(32)
        nonce = os.urandom(12)
        with open(legacy_key_path(d, "old"), "wb") as f:
            f.write(nonce + AESGCM(root).encrypt(nonce, dk, b"old"))
        assert LocalKeyEnvelope(d).data_key("old") == dk
        blob = open(legacy_key_path(d, "new"), "rb").read()
        assert AESGCM(root).decrypt(blob[:12], blob[12:], b"new") == fresh

    def test_local_provider_binds_the_namespace(self):
        p = LocalKeyProvider(os.urandom(32))
        wk = p.wrap("a", b"k" * 32)
        assert p.unwrap("a", wk) == b"k" * 32
        with pytest.raises(KeyCustodyError):
            p.unwrap("b", wk)

    def test_default_store_opens_exactly_as_before(self, tmp_path):
        m = Memory(str(tmp_path / "d"), config=_cfg())
        _write_secret(m, "default")
        m.close()
        assert os.path.exists(tmp_path / "d" / "keys" / "root.key")
        assert not os.path.exists(tmp_path / "d" / "store" / "keys"), \
            "the local provider must not write wrapped keys into the store"
        m = Memory(str(tmp_path / "d"), config=_cfg())
        assert any(SECRET in i.content for i in m.search("launch code").items)
        m.close()


# ----------------------------------------------------------------- aws-kms

moto = pytest.importorskip("moto")


@pytest.fixture()
def kms():
    import boto3
    from moto import mock_aws

    with mock_aws():
        c = boto3.client("kms", region_name="us-east-1")
        arn = c.create_key(Description="memd test CMK")["KeyMetadata"]["Arn"]
        yield c, arn


def _kms_mem(tmp_path, kms, name="d", ns="default", **kw):
    c, arn = kms
    return Memory(str(tmp_path / name), namespace=ns,
                  config=_cfg(key_provider="aws-kms", kms_client=c, kms_key_id=arn, **kw))


class TestAwsKms:
    def test_round_trip_and_any_node_can_unwrap(self, tmp_path, kms):
        m = _kms_mem(tmp_path, kms)
        _write_secret(m, "default")
        m.close()
        store_root = tmp_path / "d" / "store"
        rec = json.loads((store_root / "keys" / "default.dek").read_bytes())
        assert rec["provider"] == "aws-kms" and rec["namespace"] == "default"
        assert rec["key_id"] == kms[1]
        assert not (tmp_path / "d" / "keys" / "ns-default.key").exists(), \
            "a KMS-held namespace must not also have a local key file"
        blobs = _all_objects(str(store_root))
        assert not any(SECRET.encode() in b for b in blobs.values()), "plaintext at rest"
        # "another node": a different local dir, same store, same CMK
        other = tmp_path / "node2"
        shutil.copytree(store_root, other / "store")
        m2 = _kms_mem(tmp_path, kms, name="node2")
        assert any(SECRET in i.content for i in m2.search("launch code").items)
        m2.close()
        assert read_custody(LocalObjectStore(str(store_root)))["provider"] == "aws-kms"

    def test_a_wrapped_key_moved_to_another_namespace_does_not_unwrap(self, tmp_path, kms):
        c, arn = kms
        from memd.storage.crypto import AwsKmsProvider

        p = AwsKmsProvider(arn, client=c)
        dk, wk = p.generate("a")
        assert p.unwrap("a", wk) == dk
        with pytest.raises(Exception):
            p.unwrap("b", wk)

    def test_crypto_shred_makes_the_data_unreadable(self, tmp_path, kms):
        m = _kms_mem(tmp_path, kms)
        _write_secret(m, "victim")
        m.flush()
        store = m.engine.store
        env = m.engine.envelope
        backup = {k: store.get(k) for k in store.list("ns/victim/")}
        dek_before = env.data_key("victim")
        wrapped_before = store.get(wrapped_key_object("victim"))
        assert wrapped_before and backup
        m.destroy_namespace("victim")
        assert not store.exists(wrapped_key_object("victim"))
        assert not env.has_key("victim")
        detail = env.destroy_report("victim")
        assert detail["wrapped_key_deleted"] is True and detail["provider"] == "aws-kms"
        # another node, same CMK: nothing left to unwrap
        c, arn = kms
        from memd.storage.crypto import AwsKmsProvider

        fresh = ObjectStoreKeyEnvelope(AwsKmsProvider(arn, client=c), store)
        assert fresh.peek_data_key("victim") is None
        # an attacker restores the ciphertext into the bucket: reopening
        # refuses to mint a key over it (and none it could mint would fit)
        for k, v in backup.items():
            store.put(k, v)
        with pytest.raises(KeyCustodyError):
            m.engine.namespace("victim")
        # the ciphertext really was under the destroyed key
        frames = [v for k, v in backup.items() if k.endswith("/wal") or "/seg-" in k]
        assert frames
        m.close()
        assert dek_before != b"\x00" * 32

    def test_shared_cmk_refuses_cmk_level_shred(self, kms):
        from memd.storage.crypto import AwsKmsProvider

        with pytest.raises(ValueError, match="SHARED"):
            AwsKmsProvider(kms[1], client=kms[0], shred="schedule-deletion")

    def test_per_namespace_cmk_is_scheduled_for_deletion_on_shred(self, tmp_path, kms):
        c, _ = kms
        kid = c.create_key()["KeyMetadata"]["KeyId"]
        c.create_alias(AliasName="alias/memd-tenant1", TargetKeyId=kid)
        c.create_alias(AliasName="alias/memd-default", TargetKeyId=c.create_key()["KeyMetadata"]["KeyId"])
        m = Memory(str(tmp_path / "d"), namespace="default",
                   config=_cfg(key_provider="aws-kms", kms_client=c,
                               kms_key_id="alias/memd-{namespace}", kms_shred="schedule-deletion"))
        _write_secret(m, "tenant1")
        m.destroy_namespace("tenant1")
        assert c.describe_key(KeyId=kid)["KeyMetadata"]["KeyState"] == "PendingDeletion"
        detail = m.engine.envelope.destroy_report("tenant1")
        assert detail["provider_action"] == "kms_schedule_key_deletion"
        m.close()

    def test_existing_data_without_a_wrapped_key_is_never_rekeyed(self, tmp_path, kms):
        """A local-provider store opened with aws-kms before migrating: the
        namespace has data and a LOCAL key - minting would orphan the data."""
        m = Memory(str(tmp_path / "d"), config=_cfg())
        _write_secret(m, "default")
        m.close()
        with pytest.raises(KeyCustodyError, match="keys migrate"):
            _kms_mem(tmp_path, kms)
        # with the local key file gone too (another node), still refused
        os.unlink(legacy_key_path(str(tmp_path / "d" / "keys"), "default"))
        with pytest.raises(KeyCustodyError, match="has data"):
            _kms_mem(tmp_path, kms)

    @pytest.mark.parametrize("stamped", [True, False], ids=["stamped", "unstamped"])
    def test_another_deployments_wrapped_key_is_refused_and_nothing_is_touched(
            self, tmp_path, kms, stamped):
        """A restore mix-up under a remote provider: another deployment's
        keys/<ns>.dek (same CMK, same namespace name - it unwraps fine) put
        over this one's. The data key is simply not this data's: the open is
        refused before anything is read (the manifest's key check; a probe
        of the data on a manifest without one), nothing is truncated,
        rewritten or deleted, and the right wrapped key serves everything."""
        m = _kms_mem(tmp_path, kms)
        for i in range(5):
            m.remember(f"precious fact {i}")
        m.compact(force=True)
        m.remember("and one in the WAL")
        m.close()
        store_root = tmp_path / "d" / "store"
        man_path = store_root / "ns" / "default" / "manifest.json"
        man = json.loads(man_path.read_bytes())
        assert man.get("key_check"), "a remote key is resolved at open: stamped from the start"
        if not stamped:
            man.pop("key_check")
            man_path.write_text(json.dumps(man))
        other = _kms_mem(tmp_path, kms, name="otherdeploy")
        other.remember("unrelated")
        other.close()
        dek = store_root / "keys" / "default.dek"
        saved = dek.read_bytes()
        dek.write_bytes((tmp_path / "otherdeploy" / "store" / "keys" / "default.dek").read_bytes())
        before = {k: v for k, v in _all_objects(str(store_root)).items() if k.startswith("ns")}
        for _attempt in range(2):
            with pytest.raises(KeyCustodyError, match="key"):
                _kms_mem(tmp_path, kms, name="d")
            assert {k: v for k, v in _all_objects(str(store_root)).items()
                    if k.startswith("ns")} == before, "a refused open changed the store"
        dek.write_bytes(saved)
        m = _kms_mem(tmp_path, kms)
        assert len([ln for ln in m.export_jsonl().splitlines() if ln.strip()]) == 6
        m.close()

    def test_a_provider_backed_store_never_mints_a_local_root_key(self, tmp_path, kms):
        m = _kms_mem(tmp_path, kms)
        _write_secret(m, "default")
        m.close()
        os.unlink(tmp_path / "d" / "store" / "keys" / "default.dek")   # the wrapped key is lost
        with pytest.raises(KeyCustodyError):
            _kms_mem(tmp_path, kms)
        assert not (tmp_path / "d" / "store" / "keys" / "default.dek").exists(), "a key was minted"
        assert not (tmp_path / "d" / "keys").exists(), \
            "a KMS-held store must never mint a local root key (or any local key file)"

    def test_rewrap_keeps_the_data_key(self, tmp_path, kms):
        m = _kms_mem(tmp_path, kms)
        _write_secret(m, "default")
        env = m.engine.envelope
        before = env.data_key("default")
        rep = env.rewrap("default")
        assert rep["namespace"] == "default"
        env._cache.clear()
        assert env.data_key("default") == before
        m.close()


class TestKeysMigrate:
    def _env(self, monkeypatch, kms):
        c, arn = kms
        monkeypatch.setenv("MEMD_KMS_KEY_ID", arn)
        monkeypatch.setenv("AWS_REGION", "us-east-1")

    def test_migrate_local_to_kms(self, tmp_path, kms, monkeypatch, capsys):
        from memd.cli import main

        data = str(tmp_path / "d")
        m = Memory(data, config=_cfg())
        _write_secret(m, "default")
        _write_secret(m, "other")
        m.close()
        self._env(monkeypatch, kms)
        assert main(["keys", "migrate", "--to", "aws-kms", "--data", data]) == 0
        rep = json.loads(capsys.readouterr().out)
        assert rep["swapped"] and sorted(rep["migrated"]) == ["default", "other"]
        assert sorted(rep["local_keys_removed"]) == ["default", "other"]
        assert not os.path.exists(legacy_key_path(os.path.join(data, "keys"), "default"))
        # the old provider now refuses the store instead of minting keys
        with pytest.raises(KeyCustodyError, match="aws-kms"):
            Memory(data, config=_cfg())
        m = _kms_mem(tmp_path, kms)
        assert any(SECRET in i.content for i in m.search("launch code").items)
        assert any(SECRET in i.content for i in m.search("launch code", namespace="other").items)
        m.close()
        # idempotent
        assert main(["keys", "migrate", "--to", "aws-kms", "--data", data]) == 0

    def test_a_crash_mid_migration_leaves_the_store_usable_and_rerun_finishes(
            self, tmp_path, kms, monkeypatch, capsys):
        from memd.cli import main
        from memd.storage import objectstore

        data = str(tmp_path / "d")
        m = Memory(data, config=_cfg())
        for ns in ("a1", "a2", "a3"):
            _write_secret(m, ns)
        m.close()
        self._env(monkeypatch, kms)
        real = objectstore.LocalObjectStore.put_if_absent
        calls = {"n": 0}

        def crashy(self, key, data_):
            calls["n"] += 1
            if calls["n"] == 2:
                raise SystemExit("killed mid-migration")
            return real(self, key, data_)

        monkeypatch.setattr(objectstore.LocalObjectStore, "put_if_absent", crashy)
        with pytest.raises(SystemExit):
            main(["keys", "migrate", "--to", "aws-kms", "--data", data])
        monkeypatch.setattr(objectstore.LocalObjectStore, "put_if_absent", real)
        capsys.readouterr()
        # not swapped: the local provider still opens every namespace
        assert read_custody(LocalObjectStore(os.path.join(data, "store"))) is None
        m = Memory(data, namespace="a2", config=_cfg())
        assert any(SECRET in i.content for i in m.search("launch code").items)
        m.close()
        assert main(["keys", "migrate", "--to", "aws-kms", "--data", data]) == 0
        rep = json.loads(capsys.readouterr().out)
        assert rep["swapped"] and sorted(rep["migrated"] + rep["already"]) == ["a1", "a2", "a3", "default"]
        for ns in ("a1", "a2", "a3"):
            m = _kms_mem(tmp_path, kms, ns=ns)
            assert any(SECRET in i.content for i in m.search("launch code").items), ns
            m.close()

    def test_migrate_refuses_while_a_writer_holds_a_namespace(self, tmp_path, kms, monkeypatch, capsys):
        """A running local-provider node would MINT a fresh local key for a
        namespace it opens after the migration removed the old one. The
        single-writer lock is per process, so the writer is a child."""
        import sys

        from memd.cli import main

        data = str(tmp_path / "d")
        m = Memory(data, config=_cfg())
        _write_secret(m, "default")
        m.close()
        src = os.path.join(os.path.dirname(__file__), "..", "src")
        child = subprocess.Popen(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0, sys.argv[1]); from memd.engine.memory import Memory; "
             "m = Memory(sys.argv[2], config={'embedder': 'hash'}); print('ready', flush=True); "
             "sys.stdin.read(); m.close()", src, data],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            assert child.stdout.readline().strip() == "ready"
            self._env(monkeypatch, kms)
            assert main(["keys", "migrate", "--to", "aws-kms", "--data", data]) == 1
            assert "stop it first" in capsys.readouterr().err
            assert os.path.exists(legacy_key_path(os.path.join(data, "keys"), "default"))
            assert read_custody(LocalObjectStore(os.path.join(data, "store"))) is None
        finally:
            child.stdin.close()
            child.wait(timeout=60)
        assert main(["keys", "migrate", "--to", "aws-kms", "--data", data]) == 0


# ----------------------------------------------------------- vault-transit


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def vault():
    if shutil.which("docker") is None:
        pytest.skip("docker not available for the Vault dev server")
    image = os.environ.get("MEMD_TEST_VAULT_IMAGE", "hashicorp/vault:1.17")
    name = f"memd-test-vault-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    r = subprocess.run(["docker", "run", "-d", "--rm", "--name", name, "--cap-add=IPC_LOCK",
                        "-e", "VAULT_DEV_ROOT_TOKEN_ID=root", "-p", f"127.0.0.1:{port}:8200", image],
                       capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        pytest.skip(f"could not start a Vault container: {r.stderr.strip()[:200]}")
    import httpx

    addr = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 60
        while True:
            try:
                if httpx.get(f"{addr}/v1/sys/health", timeout=1).status_code == 200:
                    break
            except Exception:
                pass
            if time.time() > deadline:
                pytest.skip("Vault dev server did not come up")
            time.sleep(0.3)
        h = {"X-Vault-Token": "root"}
        httpx.post(f"{addr}/v1/sys/mounts/transit", json={"type": "transit"}, headers=h).raise_for_status()
        httpx.post(f"{addr}/v1/transit/keys/memd", json={}, headers=h).raise_for_status()
        yield addr
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=60)


def _vault_mem(tmp_path, addr, name="d", ns="default", **kw):
    return Memory(str(tmp_path / name), namespace=ns,
                  config=_cfg(key_provider="vault-transit", vault_addr=addr, vault_token="root", **kw))


class TestVaultTransit:
    def test_round_trip_and_any_node_can_unwrap(self, tmp_path, vault):
        m = _vault_mem(tmp_path, vault)
        _write_secret(m, "default")
        m.close()
        store_root = tmp_path / "d" / "store"
        rec = json.loads((store_root / "keys" / "default.dek").read_bytes())
        assert rec["provider"] == "vault-transit" and rec["key_version"] == "1"
        assert base64.b64decode(rec["wrapped"]).startswith(b"vault:v1:")
        assert not any(SECRET.encode() in b for b in _all_objects(str(store_root)).values())
        shutil.copytree(store_root, tmp_path / "node2" / "store")
        m2 = _vault_mem(tmp_path, vault, name="node2")
        assert any(SECRET in i.content for i in m2.search("launch code").items)
        m2.close()

    def test_namespace_is_bound_as_associated_data(self, vault):
        from memd.storage.crypto import VaultTransitProvider

        p = VaultTransitProvider(vault, "root")
        dk, wk = p.generate("a")
        assert p.unwrap("a", wk) == dk
        with pytest.raises(Exception):
            p.unwrap("b", wk)

    def test_rotation_rewraps_under_the_new_version(self, tmp_path, vault):
        import httpx

        m = _vault_mem(tmp_path, vault, ns="rot")
        _write_secret(m, "rot")
        env = m.engine.envelope
        dk = env.data_key("rot")
        httpx.post(f"{vault}/v1/transit/keys/memd/rotate", headers={"X-Vault-Token": "root"}).raise_for_status()
        rep = env.rewrap("rot")
        assert rep["to"][1] != rep["from"][1]
        env._cache.clear()
        assert env.data_key("rot") == dk
        m.close()

    def test_crypto_shred_makes_the_data_unreadable(self, tmp_path, vault):
        import httpx

        h = {"X-Vault-Token": "root"}
        httpx.post(f"{vault}/v1/transit/keys/memd-victim", json={}, headers=h).raise_for_status()
        httpx.post(f"{vault}/v1/transit/keys/memd-default", json={}, headers=h).raise_for_status()
        m = _vault_mem(tmp_path, vault, vault_transit_key="memd-{namespace}", vault_shred="delete-key")
        _write_secret(m, "victim")
        m.flush()
        store = m.engine.store
        backup = {k: store.get(k) for k in store.list("ns/victim/")}
        wrapped = WrappedKey.from_record(json.loads(store.get(wrapped_key_object("victim"))))
        m.destroy_namespace("victim")
        assert not store.exists(wrapped_key_object("victim"))
        detail = m.engine.envelope.destroy_report("victim")
        assert detail["provider_action"] == "vault_delete_key"
        # per-namespace transit key: even a stolen COPY of the wrapped key is dead
        assert httpx.get(f"{vault}/v1/transit/keys/memd-victim", headers=h).status_code == 404
        with pytest.raises(Exception):
            m.engine.envelope.provider.unwrap("victim", wrapped)
        for k, v in backup.items():
            store.put(k, v)
        with pytest.raises(KeyCustodyError):
            m.engine.namespace("victim")
        m.close()
