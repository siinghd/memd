"""Pass 9 regression tests: bounded open-namespace table (LRU + pin).

Design target: tenant mix is "many tiny, heavy tail, mostly idle" - an
engine that keeps every namespace ever touched OPEN leaks fds/RAM in
proportion to tenants-ever, not tenants-active. Stores must close cleanly
under cap pressure (all durable state lives in blobs+manifest; reopen
replays the tail) while facade-pinned namespaces stay resident.
"""
import pytest

from memd.core.schema import MemoryRecord, Scope, Source
from memd.storage.engine import StorageEngine


def _rec(ns: str, text: str) -> MemoryRecord:
    return MemoryRecord.create(namespace=ns, kind="raw_event", content=text,
                               scope=Scope(user="u1"), source=Source.USER)


class TestLruNamespaceCache:
    def test_cap_enforced_and_data_survives_eviction(self, tmp_path):
        eng = StorageEngine(str(tmp_path / "root"), max_open_namespaces=2)
        try:
            a = eng.namespace("alpha")
            a.append([_rec("alpha", "alpha data survives")])
            eng.namespace("beta")
            c = eng.namespace("gamma")  # cap 2 -> alpha (LRU) must be closed
            c.append([_rec("gamma", "gamma data")])
            assert "alpha" not in eng._namespaces, "LRU store was not evicted past cap"
            assert len(eng._namespaces) <= 2
            # reopen: replay must recover everything durably written
            a2 = eng.namespace("alpha")
            ids = [r.id for r in a2.index.all_records()]
            assert len(ids) == 1
            got = a2.index.get_by_id(ids[0])
            assert got.content == "alpha data survives"
        finally:
            eng.close()

    def test_pinned_default_never_evicted(self, tmp_path):
        eng = StorageEngine(str(tmp_path / "root"), max_open_namespaces=2)
        try:
            home = eng.namespace("home")
            eng.pin_namespace("home")
            home.append([_rec("home", "pinned row")])
            ref = home.index  # facade-style direct reference must stay valid
            for i in range(6):  # churn far past the cap
                eng.namespace(f"churn-{i}").append([_rec(f"churn-{i}", f"c{i}")])
            assert "home" in eng._namespaces, "pinned namespace was evicted"
            # the held reference is still live and consistent
            st = ref.stats()
            assert st["records"] == 1
        finally:
            eng.close()

    def test_busy_store_skipped_then_evicted_when_idle(self, tmp_path):
        # NOTE: NamespaceStore._lock is an RLock - holding it on the SAME
        # thread proves nothing (reentrant acquire succeeds), so the busy
        # holder must be another thread, like a real in-flight operation.
        import threading

        eng = StorageEngine(str(tmp_path / "root"), max_open_namespaces=1)
        try:
            busy = eng.namespace("busy")
            busy.append([_rec("busy", "held open")])
            acquired, release = threading.Event(), threading.Event()

            def hold():
                busy._lock.acquire()
                acquired.set()
                release.wait(10)
                busy._lock.release()

            holder = threading.Thread(target=hold, daemon=True)
            holder.start()
            assert acquired.wait(5), "holder never took the lock"
            eng.namespace("other")  # cap exceeded; 'busy' locked by other thread
            assert "busy" in eng._namespaces, "in-flight store was evicted"
            release.set()
            holder.join(timeout=5)
            eng.namespace("third")  # 'busy' idle now -> evicted on next pass
            assert "busy" not in eng._namespaces
        finally:
            eng.close()

    def test_destroyed_namespace_stays_gone_under_churn(self, tmp_path):
        eng = StorageEngine(str(tmp_path / "root"), max_open_namespaces=2)
        try:
            eng.namespace("doomed").append([_rec("doomed", "shred me")])
            assert eng.destroy_namespace("doomed")
            for i in range(5):
                eng.namespace(f"noise-{i}")
            assert not any(k.startswith("ns/doomed") for k in eng.store.list("ns/"))
            assert "doomed" not in eng._namespaces
        finally:
            eng.close()
