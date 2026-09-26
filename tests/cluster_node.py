"""`memd` for the multi-node tests, with an optional injected pause.

    python tests/cluster_node.py serve --http --node-id n1 ...

MEMD_TEST_PAUSE_AT=compact|rotate: the first time this process commits a
manifest inside NamespaceStore.compact (resp. a rotation) - AFTER the
lease/fence check and BEFORE the conditional PUT reaches the bucket, the
exact window no check can close - it writes the path in MEMD_TEST_PAUSE_MARK
and SIGSTOPs itself (every thread: the lease heartbeat stops too). The test
lets another node take the namespace over, then SIGCONTs it.

MEMD_TEST_ROTATE_FRAMES=N: rotate the WAL every N frames (default 512), so a
rotation is reachable from a few HTTP writes.

Test-only: nothing here ships; the hooks are monkeypatches.
"""
import os
import signal
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from memd.storage import engine as _engine  # noqa: E402
from memd.storage import s3store as _s3  # noqa: E402

_where = os.environ.get("MEMD_TEST_PAUSE_AT")
if _where:
    _in = threading.local()
    _meth = {"compact": "compact", "rotate": "_rotate_locked"}[_where]
    _orig = getattr(_engine.NamespaceStore, _meth)

    def _inside(self, *a, **kw):
        _in.on = True
        try:
            return _orig(self, *a, **kw)
        finally:
            _in.on = False
    setattr(_engine.NamespaceStore, _meth, _inside)

    _cas = _s3.S3ObjectStore._raw_cas_put
    _fired = [False]

    def _paused_cas(self, full, data, **kw):
        if getattr(_in, "on", False) and not _fired[0] and full.endswith("/manifest.json") \
                and "/ns/memd-node." not in full:
            _fired[0] = True
            with open(os.environ["MEMD_TEST_PAUSE_MARK"], "w") as f:
                f.write(str(os.getpid()))
            os.kill(os.getpid(), signal.SIGSTOP)   # ... SIGCONT resumes right here
        return _cas(self, full, data, **kw)
    _s3.S3ObjectStore._raw_cas_put = _paused_cas

_frames = os.environ.get("MEMD_TEST_ROTATE_FRAMES")
if _frames:
    _init = _engine.StorageEngine.__init__

    def _small_rotation(self, *a, **kw):
        kw["wal_rotate_frames"] = int(_frames)
        return _init(self, *a, **kw)
    _engine.StorageEngine.__init__ = _small_rotation

from memd.cli import main  # noqa: E402

sys.exit(main())
