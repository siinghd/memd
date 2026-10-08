"""A process for tests/test_write_forwarding.py: opens a Memory on a data
root another process may hold, and writes to one namespace.

    python tests/forward_worker.py spec.json

spec: root, config, namespace, encrypt, tag, n, out (the log), ready / go
(files: "opened" / start writing), stop (a file: stop writing, flush and
close once it exists), delete_every, hard_every, ryw (check
read-your-writes after each write), sleep (between writes), hold_s (stay
open after the last write), events_every (an add_events batch of 3 every
k writes), lease_lost_ok (a write refused with LeaseLostError - this
process was frozen past its lease and resumed - is logged as "failed",
not acked, and the next one goes on).

Every acknowledged operation is appended to `out` as one JSON line with a
single write(2): what was acked survives this process - or the holder it
forwards to - being SIGKILLed.
"""
import faulthandler
import json
import os
import signal
import sys
import time

faulthandler.register(signal.SIGUSR1, all_threads=True)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.storage.objectstore import LeaseLostError  # noqa: E402


def main() -> None:
    with open(sys.argv[1]) as f:
        spec = json.load(f)
    fd = os.open(spec["out"], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)

    def log(**d) -> None:
        d["t"] = time.time()
        os.write(fd, (json.dumps(d) + "\n").encode())

    ns = spec.get("namespace", "default")
    m = Memory(spec["root"], namespace=ns, encrypt=bool(spec.get("encrypt", False)),
               config=spec.get("config") or {})
    log(op="open", holds=m.ns is not None, pid=os.getpid())
    if spec.get("ready"):
        open(spec["ready"], "w").close()
    go = spec.get("go")
    while go and not os.path.exists(go):
        time.sleep(0.005)
    tag = spec["tag"]
    every, hard_every = int(spec.get("delete_every") or 0), int(spec.get("hard_every") or 0)
    events_every = int(spec.get("events_every") or 0)
    stop = spec.get("stop")
    for i in range(int(spec["n"])):
        if stop and os.path.exists(stop):
            break
        content = f"{tag} record {i} uniq{tag}x{i}"
        if spec.get("lease_lost_ok"):
            try:
                rid = m.add(content, user_id="u1")[0]
            except LeaseLostError as ex:
                log(op="failed", i=i, error=str(ex)[:200])
                continue
            log(op="add", id=rid, content=content)
        elif events_every and i % events_every == events_every - 1:
            batch = [f"{content} part{k}" for k in range(3)]
            ids = m.add_events([{"content": c, "user_id": "u1"} for c in batch])
            for rid, c in zip(ids, batch):
                log(op="add", id=rid, content=c)
            rid = ids[0]
            content = batch[0]
        else:
            rid = m.add(content, user_id="u1")[0]
            log(op="add", id=rid, content=content)
        if spec.get("ryw"):
            got = m.get(rid)
            if got is None or got["content"] != content:
                log(op="violation", id=rid, kind="get", got=got)
            if i % 10 == 0:
                hits = [h.id for h in m.search(f"uniq{tag}x{i}", user_id="u1").items]
                if rid not in hits:
                    log(op="violation", id=rid, kind="search", got=hits)
        if every and i % every == every - 1:
            hard = bool(hard_every) and i % hard_every == hard_every - 1
            m.delete(rid, hard=hard)
            log(op="delete", id=rid, hard=hard, content=content)
            if spec.get("ryw") and m.get(rid) is not None:
                log(op="violation", id=rid, kind="delete")
        if spec.get("sleep"):
            time.sleep(float(spec["sleep"]))
    m.flush()
    log(op="done", holds=m.engine.holds(ns))
    if spec.get("hold_s"):
        time.sleep(float(spec["hold_s"]))
    m.close()
    log(op="closed")


if __name__ == "__main__":
    main()
