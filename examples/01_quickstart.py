# Embedded quickstart: remember, search, forget (preview, then confirm) and a hard delete purged from disk. No server.
# Run: python examples/01_quickstart.py [DATA_DIR]   (default: a fresh temp directory)
import os
import sys
import tempfile

from memd import Memory
from memd.engine.memory import forget_fingerprint

data = sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="memd-quickstart-")
mem = Memory(data)  # owns the directory: one process per data root

# raw lane: what was said, durable and searchable when the call returns
mem.add("We deploy with `make ship`, never from CI", user_id="u1", session_id="s1")
mem.add("The staging database is postgres 16", user_id="u1", session_id="s1")
# explicit lane: "remember this", a high-trust fact on an entity key
mem.remember("The user prefers dark mode", user_id="u1", entity_keys=["user.theme"])

hits = mem.search("how do we deploy?", user_id="u1", budget_tokens=500)
print("top hit:", hits.items[0].content)
print(hits.packed_context)  # provenance-tagged, ready to put in a prompt
assert "make ship" in hits.items[0].content

# forget by query: preview what WOULD be deleted, then confirm exactly that set
ids = mem.find_ids("staging database", user_id="u1")
print("forget would delete:", [mem.get(i)["content"] for i in ids])
deleted = mem.forget("staging database", user_id="u1", expected=forget_fingerprint(ids))
assert deleted == ids and all(mem.get(i) is None for i in ids)

# hard delete: gone from reads at once; physically purged by compaction
# (scheduled within 72 h by default, forced here)
secret_id = mem.add("my door code is 4417-ZEBRA", user_id="u1")[0]
mem.delete(secret_id, hard=True)
assert mem.get(secret_id) is None
assert not mem.search("door code", user_id="u1").items
print("compaction:", mem.compact(force=True))
mem.close()

leftovers = [os.path.join(d, f) for d, _, files in os.walk(data) for f in files
             if b"4417-ZEBRA" in open(os.path.join(d, f), "rb").read()]
assert not leftovers, leftovers
print("hard-deleted text is in no file under", data)
