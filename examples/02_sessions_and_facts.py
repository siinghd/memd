# Sessions and facts: capture turns, close a session to extract facts, supersede a fact, time-travel, survive a restart.
# Run: python examples/02_sessions_and_facts.py [DATA_DIR]   (default: a fresh temp directory)
import sys
import tempfile
import time

from memd import Kind, Memory

data = sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="memd-sessions-")
mem = Memory(data)

# observe(): the glue after each LLM call - the whole turn is one durable batch
messages = [{"role": "user", "content": "I live in Berlin and I prefer dark mode"}]
mem.observe(messages, "Noted: Berlin, dark mode.", user_id="u1", session_id="s1")
mem.add("My favourite editor is Helix", user_id="u1", session_id="s1")

# closing a session extracts facts from its raw turns (no API key needed: the
# default extractor is pattern-based; set MEMD_EXTRACTION_API_KEY for an LLM)
report = mem.close_session("s1", user_id="u1")
print("close_session:", report)
facts = mem.search("where does the user live?", user_id="u1", kinds=[Kind.FACT]).items
print("facts:", [f.content for f in facts])

# a new fact on the same entity key supersedes the old one (bitemporal: the
# old version stays readable in history, and through as_of when the new one
# says from when it holds)
old = mem.remember("The user's editor is Helix", user_id="u1", entity_keys=["user.editor"])
before = int(time.time() * 1000)
time.sleep(0.01)
new = mem.remember("The user's editor is Zed", user_id="u1", entity_keys=["user.editor"],
                   valid_from=int(time.time() * 1000))

now = mem.search("which editor does the user use?", user_id="u1", kinds=[Kind.FACT])
print("current:", now.items[0].content)
assert now.items[0].id == new
chain = mem.get(new, history=True)["history"]
print("history:", [(h["content"], h["time"]["superseded_by"] is not None) for h in chain])
then = mem.search("which editor does the user use?", user_id="u1", kinds=[Kind.FACT], as_of=before)
print("as_of before the change:", then.items[0].content)
assert then.items[0].id == old and new not in [h.id for h in then.items]
mem.close()

# a fresh process (here: a fresh Memory on the same directory) recalls it all
mem = Memory(data)
again = mem.search("which editor does the user use?", user_id="u1", kinds=[Kind.FACT])
assert again.items[0].id == new
print("after restart:", again.items[0].content, "|", mem.stats()["records"], "records")
mem.close()
