# REST door: talk to `memd serve --http` with the Python HostedMemory SDK (and Memory(api_key=...), the same API).
# Run: python examples/04_http_server_sdk.py   (uses MEMD_URL + MEMD_API_KEY [+ MEMD_NAMESPACE] if set, else starts a throwaway local server)
import contextlib
import json
import os
import sys

from memd import Memory
from memd.sdk import HostedError, HostedMemory


def run(base_url: str, api_key: str, namespace: str) -> None:
    mem = HostedMemory(api_key=api_key, base_url=base_url, namespace=namespace)

    # the same calls as the embedded engine, over REST
    mem.add("We deploy with `make ship`, never from CI", user_id="u1", session_id="s1")
    print("namespaces this key sees:", mem.status()["namespaces"])
    fact = mem.remember("The user prefers dark mode", user_id="u1", entity_keys=["user.theme"])
    hits = mem.search("how do we deploy?", user_id="u1", budget_tokens=500)
    print(hits.packed_context)
    assert "make ship" in hits.items[0].content

    # two lines in an agent loop: inject packed context, then capture the turn
    messages = [{"role": "user", "content": "Remind me how we deploy"}]
    packed = mem.pack(messages, user_id="u1")
    assert packed[0]["role"] == "system" and "make ship" in packed[0]["content"]
    mem.observe(messages, "With `make ship`.", user_id="u1", session_id="s1")
    print("close_session:", mem.close_session("s1"))

    # forget: the preview's fingerprint makes the confirm delete exactly that set
    preview = mem.forget("dark mode", user_id="u1")
    print("forget preview:", preview["count"], "record(s)")
    deleted = mem.forget("dark mode", user_id="u1", confirm=True, fingerprint=preview["fingerprint"])
    assert fact in deleted and mem.get(fact) is None

    # errors are typed by status and a machine-readable code
    try:
        mem.search("x", namespace="someone-elses-namespace")
    except HostedError as e:
        print("other namespace refused:", e.status, e.code)
        assert e.status == 403

    lines = mem.export_jsonl().splitlines()
    print("export:", len(lines), "lines; first kind:", json.loads(lines[0]).get("kind"))
    mem.close()

    # Memory(api_key=...) is the embedded facade in hosted mode: same methods
    remote = Memory(api_key=api_key, base_url=base_url, namespace=namespace)
    assert remote.search("how do we deploy?", user_id="u1").items
    remote.close()


if os.environ.get("MEMD_URL") and os.environ.get("MEMD_API_KEY"):
    server = contextlib.nullcontext((os.environ["MEMD_URL"], os.environ["MEMD_API_KEY"],
                                     os.environ.get("MEMD_NAMESPACE", "default")))
else:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from local_server import local_server

    server = local_server("demo")

with server as (url, key, ns):
    run(url, key, ns)
print("ok")
