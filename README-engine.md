# memd — the SQLite of agent memory

Embedded-first agent memory engine: one process, zero external services.
Raw + fact lanes, bitemporal supersedence, provenance/trust tiers, hybrid
retrieval, budget-aware packing. Apache-2.0, capability-complete.

## Ten-minute story (acceptance-tested by `scripts/ten_minute_test.sh`)

```bash
pip install "memd-engine[local-embeddings]"   # Python >= 3.11. Without the extra, memd uses hash embeddings.
```

```python
from memd import Memory

mem = Memory("./my-data")                          # embedded; Memory(api_key=...) = hosted, same API

# two lines in any agent loop:
messages = mem.pack(messages, user_id="u1")        # inject packed, provenance-tagged context
mem.observe(messages, response, user_id="u1")      # capture the turn after your LLM call

# or explicitly:
mem.add("We deploy with `make ship`, never CI", session_id="s1", user_id="u1")
hits = mem.search("how do we deploy?", user_id="u1", budget_tokens=1500)
print(hits.packed_context)                         # ready to inject
```

Kill the process, restart, ask "what did we decide yesterday?" — cross-session
recall works (`scripts/ten_minute_test.sh` proves it from a fresh directory).

## Running it

### 1. Embedded (a library, no server)
Nothing to start. `Memory("./my-data")` opens the directory; see the snippet
above. Other processes may open it too: each namespace is written by one of
them at a time (see "Several processes on one data root" below).

### 2. REST server
```bash
export MEMD_ADMIN_KEY="$(python -c 'import secrets;print(secrets.token_urlsafe(32))')"
memd serve --http --host 127.0.0.1 --port 8700     # MEMD_DATA=./memd-data by default

# mint a per-tenant key (the admin key is namespace "*"; do not hand it to apps)
memd key create --namespace acme
```
```bash
curl -sX POST localhost:8700/v1/ns/acme/memories \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"content":"we deploy with make ship, never CI","user_id":"u1"}'

curl -sX POST localhost:8700/v1/ns/acme/search \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"how do we deploy?","user_id":"u1"}'
```
`/docs` and `/openapi.json` are **off by default** (they enumerate every route
of an otherwise authenticated API). Set `MEMD_ENABLE_DOCS=1` for development.

### 3. MCP (Claude Desktop, Cursor, any MCP client)
```json
{ "mcpServers": { "memd": { "command": "memd", "args": ["serve", "--mcp"],
                            "env": { "MEMD_DATA": "/abs/path/to/memd-data" } } } }
```

### 4. Docker
```bash
docker build -t memd/memd .
docker run -d -p 8700:8700 -e MEMD_ADMIN_KEY=... -v memddata:/data memd/memd serve --http
```
The default image does not include the Stripe SDK (~26 MB installed; only
hosted billing uses it). For `serve --http --hosted` with billing, build the
variant: `docker build --build-arg MEMD_BILLING=1 -t memd/memd:hosted .`
(the build fails if the billing install does). For `s3://` data roots and
the `aws-kms` key provider, add `--build-arg MEMD_S3=1` (boto3).
The image runs as **uid 10001**, so a *named volume* (above) works but a
**bind mount does not** unless you either pass `--user "$(id -u):$(id -g)"` or
`chown 10001` the host directory. Both are verified; pick one deliberately.

## Several processes on one data root

A namespace has **one writer at a time**: the process holding its lock file
(a local data root) or its lease (an `s3://` root, below). Two writers on one
namespace silently destroyed acked data (measured: 8 of 150 writes lost),
which is why the lock exists. A second process that opens a namespace
another one holds does not write it: it **forwards** to the holder (on by
default).

```python
# process 1                                  # process 2, same directory
mem = Memory("./my-data")                    mem = Memory("./my-data")     # "default" is held by process 1
mem.add("we ship on Fridays", user_id="u1")  mem.add("...", user_id="u1")  # runs in process 1
                                             mem.search("when do we ship?", user_id="u1")  # so does this
```

- **Writes and strong reads run in the holder.** Every process that may
  write runs a small endpoint (TCP, `127.0.0.1`, an ephemeral port) and puts
  its address in the lock file or lease it holds. A process finding a
  namespace busy reads it there and sends the call over - `add`,
  `add_events`, `observe`, `remember`, `close_session`, `delete`,
  `delete_many`, `forget`, `destroy_namespace`, `compact`, `reembed`, and
  the strong reads `search`, `get`, `find_ids`, `stats` and `export`
  (streamed). The holder runs the same code a local call runs, so
  quarantine, supersedence, session taint and the audit ledger stay the
  writer's. A forwarding process reads its own writes: its strong reads are
  answered by the process that applied them.
- **Eventual reads stay in the process.** `search(...,
  consistency="eventual")` and `get(..., consistency="eventual")` are served
  by the forwarding process's own [read replica](#read-replicas-eventual-reads):
  no hop, at most the staleness bound behind.
- **Takeover.** When the holder goes away - a clean close, a crash, `kill
  -9` - its lock frees at once (a lease after `MEMD_LEASE_TTL_S`), and the
  next forwarded call takes the namespace: that process opens it (replaying
  its log), becomes its writer and runs the call itself; the others forward
  to it. A clean close first lets the forwarded calls already running finish,
  and answers new ones "not the writer", so their callers move on; a call
  still running after the close's drain can no longer write (the closed
  store refuses it), so nothing lands after the lock is released. Taking
  a namespace over means decrypting it: on an `s3://` root with the
  default `local` key provider the data keys live in a `local_dir`, so a
  process with a `local_dir` of its own cannot take over (`KeyCustodyError`)
  - share the `local_dir` between the processes or use a remote key
  provider (`aws-kms`, `vault-transit`, below). A holder frozen past its
  lease (a VM pause, `SIGSTOP`) and resumed has its own calls in flight
  fail with `LeaseLostError` - none of them is acknowledged - and its next
  calls go to the new writer.
- **Exactly once.** Every forwarded call carries a request id, and a call
  that creates records carries their ids, generated by the caller. A retry -
  after a lost reply, a timeout, or a holder that died in the middle of the
  call - is answered from the first attempt by a live holder (or waits for
  the attempt still running), and is recognised by a new holder in the log
  it replayed: no acknowledged write is lost, none is applied twice. A
  write never runs in a process that does not hold its namespace: the
  holder checks that it still does before it runs a forwarded call, and the
  local path always goes through the lock or lease.
- **Backpressure and timeouts.** A holder runs at most
  `forward_max_inflight` (64) forwarded calls at once, on a pool of that
  many reused threads, over at most 256 connections; a call that waits
  10 s for a slot is answered "overloaded"
  and retried by its caller. A call keeps looking for a writer that answers
  for `forward_wait_s` (30 s on a local root, the lease TTL + 15 s on S3),
  then raises `ForwardingError` - a `NamespaceBusyError`; nothing was
  applied - or `ForwardTimeoutError` when an attempt of a write may have
  been. One attempt waits for its answer at most `forward_timeout_s` (300
  s); on S3 it stops waiting as soon as the holder's lease goes stale (a
  frozen holder is fenced when it resumes).
- **Who may call.** A process must hold the root's forwarding secret:
  `<data dir>/forward.secret`, created on first use with mode 0600 (on S3:
  in the `local_dir`), or `forward_secret` / `MEMD_FORWARD_SECRET` - which
  processes on an `s3://` root with different `local_dir`s need. The file is
  read without following a symbolic link and only if it is a regular file
  of the process's own user that no other user may read or write; anything
  else is refused, and that process runs with forwarding off. Both sides
  prove the secret, and every frame after that is integrity-protected;
  nothing is encrypted ([SECURITY.md](SECURITY.md)). A process with another
  secret is refused (`ForwardAuthError`, at open when the facade's own
  namespace is the busy one); one that cannot read or create the file (a
  data directory shared with another user) runs with forwarding off, and
  says so in its log.
- **Off.** `Memory(path, forwarding="off")` (config `forwarding`,
  `MEMD_FORWARDING=off`) keeps the old behaviour: a busy namespace raises
  `NamespaceBusyError`. A holder with forwarding off - or an older memd -
  advertises no endpoint, and a process finding a namespace held by it gets
  `ForwardingError` at once. Cluster nodes (`--node-id`) and hosted servers
  run with forwarding off: the cluster router routes between nodes, and a
  hosted session close settles its quota through a callback that cannot run
  in another process.

Concretely: `uvicorn --workers N` (`MEMD_DATA=./memd-data uvicorn
memd.cli:create_app_from_env --factory --workers 4`), one `memd serve --mcp`
per MCP client, or a script beside a running server all work on one data
root - each namespace is written by one of them at a time and the others
forward to it (tested: three uvicorn workers, every request served; with
forwarding off, all but the first fail to start). That adds
a hop, not write capacity: to scale writes, spread *namespaces* across
processes, which is what [multi-node serving](#multi-node) does on an
`s3://` data root. Processes in different network namespaces (two containers
on one volume, two hosts on one bucket) cannot reach each other's loopback:
bind `forward_host` (`MEMD_FORWARD_HOST`) to a reachable interface and set
`forward_advertise` to the `host:port` the others connect to - a process
finding a namespace held on another host behind a loopback-only endpoint
gets `ForwardingError` at once. `MEMD_ALLOW_MULTI_PROCESS=1` disables the
lock (and forwarding) and re-enables the data loss; it exists for recovery
tooling, not for serving.

A record deleted between a write and that write's retry stays deleted: the
holder runs the write only if none of its record ids was ever written to
the namespace - a soft-deleted record keeps its row until a compaction
drops it, and a hard delete, whose row goes at once, leaves its id in the
index for a day (replayed from the log by a holder that takes the namespace
over). The exceptions, both needing the index to be rebuilt from the bucket
alone (a holder without that index's cache taking over) before the retry
arrives: a record soft-deleted and then compacted away, or hard-deleted and
purged - its id is then no longer known, and the retry writes it again.

Not covered by the exactly-once rule: a session close, a destroy or a
compaction retried on a NEW holder (the first one died after running it)
runs again - a session close's consolidation drops the facts the first run
already wrote; a delete retried that way answers `False` (the first attempt
deleted it); and an export cut off mid-stream raises and is not resumed.
memd has no mode with several writers in one namespace (several logs).

**Measured** (`bench/forward_bench.py`: a holder process and this one on
one 8-core machine, loopback TCP, hash embedder, no reranker, encryption on;
each phase writes into a fresh namespace and reads one corpus of 2,000
records; the mean of two runs):

| | write ack p50 / p90 / p99 | writes/s, 1 / 4 / 8 threads | strong get p50 | strong search p50 / p99 |
|---|---|---|---|---|
| local root, the holder itself | 3.0 / 4.3 / 7.2 ms | 295 / 585 / 597 | 0.05 ms | 15.2 / 20.1 ms |
| local root, forwarded | 3.9 / 5.7 / 12.1 ms | 228 / 448 / 462 | 0.66 ms | 17.0 / 23.0 ms |
| S3 (MinIO on loopback), the holder itself | 9.5 / 11.8 / 30.3 ms | 59 / 58 / 72 | 0.05 ms | 15.0 / 34.7 ms |
| S3 (MinIO on loopback), forwarded | 10.4 / 13.3 / 30.9 ms | 51 / 57 / 95 | 0.69 ms | 17.7 / 62 ms |

A forwarded call costs one round trip plus its encoding: about 0.6 ms (a
strong `get`, nothing else to do), about 1 ms on a write - the holder also
checks the call's record ids against the namespace - and 1.5-3 ms on a
search, whose packed result travels back. On a local root a forwarding
process writes at ~77% of the holder's own throughput, at 1, 4 or 8 threads;
on S3, where the append itself is the cost, ~86% with one thread. (The
forwarded 8-thread S3 row is
above the local one in both runs: S3 appends are one conditional PUT each,
and the holder's own threads queue on them differently; it is not a gain to
plan on.) Failover, measured by the multi-process tests (a holder writing,
two processes forwarding, the holder killed with SIGKILL): the forwarders'
acknowledgements paused 0.7-1.1 s on a local root (the lock frees at once;
the pause is the new writer opening the namespace) and 3.0-3.2 s on S3 with
a 3 s lease TTL; no acknowledged write lost or duplicated in any run. A
holder frozen past its lease (SIGSTOP) was taken over after 3.8 s - or ~31 s
when it froze in the middle of a PUT, whose object MinIO keeps locked until
its timeout (AWS S3 does not lock).

| setting | default | |
|---|---|---|
| `forwarding` / `MEMD_FORWARDING` | `auto` | `off`: a busy namespace raises `NamespaceBusyError` |
| `forward_secret` / `MEMD_FORWARD_SECRET` | `<data dir>/forward.secret` | at least 16 characters |
| `forward_host` / `MEMD_FORWARD_HOST` | `127.0.0.1` | the endpoint's bind address |
| `forward_port` / `MEMD_FORWARD_PORT` | `0` (ephemeral) | |
| `forward_advertise` / `MEMD_FORWARD_ADVERTISE` | the bind address | `host:port` other processes connect to; required when binding every interface |
| `forward_wait_s` / `MEMD_FORWARD_WAIT_S` | 30 (S3: lease TTL + 15) | how long a call looks for a writer that answers |
| `forward_timeout_s` / `MEMD_FORWARD_TIMEOUT_S` | 300 | how long one attempt waits for its answer |
| `forward_max_inflight` / `MEMD_FORWARD_MAX_INFLIGHT` | 64 | forwarded calls a holder runs at once |
| `forward_queue_wait_s` / `MEMD_FORWARD_QUEUE_WAIT_S` | 10 | how long a forwarded call waits for one of them before the holder answers "overloaded" (its caller retries) |

### Search throughput: more processes, not more threads

The default search (a 12K session pack) is CPU work in Python. In one
process, more threads do not give more searches per second, because of the
GIL. Retrieval alone (a 2K flat pack) scales with threads, because SQLite
releases the GIL. To serve more default searches, run more processes:

- `uvicorn --workers N`, or more processes on one data root (they forward
  to the writer of each namespace, as above);
- read replicas, for eventual reads ([read replicas](#read-replicas-eventual-reads));
- more nodes on an `s3://` root ([multi-node](#multi-node)).

## Object storage as the source of truth (S3-compatible stores)

memd runs on AWS S3, Cloudflare R2 and other S3-compatible stores with
conditional writes. CI runs the S3 tests against RustFS.

```bash
pip install "memd-engine[s3]"
```
```python
mem = Memory("s3://my-bucket/memd", config={
    "local_dir": "./.memd-local",          # index cache + envelope keys stay here
    # endpoint/credentials optional: the normal boto3 chain applies
    "s3_endpoint_url": "http://127.0.0.1:9000",
})
```

S3 has no append, and `append()` is the WAL's whole contract. Each append is
its own immutable object (`<key>.__part-000000000042`) and the logical object
is their ordered concatenation — so **one durable write ack = one PUT**, and a
torn frame cannot exist. The read-modify-write alternative would transfer
O(WAL) bytes per ack and was rejected on both latency and cost.

Measured against a real S3 server (MinIO on localhost — see the caveat below):

| | measured | SLO |
|---|---|---|
| durable write ack | p50 **8.8 ms**, p90 **10.1 ms** | p50 ≤ 150 ms, p90 ≤ 300 ms |
| warm retrieval | p50 **26.0 ms**, p99 **71.2 ms** | p50 ≤ 100 ms, p99 ≤ 400 ms |
| cold node (bucket only, no local cache) | open **170 ms** + first query **38 ms** | first query p90 ≤ 1.5 s |
| **object-store round trips per write** | **1 PUT** | O(1) |
| **object-store round trips per warm search** | **0** | O(1) |

The latencies are from a server on loopback and are a floor, not a forecast for
S3-across-a-WAN. The **round-trip counts are not** — they are structural, and
they are the number that decides both the cost model and how a WAN changes
things. A warm search touches object storage zero times because retrieval is
served by the local derived index.

Timeouts: the writer's data client (segments, log parts, the manifest) has a
10 s connect timeout and botocore's 60 s read timeout - an append is a
conditional PUT, never retried early; leases and the cluster registry go
through a client of their own with 2 s / 4 s, and a read replica's bucket and
KMS calls through clients with `MEMD_REPLICA_CONNECT_TIMEOUT_S` /
`MEMD_REPLICA_READ_TIMEOUT_S` (2 s / 5 s; see "Read replicas").

What stays local: the SQLite derived index (rebuildable by contract — the
index snapshot published to the store is what makes a cold node cheap) and, with the default `local`
key provider, the envelope **keys**. Then data is remote and keys are not:
*one node with remote durability*. Crypto-shred works (destroy the local key
and the ciphertext is inert), but a second node cannot decrypt. With a remote
key provider (`aws-kms` or `vault-transit`, below) the wrapped keys live in
the bucket too, and any authorised node can serve any namespace.

Back the keys up with the data and restore them together (the keys directory
for `local`; the `keys/` objects for a remote provider). A namespace opened
with a key its data was not written with (another deployment's key files or
wrapped key object, a replaced root key), without one, or with encryption off
raises `KeyCustodyError` and nothing is changed — it is never read as empty,
and never compacted away. A complete log frame that does not read is never cut
off as a torn tail either; [SECURITY.md](SECURITY.md) has the recovery
procedure for one that is damaged.

Single-writer is still enforced, by a **lease** rather than a file lock (`flock`
cannot see another machine): the first writer claims `ns/<ns>/.owner` with a
conditional PUT, a second does not get it (it forwards to the first - see
[several processes on one data root](#several-processes-on-one-data-root) -
or, with forwarding off, gets `NamespaceBusyError`), and a lease older than the
TTL (`MEMD_LEASE_TTL_S`, default 60 s) is reclaimable - by compare-and-swap on
its ETag, so exactly one of several reclaimers wins - so a crashed node cannot
wedge a namespace forever. A holder that cannot renew for 2/3 of the TTL stops
writing (self-fencing).

### Key custody: local, AWS KMS or Vault transit

| `MEMD_KEY_PROVIDER` | root key held by | wrapped data keys | settings |
|---|---|---|---|
| `local` (default) | `<local_dir>/keys/root.key` | files beside it | - |
| `aws-kms` | AWS KMS (the CMK never leaves it) | `keys/<ns>.dek` in the store | `MEMD_KMS_KEY_ID` (ARN, id, alias; may contain `{namespace}`), `MEMD_KMS_REGION`/`AWS_REGION`, `MEMD_KMS_ENDPOINT` (LocalStack), `MEMD_KMS_SHRED` |
| `vault-transit` | Vault transit engine | `keys/<ns>.dek` in the store | `VAULT_ADDR`, `VAULT_TOKEN`, `MEMD_VAULT_TRANSIT_KEY` (default `memd`; may contain `{namespace}`), `MEMD_VAULT_TRANSIT_MOUNT`, `VAULT_NAMESPACE`, `MEMD_VAULT_SHRED` |

The same settings work as `Memory(config={"key_provider": "aws-kms",
"kms_key_id": ...})`. Every data key is bound to its namespace (KMS
encryption context / transit associated data). Moving an existing store:

```bash
memd keys status  --data s3://bucket/memd            # MEMD_LOCAL_DIR = the node holding the keys
memd keys migrate --data s3://bucket/memd --to aws-kms   # stop the server first
memd keys rotate  --data s3://bucket/memd            # re-wrap under the current key version
```

`migrate` never re-encrypts data (the data key is unchanged, only its
wrapping moves), is idempotent and crash-safe (rerun it), and removes the
local key files only after every namespace verified under the new provider.
A node still set to `local` then refuses the store instead of minting keys.
What crypto-shred means with a shared CMK is spelled out in SECURITY.md.

## Multi-node

Several `memd serve --http` processes on ONE `s3://` data root serve every
namespace between them: scale by namespace, one writer per namespace at a
time.

```bash
export MEMD_DATA=s3://bucket/memd MEMD_S3_ENDPOINT=... AWS_REGION=eu-west-1
export MEMD_KEY_PROVIDER=aws-kms MEMD_KMS_KEY_ID=arn:aws:kms:...:key/...
export MEMD_CLUSTER_SECRET=$(openssl rand -hex 32)   # the same on every node
export MEMD_STATE_DIR=/srv/memd-state                # shared: API keys / hosted admin db
memd serve --http --node-id n1 --host 10.0.0.1 --port 8700   # MEMD_LOCAL_DIR per node
memd serve --http --node-id n2 --host 10.0.0.2 --port 8700
```

Point clients (or a plain load balancer) at ANY node:

- **Routing.** The node holding a namespace's lease serves it. Another node
  receiving the request **proxies** it there (one base URL for clients; a
  handoff in progress is retried by the node for up to `MEMD_ROUTE_RETRY_S`,
  default 3 s, before a `503 namespace_unavailable` with `Retry-After`). A
  namespace nobody holds goes to the node that rendezvous hashing picks among
  the live nodes - a hint only; the lease is authoritative.
- **Membership** is a heartbeat object per node in the bucket
  (`_cluster/nodes/<id>.json`, every TTL/3), judged from one LIST on the
  bucket's own clock. No gossip, no consensus, nothing else to run.
  `--advertise` (`MEMD_ADVERTISE_URL`) is the URL peers use.
- **Handoff.** SIGTERM: the node deregisters, drains, flushes and releases
  its leases - peers take over at once. A crash: its leases go stale after
  `MEMD_LEASE_TTL_S` and the next request for each namespace is served (and
  the lease reclaimed) by a live node. A frozen node (GC pause, VM freeze) is
  fenced when it resumes: its writes fail instead of landing in the new
  owner's log.
- **Hosted billing** works through the router: a request is authenticated and
  metered by the node that executes it, once. The admin database is SQLite in
  `MEMD_STATE_DIR`, so a hosted fleet runs on one host (or a volume with
  working POSIX locks). memd has no networked admin store for hosted mode
  on several hosts.

To try it on one machine: `docker-compose.yml` runs an S3 server (RustFS),
three nodes and (with `--profile vault`) a Vault dev server holding the data
keys - see the comment at its top. `fly.toml` is a Fly.io template (one
machine = one node, node id and address from the machine; fill in every
`CHANGE-ME`; not deployed). The image needs `--build-arg MEMD_S3=1` for boto3.

Every object memd rewrites in place (the manifest commit above all) is
written with compare-and-swap, so a node frozen mid-commit and resumed after
a takeover fails with `503 lease_lost` instead of overwriting its successor.

Measured on 3 node processes (MinIO + moto KMS, TTL 4 s): leaseholder
SIGKILLed → next write acked after 3.6-3.8 s (bound: TTL + ε); SIGTERM →
0.03-1.1 s; frozen past its TTL → 4.8-6.2 s, with no acked write lost in any
case. On MinIO a node frozen in the middle of a lease write holds that
object's lock for MinIO's ~30 s timeout, and its takeover waits for it
(observed in 2 of 14 frozen runs; AWS S3 does not lock). memd has no
mode with several writers in one namespace.

### Read replicas (eventual reads)

Reads are **strong by default**: the node holding the namespace's lease
serves them, as above. A search or a get may opt into **eventual
consistency**, and is then served by a **read replica** - on whichever node
received it, with no hop - so a load balancer in front spreads a hot
namespace's reads over every node:

```bash
curl -X POST "$NODE/v1/ns/acme/search" -H "Authorization: Bearer $KEY" \
     -H "X-Memd-Read-Consistency: eventual" -H "X-Memd-Max-Staleness-Ms: 5000" \
     -d '{"query": "how do we deploy?"}'
# X-Memd-Served-By: replica   X-Memd-Replica-Seq: 4211   X-Memd-Replica-Age-Ms: 840
```

```python
mem = HostedMemory(api_key=..., base_url=..., consistency="eventual")   # or per call
mem.search("how do we deploy?"); mem.last_read   # {"served_by": "replica", "applied_seq": ..., "age_ms": ...}
```

- A replica takes **no lease and never writes or deletes an object** (no
  manifest, part, fence, key, custody marker, audit entry or snapshot) - it
  is opened over a read-only view of the store and the keys. It bootstraps
  from the published index snapshot and the segments, then follows the
  writer: every `MEMD_REPLICA_REFRESH_S` (2 s) it reads the manifest, the
  WAL and ops tails since its last read (S3: one LIST and a GET per new
  part), and the manifest again; a compaction, takeover or new tenure
  rebuilds it from the bucket, a rotation is caught up.
- **Staleness bound**: every write acknowledged before the replica's last
  refresh started is served. A read accepts a replica no older than
  `X-Memd-Max-Staleness-Ms` (default 3 x the refresh interval); for an
  older one the read waits for a refresh at most
  `MEMD_REPLICA_REFRESH_WAIT_MS` (1 s) - never on one already running that
  long (a bucket or KMS that hangs) - and a read it still cannot serve - or
  one that hits a key-custody or store error - goes to the writer instead,
  invisibly. A read that can go to the writer waits no longer for a
  replica's open either: the open goes on in the background, and reads go
  to the writer until it completes (an embedded `read_only` Memory, with no
  writer to go to, waits for it). A replica that failed to open is not
  opened again by reads for 5 s (doubling to 60 s while it keeps failing):
  they go to the writer at once. A replica's bucket and KMS calls have short timeouts of their own
  (below); the writer's data client has a 10 s connect timeout and
  botocore's 60 s read timeout. Every search and get answers
  `X-Memd-Served-By: leader|replica` (a replica adds `X-Memd-Replica-Seq`
  and `X-Memd-Replica-Age-Ms`, the age of the state the read was served
  from).
- **Read-your-writes** holds for strong reads only: an eventual read may
  miss a write acknowledged less than the bound ago, and may still serve a
  record deleted that recently. A hard delete stops being served by every
  replica within the bound, and is scrubbed from the replica's files when
  it applies it - or when it builds them from durable data that still
  holds it, its purge pending (SECURITY.md). A replica never serves an
  older state than one it has served: a rebuilt replica serves nothing
  until the refresh that rebuilt it has applied the log tail too (a read
  waits for that at most `MEMD_REPLICA_REFRESH_WAIT_MS`, then is refused:
  over the router it goes to the writer).
- Served by replicas: `search` and `get` (`GET .../memories/{id}`). Always
  the writer: every write, `export` (audited in the namespace's ledger, which
  only the writer appends to), `find_ids` / `forget` (a preview must match
  what the confirm deletes), `stats`. A replica-served search is audited in
  the serving node's own ledger (`memd-node.<id>`, `replica_search`).
- A replica needs the namespace's data key like a writer (provider access
  for `aws-kms` / `vault-transit`); it never mints one, and a key it cannot
  use refuses (over the router, the read then goes to the writer).
- Embedded: `Memory(path, read_only=True)` opens every namespace as a
  replica - every mutating call raises `ReadOnlyError`, nothing is written
  to the store or the keys directory, and reads follow the writer (another
  process) within the bound. With no writer to fall back to, a read the
  replica refuses raises `ReplicaUnavailableError` to the caller: after a
  rebuild until the log tail is applied, when the replica is staler than
  the bound and its refresh has run longer than the read's wait, or after
  a failed rebuild. Retry it, or read from the writer. `search(..., consistency="eventual")` /
  `get(...)` on a writer `Memory` reads its own store when the namespace is
  open in it, else a replica.
- A namespace destroyed (crypto-shredded) and created again under its name
  is followed like any new tenure: the replica rebuilds and resolves the
  new key from its record. A replica whose rebuild failed (the bucket or the
  key provider unreachable) serves nothing until a rebuild completes - the
  reads go to the writer - and tries again on its next refresh.

**Measured** (`bench/replica_bench.py`: three node processes on one MinIO
bucket on loopback, moto KMS, hash embedder, no reranker, one namespace of
2,000 records; everything on one 8-core machine, so the absolute latencies
are a floor and the throughput ceiling is this machine):

| replica lag, write ack -> first eventual read that sees it | p50 | p90 | p99 | max |
|---|---|---|---|---|
| refresh every 2 s (default), 300 samples | 996 ms | 1,815 ms | 2,005 ms | 2,014 ms |
| refresh every 0.5 s, 200 samples | 282 ms | 488 ms | 525 ms | 538 ms |

The lag is the phase against the refresh cycle plus one refresh (~10-20 ms
here); no write was missed. Eventual searches, 12 client threads, 20 s per
row, in two query mixes - **repeat**: 8 fixed queries, so nearly every
search is a hit in the node's repeat-query cache (this measures the request
path around the cache, not a search); **unique**: the same queries made
unique by an extra term that matches nothing, so every search is a cache
miss (the search itself). The hit rate is each run's own `/metrics` count:

| query mix | serving nodes | searches/s | p50 | p99 | cache hits |
|---|---|---|---|---|---|
| repeat | 1 (the writer) | 155.8 | 71.4 ms | 250.9 ms | 99.4% |
| repeat | 2 (writer + 1 replica) | 333.1 | 34.0 ms | 89.0 ms | 99.8% |
| repeat | 3 (writer + 2 replicas) | 509.1 | 22.1 ms | 47.4 ms | 99.9% |
| unique | 1 (the writer) | 35.4 | 338.7 ms | 571.5 ms | 0.0% |
| unique | 2 (writer + 1 replica) | 94.0 | 125.6 ms | 252.2 ms | 0.0% |
| unique | 3 (writer + 2 replicas) | 170.3 | 67.6 ms | 147.3 ms | 0.0% |

Each serving node took an equal share. The cache-miss mix is the measure of
the read capacity replicas add; it scales better than linearly here only
because one node is far past saturation with all 12 clients on it (35
searches/s per node with 12 clients, 47 with 6, 57 with 4) - at a load one
node absorbs, expect about linear. The writer's own latency with replicas
attached (300 writes and strong searches): write p50 / p99 16.5 / 32.3 ms
with no replica open, 16.4 / 27.9 ms with two attached, 17.4 / 43.8 ms with
two serving a read load (shared CPU). A refresh with nothing new is 4
object-store requests (2 manifest GETs, a LIST of each log) - ~2 requests/s
per open replica at the default interval, which is why idle replicas close.

**Outages** were measured separately, not by the benchmark: on the same
machine, with a TCP proxy that refuses connections, or accepts them and
never answers, in front of the replica's bucket or of one node's KMS. The
bucket hanging for 150 s under an embedded `read_only` Memory refreshing
every 0.5 s: no eventual read waited longer than 1.0 s before it was
refused (over the router it goes to the writer), and the replica served
again 0.9 s after the bucket's return. One node's KMS, in a four-node HTTP
cluster on the same MinIO and moto KMS refreshing every 0.5 s, for three
namespaces another node writes: refusing connections, the first eventual
read of each namespace on that node went to the writer after 0.2-0.5 s
(up to about 1.1 s on a loaded machine);
hanging, after 1.0-1.1 s (the read's wait - the replica's open timed out
in the background, 2 x 5 s); the following reads went to the writer in
~20 ms.

| setting | default | |
|---|---|---|
| `--node-id` / `MEMD_NODE_ID` | - | turns cluster mode on |
| `MEMD_CLUSTER_SECRET` | - | required, >= 16 chars, signs proxied requests |
| `--advertise` / `MEMD_ADVERTISE_URL` | `http://HOST:PORT` | required when binding 0.0.0.0 |
| `MEMD_LEASE_TTL_S` | 60 | failover bound; keep clock skew under TTL/3 |
| `MEMD_ROUTE_RETRY_S` | 3 | how long an entry node absorbs a handoff |
| `MEMD_STATE_DIR` | - | required: keys.toml.json / hosted admin db |
| `MEMD_KEY_PROVIDER` | - | required: `aws-kms` or `vault-transit` (a `local` key file cannot be shared) |
| `MEMD_LOCAL_DIR` | `.memd-local` | node-local cache, under `<dir>/node-<id>` |
| `MEMD_CACHE_SWEEP_S` | 300 | how often a node drops its local copies of namespaces another node took over (also at startup; `0` = never) |
| `MEMD_REPLICA_REFRESH_S` | 2 | how often a read replica follows its writer (the staleness bound's base; `replica_refresh_s` in `Memory(config=...)`) |
| `MEMD_REPLICA_MAX_STALENESS_MS` | 3 x refresh | the default bound of an eventual read (`X-Memd-Max-Staleness-Ms` per request) |
| `MEMD_MAX_REPLICAS` | 64 | replica namespaces per node (LRU; a closed replica's cache is deleted) |
| `MEMD_REPLICA_IDLE_S` | 300 | a replica nobody read for this long is closed (each costs 4 object-store requests per refresh with nothing new) |
| `MEMD_REPLICA_REFRESH_WAIT_MS` | 1000 | the longest an eventual read waits for a replica's refresh (or its open) before it goes to the writer |
| `MEMD_REPLICA_CONNECT_TIMEOUT_S` / `MEMD_REPLICA_READ_TIMEOUT_S` / `MEMD_REPLICA_MAX_ATTEMPTS` | 2 / 5 / 2 | a replica's bucket and KMS calls (its own clients; the writer's data client has a 10 s connect timeout and keeps botocore's 60 s read timeout - an append is a conditional PUT that must not be retried early) |
| `MEMD_S3_ACCESS_KEY` / `MEMD_S3_SECRET_KEY` | boto3 chain | bucket credentials, separate from the `AWS_*` ones KMS uses |
| `MEMD_SHUTDOWN_GRACE_S` | 30 | uvicorn graceful drain on SIGTERM |

## Hosted mode & billing

**Off by default.** Embedded memd and a plain `memd serve --http` behave exactly
as described above and never import `stripe`. Hosted mode adds tenancy, usage
metering, plan entitlements and Stripe billing for running memd as a service:

```bash
pip install "memd-engine[billing]"                      # the Stripe SDK, imported lazily
export MEMD_HOSTED=1                             # or: memd serve --http --hosted
memd org create --name acme                      # -> {"org": "org_..."}; plan "free"
memd key create --hosted --org org_... --ns acme --scopes memory,billing
memd serve --http --hosted
```

**Tenancy: org -> namespaces -> API keys.** An org is the billing unit. It owns
namespaces (a namespace is claimed by the first org that mints a key into it
and can never be claimed by another), and every key belongs to one org and is
bound to one of its namespaces. Scopes are exact and combine only
explicitly (`--scopes memory,override`):

| scope | grants |
|---|---|
| `memory` | the data routes `/v1/ns/{ns}/...` and the namespace's operational views `/v1/status`, `/metrics`, `/v1/metrics/json` |
| `billing` | `/v1/billing/checkout`, `/portal`, `/usage` - nothing else |
| `override` | only the cross-user capability of a `memory` key (reads across users, `include_deleted`, namespace crypto-shred); no route by itself |
| operator key (`MEMD_ADMIN_KEY`) | every namespace, every metric series; no org, never metered |

Namespace names follow the engine's grammar, matched in full
(`[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`, no trailing newline); `_`-prefixed
names are reserved.
Keys keep the `memd_<ns>_<kid>_<secret>` format; only a SHA-256 hash of the
secret is stored. The state lives in an admin SQLite database at
`<data root>/admin/admin.sqlite3` - beside the tenant store, never inside a
tenant namespace, so it is neither exported with nor crypto-shredded by a
tenant. `MEMD_ADMIN_KEY` stays the operator key: it spans namespaces, has no
org and is never metered. Self-hosted keys (`keys.toml.json`) are not honoured
in hosted mode; adopt them into an org with
`memd key migrate --hosted --org org_...` (the key strings keep working).

**Plans** (the defaults are config, not code - override any value with a JSON
file at `MEMD_PLANS_PATH`):

| meter | free (hard caps) | dev, $29/mo | scale (usage-based) |
|---|---|---|---|
| `memories_stored` (live gauge) | 50K | 500K (hard) | unlimited |
| `searches` / month | 10K | 250K (hard) | metered |
| `extractions_our_key` / month | 10K | 100K included, then metered ($8/100K) | metered |
| `reranked_searches` / month | 1K | 10K included, then metered | metered |
| `writes` / month | - | - | metered |
| `stored_gb` (daily gauge) | - | - | metered |

Enforcement happens before the operation: over a hard cap the request gets
`402 {"code": "quota_exceeded", "meter": ..., "limit": ..., "used": ...}`; on a
paid plan a soft limit never refuses - the part above the included quantity is
recorded as billable overage. Hard caps hold under concurrency: the check and
a *reservation* of the requested quantity happen in one `BEGIN IMMEDIATE`
transaction against the rollup plus every in-flight reservation; the usage
commit releases the reservation in the same transaction, a failed operation
releases it; a reservation never expires while its request is still running
(however long it takes), and one left by a crashed process stops counting
after 10 minutes. 50 concurrent requests at a cap of 20 admit exactly 20.
Nothing is ever recorded beyond what was reserved. Quota periods are calendar
months (UTC). **A spent `reranked_searches` quota never refuses a search**:
the search is served unreranked (`memd_rerank_fallback_total{reason="quota"}`)
and only `searches` is metered; searches are limited by the `searches` cap
alone. A session close is a write: at the memories cap it answers 402. While
its extractor runs it holds room for one memory only - writes alongside it
are not starved - and once the facts are extracted it reserves exactly
min(facts extracted, remaining headroom); anything beyond is not written
(`facts_capped` in the response). **Extraction runs on our key when the
server's environment has `MEMD_EXTRACTION_API_KEY`** (a hosted server
builds its engine from the environment, never from a client): then every
session close is metered as `extractions_our_key`, one unit per raw turn
the LLM extracted - the turns of a call that failed went through the local
pattern extractor (`raw_failed` in the response) and are not counted.
Under a hard cap (the free plan's 10K a month), the session's raw records
are extracted up to the remaining allowance and the rest stay raw-only -
searchable, never extracted (`raw_skipped`); with no allowance left the
close answers 402 (the pattern extractor does not take over). Without the
key, extraction runs locally, is never metered and never refuses. After
`invoice.payment_failed` the org has a 7-day grace period
(`MEMD_BILLING_GRACE_DAYS`); after it the org is **read-only**: writes and
session extraction answer `402 {"code": "payment_required"}`, searches, reads
and exports keep working. **Deletes (`DELETE /memories/{id}`, `forget`,
namespace crypto-shred) are always allowed**, whatever the plan or payment
state, and no data is ever dropped for non-payment.

**How metering works.** Each metered request writes a usage event (a UUID,
the org, the namespace, the meter, the quantity) to an append-only ledger in
the admin store, together with the period rollup that quota checks read, in
one transaction, committed with `synchronous=FULL` *after* the operation
succeeded and *before* the response is sent: every acknowledged operation is
billed, and an operation the client never saw acknowledged (a crash in
between) is not. Hourly (`MEMD_BILLING_PUSH_INTERVAL_S`), a background job
claims the unpushed rows into batches (per org, meter and hour), persists
each batch id - a UUIDv5 of its member event UUIDs - and sends it as a
[Stripe Billing Meter Event](https://docs.stripe.com/api/billing/meter-event/create)
with that id as both `identifier` and idempotency key; a crash anywhere
re-sends the same key, so Stripe records each batch once. Only the billable
part is pushed (overage on dev, everything metered on scale).
**Stripe only remembers an idempotency key for about 24 hours**, so usage
older than 20 hours (`MEMD_BILLING_MAX_PUSH_AGE_S`, capped at 20 h) is never
pushed automatically, and a batch that has been pending that long is
abandoned rather than retried: after an outage, a re-send could otherwise
bill a batch Stripe already has. Those events become `needs_reconcile`, and
every job tick settles them against Stripe's meter event summary for their
quota period (up to their last hour): what Stripe should hold (every settled
batch in that window plus these events) minus what it reports is the
verified missing quantity. Zero means the earlier attempts landed (nothing is
sent); up to the events' own units is pushed once, under a fresh idempotency
key recorded before the send. Anything else - Stripe holding more than the
ledger, or lacking more than these events - is not guessed at: drift alert,
nothing sent, the events stay `needs_reconcile` for a human. A window with a
batch still pending or sent within the last hour (`MEMD_BILLING_SETTLE_S`;
summaries lag) is retried on a later tick. The gauges
(`memories_stored`, `stored_gb`) are snapshotted once per UTC day; `stored_gb`
is sent in milli-GB so small tenants are not rounded up to a whole GB (price
that meter per 1/1000 GB and give it the `last` aggregation). A daily
drift report compares what the ledger settled for the quota period to date
with Stripe's meter event summaries and raises `memd_billing_drift_alerts_total{meter}` (and a
`drift_alert` row in the admin store's `billing_log`) when they differ by
more than `MEMD_BILLING_DRIFT_TOLERANCE` (1%). Billing metrics carry
`ns="_billing"`, so only operator keys see them on `/metrics`.

**Routes** (billing-scoped key; the org is always the key's own):

| route | does |
|---|---|
| `POST /v1/billing/checkout` `{"plan": "dev"\|"scale", "interval": "month"\|"year"}` | a Stripe Checkout Session (subscription: the flat price plus the plan's metered prices, open for 1 h); returns `{url, id}`. One subscription per org: `409 already_subscribed` while one is live, `409 checkout_pending` (with the open session's `url`) while a checkout is open |
| `POST /v1/billing/portal` | a Billing Portal session; returns `{url}` |
| `GET /v1/billing/usage` | current-period usage per meter: used, limit, hard, metered, billable; plan, status, grace, read-only |
| `POST /v1/billing/webhook` | Stripe's webhook endpoint (no bearer: the `Stripe-Signature` is the authentication) |

The webhook is verified with `stripe.Webhook.construct_event` (HMAC-SHA256 over
the raw body, timestamp tolerance 300 s) and is idempotent by Stripe event id:
the `processed_events` row commits in the same transaction as the event's
effect; bodies over 1 MiB (counted as read, chunked or not) are refused.
Handled:
- `checkout.session.completed`: only `mode=subscription` sessions with a
  subscription; the subscription is **re-read from Stripe** and *its* status
  applies (a payment-mode session never grants a plan nor clears past_due).
- `customer.subscription.created/updated/deleted`: `active`/`trialing` grant
  the plan the subscription's prices bill; `past_due` keeps it and starts the
  grace clock; `incomplete`, `incomplete_expired`, `unpaid`, `paused` and
  `canceled` grant nothing (free plan). A per-org cursor on the event's
  `created` ignores re-ordered older events, so a late
  `checkout.session.completed` cannot resurrect a deleted subscription.
- One subscription per org: an event for a subscription that is not the
  org's current one never changes the plan (canceling it does not downgrade
  the org); every *other live* subscription is listed as a duplicate
  (`memd_billing_duplicate_subscriptions_total`, a `duplicate_subscription`
  log row) and the org's metered usage is **held** - not pushed, since each
  would bill it - while ANY duplicate is listed. When the current
  subscription ends, the duplicates are re-read from Stripe, each on its
  own, and the first one still live is promoted to current (its invoices
  count from then on); dead ones - and ones Stripe no longer has (404) -
  leave the list; one that cannot be read right now stays listed (and held)
  until its own next event. None of this ever fails the webhook: the
  cancellation always takes effect. Each subscription also keeps its own
  event cursor and a tombstone once it ends, so a late or re-ordered event
  can neither re-list an ended subscription nor undo a newer one.
- `invoice.payment_failed` (starts the grace period once) and
  `invoice.payment_succeeded` count only for the org's current
  subscription; `checkout.session.expired` closes the open checkout.
- An org is bound to a Stripe customer only when it has none yet and the
  customer was created by memd's checkout for that org (its
  `metadata.memd_org_id`); anything else is logged and ignored.
- Events for customers memd does not know are acknowledged (200) and counted
  (`memd_billing_webhook_unmapped_total`); a Stripe read that fails answers
  500 so Stripe retries.

**Configuration** (environment):

| variable | meaning |
|---|---|
| `MEMD_HOSTED` | `1` enables hosted mode (same as `serve --hosted`) |
| `MEMD_STRIPE_SECRET_KEY` | Stripe secret key. `sk_live_`/`rk_live_` keys are **refused** unless `MEMD_ALLOW_LIVE_BILLING=1` (exactly `1`); a key or webhook secret containing whitespace is refused |
| `MEMD_STRIPE_WEBHOOK_SECRET` | the webhook endpoint's signing secret (`whsec_...`) |
| `MEMD_STRIPE_PRICE_DEV_MONTHLY`, `..._DEV_YEARLY` | flat subscription prices (`MEMD_STRIPE_PRICE_<PLAN>_MONTHLY/_YEARLY`) |
| `MEMD_STRIPE_PRICE_<PLAN>_<METER>` | metered prices, e.g. `MEMD_STRIPE_PRICE_DEV_EXTRACTIONS_OUR_KEY`, `MEMD_STRIPE_PRICE_SCALE_STORED_GB`; every metered meter of a plan needs one |
| `MEMD_STRIPE_METER_<METER>` | the Stripe meter's `event_name` (default `memd_<meter>`) |
| `MEMD_STRIPE_METER_ID_<METER>` | the meter id for reconciliation (default: looked up by event name) |
| `MEMD_PUBLIC_URL`, `MEMD_BILLING_SUCCESS_URL`, `MEMD_BILLING_CANCEL_URL`, `MEMD_BILLING_RETURN_URL` | Checkout/Portal redirect targets |
| `MEMD_PLANS_PATH` | JSON plan overrides |
| `MEMD_BILLING_GRACE_DAYS`, `MEMD_BILLING_PUSH_INTERVAL_S`, `MEMD_BILLING_DRIFT_TOLERANCE`, `MEMD_STRIPE_WEBHOOK_TOLERANCE_S` | 7, 3600, 0.01, 300 (must be > 0: 0 would disable the replay check) |
| `MEMD_STRIPE_API_BASE` | point the Stripe client elsewhere (stripe-mock in tests) |
| `MEMD_BILLING_MAX_PUSH_AGE_S`, `MEMD_BILLING_SETTLE_S`, `MEMD_STRIPE_MAX_NETWORK_RETRIES` | 72000 (the maximum), 3600, 2 |
| `MEMD_BILLING_JOBS` | `0` disables the in-process push/snapshot/reconcile loop |

Without `MEMD_STRIPE_SECRET_KEY`, hosted mode still enforces tenancy and
quotas (orgs keep the plan set with `memd org set-plan`) and the billing
routes answer `503 billing_not_configured`.

**Privacy.** The ledger records counts - org, namespace, meter, quantity,
time - never memory content, queries or user ids. What reaches Stripe is the
customer id, the meter's event name, a number and a timestamp; the org's
name goes into the Stripe customer record once, at first checkout.
`extractions_our_key` counts only when extraction runs on the operator's LLM
key (`MEMD_EXTRACTION_API_KEY` in the server's environment), and only the
turns the LLM extracted; the local pattern extractor is never billed.

## Doors (one engine)

| Door | Command | Surface |
|---|---|---|
| Python SDK | `from memd import Memory` | add/search/remember/forget/pack/observe/export |
| REST | `memd serve --http` (:8700) | `/v1/ns/{ns}/events`, `/memories`, `/search`, `/export`, ... |
| MCP | `memd serve --mcp` | exactly 4 tools: memory_search / memory_save / memory_forget / memory_status |
| TypeScript SDK | `npm install memd-engine` ([sdk-ts/](sdk-ts/README.md)) | the REST door, typed, for Node ≥ 18, Bun, Deno and edge runtimes (Workers, Vercel Edge) |

## What's inside

- **Storage**: per-namespace WAL → immutable segments → manifest on an
  object-store interface (local FS embedded; S3/R2 backend is the same
  interface). Durable write ack = fsync'd append; no LLM/embedding on the
  write path. Compaction folds tombstones and enforces hard-delete deadlines.
- **Revisability**: bitemporal records — new fact on an entity key
  supersedes the old cluster-locally. `search(as_of=...)` time-travels;
  `?history=true` walks supersedence chains.
- **Provenance & trust**: every record carries source tier
  (user > agent > tool > web > import), lineage, actor. Untrusted content is
  fenced in packed context; explicit saves inherit session taint; quarantine +
  rate limits catch MINJA-style injection; hash-chained audit log; namespace
  crypto-shred; record-level hard delete with ≤72h physical purge deadline.
- **Retrieval**: rules-based planner (no reflection loop) → fan-out over
  BM25 (SQLite FTS5/porter, ranked by bm25; optionally accelerated by
  tantivy), the entity lane, the time lane on recency intent, and the
  vector lane with a real embedder (an exact flat scan; from
  `ann_min_vectors` vectors on, an optional usearch HNSW sidecar) → RRF
  fusion with trust-aware tie-breaks →
  optional rerank of the lexical top-30 → validity filter (current/as_of) →
  packing into `budget_tokens` (default 12,000), with each piece of evidence
  counted once. The default layout is dated session excerpts. The other
  layout is a flat list in prefix-stable order (KV-cache friendly). Gated
  evidence packing is an experimental opt-in. See "Packing and the budget",
  below. The hash embedder's vector lane is not fused (`fuse_vector`,
  below).
- **Extraction**: async, batched, re-runnable. BYO OpenAI-compatible
  key for LLM extraction/embeddings; heuristic provider keeps facts working
  with zero keys; local ONNX embeddings via the optional fastembed extra.

## Keys & providers (add later, everything works now)

```bash
export MEMD_EMBEDDING_API_KEY=...      # optional: real embeddings
export MEMD_EXTRACTION_API_KEY=...     # optional: LLM fact extraction
```

Without these keys, memd uses local embeddings or hash embeddings, and
pattern extraction:

- With the `local-embeddings` extra (`pip install "memd-engine[local-embeddings]"`),
  memd uses BAAI/bge-small-en-v1.5 on the CPU, through fastembed. The model
  downloads on first use. memd fuses its vector lane with BM25.
- Without the extra, memd uses deterministic hash embeddings. Search then
  ranks by its lexical lanes only: memd does not fuse the hash vector lane.
  memd logs one warning for each process when this occurs.

All modes work fully. `stats()` shows which mode is active. Install the
extra: it gives better recall. These are lane-level measurements of
session retrieval on LongMemEval_S (153 questions). recall_all@10 is the
share of questions with all their evidence sessions in the top 10:

| ranking | recall_all@10 | multi-session recall_all@10 |
|---|---|---|
| BM25 alone (what search uses with the hash embedder) | 0.920 | 0.827 |
| bge-small fused with BM25 (what search uses with the extra) | 0.975 | not reported |
| bge-small vector lane alone | 0.980 | 0.962 |
| hash vector lane alone | 0.640 | 0.385 |

Fused bge-small is +0.056 [+0.021, +0.095] over BM25 alone.

Embeddings: `embedding_api_key` / `MEMD_EMBEDDING_API_KEY` (set = an
OpenAI-compatible embeddings API when `embedder` is `auto` or `openai`),
`embedding_model` / `MEMD_EMBEDDING_MODEL` (`text-embedding-3-small`),
`embedding_base_url` / `MEMD_EMBEDDING_BASE_URL`
(`https://api.openai.com/v1`); as below, a config value wins over the env
var, and config `embedding_api_key=""` turns an env key off.

### Extraction options (`Memory(config={...})` or the env var)

A config value wins over the env var; config `extraction_api_key=""` turns
an env key off.

| key / env | values | default |
|---|---|---|
| `extraction_api_key` / `MEMD_EXTRACTION_API_KEY` | a key for an OpenAI-compatible chat completions API; set = the LLM extractor | unset: the pattern extractor |
| `extraction_model` / `MEMD_EXTRACTION_MODEL` | model id | `gpt-4o-mini` |
| `extraction_base_url` / `MEMD_EXTRACTION_BASE_URL` | API base, e.g. `https://openrouter.ai/api/v1` | `https://api.openai.com/v1` |
| `extraction_max_tokens` / `MEMD_EXTRACTION_MAX_TOKENS` | output-token cap per call, sent as `max_tokens`; `0` sends none | 4096 |
| `extraction_timeout_s` / `MEMD_EXTRACTION_TIMEOUT_S` | seconds one call may take, start to finish | 120 |
| `extraction_max_response_bytes` / `MEMD_EXTRACTION_MAX_RESPONSE_BYTES` | a larger reply is refused, read no further | 4194304 (4 MiB) |
| `extraction_request_options` / `MEMD_EXTRACTION_REQUEST_OPTIONS` | a dict (env: a JSON object) merged into every request body last; a `null` value removes a field | none |

- **What the model sees.** A session's raw turns, in chunks of at most 40
  turns / 24,000 characters (counting each whole line), one line per turn:
  `[<turn id>] <time, UTC> <speaker>: text`, the speaker being `user`,
  `assistant`, `agent`, `system` or `tool` (from the writer's `role`). A
  line break in a turn's text is written as `\n`, so a turn cannot pose as
  another line or speaker, and turn ids are made up for each call
  (`9f3a1c-1`, `9f3a1c-2`, ...): a turn's text cannot name another turn,
  and record ids are not sent. The model is told to attribute each
  fact to who said it (an assistant's suggestion is not the user's fact)
  and to name the turns it came from; a fact takes its scope, actor and
  time from those turns (one naming none of its chunk's turns: from the
  chunk's first user turn, else its first turn). Facts record the prompt
  version (`v2`).
- **Bounded calls.** Every call sends `max_tokens` (an uncapped call to a
  model that looped once ran to 131,072 output tokens and 413 s). The whole
  call - connecting, sending, the response headers and body - is cut off
  at `extraction_timeout_s`, however the provider trickles bytes (OpenRouter
  keeps a connection alive with whitespace while the model generates; a
  per-read timeout restarts with every byte). The call runs on its own
  thread and connection: at the deadline the session close moves on and
  the connection is closed; the abandoned thread ends at its next read, or
  after `extraction_timeout_s` more for a provider that has gone silent.
- **Request options** pass the provider its own settings: OpenRouter
  routing (`{"provider": {"order": ["deepinfra"], "allow_fallbacks":
  false}}`), reasoning off or lower for a reasoning model (`{"reasoning":
  {"enabled": false}}`, `{"reasoning": {"effort": "low"}}`), sampling
  (`{"temperature": 0.2}`), or `{"max_tokens": null,
  "max_completion_tokens": 4096, "temperature": null}` for a model that
  rejects `max_tokens` and `temperature`. `model`, `messages` and `stream`
  are the extractor's own and are refused, as are invalid JSON and a
  non-object.
- **Failures degrade to the pattern extractor.** A failed call - an HTTP
  error, the provider unreachable, the timeout, a reply cut off at the cap
  (`finish_reason: "length"`), an empty reply (a reasoning model that
  answered with its reasoning only), one over the size cap, or one without
  a JSON array - is never
  retried: that chunk's turns go through the pattern extractor instead, the
  failure is counted as
  `memd_extraction_chunks_failed_total{model, reason}` (`http_status`,
  `transport`, `timeout`, `truncated`, `empty`, `oversize`, `malformed`), and
  `close_session` returns `extraction_errors` (the number of failed calls)
  and `raw_failed` (their turns), and audits `extraction_degraded` with the
  reasons. The raw turns are stored either way. Within a reply that parses,
  a malformed item (no text, or `entity_keys` / `lineage` not a string or a
  list of strings) is dropped and counted as
  `memd_extraction_items_malformed_total{model}`; its other facts are kept.
- **Privacy: with the LLM extractor active, every closed session's raw
  turns are sent to the extraction provider** (see SECURITY.md).
- **Measured.** On 30 LongMemEval_S questions, the LLM extractor gave no
  measurable accuracy gain. It scored 0.667, and the pattern extractor
  0.700 (difference -0.033, 95% CI [-0.167, +0.067]). It also made a
  session close slower (1.5 s against 0.08 s at the median). A larger
  evaluation is necessary before we recommend it.

### Retrieval options (`Memory(config={...})` or the env var)

| key / env | values | default |
|---|---|---|
| `reranker` / `MEMD_RERANKER` | `auto` \| `none` \| `jev` \| `local` | `auto`: Jev when a TypeSafe key is set (`TYPESAFE_API_KEY`, or config `typesafe_api_key`) and `typesafe-sdk` is installed (`pip install "memd-engine[jev]"`), else none |
| `jev_model`, `rerank_timeout_s`, `rerank_k` | model pin, deadline, shortlist | `jev-latest`, 1.5s (5s local), 30 |
| `local_rerank_model` | fastembed cross-encoder | `BAAI/bge-reranker-base` |
| `packing` / `MEMD_PACKING` | `sessions` \| `flat` (any case). Per call: `search(packing=...)`, REST `"packing"`, MCP `packing` | `sessions` |
| `pack_resolve_dates` / `MEMD_PACK_RESOLVE_DATES` | add the calendar date after a relative date in a packed user turn ("yesterday [= Fri 2023-05-19]") | `false` |
| `pack_mode` / `MEMD_PACK_MODE` | `auto` \| `ranked` \| `gated` | `auto` = the `packing` layout with every reranker; `gated` is an experimental opt-in |
| `rerank_gate` | gated-packing threshold (opt-in mode) | 0.5 |
| `fuse_vector` / `MEMD_FUSE_VECTOR` | `auto` \| `true` \| `false` | `auto`: fuse unless the embedder is the hash embedder |
| `lexical_backend` / `MEMD_LEXICAL_BACKEND` | `auto` \| `fts5` \| `tantivy` | `auto`: tantivy when installed (`pip install "memd-engine[fast]"`) |
| `vector_index` / `MEMD_VECTOR_INDEX` | `auto` \| `flat` \| `usearch` | `auto`: the usearch sidecar (`pip install "memd-engine[ann]"`) for a namespace holding ≥ `ann_min_vectors` vectors, else the exact scan; an explicit `usearch` that cannot be honoured raises |
| `ann_min_vectors`, `ann_overfetch`, `ann_exact_max`, `ann_dtype`, `ann_expansion_search` | auto threshold, candidate over-fetch, exact-answer cutoff, stored precision, HNSW search-depth floor | 20000, 4, 2000, `f16` (or `i8`), 128 (a search for k candidates explores at least k) |
| `flat_max_vectors` | while the sidecar is loading or rebuilding, a namespace with more vectors than this never loads the exact scan's float32 matrix | 200000 |

- **Reranker.** The top-30 of the bm25 lane (plus the vector lane with a real
  embedder) is reordered by a relevance judge; the rest follows in fused
  order. On LongMemEval_S, Jev reranking took session ndcg@5 from 0.89 to
  0.95. A failed, slow (> `rerank_timeout_s`) or malformed judgement keeps
  the unreranked order and counts `memd_rerank_fallback_total{reason}`;
  search never fails because of it. `stats()["reranker"]` reports name,
  model, calls, fallbacks and p50 latency. **Privacy: with Jev active, the
  query and the top-30 candidate texts of every search are sent to
  TypeSafe's API** (see SECURITY.md). No key, no egress.
- **Gated packing** (`pack_mode="gated"`, experimental opt-in, meant for a
  calibrated reranker such as Jev): the packed context holds only the
  candidates judged relevant (p ≥ `rerank_gate`, else the top 3), each with
  its neighbouring turns, grouped by session under a session-date header,
  still capped by `budget_tokens` and with the same provenance fencing. It
  trades recall for tokens: in an experiment it matched top-k QA accuracy at 27%
  fewer tokens over a 100-candidate shortlist, but over the product's top-30
  it drops second evidence sessions (LongMemEval_S session recall_all@5
  0.803 gated vs 0.928 ranked, with Jev). When a reranker ran, gated
  packing has priority over `packing`. The default packs all candidates, in
  the `packing` layout.
- **tantivy accelerator.** A derived index next to SQLite, fed in the
  background (every 500ms or 512 changes); FTS5 stays the synchronous source
  of truth, so the write ack is unchanged, and writes not yet indexed are
  served from FTS5. It is rebuilt in the background when missing, corrupt or
  not closed cleanly. A search error sends that query to FTS5; only damage
  (I/O, missing or corrupt files) rebuilds it while running, with
  exponential backoff on a failure history that a successful rebuild does
  not erase (it decays after 10 minutes without a failure);
  `stats()["lexical"]` shows `rebuilds`, `failures`, `failures_total` and
  `retry_in_s`. Tied bm25 scores are ordered the same way on both backends
  (score, -t_event, content hash, id). With tantivy the top-k is
  deterministic for the same operation history *and commit schedule*: its
  BM25 statistics count deleted and superseded docs until their segments
  merge, so the same history committed in a different rhythm can order two
  near-equal docs differently.

- **usearch sidecar** (vector lane). A derived HNSW index (cosine,
  connectivity 16, f16) beside SQLite, keyed by record rowid and holding the
  live records' vectors; SQLite's vectors table stays the source of truth.
  Vectors reach it as the embed worker applies them (queued under the index
  lock, applied right after it: the write ack never waits on it); deletes,
  supersession, quarantine and hard deletes remove entries at once. A query
  takes usearch's top k × `ann_overfetch` (widened once), then the same SQL
  filter and post-check as every lane, re-scored exactly from SQLite, ties
  ordered like fusion. Answered exactly instead: filters admitting at most
  `ann_exact_max` rows, sweeps (`find_ids`), a window still short after
  widening, and a sidecar not ready yet (`stats()["vector_index"]` counts
  them in `fallback_exact_total`). Its file is used only when it matches the
  SQLite file's vector watermark exactly; missing, corrupt, foreign, stale or
  pre-purge files are rebuilt in the background (temp file + fsync +
  rename; a kill never leaves a torn file in use). A hard-delete purge
  deletes its files and rebuilds it from SQLite (usearch removal only marks
  entries), and it is published with the index snapshot so a cold node
  installs it instead of rebuilding. recall@10 is 0.99+ on dense embeddings
  up to 1M vectors, ~0.94 on the hash embedder's sparse vectors (see
  BENCHMARKS.md); a crash costs a rebuild (~5 min at 1M on 4 threads).
  Files and published images carry a blake2b checksum, verified before
  usearch reads them (usearch trusts what it loads: a file corrupted in
  place crashed the process in search); a failed check, or a process that
  died while a loaded graph was still on probation (until it has served
  200 searches or 300 s; a clean close ends it), rebuilds it from SQLite.
  A loaded graph's bookkeeping is checked at load and its first answers
  against the exact scan (recall < 0.8 rebuilds it). A checksum-valid
  file that memd did not write takes write access to the data directory:
  an adversarial local file, outside the threat model (see SECURITY.md). At least 100
  candidates are re-ranked by exact cosine, writes still queued for the
  index are searched exactly (read-your-writes), and after each build
  poorly linked nodes are re-inserted (a duplicate vector's too, when a
  search reaches only its identical twin), so independent rebuilds give
  the same top-10 and a scoped search that drops the twin still finds it. Every vector path admits only live rows, whatever
  `include_quarantined` / `include_invalid` ask, and sweeps beside the
  sidecar stream exactly from SQLite.
- **While the sidecar is not serving** (loading at open, rebuilding, or
  failed to attach), the exact scan serves up to `flat_max_vectors`. Above
  that its float32 matrix (1.5 GB at 1M × 384) is never loaded: sweeps and
  filters admitting at most `ann_exact_max` rows are answered exactly,
  streamed from SQLite, and other queries skip the vector lane (bm25 and the
  other lanes serve them; `memd_vector_lane_skipped_total{reason="ann_rebuilding"}`,
  `stats()["vector_index"]["skipped_total"]`; such results are not cached).
- **Sidecar save and load are off the request path.** Opening a namespace
  does not wait for its sidecar to load (a background thread does it), and
  closing hands the final save to a background thread (engine close waits
  for it; a destroy cancels it). usearch holds the GIL for the whole of a
  save or load, so the process pauses for it wherever it runs: measured at
  200K × 384 f16, ~100 ms per save and ~105 ms per load (files up to 256 MB
  are read and written by Python outside the GIL, so the pause is a memory
  copy; larger ones are saved and loaded by usearch directly and pause
  longer). `stats()["vector_index"]["last_gil_hold_ms"]` and
  `memd_vector_index_gil_hold_ms` report it.

### Packing and the budget

A search returns `packed_context`: its candidates, packed into
`budget_tokens`. The default budget is **12,000** tokens. Over REST and MCP,
the budget is 64 to 128,000 tokens. memd counts tokens with its own
estimate (4 characters per token). The text never goes above the budget.
The text contains only whole lines: memd does not cut a record to make it
fit.

The `packing` setting selects one of two layouts. Set it in the config, in
`MEMD_PACKING`, or for each call. The value can be in any case.

**`sessions` (the default): dated session excerpts.**

- memd takes the candidates in rank order.
- A retrieved turn brings the turn before it and the turn after it in its
  session.
- A fact shows under the turn that it came from. That turn and its
  neighbours come with the fact. The fact line names the speaker of that
  turn.
- A fact without a source turn (an explicit `remember`) gets its own line,
  under its own date.
- Sessions show oldest first, each under its date. Turns show in session
  order. `[...]` shows where turns are not included.
- Each turn's line starts with its speaker: `user:`, `assistant:`,
  `tool:`, `web:` or `import:`.
- Each record is one line. memd writes a line break in a record's text as
  `\n`. Thus a record cannot add a line of its own, for example a false
  session header, speaker or fact.
- Lower-trust content (tool, web, import, quarantined) shows in an
  `untrusted-data` fence, escaped, as in the flat layout. memd also escapes
  a fence tag in other text. Thus each fence in the text is a real one.
- The text does not contain record ids or session ids. The ids are in
  `items`.
- memd does not show a turn that a newer fact replaced (the source of a
  superseded value). The flat layout also leaves it out.
- memd shows a long turn as an excerpt, marked with "...". A retrieved turn
  gets up to 4,000 characters, around the fact that it carries, if any. A
  neighbour gets up to 1,000 characters.
- If a unit does not fit whole, memd packs its anchor turn alone. If that
  does not fit, memd skips the unit. A later, smaller unit can still fit.
- The turns that memd adds pass the same scope, validity and quarantine
  filter as the search's hits. A `kinds` filter without `raw_event` adds no
  turns.
- `items` lists each record that the text shows: the hits in rank order,
  each followed by the turns that it brought (`lanes` `["source"]` or
  `["neighbour"]`, score 0). Each item carries the text as the line shows
  it, so a long turn's item carries the excerpt. The full record is
  available with `get(id)`.
- `pack_resolve_dates=True` adds the calendar date after a relative time
  expression in a user turn, from the turn's own date: "two weeks ago [=
  Sat 2023-05-06]". This option is off by default (see below).

**`flat`: one `<memory>` element for each candidate.** Each element has
provenance tags (`source`, `kind`, `date`, `id`). The elements show in rank
order, until the budget is full. The order is prefix-stable (KV-cache
friendly). This was the default layout before, with a default budget of
2,000 tokens. To get the old behaviour, use
`search(..., budget_tokens=2000, packing="flat")`, or set `packing="flat"`
in the config and give the budget.

A session-packed context:

```text
Relevant excerpts from past conversations, retrieved by memory search. Sessions oldest first; [...] = turns omitted.

### Session 1:
Session Date: 2023/05/20 (Sat) 02:21
Session Content:

assistant: hi, how can I help?
user: I prefer jazz. I went to a gig yesterday.
[memory fact, said by the user: The user prefers jazz]
assistant: Nice! Which band?
```

**Measurement on LongMemEval_S.** This is end-to-end QA on 160 questions.
The questions are stratified by type. Multi-session and temporal-reasoning
questions are over-sampled two times. The results are re-weighted to the
type mix of the dataset. There was one run. The reader is DeepSeek V4.1
Flash. The judge is gpt-6-luna-pro, with the per-type judge prompts of
LongMemEval. The intervals are 95% bootstrap CIs.

| context given to the reader | accuracy | multi-session | temporal | reader prompt tokens |
|---|---|---|---|---|
| memd's previous defaults: hash embedder, no reranker, 2K flat | 0.779 [0.718, 0.838] | 0.577 | 0.796 | ~2.0K |
| bge-small + local cross-encoder reranker, 2K flat | 0.789 [0.726, 0.847] | 0.654 | 0.759 | ~2.0K |
| bge-small + local cross-encoder reranker, 12K flat | 0.823 [0.763, 0.879] | 0.750 | 0.833 | ~10.6K |
| bge-small + local cross-encoder reranker, **12K sessions, relative dates on** | **0.875 [0.823, 0.920]** | 0.769 | 0.870 | ~10.4K |
| the whole history (no retrieval; exploratory, see below) | 0.906 [0.858, 0.949] | 0.904 | 0.944 | ~105K |

What the numbers show:

- The 12K session pack scored +0.095 [+0.034, +0.156] over the previous
  defaults (McNemar p = 0.0015).
- It used a tenth of the tokens of the whole history. It scored lower than
  the whole history: the paired, unweighted difference is -0.056 [-0.106,
  -0.006].
- With the same retrieval, it scored +0.052 [-0.010, +0.113] over the 12K
  flat pack. This difference is positive, but it is not significant at
  this size.
- Each step alone is also not significant: the embedder and reranker at 2K
  give +0.009, and 2K to 12K gives +0.034.
- Multi-session questions are the weakest of the large categories.
  Abstention went down with more context: 0.857 to 0.714 (n = 7).

What the numbers do not show:

- The measured run used bge-small embeddings and a local cross-encoder
  reranker. The defaults use bge-small only with the `local-embeddings`
  extra, and the hash embedder without it. The defaults use no reranker.
  Nobody measured the new defaults as shipped, end to end.
- The measured session pack had the relative-date annotations on. memd
  ships them off: on a pilot of 20 questions, the pack without them scored
  0.85, and the pack with them 0.75. This pilot has no statistical power.
- The previous-defaults row and the whole-history row use the answers of
  an earlier run with the same reader and judge. The whole-history
  comparison is exploratory: it is not part of the plan of the run.
- memd's session pack is different from the measured pack in these
  points:
  - A fact line names the speaker of its source turn. The measured pack
    named the trust tier of the fact, which is usually "assistant".
  - Each record is one line (`\n` for a line break). The measured pack
    kept the line breaks.
  - memd packs a fact without a source turn. The measured pack dropped it.
  - memd does not bring back a replaced turn as a neighbour.
  - memd fences and escapes lower-trust content.
  - If the header alone is larger than the budget, memd packs nothing.
- A test (`tests/test_evidence_packing.py`) gives fixed inputs to the
  measured implementation and to memd, and compares the outputs. memd
  gives the same text, byte for byte, in 92 of 96 cases. In the 4 other
  cases, the budget is smaller than the header. With dates off, the
  reference is the measured implementation, modified to check the budget
  on the text without annotations.

**Cost.**

- A search can use up to 6 times the tokens of the old default, if there
  is that much to pack. `tokens_used` gives the count.
- The response size follows the budget. A REST response is at most about
  two times the budget's characters, plus the metadata of each item.
  memory_search (MCP) returns the text once, and only the metadata of the
  hits. For five 1 MB turns at a budget of 2,000 tokens, the REST response
  was 15.6 KB.
- Packing adds a few milliseconds. These are medians on a shared 8-core
  arm64 host, with load (load average 4 to 7), hash embedder, no reranker:

| namespace | search, sessions 12K | search, flat 12K | pack stage, sessions 12K / 2K | pack stage, flat |
|---|---|---|---|---|
| LongMemEval_S-shaped: 50 sessions, 800 records | 7.2 ms | 4.7 ms | 3.4 ms / 1.6 ms | 0.9 ms |
| one session of 50,000 turns | 20.0 ms | 8.6 ms | | |
| 10 sessions of 5,000 turns | 17.2 ms | 6.6 ms | | |
| `bench/slo_bench.py` (3,000 records, defaults) | 25 ms | 18 ms (2K flat) | | |

- memd finds the neighbours of a turn with two seeks on the
  `ix_rec_session_t` index (`scope_session`, `kind`, `t_event`). Thus the
  cost does not increase with the length of a session. Turns that share
  one timestamp (for example, a session imported with one date) are also
  found by a seek: a 20,000-turn session with one timestamp has a median
  default search of 21 ms. The first open of
  an existing namespace builds this index (0.1 s for 50,000 records).
- The SLO bench has two retrieval targets. Retrieval with a 2K flat pack
  must have a median of 20 ms or less (measured: 18 ms on this host). A
  search with the defaults (12K session pack) must have a median of 40 ms
  or less (measured: 25 ms). The session layout reads approximately 3
  times more rows, thus it has a separate target.

## Ops

```bash
python bench/slo_bench.py                        # SLO acceptance numbers
python -m memd.harness.run --suite all --gate    # quality+cost gate
python bench/lme_gate.py                         # real-data gate: LongMemEval_S, 60 q (nightly)
python bench/lexical_bench.py                    # FTS5 vs tantivy, filtered, 10K-150K records
python bench/ann_bench.py                        # vector lane: usearch vs exact, 50K-1M vectors
python bench/forward_bench.py                    # write forwarding: holder vs forwarded calls
MEMD_TEST_S3_ENDPOINT=... python bench/replica_bench.py   # read replicas: lag and read throughput (an S3 API)
memd export --out backup.jsonl                   # anti-lock-in, symmetric
memd import memd --export backup.jsonl           # restore a memd export
memd import mem0 --export mem0.json              # migration path
memd keys status --data ./memd-data              # data-key custody: provider, each namespace's key
memd migrate --report ./memd-data                # store-format upgrade: preview / what it did (JSON)
memd key create --ns acme [--pin-user u1]        # scoped API keys
```

The SLO targets of `bench/slo_bench.py`, with the values measured on one
machine:

- Durable write ack: the target is p99 ≤ 10 ms (embedded). Measured: p99
  ≈ 7 ms.
- Warm retrieval with a 2K flat pack: the target is p50 ≤ 20 ms and p99 ≤
  100 ms. Measured: p50 ≈ 12 ms, p99 ≈ 52 ms.
- Warm search with the defaults (12K session pack): the target is p50 ≤
  40 ms and p99 ≤ 150 ms. Measured: p50 25 ms on a loaded host (see
  "Packing and the budget").
- Cold restart and first query: the target is p90 ≤ 1.5 s. Measured: ≈
  44 ms.

The eval harness stamps its version hash into every result file — a result
without a hash doesn't exist.
