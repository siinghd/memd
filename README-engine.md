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
The image runs as **uid 10001**, so a *named volume* (above) works. A
**bind mount does not** work, unless you either pass `--user "$(id -u):$(id -g)"` or
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
  write runs a small endpoint (TCP, `127.0.0.1`, an ephemeral port). It puts
  its address in the lock file or lease it holds. A process that finds a
  namespace busy reads the address there and sends the call over. This applies to `add`,
  `add_events`, `observe`, `remember`, `close_session`, `delete`,
  `delete_many`, `forget`, `destroy_namespace`, `compact`, `reembed`, and
  the strong reads `search`, `get`, `find_ids`, `stats` and `export`
  (streamed). The holder runs the same code a local call runs, so
  quarantine, supersedence, session taint and the audit ledger stay the
  writer's. A forwarding process reads its own writes: the process that applied them
  answers its strong reads.
- **Eventual reads stay in the process.** The forwarding process's own [read replica](#read-replicas-eventual-reads)
  serves `search(...,
  consistency="eventual")` and `get(..., consistency="eventual")`:
  no hop, at most the staleness bound behind.
- **Takeover.** When the holder goes away (a clean close, a crash, `kill
  -9`), its lock becomes free at once (a lease after `MEMD_LEASE_TTL_S`).
  Then the next forwarded call takes the namespace: that process opens it
  (it replays its log), becomes its writer and runs the call itself. The
  other processes forward to it. A clean close first lets the forwarded
  calls that are in progress finish. It answers new calls "not the writer",
  so their callers move on. A call that continues after the close's drain
  cannot write (the closed store refuses it), so no write lands after the
  process releases the lock. To take
  a namespace over, a process must decrypt it. On an `s3://` root with the
  default `local` key provider, the data keys live in a `local_dir`. Thus a
  process with a `local_dir` of its own cannot take over (`KeyCustodyError`).
  Share the `local_dir` between the processes, or use a remote key
  provider (`aws-kms`, `vault-transit`, below). A holder can freeze past its
  lease (a VM pause, `SIGSTOP`) and then resume. Then its own calls in flight
  fail with `LeaseLostError`, and memd acknowledges none of them. Its next
  calls go to the new writer.
- **Exactly once.** Every forwarded call carries a request id, and a call
  that creates records carries their ids, generated by the caller. A retry
  can come after a lost reply, a timeout, or a holder that died in the
  middle of the call. A live holder answers it with the result of the first call
  (or waits for that call, if it is still in progress). A new holder finds the
  call in the log that it replayed. Thus memd loses no acknowledged write, and
  applies no write two times. A
  write never runs in a process that does not hold its namespace. Before
  the holder runs a forwarded call, it checks that it still holds the
  namespace. The local path always goes through the lock or lease.
- **Backpressure and timeouts.** A holder runs at most
  `forward_max_inflight` (64) forwarded calls at once, on a pool of that
  many reused threads, over at most 256 connections. If a call waits 10 s
  for a slot, the holder answers "overloaded", and the caller sends the
  call again. A call looks for a writer that answers for
  `forward_wait_s` (30 s on a local root, the lease TTL + 15 s on S3).
  Then it raises `ForwardingError` (a `NamespaceBusyError`; the holder
  applied nothing), or `ForwardTimeoutError` if the holder possibly applied
  the write. One send of a call waits for its answer at most `forward_timeout_s` (300
  s). On S3, it stops waiting when the holder's lease goes stale (memd
  fences a frozen holder when it resumes). The REST server sends
  `ForwardingError` as `503 forward_unavailable` with `Retry-After: 1`,
  `ForwardAuthError` as `503 forward_refused` with no retry hint, and
  `ForwardTimeoutError` as `504 forward_timeout` with `Retry-After: 1`.
  The body's `may_be_applied` is `true` only for `forward_timeout`. Then,
  before you send a write again, read the namespace to find if the holder
  applied the write.
- **Who may call.** A process must hold the root's forwarding secret.
  This is `<data dir>/forward.secret`, which memd creates on first use with mode 0600 (on S3:
  in the `local_dir`). It can also be `forward_secret` / `MEMD_FORWARD_SECRET`, which
  processes on an `s3://` root with different `local_dir`s need. memd
  reads the file without a symbolic link. It reads it only if it is a regular file
  of the user of the process, and no other user can read or write it. memd
  refuses all other files, and then that process runs with forwarding off.
  Both sides prove the secret, and every frame after that is
  integrity-protected; memd encrypts nothing ([SECURITY.md](SECURITY.md)).
  The holder refuses a process with another secret (`ForwardAuthError`, at
  open when the namespace of the facade is the busy one). A process that
  cannot read or create the file (a data directory shared with another
  user) runs with forwarding off, and its log tells this.
- **Off.** `Memory(path, forwarding="off")` (config `forwarding`,
  `MEMD_FORWARDING=off`) keeps the old behaviour: a busy namespace raises
  `NamespaceBusyError`. A holder with forwarding off - or an older memd -
  advertises no endpoint, and a process finding a namespace held by it gets
  `ForwardingError` at once. Cluster nodes (`--node-id`) and hosted servers
  run with forwarding off. The cluster router routes between nodes, and a
  hosted session close settles its quota through a callback that cannot run
  in another process.

These work on one data root: `uvicorn --workers N` (`MEMD_DATA=./memd-data
uvicorn memd.cli:create_app_from_env --factory --workers 4`), one `memd
serve --mcp` for each MCP client, or a script beside a server. One of them
at a time writes each namespace, and the others forward to it. (A test ran
three uvicorn workers and each request got an answer. With forwarding off,
all workers but the first fail to start.) Forwarding adds a hop, not write
capacity. To scale writes, spread *namespaces* across processes:
[multi-node serving](#multi-node) does this on an `s3://` data root.
Processes in different network namespaces (two containers on one volume,
two hosts on one bucket) cannot reach the loopback of the other process.
For them, bind `forward_host` (`MEMD_FORWARD_HOST`) to an interface that
they can reach, and set `forward_advertise` to the `host:port` that the
others connect to. If the holder is on another host behind a loopback-only
endpoint, the call gets `ForwardingError` at once.
`MEMD_ALLOW_MULTI_PROCESS=1` turns off the lock (and forwarding), and the
data loss can occur again. It is for recovery tools, not for serving.

A record deleted between a write and the retry of that write stays deleted.
The holder runs the write only if the namespace never had a record with one
of its record ids. A soft-deleted record keeps its row until a compaction
drops it. A hard delete removes the row at once, but it keeps the id in the
index for a day. A holder that takes the namespace over replays it from the
log. There are two exceptions: a record soft-deleted and then compacted
away, or a record hard-deleted and purged. In both, a holder without the
cache of that index takes over, and it rebuilds the index from the bucket
alone before the retry arrives. The id is then not known, and the retry
writes the record again.

The index keeps at most the newest 100,000 hard-deleted ids
(`MEMD_HARD_DELETED_KEEP_MAX`). It removes the older ids at most once a
minute, when a hard delete occurs. A bulk hard delete thus cannot make the
table grow without a limit. A retry of an id that this limit removed is the
same as a retry after the day. The id is not known, and the retry writes
the record again.

The exactly-once rule does not cover these calls. The first holder can stop after it runs a session close, a
destroy or a compaction. If a caller then retries the call on a NEW holder,
the call runs again. The consolidation of the second
session close drops the facts that the first run wrote. A delete retried
that way answers `False` (the first call deleted the record). An export
cut off mid-stream raises an error, and it does not continue.
memd has no mode with several writers in one namespace (several logs).

**Measured** with `bench/forward_bench.py`: a holder process and this one on
one 8-core machine, loopback TCP, hash embedder, no reranker, encryption on.
Each phase writes into a fresh namespace and reads one corpus of 2,000
records. The values are the mean of two runs:

| | write ack p50 / p90 / p99 | writes/s, 1 / 4 / 8 threads | strong get p50 | strong search p50 / p99 |
|---|---|---|---|---|
| local root, the holder itself | 3.0 / 4.3 / 7.2 ms | 295 / 585 / 597 | 0.05 ms | 15.2 / 20.1 ms |
| local root, forwarded | 3.9 / 5.7 / 12.1 ms | 228 / 448 / 462 | 0.66 ms | 17.0 / 23.0 ms |
| S3 (MinIO on loopback), the holder itself | 9.5 / 11.8 / 30.3 ms | 59 / 58 / 72 | 0.05 ms | 15.0 / 34.7 ms |
| S3 (MinIO on loopback), forwarded | 10.4 / 13.3 / 30.9 ms | 51 / 57 / 95 | 0.69 ms | 17.7 / 62 ms |

A forwarded call costs one round trip plus its encoding. This is about
0.6 ms for a strong `get` (it has nothing else to do). It is about 1 ms for
a write (the holder also compares the record ids of the call with the
namespace). It is 1.5-3 ms for a search, because the packed result travels
back. On a local root, a forwarding
process writes at ~77% of the holder's own throughput, at 1, 4 or 8 threads.
On S3, where the append itself is the cost, it writes at ~86% with one thread. (The
forwarded 8-thread S3 row is
above the local one in both runs. S3 appends are one conditional PUT each,
and the holder's own threads queue on them differently. Do not plan on this
gain.) The multi-process tests measured failover: a holder writes, two
processes forward to it, and a SIGKILL stops the holder. The
acknowledgements of the forwarders paused for 0.7-1.1 s on a local root.
The lock becomes free at once; the pause is the time that the new writer
needs to open the namespace. On S3 with a 3 s lease TTL, they paused for
3.0-3.2 s. No run lost or duplicated an acknowledged write.
Another process took over a holder frozen past its lease (SIGSTOP) after 3.8 s.
When the holder froze in the middle of a PUT, this took ~31 s. MinIO keeps
the object of that PUT locked until its timeout (AWS S3 does not lock).

| setting | default | |
|---|---|---|
| `forwarding` / `MEMD_FORWARDING` | `auto` | `off`: a busy namespace raises `NamespaceBusyError` |
| `forward_secret` / `MEMD_FORWARD_SECRET` | `<data dir>/forward.secret` | at least 16 characters |
| `forward_host` / `MEMD_FORWARD_HOST` | `127.0.0.1` | the endpoint's bind address |
| `forward_port` / `MEMD_FORWARD_PORT` | `0` (ephemeral) | |
| `forward_advertise` / `MEMD_FORWARD_ADVERTISE` | the bind address | `host:port` other processes connect to; you must set it when you bind every interface |
| `forward_wait_s` / `MEMD_FORWARD_WAIT_S` | 30 (S3: lease TTL + 15) | how long a call looks for a writer that answers |
| `forward_timeout_s` / `MEMD_FORWARD_TIMEOUT_S` | 300 | how long one send of a call waits for its answer |
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
its own immutable object (`<key>.__part-000000000042`), and the logical object
is their ordered concatenation. Thus **one durable write ack = one PUT**, and a
torn frame cannot exist. The read-modify-write alternative would transfer
O(WAL) bytes per ack. Its latency and cost are too high, so memd does not use it.

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
things. A warm search touches object storage zero times because the local
derived index serves retrieval.

Timeouts: the writer's data client (segments, log parts, the manifest) has a
10 s connect timeout and botocore's 60 s read timeout. An append is a
conditional PUT, and memd never retries it early. Leases and the cluster
registry use a client of their own with 2 s / 4 s. The bucket and KMS calls
of a read replica use clients with `MEMD_REPLICA_CONNECT_TIMEOUT_S` /
`MEMD_REPLICA_READ_TIMEOUT_S` (2 s / 5 s; see "Read replicas").

What stays local: the SQLite derived index and, with the default `local`
key provider, the envelope **keys**. The index is rebuildable by contract; the
index snapshot published to the store is what makes a cold node cheap. Then data is remote and keys are not:
*one node with remote durability*. Crypto-shred works (destroy the local key
and the ciphertext is inert), but a second node cannot decrypt. With a remote
key provider (`aws-kms` or `vault-transit`, below) the wrapped keys live in
the bucket too, and any authorised node can serve any namespace.

Back the keys up with the data and restore them together (the keys directory
for `local`; the `keys/` objects for a remote provider). memd can open a
namespace with a key that did not write its data. Examples are the key files or wrapped
key object of another deployment, or a replaced root key. memd can also open
it with no key, or with encryption off. Then the open raises `KeyCustodyError` and changes nothing.
memd never reads such a namespace as empty, and never compacts it away. memd
never cuts off a complete log frame that does not read as a torn tail
either. [SECURITY.md](SECURITY.md) has the recovery
procedure for a damaged frame.

A **lease** keeps one writer for each namespace, not a file lock (`flock`
cannot see another machine). The first writer claims `ns/<ns>/.owner` with
a conditional PUT. A second writer does not get it. It forwards to the
first (see
[several processes on one data root](#several-processes-on-one-data-root)),
or, with forwarding off, it gets `NamespaceBusyError`. When a lease is older than
the TTL (`MEMD_LEASE_TTL_S`, default 60 s), a node can claim it again by
compare-and-swap on its ETag. Exactly one of several claimers wins.
Thus a crashed node cannot block a namespace forever. A holder that cannot
renew for 2/3 of the TTL stops writing (self-fencing).

### Key custody: local, AWS KMS or Vault transit

| `MEMD_KEY_PROVIDER` | root key held by | wrapped data keys | settings |
|---|---|---|---|
| `local` (default) | `<local_dir>/keys/root.key` | files beside it | - |
| `aws-kms` | AWS KMS (the CMK never leaves it) | `keys/<ns>.dek` in the store | `MEMD_KMS_KEY_ID` (ARN, id, alias; may contain `{namespace}`), `MEMD_KMS_REGION`/`AWS_REGION`, `MEMD_KMS_ENDPOINT` (LocalStack), `MEMD_KMS_SHRED` |
| `vault-transit` | Vault transit engine | `keys/<ns>.dek` in the store | `VAULT_ADDR`, `VAULT_TOKEN`, `MEMD_VAULT_TRANSIT_KEY` (default `memd`; may contain `{namespace}`), `MEMD_VAULT_TRANSIT_MOUNT`, `VAULT_NAMESPACE`, `MEMD_VAULT_SHRED` |

The same settings work as `Memory(config={"key_provider": "aws-kms",
"kms_key_id": ...})`. memd binds every data key to its namespace (KMS
encryption context / transit associated data). Moving an existing store:

```bash
memd keys status  --data s3://bucket/memd            # MEMD_LOCAL_DIR = the node holding the keys
memd keys migrate --data s3://bucket/memd --to aws-kms   # stop the server first
memd keys rotate  --data s3://bucket/memd            # re-wrap under the current key version
```

`migrate` never re-encrypts data: the data key does not change, only its
wrapping moves. It is idempotent and crash-safe (rerun it). It removes the
local key files only after every namespace verified under the new provider.
A node still set to `local` then refuses the store instead of minting keys.
SECURITY.md tells what crypto-shred means with a shared CMK.

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
  that receives the request **proxies** it there (one base URL for clients).
  During a handoff, the node retries the request for up to `MEMD_ROUTE_RETRY_S`
  (default 3 s), and then answers `503 namespace_unavailable` with `Retry-After`. A
  namespace nobody holds goes to the node that rendezvous hashing picks among
  the live nodes - a hint only; the lease is authoritative.
- **Membership** is a heartbeat object per node in the bucket
  (`_cluster/nodes/<id>.json`, every TTL/3), judged from one LIST on the
  bucket's own clock. No gossip, no consensus, nothing else to run.
  `--advertise` (`MEMD_ADVERTISE_URL`) is the URL peers use.
- **Handoff.** SIGTERM: the node deregisters, drains, flushes and releases
  its leases - peers take over at once. A crash: its leases go stale after
  `MEMD_LEASE_TTL_S` and a live node serves the next request for each namespace (and
  claims the lease again). memd fences a frozen node (GC pause, VM freeze)
  when it resumes: its writes fail instead of landing in the new
  owner's log.
- **Hosted billing** works through the router: the node that executes a request
  authenticates and meters it, once. The admin database is SQLite in
  `MEMD_STATE_DIR`, so a hosted fleet runs on one host (or a volume with
  working POSIX locks). memd has no networked admin store for hosted mode
  on several hosts.

To try it on one machine, use `docker-compose.yml`. It runs an S3 server (RustFS),
three nodes and (with `--profile vault`) a Vault dev server that holds the data
keys. See the comment at its top. `fly.toml` is a Fly.io template (one
machine = one node, node id and address from the machine; fill in every
`CHANGE-ME`; not deployed). The image needs `--build-arg MEMD_S3=1` for boto3.

memd writes every object that it rewrites in place (the manifest commit above all)
with compare-and-swap. Thus a node frozen mid-commit and resumed after
a takeover fails with `503 lease_lost`, and does not overwrite its successor.

Measured on 3 node processes (MinIO + moto KMS, TTL 4 s): leaseholder
SIGKILLed → next write acked after 3.6-3.8 s (bound: TTL + ε). SIGTERM →
0.03-1.1 s. Frozen past its TTL → 4.8-6.2 s. No case lost an acked write.
On MinIO, a node frozen in the middle of a lease write holds that
object's lock for MinIO's ~30 s timeout. Its takeover waits for it
(observed in 2 of 14 frozen runs; AWS S3 does not lock). memd has no
mode with several writers in one namespace.

### Read replicas (eventual reads)

Reads are **strong by default**: the node holding the namespace's lease
serves them, as above. A search or a get may opt into **eventual
consistency**. Then a **read replica** serves it, on whichever node
received it, with no hop. Thus a load balancer in front spreads a hot
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
  manifest, part, fence, key, custody marker, audit entry or snapshot). memd
  opens it over a read-only view of the store and the keys. It starts
  from the published index snapshot and the segments, and then follows the
  writer. Every `MEMD_REPLICA_REFRESH_S` (2 s), it reads the manifest, then the
  WAL and ops tails since its last read, and then the manifest again. On S3,
  the tails cost one LIST and a GET for each new part. After a compaction, a takeover or a new
  tenure, it rebuilds from the bucket. It catches up with a rotation.
- **Staleness bound**: the replica serves every write acknowledged before
  its last refresh started. A read accepts a replica no older than
  `X-Memd-Max-Staleness-Ms` (default 3 x the refresh interval). For an
  older replica, the read waits for a refresh for at most
  `MEMD_REPLICA_REFRESH_WAIT_MS` (1 s). It does not wait for a refresh that
  already ran that long (a bucket or KMS that hangs). Some reads go to the writer: a read that the
  replica still cannot serve, or that gets a key-custody or store error.
  The caller does not see this. A read that can go
  to the writer also does not wait for the open of a replica. The open
  continues in the background, and reads go to the writer until it is
  complete. (An embedded `read_only` Memory has no writer to go to, so it
  waits for the open.) If a replica failed to open, reads do not
  open it again for 5 s (doubling to 60 s while it keeps failing).
  They go to the writer at once. A replica's bucket and KMS calls have short timeouts of their own
  (below). The writer's data client has a 10 s connect timeout and
  botocore's 60 s read timeout. Every search and get answers
  `X-Memd-Served-By: leader|replica` (a replica adds `X-Memd-Replica-Seq`
  and `X-Memd-Replica-Age-Ms`, the age of the state the read was served
  from).
- **Read-your-writes** holds for strong reads only. An eventual read may
  miss a write acknowledged less than the bound ago. It may also still serve a
  record deleted that recently. Every replica stops serving a hard-deleted
  record within the bound. The replica scrubs the record from its files when
  it applies the delete. It also does this when it builds the files from
  durable data that still holds the record, with its purge pending (SECURITY.md). A replica never serves a
  state older than a state that it served. A rebuilt replica serves
  nothing until the refresh that rebuilt it also applied the log tail. A
  read waits for that for at most `MEMD_REPLICA_REFRESH_WAIT_MS`, and then
  the replica refuses it (over the router, the read goes to the writer).
- Served by replicas: `search` and `get` (`GET .../memories/{id}`). Always
  the writer: every write, `export`, `find_ids` / `forget` and `stats`. The
  namespace's ledger audits an export, and only the writer appends to it.
  For a forget, a preview must match what the confirm deletes. The serving
  node audits a replica-served search in its own ledger (`memd-node.<id>`, `replica_search`).
- A replica needs the namespace's data key like a writer (provider access
  for `aws-kms` / `vault-transit`). It never mints one. If it cannot use the
  key, it refuses the read (over the router, the read then goes to the writer).
- Embedded: `Memory(path, read_only=True)` opens every namespace as a
  replica. Every mutating call raises `ReadOnlyError`, and memd writes nothing
  to the store or the keys directory. Reads follow the writer (another
  process) within the bound. There is no writer to go to, so a read that
  the replica refuses raises `ReplicaUnavailableError` to the caller. This
  occurs after a rebuild until the replica applies the log tail, and after a failed
  rebuild. It also occurs when the replica is older than the bound and its refresh ran
  longer than the wait of the read. Retry it, or read from the writer. `search(..., consistency="eventual")` /
  `get(...)` on a writer `Memory` reads its own store when the namespace is
  open in it, else a replica.
- A replica follows a namespace destroyed (crypto-shredded) and created again
  under its name like any new tenure. It rebuilds and resolves the
  new key from its record. A replica whose rebuild failed (the bucket or the
  key provider unreachable) serves nothing until a rebuild completes. The
  reads go to the writer, and the replica tries again on its next refresh.

**Measured** with `bench/replica_bench.py`: three node processes on one MinIO
bucket on loopback, moto KMS, hash embedder, no reranker, one namespace of
2,000 records. Everything ran on one 8-core machine. Thus the absolute latencies
are a floor, and the throughput ceiling is this machine:

| replica lag, write ack -> first eventual read that sees it | p50 | p90 | p99 | max |
|---|---|---|---|---|
| refresh every 2 s (default), 300 samples | 996 ms | 1,815 ms | 2,005 ms | 2,014 ms |
| refresh every 0.5 s, 200 samples | 282 ms | 488 ms | 525 ms | 538 ms |

The lag is the phase against the refresh cycle plus one refresh (~10-20 ms
here); no read missed a write. The table below gives eventual searches, with
12 client threads and 20 s for each row, in two query mixes.
**repeat**: 8 fixed queries, so almost every search is a hit in the
repeat-query cache of the node. This measures the request path around the
cache, not a search. **unique**: the same queries, each made unique by an
extra term that matches nothing, so every search is a cache miss. This
measures the search itself. The hit rate is the `/metrics` count of each
run:

| query mix | serving nodes | searches/s | p50 | p99 | cache hits |
|---|---|---|---|---|---|
| repeat | 1 (the writer) | 155.8 | 71.4 ms | 250.9 ms | 99.4% |
| repeat | 2 (writer + 1 replica) | 333.1 | 34.0 ms | 89.0 ms | 99.8% |
| repeat | 3 (writer + 2 replicas) | 509.1 | 22.1 ms | 47.4 ms | 99.9% |
| unique | 1 (the writer) | 35.4 | 338.7 ms | 571.5 ms | 0.0% |
| unique | 2 (writer + 1 replica) | 94.0 | 125.6 ms | 252.2 ms | 0.0% |
| unique | 3 (writer + 2 replicas) | 170.3 | 67.6 ms | 147.3 ms | 0.0% |

Each serving node took an equal share. The cache-miss mix measures the
read capacity that replicas add. Here it scales better than linearly, but
only because one node with all 12 clients is far past saturation. Each node
gives 35 searches/s with 12 clients, 47 with 6 and 57 with 4. At a load
that one node can serve, expect about linear. The benchmark also measured the writer's own latency with replicas
attached (300 writes and strong searches). Write p50 / p99 was 16.5 / 32.3 ms
with no replica open, and 16.4 / 27.9 ms with two attached. It was 17.4 / 43.8 ms with
two serving a read load (shared CPU). A refresh with nothing new is 4
object-store requests (2 manifest GETs, a LIST of each log). That is ~2 requests/s
per open replica at the default interval, which is why idle replicas close.

A separate test, not the benchmark, measured **outages** on the same
machine. A TCP proxy in front of the replica's bucket or of one node's KMS
refused connections, or accepted them and never answered.
First, the bucket did not answer for 150 s, under an embedded `read_only`
Memory with a refresh every 0.5 s. No eventual read waited longer than
1.0 s before the replica refused it (over the router it goes to the
writer). The replica served again 0.9 s after the bucket came back.
Second, the KMS of one node failed, in a four-node HTTP cluster on the
same MinIO and moto KMS. The refresh was every 0.5 s, for three namespaces
that another node writes. When the KMS refused connections, the first
eventual read of each namespace on that node went to the writer. This took
0.2-0.5 s (up to about 1.1 s on a loaded machine). When the KMS did not
answer, this took 1.0-1.1 s (the wait of the read). The open of the replica
timed out in the background (2 x 5 s). The next reads went to the
writer in ~20 ms.

| setting | default | |
|---|---|---|
| `--node-id` / `MEMD_NODE_ID` | - | turns cluster mode on |
| `MEMD_CLUSTER_SECRET` | - | must be set, >= 16 chars, signs proxied requests |
| `--advertise` / `MEMD_ADVERTISE_URL` | `http://HOST:PORT` | must be set when you bind 0.0.0.0 |
| `MEMD_LEASE_TTL_S` | 60 | failover bound; keep clock skew under TTL/3 |
| `MEMD_ROUTE_RETRY_S` | 3 | how long an entry node absorbs a handoff |
| `MEMD_STATE_DIR` | - | must be set: keys.toml.json / hosted admin db |
| `MEMD_KEY_PROVIDER` | - | must be set: `aws-kms` or `vault-transit` (a `local` key file cannot be shared) |
| `MEMD_LOCAL_DIR` | `.memd-local` | node-local cache, under `<dir>/node-<id>` |
| `MEMD_CACHE_SWEEP_S` | 300 | how often a node drops its local copies of namespaces another node took over (also at startup; `0` = never) |
| `MEMD_REPLICA_REFRESH_S` | 2 | how often a read replica follows its writer (the staleness bound's base; `replica_refresh_s` in `Memory(config=...)`) |
| `MEMD_REPLICA_MAX_STALENESS_MS` | 3 x refresh | the default bound of an eventual read (`X-Memd-Max-Staleness-Ms` per request) |
| `MEMD_MAX_REPLICAS` | 64 | replica namespaces per node (LRU; memd deletes a closed replica's cache) |
| `MEMD_REPLICA_IDLE_S` | 300 | memd closes a replica that nobody read for this long (each costs 4 object-store requests per refresh with nothing new) |
| `MEMD_REPLICA_REFRESH_WAIT_MS` | 1000 | the longest an eventual read waits for a replica's refresh (or its open) before it goes to the writer |
| `MEMD_REPLICA_CONNECT_TIMEOUT_S` / `MEMD_REPLICA_READ_TIMEOUT_S` / `MEMD_REPLICA_MAX_ATTEMPTS` | 2 / 5 / 2 | a replica's bucket and KMS calls (its own clients). The writer's data client has a 10 s connect timeout and keeps botocore's 60 s read timeout. An append is a conditional PUT, and memd must not retry it early. |
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
namespaces. The first org that mints a key into a namespace claims it, and no
other org can ever claim it. Every key belongs to one org, and to one
namespace of that org. Scopes are exact and combine only
explicitly (`--scopes memory,override`):

| scope | grants |
|---|---|
| `memory` | the data routes `/v1/ns/{ns}/...` and the namespace's operational views `/v1/status`, `/metrics`, `/v1/metrics/json` |
| `billing` | `/v1/billing/checkout`, `/portal`, `/usage` - nothing else |
| `override` | only the cross-user capability of a `memory` key (reads across users, `include_deleted`, namespace crypto-shred); no route by itself |
| operator key (`MEMD_ADMIN_KEY`) | every namespace, every metric series; no org, never metered |

Namespace names follow the engine's grammar, matched in full
(`[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`, no trailing newline); memd reserves
`_`-prefixed names.
Keys keep the `memd_<ns>_<kid>_<secret>` format; memd stores only a SHA-256 hash of the
secret. The state lives in an admin SQLite database at
`<data root>/admin/admin.sqlite3`. It is beside the tenant store, never inside a
tenant namespace. Thus a tenant export does not include it, and a tenant
crypto-shred does not destroy it. `MEMD_ADMIN_KEY` stays the operator key: it spans namespaces, has no
org, and memd never meters it. Hosted mode does not honour self-hosted keys
(`keys.toml.json`); adopt them into an org with
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

Enforcement happens before the operation. Over a hard cap, the request gets
`402 {"code": "quota_exceeded", "meter": ..., "limit": ..., "used": ...}`. On a
paid plan, a soft limit never refuses: memd records the part above the included
quantity as billable overage. Hard caps hold under concurrency. The check
and a *reservation* of the requested quantity occur in one `BEGIN
IMMEDIATE` transaction, against the rollup and all the reservations in
progress. The usage commit releases the reservation in the same
transaction, and a failed operation also releases it. A reservation does
not expire while its request continues (for all the time that it takes).
A reservation of a crashed process stops counting after 10 minutes. 50
concurrent requests at a cap of 20 admit exactly 20.
memd never records more than the reserved quantity. Quota periods are calendar
months (UTC). **A spent `reranked_searches` quota never refuses a search**.
memd serves the search unreranked (`memd_rerank_fallback_total{reason="quota"}`)
and meters only `searches`. Only the `searches` cap limits
searches. A session close is a write: at the memories cap it answers 402. While
its extractor runs, it holds room for one memory only, so writes alongside it
do not starve. After the extraction, it reserves exactly
min(facts extracted, remaining headroom). memd does not write the facts beyond that
(`facts_capped` in the response). **Extraction runs on our key when the
server's environment has `MEMD_EXTRACTION_API_KEY`**. A hosted server
builds its engine from the environment, never from a client. Then memd meters every
session close as `extractions_our_key`, one unit per raw turn
the LLM extracted. The turns of a call that failed go through the local
pattern extractor (`raw_failed` in the response, and `raw_failed_by_reason`
by reason). If the provider sent a reply, the provider can bill the call,
so memd meters its turns too: a `malformed`, `empty`, `truncated` or
`oversize` reply. If the call got no reply (`http_status`, `transport`,
`timeout`), memd does not meter its turns.
Under a hard cap (the free plan's 10K a month), memd extracts the raw
records of the session up to the allowance that is left. The other records
stay raw-only: you can search them, but memd does not extract them
(`raw_skipped`). With no allowance left, the close answers 402 (the pattern
extractor does not take over). Without the
key, extraction runs locally, memd never meters it, and it never refuses. After
`invoice.payment_failed`, the org has a 7-day grace period
(`MEMD_BILLING_GRACE_DAYS`). After it, the org is **read-only**: writes and
session extraction answer `402 {"code": "payment_required"}`, searches, reads
and exports keep working. **Deletes (`DELETE /memories/{id}`, `forget`,
namespace crypto-shred) always work**, whatever the plan or payment
state, and no data is ever dropped for non-payment.

**How metering works.** Each metered request writes a usage event (a UUID,
the org, the namespace, the meter, the quantity) to an append-only ledger in
the admin store. The same transaction writes the period rollup that quota
checks read. It commits with `synchronous=FULL` *after* the operation
succeeded and *before* the response is sent. Thus memd bills every acknowledged
operation, and does not bill an operation that the client never saw
acknowledged (a crash between the two). Each hour
(`MEMD_BILLING_PUSH_INTERVAL_S`), a background job puts the rows that are
not pushed into batches (for each org, meter and hour). It stores the id of
each batch (a UUIDv5 of the UUIDs of its events). Then it sends the batch as a
[Stripe Billing Meter Event](https://docs.stripe.com/api/billing/meter-event/create)
with that id as both `identifier` and idempotency key. After a crash at any
point, the job sends the same key again, so Stripe records each batch one
time. The job pushes only the billable
part (overage on dev, everything metered on scale).
**Stripe only remembers an idempotency key for about 24 hours**. Thus the job
never pushes usage older than 20 hours (`MEMD_BILLING_MAX_PUSH_AGE_S`, capped at 20 h)
automatically. The job abandons a batch that waited that long, and does not send it
again. After an outage, a second send could bill a batch that Stripe has
already. Those events become `needs_reconcile`. Each job tick compares them
with the Stripe meter event summary for their quota period (up to their
last hour). The verified missing quantity is what Stripe must hold (all
settled batches in that window, plus these events) minus what Stripe
reports. Zero means that the earlier sends landed, and the job sends
nothing. The job pushes a missing quantity of up to the events' own units once, under a fresh idempotency
key recorded before the send. The job does not guess in the other cases: Stripe holds more than the
ledger, or lacks more than these events. Then it raises a drift alert and
sends nothing, and the events stay `needs_reconcile` for a human. The job retries a window with a
batch still pending or sent within the last hour (`MEMD_BILLING_SETTLE_S`;
summaries lag) on a later tick. memd takes a snapshot of the gauges
(`memories_stored`, `stored_gb`) once per UTC day. It sends `stored_gb`
in milli-GB, so it does not round small tenants up to a whole GB. Price
that meter per 1/1000 GB, and give it the `last` aggregation. A daily
drift report compares what the ledger settled for the quota period to date
with Stripe's meter event summaries. When they differ by
more than `MEMD_BILLING_DRIFT_TOLERANCE` (1%), it raises `memd_billing_drift_alerts_total{meter}` (and a
`drift_alert` row in the admin store's `billing_log`). Billing metrics carry
`ns="_billing"`, so only operator keys see them on `/metrics`.

**Routes** (billing-scoped key; the org is always the key's own):

| route | does |
|---|---|
| `POST /v1/billing/checkout` `{"plan": "dev"\|"scale", "interval": "month"\|"year"}` | a Stripe Checkout Session (subscription: the flat price plus the plan's metered prices, open for 1 h); returns `{url, id}`. One subscription per org: `409 already_subscribed` while one is live, `409 checkout_pending` (with the open session's `url`) while a checkout is open |
| `POST /v1/billing/portal` | a Billing Portal session; returns `{url}` |
| `GET /v1/billing/usage` | current-period usage per meter: used, limit, hard, metered, billable; plan, status, grace, read-only |
| `POST /v1/billing/webhook` | Stripe's webhook endpoint (no bearer: the `Stripe-Signature` is the authentication) |

memd verifies the webhook with `stripe.Webhook.construct_event`
(HMAC-SHA256 over the raw body, timestamp tolerance 300 s). The webhook is
idempotent by Stripe event id: the `processed_events` row commits in the
same transaction as the effect of the event. memd refuses a body over
1 MiB (counted as it reads it, chunked or not).
Handled:
- `checkout.session.completed`: only `mode=subscription` sessions with a
  subscription. memd **reads the subscription again from Stripe**, and *its* status
  applies (a payment-mode session never grants a plan nor clears past_due).
- `customer.subscription.created/updated/deleted`: `active`/`trialing` grant
  the plan the subscription's prices bill. `past_due` keeps it and starts the
  grace clock. `incomplete`, `incomplete_expired`, `unpaid`, `paused` and
  `canceled` grant nothing (free plan). A per-org cursor on the event's
  `created` ignores re-ordered older events, so a late
  `checkout.session.completed` cannot resurrect a deleted subscription.
- One subscription for each org. An event for a subscription that is not
  the current subscription of the org never changes the plan. A cancel of
  it does not downgrade the org. memd lists each *other live*
  subscription as a duplicate (`memd_billing_duplicate_subscriptions_total`,
  a `duplicate_subscription` log row). While ANY duplicate is in the list,
  memd **holds** the metered usage of the org. It does not push it, because
  each subscription would bill it. When the current subscription ends,
  memd reads each duplicate again from Stripe, one at a time. The first
  one that is still live becomes current (its invoices count from then on).
  Dead ones, and ones that Stripe no longer has (404), leave the list. One
  that memd cannot read now stays in the list (and held) until its next
  event. None of this ever fails the webhook: the
  cancellation always takes effect. Each subscription also keeps its own
  event cursor, and a tombstone after it ends. Thus a late or re-ordered event
  can neither re-list an ended subscription nor undo a newer one.
- `invoice.payment_failed` (starts the grace period once) and
  `invoice.payment_succeeded` count only for the org's current
  subscription; `checkout.session.expired` closes the open checkout.
- memd binds an org to a Stripe customer only when the org has none yet.
  Also, memd's checkout for that org must have created the customer (its
  `metadata.memd_org_id`). memd logs and ignores anything else.
- memd acknowledges (200) and counts events for customers that it does not know
  (`memd_billing_webhook_unmapped_total`); a Stripe read that fails answers
  500 so Stripe retries.

**Configuration** (environment):

| variable | meaning |
|---|---|
| `MEMD_HOSTED` | `1` turns on hosted mode (same as `serve --hosted`) |
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
customer id, the meter's event name, a number and a timestamp. The org's
name goes into the Stripe customer record once, at first checkout.
`extractions_our_key` counts only when extraction runs on the operator's LLM
key (`MEMD_EXTRACTION_API_KEY` in the server's environment), and only the
turns the LLM extracted. memd never bills the local pattern extractor.

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
  (user > agent > tool > web > import), lineage, actor. Packed context fences
  untrusted content. Explicit saves inherit session taint, and quarantine +
  rate limits catch MINJA-style injection. It also has a hash-chained audit log, namespace
  crypto-shred, and record-level hard delete with ≤72h physical purge deadline.
- **Retrieval**: rules-based planner (no reflection loop) → fan-out over
  the lanes → RRF fusion with trust-aware tie-breaks →
  optional rerank of the lexical top-30. Then: validity filter (current/as_of) →
  packing into `budget_tokens` (default 12,000). The packing counts each piece of
  evidence once. The lanes are BM25 (SQLite FTS5/porter, ranked by bm25; optionally accelerated by
  tantivy), the entity lane and the time lane on recency intent. With a
  real embedder, there is also the vector lane (an exact flat scan; from
  `ann_min_vectors` vectors on, an optional usearch HNSW sidecar). The default
  layout uses session excerpts with dates. The other
  layout is a flat list in prefix-stable order (KV-cache friendly). Gated
  evidence packing is an experimental opt-in. See "Packing and the budget",
  below. memd does not fuse the hash embedder's vector lane (`fuse_vector`,
  below).
- **Extraction**: async, batched, re-runnable. BYO OpenAI-compatible
  key for LLM extraction/embeddings; heuristic provider keeps facts working
  with zero keys; local ONNX embeddings with the optional fastembed extra.

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
OpenAI-compatible embeddings API when `embedder` is `auto` or `openai`).
Also `embedding_model` / `MEMD_EMBEDDING_MODEL` (`text-embedding-3-small`) and
`embedding_base_url` / `MEMD_EMBEDDING_BASE_URL`
(`https://api.openai.com/v1`). As below, a config value wins over the env
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
  `[<turn id>] <time, UTC> <speaker>: text`. The speaker is `user`,
  `assistant`, `agent`, `system` or `tool` (from the writer's `role`). memd
  writes a line break in the text of a turn as `\n`, so a turn cannot
  pose as another line or speaker. The turn ids are new for each call
  (`9f3a1c-1`, `9f3a1c-2`, ...). Thus the text of a turn cannot name
  another turn, and memd does not send record ids. The prompt tells the model to
  attribute each fact to the speaker: an assistant's suggestion is not the
  user's fact. It also tells the model to name the turns that the fact comes from. A fact
  takes its scope, actor and time from those turns. If a fact names no
  turn of its chunk, it takes them from the first user turn of the chunk,
  else from the first turn. Facts record the prompt
  version (`v2`).
- **Bounded calls.** Every call sends `max_tokens` (an uncapped call to a
  model that looped once ran to 131,072 output tokens and 413 s). The whole
  call (the connection, the request, the response headers and body) stops
  at `extraction_timeout_s`. This is also true when the provider sends bytes slowly. (OpenRouter
  keeps a connection alive with whitespace while the model generates, and a
  per-read timeout restarts with every byte.) The call runs on its own
  thread and connection. At the deadline, the session close continues and
  memd closes the connection. The abandoned thread stops at its next read,
  or, for a provider that stopped sending, after `extraction_timeout_s`
  more.
- **Request options** pass the provider its own settings: OpenRouter
  routing (`{"provider": {"order": ["deepinfra"], "allow_fallbacks":
  false}}`), reasoning off or lower for a reasoning model (`{"reasoning":
  {"enabled": false}}`, `{"reasoning": {"effort": "low"}}`), sampling
  (`{"temperature": 0.2}`). Another option is `{"max_tokens": null,
  "max_completion_tokens": 4096, "temperature": null}` for a model that
  rejects `max_tokens` and `temperature`. `model`, `messages` and `stream`
  are the extractor's own, and memd refuses them. It also refuses invalid JSON and a
  non-object.
- **Failures degrade to the pattern extractor.** A call fails on an HTTP
  error, when the provider is not reachable, or at the timeout. It also fails on a reply cut off at
  the cap (`finish_reason: "length"`), or an empty reply (a reasoning model
  that answered with its reasoning only). A reply over the size cap, or a
  reply without a JSON array, also fails the call. memd never sends a failed call again. The
  turns of that chunk go through the pattern extractor instead. memd
  counts the failure as
  `memd_extraction_chunks_failed_total{model, reason}` (`http_status`,
  `transport`, `timeout`, `truncated`, `empty`, `oversize`, `malformed`).
  `close_session` returns `extraction_errors` (the number of failed calls),
  `raw_failed` (their turns) and `raw_failed_by_reason` (their turns by
  reason), and audits `extraction_degraded` with the reasons. memd stores the raw
  turns in all cases. Within a reply that parses, memd drops
  a malformed item (no text, or `entity_keys` / `lineage` not a string or a
  list of strings). It counts the item as
  `memd_extraction_items_malformed_total{model}`, and keeps the other facts of the reply.
- **Privacy: with the LLM extractor active, memd sends every closed session's raw
  turns to the extraction provider** (see SECURITY.md).
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

- **Reranker.** A relevance judge reorders the top-30 of the bm25 lane (plus the vector lane with a real
  embedder). The rest follows in fused
  order. On LongMemEval_S, Jev reranking took session ndcg@5 from 0.89 to
  0.95. A failed, slow (> `rerank_timeout_s`) or malformed judgement keeps
  the unreranked order and counts `memd_rerank_fallback_total{reason}`;
  search never fails because of it. `stats()["reranker"]` reports name,
  model, calls, fallbacks and p50 latency. **Privacy: with Jev active, memd sends the
  query and the top-30 candidate texts of every search to
  TypeSafe's API** (see SECURITY.md). No key, no egress.
- **Gated packing** (`pack_mode="gated"`, an experimental opt-in for a
  calibrated reranker such as Jev). The packed context holds only the
  candidates judged relevant (p ≥ `rerank_gate`, else the top 3). Each
  candidate comes with the turns next to it, grouped by session under a
  session-date header. `budget_tokens` still caps the context, and the
  provenance fencing is the same. It
  trades recall for tokens. In an experiment it matched top-k QA accuracy with 27%
  fewer tokens over a 100-candidate shortlist. But over the product's top-30,
  it drops second evidence sessions (LongMemEval_S session recall_all@5
  0.803 gated vs 0.928 ranked, with Jev). When a reranker ran, gated
  packing has priority over `packing`. The default packs all candidates, in
  the `packing` layout.
- **tantivy accelerator.** A derived index next to SQLite, fed in the
  background (every 500ms or 512 changes). FTS5 stays the synchronous source
  of truth, so the write ack does not change. FTS5 serves the writes that are
  not yet in the index. memd rebuilds it in the background when it is missing,
  corrupt or not closed cleanly. A search error sends that query to FTS5.
  Only damage (I/O, missing or corrupt files) rebuilds it while it runs,
  with exponential backoff. A successful rebuild does not erase the
  failure history (it decays after 10 minutes without a failure).
  `stats()["lexical"]` shows `rebuilds`, `failures`, `failures_total` and
  `retry_in_s`. Both backends order tied bm25 scores the same way
  (score, -t_event, content hash, id). With tantivy the top-k is
  deterministic for the same operation history *and commit schedule*. Its
  BM25 statistics count deleted and superseded docs until their segments
  merge. Thus the same history, committed in a different rhythm, can put
  two near-equal docs in a different order.

- **usearch sidecar** (vector lane). A derived HNSW index (cosine,
  connectivity 16, f16) beside SQLite, keyed by record rowid and holding the
  live records' vectors. SQLite's vectors table stays the source of truth.
  Vectors reach it as the embed worker applies them (queued under the index
  lock, applied right after it: the write ack never waits on it). Deletes,
  supersession, quarantine and hard deletes remove entries at once. A query
  takes usearch's top k × `ann_overfetch` (widened once), then the same SQL
  filter and post-check as every lane. It re-scores exactly from SQLite, and
  orders ties like fusion. Answered exactly instead: filters admitting at most
  `ann_exact_max` rows, sweeps (`find_ids`), a window still short after
  widening, and a sidecar not ready yet. `stats()["vector_index"]` counts
  them in `fallback_exact_total`. memd uses its file only when it matches the
  SQLite file's vector watermark exactly. It rebuilds missing, corrupt, foreign, stale or
  pre-purge files in the background (temp file + fsync +
  rename; a kill never leaves a torn file in use). A hard-delete purge
  deletes its files and rebuilds it from SQLite (usearch removal only marks
  entries). memd publishes it with the index snapshot, so a cold node
  installs it and does not rebuild it. recall@10 is 0.99+ on dense
  embeddings up to 1M vectors, ~0.94 on the sparse vectors of the hash
  embedder (see BENCHMARKS.md). A crash costs a rebuild (~5 min at 1M on 4
  threads). Files and published images carry a blake2b checksum. memd
  verifies it before usearch reads them, because usearch trusts what it
  loads (a file corrupted in place crashed the process in search). A
  rebuild from SQLite follows a failed check. It also follows a process
  that stopped while a loaded graph was on probation. The probation lasts until the graph has
  served 200 searches or 300 s, and a clean close ends it.
  memd checks a loaded graph's bookkeeping at load, and its first answers
  against the exact scan (recall < 0.8 rebuilds it). A checksum-valid
  file that memd did not write takes write access to the data directory:
  an adversarial local file, outside the threat model (see SECURITY.md). At least 100
  candidates are re-ranked by exact cosine. memd searches the writes that are still in the
  queue for the index exactly (read-your-writes). After each
  build, memd inserts poorly linked nodes again (also a duplicate vector,
  when a search gets only to its identical twin). Thus independent rebuilds
  give the same top-10, and a scoped search that drops the twin still finds
  it. Every vector path admits only live rows, whatever
  `include_quarantined` / `include_invalid` ask, and sweeps beside the
  sidecar stream exactly from SQLite.
- **While the sidecar is not serving** (loading at open, rebuilding, or
  failed to attach), the exact scan serves up to `flat_max_vectors`. Above
  that, memd never loads its float32 matrix (1.5 GB at 1M × 384). Sweeps,
  and filters that admit at most `ann_exact_max` rows, get an exact answer,
  streamed from SQLite. Other queries skip the vector lane: bm25 and the
  other lanes serve them
  (`memd_vector_lane_skipped_total{reason="ann_rebuilding"}`,
  `stats()["vector_index"]["skipped_total"]`), and such results are not
  cached.
- **Sidecar save and load are off the request path.** The open of a
  namespace does not wait for its sidecar to load (a background thread
  loads it). The close gives the final save to a background thread (the
  engine close waits for it; a destroy cancels it). usearch holds the GIL
  for all of a save or load, so the process pauses for it on each thread.
  At 200K × 384 f16 we measured ~100 ms for each save and ~105 ms for each
  load. Python reads and writes files up to 256 MB outside the GIL, so the
  pause is a memory copy. usearch saves and loads larger files directly,
  and the pause is longer. `stats()["vector_index"]["last_gil_hold_ms"]` and
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
- `items` lists each record that the text shows: the hits in rank order.
  After each hit come the turns that it brought (`lanes` `["source"]` or
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
The questions form a sample stratified by type. Multi-session and temporal-reasoning
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
  cost does not increase with the length of a session. A seek also finds turns that share
  one timestamp (for example, a session imported with one date). A
  20,000-turn session with one timestamp has a median
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
