# Security

## Reporting

Please report suspected vulnerabilities privately via GitHub's "Report a
vulnerability" (Security -> Advisories) rather than a public issue. Include a
reproduction if you have one; this project's convention is that a finding is
confirmed by a runnable probe.

## What the threat model covers

memd's controls are mapped in [07-security.md](07-security.md) and gated by an
adversarial suite that runs in CI (`make gate`): cross-tenant isolation,
untrusted-source fencing, injection quarantine, stale-fact-after-update,
supersedence history integrity, and taint escalation. A change that regresses
any probe fails the build.

## What it does NOT cover - read this before deploying

- **Single writer per data root.** A second process opening the same namespace
  fails fast with `NamespaceBusyError`. `uvicorn --workers N` with N>1 and two
  containers on one volume do not work. `MEMD_ALLOW_MULTI_PROCESS=1` disables
  the check and re-enables silent data loss; it exists for recovery tooling.
- **At-rest encryption with the default `local` key provider is local-file
  envelope encryption.** It protects the volume and makes crypto-shred
  possible. It is *not* protection against someone with filesystem read
  access on the same machine (the root key file sits beside the data). See
  "Key custody" below for the KMS / Vault providers.
- **Key custody fails closed.** Restore the keys together with the data they
  were written with (the keys directory for `local`; the `keys/` objects for
  a remote provider). A key that is valid but not that data's (another
  deployment's `keys/` directory or `keys/<ns>.dek` object, a replaced
  `root.key`), no key at all, or encryption turned off for an encrypted
  namespace makes the open raise `KeyCustodyError`; nothing is read,
  truncated or deleted, no key is
  created, and the namespace opens normally once the right keys are back.
  The manifest holds a fingerprint of each namespace's data key (an HMAC of
  a fixed label under the key) to check this before anything is touched; it
  reveals nothing about the key. A manifest without one (every namespace
  0.2.0 wrote) is probed: its encrypted objects are tried until one
  decrypts, and the key is refused only if none does; with encryption off,
  a namespace none of whose data parses as plaintext is refused. The
  fingerprint is written only once the key has decrypted something, or when
  nothing encrypted exists yet. A complete WAL or ops frame that does not
  read - does not decrypt, or does not parse - is never cut off or folded
  away, under any key and with encryption on or off: only a frame cut short
  by its length prefix is a torn tail. Under the proven key a damaged
  segment is skipped by reads but kept on disk (and the deletes a compaction
  could not apply to it stay pending until it reads again); a damaged log
  frame refuses the open - see "Recovering from an unreadable log frame"
  below. In a namespace already open it refuses every fold, and only that:
  writes and deletes still succeed (the size-triggered rotate after them
  is deferred and reported), and export leaves the frame out.
- **With the S3 backend and `local` keys, data is remote but KEYS ARE LOCAL.**
  Crypto-shred works (destroying the local key makes the remote ciphertext
  inert), but a second node cannot decrypt the bucket - it refuses to open a
  namespace it has no key for (it used to mint a new one). Back up the local
  key directory separately and treat it as the crown jewels - losing it is
  equivalent to shredding every namespace. Cluster mode refuses `local` keys.
- **Single-writer on S3 is a LEASE, not a distributed lock.** The first writer
  claims `ns/<ns>/.owner` with a conditional PUT and a second gets
  `NamespaceBusyError`; a lease older than the TTL is reclaimable (by
  compare-and-swap: one winner) so a crashed node cannot wedge a namespace. A
  holder that cannot renew for 2/3 of the TTL stops writing, a node taking
  over a stale lease fences the previous holder's append logs, and every
  object rewritten in place - the manifest commit above all - is written
  with compare-and-swap, so a holder frozen between its check and its write
  fails when it resumes instead of overwriting its successor. What remains
  open (ADR-12): the audit ledger's rotation can still overwrite a
  successor's sealed audit segment in that window (the hash chain then fails
  verification), and clock skew between nodes must stay under TTL/3. Do not
  run two writers on one namespace and rely on it; the cluster router
  (ADR-12) never does.
- **The local data directory is trusted.** Derived caches under it (the
  SQLite index, the tantivy and usearch sidecars) are checksummed or
  rebuildable against accidental damage, not against someone who can write
  there: a crafted, checksum-valid usearch file is an adversarial local
  file. memd bounds what one can do - a graph that crashes the process
  during its probation is rebuilt at the next open, one whose structure or
  first answers disagree with SQLite is rebuilt - but does not make usearch
  safe to load it. Protect the data directory like the key directory.

## Key custody (ADR-12)

- **Where the root key lives.** `MEMD_KEY_PROVIDER=local` (default): a file,
  `<local_dir>/keys/root.key` (0600), on the node. `aws-kms`: in AWS KMS; memd
  holds only wrapped data keys (`keys/<ns>.dek` in the bucket) and asks KMS to
  unwrap them. `vault-transit`: in Vault's transit engine, likewise. With a
  remote provider neither the bucket alone (ciphertext + wrapped keys) nor
  the provider alone (no data) is enough; an attacker needs both bucket read
  and `kms:Decrypt` (or the transit `decrypt` policy). Grant the latter only
  to memd nodes, with the encryption context condition
  `kms:EncryptionContext:memd:namespace` if you want per-tenant policies.
- **Binding.** Each wrapped data key is bound to its namespace (KMS encryption
  context / transit `associated_data`): a wrapped key copied under another
  namespace's name does not unwrap. A Vault that silently ignores
  `associated_data` is refused rather than used unbound.
- **Custody fails closed.** An unreadable `keys/_custody.json` refuses the
  open on every root (never delete it to get past that - restore it). A
  namespace with encrypted data is never given a freshly minted key, and a
  data key that is not its data's is refused before anything is read, on
  every root and under every provider ("Key custody fails closed" above: the
  manifest's key fingerprint, or a probe of the data). The two checks are
  complementary: the custody marker says which PROVIDER wraps the store's
  data keys and refuses a node configured for another one before it can
  mint anything; the fingerprint is of the data key itself, so it survives
  `memd keys migrate` and `memd keys rotate` (both re-wrap the same key) and
  catches a wrapped key that unwraps fine but is not this data's - another
  deployment's `keys/<ns>.dek` restored over this one's. A remote provider
  resolves the key when the namespace opens, and refuses to mint one for a
  namespace that already has a manifest (`MEMD_KEYS_ALLOW_MINT_EXISTING=1`
  only for a namespace that was never encrypted). Log frames that are
  complete but do not read are never truncated as a "torn tail" - the open
  is refused instead.
- **Plaintext data keys are in node memory** while a namespace is open (they
  must be, to encrypt), LRU-bounded to 1024 namespaces per process.
- **Crypto-shred with a SHARED CMK / transit key** (the normal deployment)
  deletes the namespace's wrapped key object - including every noncurrent
  version on a versioned bucket (a versioned bucket whose versions API memd
  may not use makes the destroy fail loudly). It cannot delete the CMK, which
  every other namespace needs. So a copy of the wrapped key made BEFORE the
  shred - a bucket backup, cross-region replication, a snapshot - together
  with `kms:Decrypt` on the CMK still recovers the data key. Keep backups of
  the `keys/` prefix under the same retention you promise for erasure, or
  exclude it from them. For tenants who need the stronger guarantee, give them
  their OWN key (`MEMD_KMS_KEY_ID=alias/memd-{namespace}` /
  `MEMD_VAULT_TRANSIT_KEY=memd-{namespace}`) and set
  `MEMD_KMS_SHRED=schedule-deletion` (or `disable`) / `MEMD_VAULT_SHRED=delete-key`:
  the shred then also destroys that key, and old copies of the wrapped key
  are dead. These key-level actions are refused at startup for a shared key.
  The destroy is recorded in the node's audit ledger with what the provider
  did.
- **Migration** (`memd keys migrate`) removes the local key files only after
  every namespace verified under the new provider; `--keep-local` keeps them,
  and then crypto-shred does NOT cover those copies until they are removed.
- **Cluster traffic.** Nodes proxy requests to each other over plain HTTP
  unless you front them with TLS; `MEMD_CLUSTER_SECRET` authenticates the
  routing header (HMAC over node, time, client, method and path, 60 s
  window) but does not encrypt anything. Run the node-to-node network as a
  private segment. A request without a valid signature is simply routed like
  any client request.
- **Audit retention is bounded** (16 sealed segments, 64MB each by default).
  Past that the oldest is dropped and the hash chain is re-anchored, so
  `verify()` proves tamper-evidence over the *retained window*.
  `memd_audit_segments_pruned_total` records the truncation. Compliance tiers
  needing unbounded history must ship segments off-box before they roll.
- **`/metrics`, `/v1/metrics/json` and `/v1/status` are namespace-scoped** for
  a scoped key and unscoped for a `*` key: series labelled with a namespace
  are served only to that namespace's keys and the operator. Series without a
  namespace label (the http counters) are visible to every key, so their
  labels carry nothing a caller chose: the route is one of the server's
  route templates (`/v1/ns/:ns/memories/:id`) or `other`, and an unknown HTTP
  method is `OTHER`. A namespace label is always the key's authorized
  namespace, never one taken from the request path (a denied request's
  rate-limit rejection counts under the key's own namespace). `MEMD_METRICS_PUBLIC=1` allows
  unauthenticated scraping - only do that on a trusted network segment.
- **Interactive docs are off by default** (`MEMD_ENABLE_DOCS=1` to enable):
  the OpenAPI schema enumerates every route of an otherwise authenticated API.
- **Trust tiers are advisory to the model, not a sandbox.** Fencing marks
  untrusted content as data; it cannot force a model to respect that.
- **With the Jev reranker active, search text leaves the machine.** For every
  search, the query and the top-30 candidate texts (date, role and the first
  2,000 characters of each; no ids, scopes or metadata) are sent to
  TypeSafe's API. `reranker="auto"` (the default) selects Jev only when a
  TypeSafe key is configured and `typesafe-sdk` is installed; with no key,
  nothing leaves the machine. A key counts from EITHER source:
  `TYPESAFE_API_KEY` in the environment, or `typesafe_api_key` in the
  `Memory(config=...)` dict - so passing the key in config for some other
  purpose also turns on this egress unless `reranker` is set explicitly. Set
  `MEMD_RERANKER=none` (or `local`, a cross-encoder that runs in-process) to
  keep a keyed deployment local. In hosted mode the key is read from the
  server's environment only; clients never send one.
- **The derived indexes hold plaintext.** The SQLite index and, with
  `memd[fast]`, the tantivy index (`<ns>.tantivy/` beside it) contain record
  text unencrypted, with owner-only permissions. Both are deleted on
  crypto-shred and are rebuildable from the (encrypted) log.
  In a multi-node deployment each node keeps its own local copy for the
  namespaces it has served, and a hard delete is scrubbed physically only
  on the node that owns the namespace when it runs. A node that served a
  namespace earlier and has not taken it back since still holds the text
  it indexed then, including records hard-deleted later on another node,
  until it next opens that namespace (its stale copy is then discarded) or
  its cache directory is removed. It never serves that copy, but it is on
  its disk: when a hard delete must reach every disk, clear the local cache
  directory (`local_dir`) of the nodes that no longer own the namespace.
  On the owning node the purge's scrub waits until no reader holds a
  snapshot of the index older than it: a process outside memd that keeps a
  read transaction open on the SQLite file (a backup tool, an ad-hoc
  `sqlite3` shell) keeps the erased text in the file's WAL for as long as
  it holds on - memd keeps retrying and logs a warning every 60 s while
  serving normally (the namespace stays open: the LRU does not close it
  while its scrub runs). A close in the meantime leaves the scrub to the
  next open, which finishes it in the background if such a reader still
  holds on; no index snapshot is published until it is done. Do not attach
  long-lived readers to the cache files.

- **Hosted mode & billing (`--hosted`, off by default).**
  - *The Stripe webhook is authenticated by its signature only.*
    `POST /v1/billing/webhook` takes no bearer key; it verifies the
    `Stripe-Signature` HMAC-SHA256 over the raw body with
    `MEMD_STRIPE_WEBHOOK_SECRET` (constant-time, via
    `stripe.Webhook.construct_event`) and rejects timestamps outside the
    tolerance (300 s), so a captured delivery cannot be replayed later; a
    replay inside the window is a no-op (idempotent by event id). Treat the
    webhook secret like a password: anyone holding it can forge plan
    upgrades. Rotate it in the Stripe dashboard if it leaks.
  - *Live keys are refused.* A `sk_live_`/`rk_live_` key stops hosted mode
    from starting unless `MEMD_ALLOW_LIVE_BILLING=1` is set - set it only on
    the production deployment, never in a dev or CI environment.
  - *API keys are hashed at rest.* Hosted keys live in the admin store
    (`<data root>/admin/admin.sqlite3`; the directory is 0700 and the
    database with its `-wal`/`-shm` files 0600) as SHA-256 hashes of a
    192-bit random secret; the key is shown once at creation. A fast hash is
    appropriate for secrets of that entropy (there is nothing to brute-force);
    it would not be for passwords. Revocation takes effect within 5 s in
    every process (the lookup cache's TTL).
  - *Tenant isolation is enforced twice:* a key is bound to one namespace,
    and that namespace must belong to the key's org (a namespace can never
    change org; `_`-prefixed names are reserved and never bound to a
    tenant; names are matched in full, so a trailing newline is refused).
    Billing routes act on the key's own org only - the request body cannot
    name another one. Scopes are exact: `billing` for the billing routes
    only; `memory` for the data routes and the namespace's `/v1/status`,
    `/metrics` and `/v1/metrics/json`; `override` grants no route by itself.
  - *Stripe state is verified, not taken from payloads.* A completed
    Checkout Session applies only with `mode=subscription`, and the
    subscription is re-read from Stripe; only `active`/`trialing` grant a
    plan. An org is bound to a Stripe customer only when it has none and the
    customer carries the `memd_org_id` metadata memd's checkout wrote - a
    forged `client_reference_id` (e.g. on a payment link) is logged and
    ignored. One subscription per org; a duplicate holds metered pushes.
  - *No secret in logs or the admin store.* Stripe error text (which can
    echo the API key) is redacted before it is logged or stored - `sk_`/
    `rk_`/`pk_` keys, `whsec_` secrets, bearer tokens and memd keys - and a
    filter redacts the Stripe SDK's own log records. `repr()` of the billing
    configuration carries no secret.
  - *The admin store is not tenant data.* It holds org names, Stripe customer
    ids, key hashes and usage counts (no content). It is not encrypted by the
    namespace envelope keys and is not included in exports; back it up with
    the data root.

## Recovering from an unreadable log frame

An open that refuses raises `KeyCustodyError`, and its message says which
case it is. Nothing was changed in any of them.

- *"the key does not match"*, *"none of its data decrypts"*, *"no data
  key"*: key custody. Restore the `keys/` directory the data was written
  with (`root.key` and `ns-<namespace>.key` together) and open again with
  encryption on. Nothing is lost.
- *"looks encrypted ... opened it with encryption off"*: open with
  encryption on (the default) and those keys.
- *"a complete WAL frame (at byte N of ns/<namespace>/wal) is unreadable
  although the data key in hand is the right one: the frame is damaged"*
  (or an `ops` frame; with encryption off, *"(it does not parse)"*): the log
  is damaged on disk - bit rot, a bad copy, a hand edit. memd will not cut it
  off, because it may hold acknowledged writes, and everything logged after
  it is behind it. To recover:
  1. Stop memd and back up the data root (`store/` and `keys/`; on S3 the
     `ns/<namespace>/` prefix and the local keys directory).
  2. If a backup holds the log from before the damage, restoring it is the
     lossless fix.
  3. Otherwise remove exactly the frame the message names - its 4-byte
     length prefix and the payload that prefix announces - and nothing
     else. Frames are self-contained (each carries its own sequence
     number), so the ones after it keep their place; only the writes or ops
     in the damaged frame are lost, and the backup from step 1 still holds
     its bytes:

     ```python
     p, n = "store/ns/<namespace>/wal", N   # the path and byte from the message
     b = open(p, "rb").read()
     ln = int.from_bytes(b[n:n + 4], "big")
     open(p, "wb").write(b[:n] + b[n + 4 + ln:])
     ```

     On S3 the log is not one object: it is stored as part objects
     (`ns/<namespace>/wal.__part-NNN`, plus `wal.__seq`), and byte N counts
     across the parts in order. Find the part that holds byte N and delete
     that one part object (every frame in it is lost; the backup from step 1
     still holds them). Do not re-upload an edited single `wal` object: memd
     does not read one.

     Removing an `ops` frame loses the deletes it held: records it
     tombstoned come back, and a hard delete in it is undone, so the purged
     text is served again. After recovering, repeat any delete that was
     acknowledged around the time of the damage.
  4. Open again; another damaged frame, if any, is reported the same way.

While the namespace stays open with a damaged frame (a warm open does not
read every frame, so it may only show up at the next rotate), it keeps
serving, and fails closed only where the frame would be lost:

- *Rotate and compaction refuse*, with the message above: each deletes the
  log it read. Writes and deletes succeed all the same - the rotate or
  purge compaction they trigger is deferred, not raised: it is logged,
  counted (`memd_ns_maintenance_failures_total{op}`,
  `memd_ns_maintenance_failing`), shown as `maintenance` in `stats()` and
  `status()` (`/v1/status`) and as a count in `/health`
  (`maintenance_failing`), and retried after a backoff (30 s, doubling to
  10 min). The logs grow until a fold succeeds, and a due hard delete is not
  purged until then: alert on `memd_ns_maintenance_failing`.
- *Export leaves a damaged WAL frame out* and exports everything else -
  run it before step 1 to have the readable data in hand. A warning names
  the frame's byte offset, `memd_export_frames_skipped_total` counts it,
  and the export's audit entry lists it (`skipped_frames`). Over REST every
  export answers `X-Memd-Export-Skipped-Frames: N` (`0` when it is
  complete; the NDJSON body holds records only); the Python SDK's
  `export_jsonl()` sets `last_export_skipped_frames` (and logs a warning
  when it is not 0), the TypeScript SDK sets `lastExportSkippedFrames`, and
  embedded `Memory` sets `last_export_skipped_frames` too. A damaged `ops`
  frame still refuses the export (now with an error status, not a 200
  stream cut short): leaving out a delete would export the records it
  deleted, hard-deleted text included.
- *Every acknowledged delete is applied at once* - to the index and, for a
  hard delete, to the purge schedule - so `get()` and search stop serving
  the record whatever the rotate does. (Through 0.3.0 the refused rotate ran
  first and skipped both; a cache left that way is healed by the first
  compaction after recovery, which removes anything the log deleted that
  the index still serves - `memd_index_settled_total`.)

A damaged segment never blocks an open: it is skipped by reads, kept by
compaction (`"unreadable": true` in the manifest,
`memd_segments_quarantined_total`), and served again once its bytes are
restored from a backup.

## Hardening checklist for a real deployment

1. Set `MEMD_ADMIN_KEY` to a high-entropy value; never hand it to applications.
   Mint per-namespace keys with `memd key create --namespace <ns>`.
2. Terminate TLS in front of the process. memd speaks plain HTTP.
3. Bind to `127.0.0.1` unless something else is enforcing authn at the edge.
4. Leave `MEMD_ENABLE_DOCS` and `MEMD_METRICS_PUBLIC` unset.
5. Set `MEMD_NS_RATE_LIMIT_PER_MIN` to a real per-tenant ceiling.
6. Back up the data root - the object store is the source of truth; the
   SQLite index beside it is disposable. In hosted mode also back up
   `admin/admin.sqlite3` (orgs, keys, the usage ledger): it is not
   rebuildable.
7. Hosted billing: use an `sk_test_` key until go-live; restrict the
   webhook endpoint to Stripe's events; keep `MEMD_STRIPE_WEBHOOK_SECRET`
   and `MEMD_STRIPE_SECRET_KEY` out of images and logs.
