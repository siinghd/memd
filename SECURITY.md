# Security

## Reporting

Report a suspected vulnerability privately, through GitHub's "Report a
vulnerability" (Security -> Advisories). Do not open a public issue for it.
If you have a reproduction, include it. The convention of this project is
that a runnable probe confirms a finding.

## What the threat model covers

memd has these controls: trust tiers and provenance fencing, session taint,
injection quarantine and per-namespace isolation. It also has a hash-chained
audit log, hard delete with a physical-purge deadline, and per-namespace
crypto-shred. An
adversarial suite that runs in CI (continuous integration) gates them
(`make gate`). The suite covers cross-tenant isolation, untrusted-source
fencing, injection quarantine, stale-fact-after-update, supersedence history
integrity, and taint escalation. If a change causes a regression in any
probe, the build fails.

## What it does NOT cover - read this before deploying

- **Single writer per namespace.** Only the one process that holds the lock
  (or lease) of a namespace writes to it. A second process that opens the
  namespace forwards its calls to that process (next item). With
  `forwarding="off"`, the second process fails immediately with
  `NamespaceBusyError`. `MEMD_ALLOW_MULTI_PROCESS=1` disables the check and
  makes silent data loss possible again; it exists for recovery tools.

- **The write-forwarding endpoint.** Every process that can write a data root
  listens on a TCP port (forwarding is on by default). This is `127.0.0.1`
  and an ephemeral port, unless `MEMD_FORWARD_HOST` / `MEMD_FORWARD_PORT` set
  other values. The process writes its address into the lock files and
  leases that it holds.

    Any caller that can call the endpoint can run writes and strong reads
    (export included) on every namespace that the process holds, as that
    process. Thus, the endpoint is as powerful as the data directory, and it
    is not a tenant boundary. A REST server checks API keys before a call
    gets to the engine. But the endpoint trusts its caller like a local
    caller, source tier and actor included.

    A **secret** protects the endpoint: `<data dir>/forward.secret` (on S3,
    in the `local_dir`), 32 random bytes in hex, created at first use with
    mode 0600. Thus, only the processes that can read the data directory can
    call. That directory already holds the data and, with the `local`
    provider, its keys. Instead of the file, the secret can be
    `MEMD_FORWARD_SECRET`. Then you must protect it like `MEMD_CLUSTER_SECRET`.

    memd does not follow a symbolic link when it opens the file. It uses the
    file only if it is a regular file of the process's own user, with mode
    0600 or narrower. memd refuses a secret that another user could have
    planted or read. That process then runs with forwarding off and logs the
    reason.

    memd authenticates each connection in both directions before it reads a
    call:

    1. The endpoint sends its id and a nonce.
    2. The caller answers with its own nonce and an HMAC-SHA256 under the
       secret over both nonces. This HMAC (hash-based message authentication
       code) is the request signature of the cluster router.
    3. The endpoint answers with its own HMAC.

    The caller also checks that the endpoint id is the one that the lock or
    lease named. Thus, a caller does not talk to a process that now uses the
    port of a dead holder. After the handshake, every frame has an HMAC-SHA256
    over its direction and sequence number, under a key derived for the
    connection from both nonces. If a frame is altered, replayed, reordered
    or reflected, the connection closes, and no part of the frame runs
    (`memd_forward_auth_failures_total`).

    What the endpoint does NOT give: **confidentiality**. Frames are plain
    TCP. On loopback, only a local root user can read them. WARNING: If you
    bind `MEMD_FORWARD_HOST` to a network interface, memd sends memory content
    in clear text across that network. Keep that network private: use a VPN
    (virtual private network) or a private segment.

    The endpoint also does NOT give **availability** against a local process.
    Anyone can open connections (up to 256 at once). memd drops an
    unauthenticated connection after 5 s, before it reads more than a 4 KiB
    hello. memd counts each refusal and logs it as one line. A caller that
    cannot get a connection waits up to `MEMD_FORWARD_WAIT_S`, and then fails
    with nothing applied.

    A holder applies a forwarded write only while it holds the namespace.
    memd applies a retried call one time (request ids, caller-generated
    record ids).

- **At-rest encryption with the default `local` key provider is local-file
  envelope encryption.** It protects the volume and makes crypto-shred
  possible. It does *not* protect against a person with filesystem read
  access on the same machine (the root key file is next to the data). For
  the KMS (Key Management Service) / Vault providers, refer to "Key custody"
  below.

- **Key custody fails closed.** Restore the keys together with the data that
  memd wrote with them (the keys directory for `local`; the `keys/` objects
  for a remote provider). The open raises `KeyCustodyError` in these cases:

    - The key is valid, but it is not the key of that data. Examples: the
      `keys/` directory or a `keys/<ns>.dek` object of another deployment,
      or a replaced `root.key`.
    - There is no key at all.
    - Encryption is off for an encrypted namespace.

    In these cases, memd does not read, truncate or delete anything, and it
    creates no key. When the correct keys are back, the namespace opens
    normally.

    The manifest holds a fingerprint of the data key of each namespace (an
    HMAC of a fixed label under the key). memd uses it to do this check
    before it touches anything. The fingerprint tells nothing about the key.
    memd probes a manifest without a fingerprint (every namespace that 0.2.0
    wrote). It tries the encrypted objects until one decrypts, and refuses
    the key only if none does. With encryption off, memd refuses a namespace
    if none of its data parses as plaintext.

    memd writes the fingerprint only after the key has decrypted something,
    or if nothing encrypted exists yet. A complete WAL (write-ahead log) or
    ops frame can fail to read: it does not decrypt, or it does not parse.
    memd never cuts off or folds away such a frame. This is true under any
    key, with encryption on or off. Only a frame cut short by its length
    prefix is a torn tail.

    Under the proven key, reads skip a damaged segment, but memd keeps it on
    disk. The deletes that a compaction could not apply to it stay pending
    until it reads again. A damaged log frame refuses the open: refer to
    "Recovering from an unreadable log frame" below. In a namespace that is
    already open, a damaged log frame refuses every fold, and only that.
    Writes and deletes still succeed (memd defers and reports the
    size-triggered rotate after them), and export leaves the frame out.

- **With the S3 backend and `local` keys, data is remote but KEYS ARE LOCAL.**
  Crypto-shred works: when you destroy the local key, the remote ciphertext
  becomes inert. But a second node cannot decrypt the bucket. It refuses to
  open a namespace that it has no key for (older versions minted a new key).
  WARNING: If you lose the local key directory, the result is the same as a
  shred of every namespace. Make a separate backup of it. Protect it as your
  most valuable data. Cluster mode refuses `local` keys.

- **Single-writer on S3 is a LEASE, not a distributed lock.** The first
  writer claims `ns/<ns>/.owner` with a conditional PUT. A second writer
  does not get it: it forwards to the first, or gets `NamespaceBusyError`.
  When a lease is older than the TTL (time to live), a node can claim it
  again by compare-and-swap (only one node wins). Thus, a node that crashed cannot
  block a namespace. A holder that cannot renew for 2/3 of the TTL stops
  writing. A node that takes a stale lease fences the append logs of the
  previous holder.

    memd uses compare-and-swap to write every object that it rewrites in
    place, most of all the manifest commit. Thus, a holder that froze between
    its check and its write fails when it continues, and does not overwrite
    its successor. Two limits remain. First, in that window, the rotation of
    the audit ledger can still overwrite a sealed audit segment of a
    successor. The hash chain then fails verification. Second, clock skew
    between nodes must stay under TTL/3.

    Do not run two writers on one namespace and rely on the lease. The
    cluster router never does this.

- **memd trusts the local data directory.** The derived caches in it (the
  SQLite index, the tantivy and usearch sidecars) have checksums, or memd
  can rebuild them. This protects them against accidental damage, not against a
  person who can write in the directory. A crafted usearch file with a valid
  checksum is an adversarial local file.

    memd limits what such a file can do. If a graph crashes the process
    during its probation, memd rebuilds it at the next open. memd also
    rebuilds a graph if its structure or first answers do not agree with
    SQLite. But memd does not make usearch safe to load such a file. Protect
    the data directory as you protect the key directory.

## Key custody

- **Where the root key lives.** `MEMD_KEY_PROVIDER=local` (default): in a
  file on the node, `<local_dir>/keys/root.key` (0600). `aws-kms`: in AWS
  KMS. memd holds only wrapped data keys (`keys/<ns>.dek` in the bucket) and
  asks KMS to unwrap them. `vault-transit`: in the transit engine of Vault,
  in the same way.

    With a remote provider, the bucket alone (ciphertext + wrapped keys) is
    not enough, and the provider alone (no data) is not enough. An
    attacker needs both read access to the bucket and `kms:Decrypt` (or the
    transit `decrypt` policy). Give that permission only to memd nodes. For
    per-tenant policies, add the encryption context condition
    `kms:EncryptionContext:memd:namespace`.

- **Binding.** memd binds each wrapped data key to its namespace (KMS
  encryption context / transit `associated_data`). If you copy a wrapped key
  under the name of another namespace, it does not unwrap. If a Vault
  ignores `associated_data` and gives no error, memd refuses it. memd does
  not use the key unbound.

- **Custody fails closed.** If `keys/_custody.json` is unreadable, memd
  refuses the open on every root. WARNING: Never delete this file to get
  past the refusal. Restore it.

    memd never gives a new key to a namespace with encrypted data. memd
    refuses a data key that is not the key of its data before it reads
    anything. This is true on every root and under every provider. (Refer to
    "Key custody fails closed" above: the key fingerprint of the manifest, or
    a probe of the data.)

    The two checks are complementary. The custody marker tells which
    PROVIDER wraps the data keys of the store. It refuses a node
    configured for another provider before that node can mint anything. The
    fingerprint is of the data key itself, so it survives `memd keys migrate`
    and `memd keys rotate` (both re-wrap the same key). It also finds a
    wrapped key that unwraps correctly but is not the key of this data. An
    example is the `keys/<ns>.dek` of another deployment, restored over the
    key of this deployment.

    A remote provider resolves the key when the namespace opens. It refuses
    to mint a key for a namespace that already has a manifest. (Use
    `MEMD_KEYS_ALLOW_MINT_EXISTING=1` only for a namespace that was never
    encrypted.) If a log frame is complete but does not read, memd never
    truncates it as a "torn tail". It refuses the open instead.

- **Plaintext data keys are in node memory** while a namespace is open (this
  is necessary for encryption). An LRU (least recently used) cache holds
  them, with a limit of 1024 namespaces per process. Known limitation: a
  process keeps the key of a namespace in that cache after it stops writing
  the namespace (LRU eviction, a lost lease).

    Another process (another node) can destroy the namespace and create it
    again under the same name. Then this process can open it again as its
    writer. In that case, the open checks the new data with the cached old
    key and refuses it (`KeyCustodyError`: fails closed, nothing read or
    written). This continues until the process restarts or the key leaves
    the cache.

    Read replicas do not have this problem. A replica resolves the key from
    its record again when it rebuilds for another tenure (a new lineage), and
    after a custody refusal. It never keeps a key that failed verification.

- **Crypto-shred with a SHARED CMK / transit key** (the normal deployment)
  deletes the wrapped key object of the namespace. This includes every
  noncurrent version on a versioned bucket. (If memd cannot use the versions
  API of a versioned bucket, the destroy fails with an error.) The shred
  cannot delete the CMK (customer master key), because every other
  namespace needs it. Thus, a copy of the wrapped key made BEFORE the shred,
  together with `kms:Decrypt` on the CMK, still recovers the data key. Such
  a copy can be a bucket backup, cross-region replication or a snapshot.

    WARNING: Keep backups of the `keys/` prefix under the same retention that
    you promise for erasure, or do not include the prefix in backups.

    Some tenants need the stronger guarantee. Give them their OWN key
    (`MEMD_KMS_KEY_ID=alias/memd-{namespace}` /
    `MEMD_VAULT_TRANSIT_KEY=memd-{namespace}`). Then set
    `MEMD_KMS_SHRED=schedule-deletion` (or `disable`) / `MEMD_VAULT_SHRED=delete-key`.
    The shred then also destroys that key, and old copies of the wrapped key
    become useless. memd refuses these key-level actions at startup for a
    shared key. memd records the destroy, with what the provider did, in the
    audit ledger of the node.

- **Migration** (`memd keys migrate`) removes the local key files only after
  it verifies every namespace under the new provider. `--keep-local` keeps
  them. Then crypto-shred does NOT cover those copies until you remove them.

- **Cluster traffic.** Nodes proxy requests to each other over plain HTTP,
  unless you put TLS (Transport Layer Security) in front of them.
  `MEMD_CLUSTER_SECRET` authenticates the routing header (HMAC over node,
  time, client, method and path, 60 s window), but it does not encrypt
  anything. Run the node-to-node network as a private segment. memd routes
  a request without a valid signature like any client request.

- **Audit retention has a limit** (by default, 16 sealed segments of 64MB
  each). After that limit, memd drops the oldest segment and re-anchors the
  hash chain. Thus, `verify()` proves tamper-evidence over the *retained
  window*. `memd_audit_segments_pruned_total` records the truncation. If a
  compliance tier needs unbounded history, it must copy the segments off the
  machine before they roll.

- **`/metrics`, `/v1/metrics/json` and `/v1/status` are namespace-scoped** for
  a scoped key and unscoped for a `*` key. memd serves series labelled with a
  namespace only to the keys of that namespace and to the operator. Series
  without a namespace label (the http counters) are visible to every key.
  Thus, their labels contain nothing that a caller chose. The route is one of
  the route templates of the server (`/v1/ns/:ns/memories/:id`) or `other`,
  and an unknown HTTP method is `OTHER`.

    A namespace label is always the authorized namespace of the key, never a
    namespace from the request path. (The rate-limit rejection of a denied
    request counts under the namespace of the key.) `MEMD_METRICS_PUBLIC=1`
    lets a client read the metrics without authentication. WARNING: Use it only on a trusted
    network segment.

- **Interactive docs are off by default** (`MEMD_ENABLE_DOCS=1` turns them on).
  The OpenAPI schema shows every route of an API that otherwise needs
  authentication.

- **Trust tiers are advisory to the model, not a sandbox.** Fencing marks
  untrusted content as data. It cannot force a model to obey that mark.

- **With the Jev reranker active, search text leaves the machine.** For
  every search, memd sends the query and the top-30 candidate texts to the
  API of TypeSafe. For each candidate, it sends the date, the role and the
  first 2,000 characters; it sends no ids, scopes or metadata.
  `reranker="auto"` (the default) selects Jev only if you configure a
  TypeSafe key and install `typesafe-sdk`. With no key, nothing leaves
  the machine.

    A key counts from EITHER source: `TYPESAFE_API_KEY` in the environment,
    or `typesafe_api_key` in the `Memory(config=...)` dict. Thus, if you put
    the key in config for some other purpose, this egress also starts, unless
    you set `reranker` explicitly. To keep a deployment with a key local, set
    `MEMD_RERANKER=none` (or `local`, a cross-encoder that runs in-process).
    In hosted mode, memd reads the key only from the environment of the
    server. Clients never send a key.

- **With an embedding key, record text leaves the machine.** The
  OpenAI-compatible embedder sends the text of every record and every search
  query to the embeddings API (`embedding_base_url`, OpenAI by default).
  memd embeds the record text in the background after the write. This
  embedder is active with `embedder="openai"`,
  or with `auto` and an embedding key. The key can come from EITHER source:
  `MEMD_EMBEDDING_API_KEY` in the environment or `embedding_api_key` in
  the `Memory(config=...)` dict. Config `embedding_api_key=""` (or
  `embedder="hash"` / `"fastembed"`) keeps a process local when you set the env
  var.

- **With the LLM extractor active, session turns leave the machine.** On
  `close_session`, memd sends the raw turns of the session to the extraction
  provider (`extraction_base_url`, OpenAI by default). For each turn, it
  sends the time, the speaker and the full text, under a turn number created
  for the call. The LLM (large language model) extractor is active when you
  configure an extraction key. The key can come from EITHER source:
  `MEMD_EXTRACTION_API_KEY` in the environment, or `extraction_api_key` in
  the `Memory(config=...)` dict. Config `extraction_api_key=""` keeps a
  process local when you set the env var. With no key, the pattern extractor
  runs in-process, and nothing leaves the machine.

- **The derived indexes hold plaintext.** The SQLite index and, with
  `memd-engine[fast]`, the tantivy index (`<ns>.tantivy/` next to it) contain record
  text that is not encrypted, with owner-only permissions. memd deletes both
  on crypto-shred, and can rebuild both from the (encrypted) log.

    In a multi-node deployment, each node keeps its own local copy of the
    namespaces that it serves. This copy is the SQLite index with its
    `-wal`/`-shm`, the tantivy copy and the ANN (approximate nearest
    neighbor) sidecar. memd
    scrubs a hard delete physically only on the node that owns the namespace
    when the scrub runs.

    A node drops its copy of a namespace if another node became its owner
    after this node last served it. This is the case if the manifest shows
    the lineage of another tenure, or if the namespace is gone. The node
    drops the copy at these times:

    - When the namespace closes after this node lost its lease, if the new
      owner has already committed its tenure. If not, at the next sweep.
    - At startup.
    - Every `cache_sweep_s` (300 s; `MEMD_CACHE_SWEEP_S`, `0` = never), for
      every namespace that the node does not have open. Each sweep reads one
      manifest for each such namespace.

    Thus, text that another node hard-deletes leaves the disk of this node
    within that interval after the other node became the owner of the
    namespace.
    Usually, this occurs before the delete itself. It never served that copy.

    The node keeps its copy of a namespace if it was the last node to serve
    it (its reopen stays warm). It also keeps a copy that another process has
    open (processes that share a `local_dir` serve from the same files). A
    stopped node keeps its copies until it starts again. When you
    take a node out of service, remove its local cache directory
    (`local_dir`).

    On the owning node, the scrub of the purge waits until no reader holds a
    snapshot of the index that is older than the scrub. A process outside
    memd can keep a read transaction open on the SQLite file (a backup tool,
    an ad-hoc `sqlite3` shell). Then the erased text stays in the WAL of the
    file for as long as that process holds the transaction. During that
    time, memd tries again, logs a warning every 60 s and serves normally.
    The namespace stays open: the LRU does not close it while its scrub
    runs.

    If the namespace closes in the meantime, the next open does the scrub.
    If such a reader still holds its transaction, the next open completes the
    scrub in the background. memd publishes no index snapshot until the
    scrub is complete. Do not attach long-lived readers to the cache files.

- **Read replicas keep a plaintext copy too, and are
  eventually consistent.** memd uses a replica for an eventual read on a
  node that does not hold the lease of the namespace, and for
  `Memory(..., read_only=True)`. The replica keeps its own derived cache
  (SQLite index, tantivy copy, ANN sidecar) under
  `<cache dir>/replicas/<pid>-<token>/`, for exactly as long as it is open.
  When the replica closes (LRU eviction, `MEMD_REPLICA_IDLE_S` without
  reads, shutdown), memd deletes the files. If a process died with replicas
  open, the next memd process that starts on the same cache directory
  deletes its directory. Each process holds an `flock` on its own.

    On a replica, the next refresh of the replica applies a hard delete that
    the writer acknowledged. The replica stops serving the record within the
    staleness bound (default 3 x `MEMD_REPLICA_REFRESH_S`, 6 s). memd does
    not read a replica that is older than the bound. Then the replica
    scrubs its own files in the background (FTS (full-text search) merge,
    vacuum, WAL truncation, tantivy and ANN sidecar rebuilt). That is sooner
    than the purge of the writer (up to the 72 h deadline).

    A compaction, a takeover or a new tenure makes the replica delete its
    files and rebuild from the bucket. The durable data in the bucket still
    holds a hard-deleted record until the purge of the writer. A replica
    built from that data (bootstrapped or rebuilt) applies the delete and
    scrubs its files in the same way. A rebuilt replica serves nothing until
    it has also applied the log tail. Thus, after a replica has served a
    delete, it never serves the deleted record again.

    At its next refresh, the replica of a destroyed (crypto-shredded)
    namespace deletes its files and drops the data key that it held. The same
    occurs if the namespace was destroyed and created again under its name
    before that refresh. The rebuild resolves the key of the new incarnation
    from its record (never from a cache). memd never keeps a key that fails
    verification.

    A replica never writes or deletes an object: no manifest, log part,
    fence, wrapped key, custody marker, audit entry or snapshot. memd opens
    it over a read-only view of the store and the keys, and every write path
    refuses on it. A replica never mints a data key. It needs the key of the
    namespace exactly like a writer (the same `keys/` directory with
    `local`, provider access with `aws-kms` / `vault-transit`). A missing or
    wrong key is a `KeyCustodyError`, never an empty replica (over the
    router, the read goes to the writer).

    Eventual reads are opt-in (`X-Memd-Read-Consistency: eventual`). An
    eventual read can miss a write. It can also still serve a record whose
    delete memd acknowledged less than the bound ago. memd audits a search
    that a replica serves in the ledger of the serving node
    (`memd-node.<id>`, action `replica_search`, with the name of the
    namespace). A replica cannot append to the ledger of the namespace. An
    embedded `read_only` Memory audits nothing.

- **Hosted mode & billing (`--hosted`, off by default).**
  - *memd authenticates the Stripe webhook by its signature only.*
    `POST /v1/billing/webhook` takes no bearer key: it verifies the
    `Stripe-Signature` HMAC-SHA256 over the raw body with
    `MEMD_STRIPE_WEBHOOK_SECRET` (constant-time, through
    `stripe.Webhook.construct_event`). It rejects timestamps outside the
    tolerance (300 s), so nobody can replay a captured delivery later. A
    replay inside the window has no effect (idempotent by event id).
    WARNING: Protect the webhook secret like a password: a person who has it
    can forge plan upgrades. If it leaks, rotate it in the Stripe dashboard.
  - *memd refuses live keys.* A `sk_live_`/`rk_live_` key prevents the start
    of hosted mode, unless you set `MEMD_ALLOW_LIVE_BILLING=1`. Set it only on
    the production deployment, never in a development or CI environment.
  - *memd hashes API keys at rest.* Hosted keys are in the admin store
    (`<data root>/admin/admin.sqlite3`; the directory is 0700, and the
    database with its `-wal`/`-shm` files is 0600). They are SHA-256 hashes
    of a 192-bit random secret. memd shows the key one time, when it creates
    it. A fast hash is correct for secrets of that entropy (there is nothing
    to brute-force), but it would not be correct for passwords. Revocation
    has effect within 5 s in every process (the TTL of the lookup cache).
  - *memd enforces tenant isolation twice:* each key belongs to one
    namespace, and that namespace must belong to the org of the key. A
    namespace can never change org. memd reserves names with a `_` prefix,
    and never binds them to a tenant. memd matches names in full, so it refuses a trailing
    newline. Billing routes operate on the org of the key only; the request
    body cannot name another org. Scopes are exact: `billing` gives the
    billing routes only; `memory` gives the data routes and the `/v1/status`,
    `/metrics` and `/v1/metrics/json` of the namespace. `override` gives no
    route by itself.
  - *memd verifies Stripe state, and does not take it from payloads.* A completed
    Checkout Session applies only with `mode=subscription`, and memd reads
    the subscription again from Stripe. Only `active`/`trialing` give a
    plan. memd binds an org to a Stripe customer only if the org has no
    customer. The customer must also have the `memd_org_id` metadata that
    the memd checkout wrote. memd logs and ignores a forged `client_reference_id`
    (for example, on a payment link). Each org has one subscription; a
    duplicate holds metered pushes.
  - *No secret in logs or the admin store.* Stripe error text can echo the
    API key. Before memd logs or stores this text, it redacts these items in
    it: `sk_`/`rk_`/`pk_` keys, `whsec_` secrets, bearer tokens and memd
    keys. A filter also redacts the log records of the Stripe SDK (software
    development kit) itself. `repr()` of the billing configuration contains
    no secret.
  - *The admin store is not tenant data.* It holds org names, Stripe
    customer ids, key hashes and usage counts (no content). The namespace
    envelope keys do not encrypt it, and exports do not include it. Include
    it in the backup of the data root.

## Recovering from an unreadable log frame

An open that refuses raises `KeyCustodyError`. Its message tells which case
it is. In all cases, memd changed nothing.

- *"the key does not match"*, *"none of its data decrypts"*, *"no data
  key"*: key custody. Restore the `keys/` directory that memd used to
  write the data (`root.key` and `ns-<namespace>.key` together). Then open
  again with encryption on. You lose nothing.
- *"looks encrypted ... opened it with encryption off"*: open again with
  encryption on (the default) and with those keys.
- *"a complete WAL frame (at byte N of ns/<namespace>/wal) is unreadable
  although the data key in hand is the right one: the frame is damaged"*
  (or an `ops` frame; with encryption off, *"(it does not parse)"*): the log
  is damaged on disk (bit rot, a bad copy, a manual edit). memd does not cut
  off the frame, because it can hold acknowledged writes, and everything
  logged after it is behind it. To recover:
    1. Stop memd. Then make a backup of the data root (`store/` and `keys/`;
        on S3, the `ns/<namespace>/` prefix and the local keys directory).

    2. If a backup holds the log from before the damage, restore it. This is
        the fix without loss.

    3. WARNING: This step loses the writes or ops in the damaged frame. The
        backup from step 1 still holds its bytes. If you remove an `ops`
        frame, you lose the deletes that it held. Records that it tombstoned
        come back. The removal also undoes a hard delete in it, so memd serves the
        purged text again.

        If no such backup exists, remove exactly the frame that the message
        names: its 4-byte length prefix and the payload that this prefix
        announces. Remove nothing else. Frames are self-contained (each has
        its own sequence number), so the frames after it keep their place:

        ```python
        p, n = "store/ns/<namespace>/wal", N   # the path and byte from the message
        b = open(p, "rb").read()
        ln = int.from_bytes(b[n:n + 4], "big")
        open(p, "wb").write(b[:n] + b[n + 4 + ln:])
        ```

        On S3, the log is not one object. memd stores it as part objects
        (`ns/<namespace>/wal.__part-NNN`, plus `wal.__seq`), and byte N
        counts across the parts in order. Find the part that holds byte N.
        WARNING: When you delete that part, you lose every frame in it (the
        backup from step 1 still holds them). Delete that one part object.
        Do not upload an edited single `wal` object again: memd does not
        read one.

        After you recover, do again each delete that memd acknowledged at
        about the time of the damage.

    4. Open again. If there is another damaged frame, memd reports it in the
        same way.

A namespace can stay open with a damaged frame. A warm open does not read
every frame, so the damage can show only at the next rotate. In that case,
the namespace continues to serve, and fails closed only where the frame would
be lost:

- *Rotate and compaction refuse*, with the message above, because each of
  them deletes the log that it read. Writes and deletes succeed all the
  same. memd defers the rotate or purge compaction that they trigger, and
  does not raise its error. memd logs it, counts it
  (`memd_ns_maintenance_failures_total{op}`, `memd_ns_maintenance_failing`),
  and shows it as `maintenance` in `stats()` and `status()` (`/v1/status`)
  and as a count in `/health` (`maintenance_failing`). memd tries it again
  after a backoff (30 s, doubled each time, up to 10 min).

    The logs grow until a fold succeeds, and memd does not purge a due hard
    delete until then. Set an alert on `memd_ns_maintenance_failing`.

- *Export leaves a damaged WAL frame out* and exports everything else. Run
  it before step 1, so that you have the readable data. A warning names the
  byte offset of the frame, `memd_export_frames_skipped_total` counts it,
  and the audit entry of the export lists it (`skipped_frames`).

    Over REST, every export answers `X-Memd-Export-Skipped-Frames: N` (`0`
    when the export is complete; the NDJSON (newline-delimited JSON) body
    holds records only). The `export_jsonl()` of the Python SDK sets
    `last_export_skipped_frames` (and logs a warning when it is not 0). The
    TypeScript SDK sets `lastExportSkippedFrames`, and embedded `Memory` also
    sets `last_export_skipped_frames`.

    A damaged `ops` frame still refuses the export (now with an error
    status, not a 200 stream cut short). If memd left out a delete, the
    export would contain the records that it deleted, hard-deleted text
    included.

- *memd applies every acknowledged delete at once*, to the index and, for a
  hard delete, to the purge schedule. Thus, `get()` and search stop serving
  the record, whatever the rotate does.

    Up to and including 0.3.0, the refused rotate ran first and skipped
    both. The first compaction after recovery repairs a cache left in that
    state. It removes anything that the log deleted and that the index still
    serves. It also applies a supersede or quarantine that the index missed
    (`memd_index_settled_total`).

A damaged segment never blocks an open. Reads skip it, and compaction keeps
it (`"unreadable": true` in the manifest, `memd_segments_quarantined_total`).
When you restore its bytes from a backup, memd serves it again.

## Hardening checklist for a real deployment

1. Set `MEMD_ADMIN_KEY` to a high-entropy value. Never give it to
   applications. Mint per-namespace keys with
   `memd key create --namespace <ns>`.
2. Put a TLS proxy in front of the process, so that TLS ends at the proxy.
   memd speaks plain HTTP.
3. Bind to `127.0.0.1`, unless something else enforces authentication
   (authn) at the edge. Do the same for the write-forwarding endpoint
   (`MEMD_FORWARD_HOST`, default `127.0.0.1`). Make sure that only the
   service user can read `forward.secret` in the data directory (memd
   creates it with mode 0600). As an alternative, set `MEMD_FORWARDING=off` on a root
   that only one process ever opens.
4. Do not set `MEMD_ENABLE_DOCS` and `MEMD_METRICS_PUBLIC`.
5. Set `MEMD_NS_RATE_LIMIT_PER_MIN` to a real per-tenant limit.
6. Make a backup of the data root. The object store is the source of truth;
   the SQLite index next to it is disposable. In hosted mode, also make a
   backup of `admin/admin.sqlite3` (orgs, keys, the usage ledger), because
   memd cannot rebuild it.
7. Hosted billing: use an `sk_test_` key until go-live. Restrict the webhook
   endpoint to the events of Stripe. Keep `MEMD_STRIPE_WEBHOOK_SECRET` and
   `MEMD_STRIPE_SECRET_KEY` out of images and logs.
