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
- **At-rest encryption is local-file envelope encryption.** It protects the
  volume and makes crypto-shred possible. It is *not* protection against
  someone with filesystem read access on the same machine. Hosted deployments
  are expected to swap the root-key provider for a KMS; that provider does not
  ship yet.
- **Key custody fails closed.** Restore the keys directory together with the
  data it was written with. A key that is valid but not that data's (another
  deployment's `keys/`, a replaced `root.key`), no key at all, or encryption
  turned off for an encrypted namespace makes the open raise
  `KeyCustodyError`; nothing is read, truncated or deleted, and the
  namespace opens normally once the right keys are back. The manifest holds
  a fingerprint of each namespace's data key (an HMAC of a fixed label under
  the key) to check this before anything is touched; it reveals nothing
  about the key. Data that does not authenticate is never repaired or
  compacted away: under a key that is not proven the open is refused, and
  under the proven key a damaged segment is skipped by reads but kept on
  disk, a damaged log frame refused.
- **With the S3 backend, data is remote but KEYS ARE LOCAL.** That is a
  deliberate, load-bearing asymmetry: crypto-shred still works (the key never
  left the node, so destroying it makes the remote ciphertext inert), but a
  second node cannot decrypt the bucket. Back up the local key directory
  separately and treat it as the crown jewels - losing it is equivalent to
  shredding every namespace. This is "one node with remote durability", not
  "any node serves any namespace".
- **Single-writer on S3 is a LEASE, not a distributed lock.** The first writer
  claims `ns/<ns>/.owner` with a conditional PUT and a second gets
  `NamespaceBusyError`; a lease older than the TTL is reclaimable so a crashed
  node cannot wedge a namespace. It makes split-brain loud, not impossible.
  Do not run two writers and rely on it.
- **Audit retention is bounded** (16 sealed segments, 64MB each by default).
  Past that the oldest is dropped and the hash chain is re-anchored, so
  `verify()` proves tamper-evidence over the *retained window*.
  `memd_audit_segments_pruned_total` records the truncation. Compliance tiers
  needing unbounded history must ship segments off-box before they roll.
- **`/metrics`, `/v1/metrics/json` and `/v1/status` are namespace-scoped** for
  a scoped key and unscoped for a `*` key. `MEMD_METRICS_PUBLIC=1` allows
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

## Hardening checklist for a real deployment

1. Set `MEMD_ADMIN_KEY` to a high-entropy value; never hand it to applications.
   Mint per-namespace keys with `memd key create --namespace <ns>`.
2. Terminate TLS in front of the process. memd speaks plain HTTP.
3. Bind to `127.0.0.1` unless something else is enforcing authn at the edge.
4. Leave `MEMD_ENABLE_DOCS` and `MEMD_METRICS_PUBLIC` unset.
5. Set `MEMD_NS_RATE_LIMIT_PER_MIN` to a real per-tenant ceiling.
6. Back up the data root - the object store is the source of truth; the
   SQLite index beside it is disposable.
