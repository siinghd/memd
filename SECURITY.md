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

## Hardening checklist for a real deployment

1. Set `MEMD_ADMIN_KEY` to a high-entropy value; never hand it to applications.
   Mint per-namespace keys with `memd key create --namespace <ns>`.
2. Terminate TLS in front of the process. memd speaks plain HTTP.
3. Bind to `127.0.0.1` unless something else is enforcing authn at the edge.
4. Leave `MEMD_ENABLE_DOCS` and `MEMD_METRICS_PUBLIC` unset.
5. Set `MEMD_NS_RATE_LIMIT_PER_MIN` to a real per-tenant ceiling.
6. Back up the data root - the object store is the source of truth; the
   SQLite index beside it is disposable.
