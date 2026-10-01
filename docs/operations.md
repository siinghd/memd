# Operations

Running memd as a server, on object storage, across nodes, and getting it
back after something went wrong. The sections below are the engine README
and [SECURITY.md](security.md), included here so they cannot drift.

## One process per data root

{% include-markdown "../README-engine.md" start="## Deployment constraint: ONE process per data root" end="## Object storage as the source of truth (S3 / R2 / MinIO)" %}

## Object storage (S3 / R2 / MinIO)

{% include-markdown "../README-engine.md" start="## Object storage as the source of truth (S3 / R2 / MinIO)" end="### Key custody: local, AWS KMS or Vault transit" %}

A runnable walk-through on MinIO: [`examples/06_s3_minio.py`](../examples/06_s3_minio.py).

## Key custody

{% include-markdown "../README-engine.md" start="### Key custody: local, AWS KMS or Vault transit" end="## Multi-node" %}

The threat model of each provider, binding, and what crypto-shred can and
cannot promise with a shared KMS key: [Security: key custody](security.md#key-custody).

## Multi-node

{% include-markdown "../README-engine.md" start="## Multi-node" end="## Hosted mode & billing" %}

## Backups

What to back up, as the rest of this page and [Security](security.md) state it:

- **The data root** (the directory, or the bucket prefix). The object store
  is the source of truth; the SQLite index beside it is a cache and is
  rebuilt from it.
- **The keys, with the data, restored together.** With the `local` provider
  they are the `keys/` directory (`root.key` and one key file per
  namespace) under the data directory, or under `local_dir` for an `s3://`
  root. With `aws-kms` or `vault-transit` they are the `keys/` objects in
  the store. Data restored without its keys, or with another deployment's,
  is refused with `KeyCustodyError`, never read as empty.
- **Erasure and backups.** A backup taken before a crypto-shred still holds
  the namespace's wrapped key: keep the `keys/` prefix under the retention
  you promise for erasure, or exclude it (details under
  [Security: key custody](security.md#key-custody)).
- **Hosted mode:** `admin/admin.sqlite3` (orgs, keys, the usage ledger) is
  not rebuildable; back it up with the data root.
- **A portable copy:** `memd export --out backup.jsonl` writes a
  namespace's records as JSON lines; `memd import memd --export backup.jsonl`
  restores one.
- Do not attach long-lived readers (a backup tool holding a read
  transaction, an `sqlite3` shell) to the index cache files: a hard-delete
  purge waits for them.

## Recovering from an unreadable log frame

{% include-markdown "../SECURITY.md" start="## Recovering from an unreadable log frame" end="## Hardening checklist for a real deployment" %}

## Hardening checklist

{% include-markdown "../SECURITY.md" start="## Hardening checklist for a real deployment" %}

## Hosted mode & billing

{% include-markdown "../README-engine.md" start="## Hosted mode & billing" end="## Doors (one engine)" %}

## Benchmarks and maintenance commands

{% include-markdown "../README-engine.md" start="## Ops" %}
