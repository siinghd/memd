# Operations

This page tells how to run memd as a server, on object storage and on more
than one node. It also tells how to recover memd after a failure. The sections
below are the engine README and [SECURITY.md](security.md). The docs build
includes them here, so that they cannot become different.

## Several processes on one data root

{% include-markdown "../README-engine.md" start="## Several processes on one data root" end="## Object storage as the source of truth (S3 / R2 / MinIO)" %}

## Object storage (S3 / R2 / MinIO)

{% include-markdown "../README-engine.md" start="## Object storage as the source of truth (S3 / R2 / MinIO)" end="### Key custody: local, AWS KMS or Vault transit" %}

A walk-through on MinIO that you can run: [`examples/06_s3_minio.py`](../examples/06_s3_minio.py).

## Key custody

{% include-markdown "../README-engine.md" start="### Key custody: local, AWS KMS or Vault transit" end="## Multi-node" %}

[Security: key custody](security.md#key-custody) gives the threat model of
each provider and the key binding. It also tells what crypto-shred can promise
with a shared KMS (Key Management Service) key, and what it cannot promise.

## Multi-node

{% include-markdown "../README-engine.md" start="## Multi-node" end="## Hosted mode & billing" %}

## Backups

This list tells what to include in a backup. It agrees with the rest of
this page and with [Security](security.md):

- **The data root** (the directory, or the bucket prefix). The object store
  is the source of truth. The SQLite index next to it is a cache, and memd
  rebuilds the index from the object store.
- **The keys, with the data, restored together.** With the `local` provider,
  the keys are the `keys/` directory (`root.key` and one key file for each
  namespace). This directory is in the data directory, or in `local_dir` for
  an `s3://` root. With `aws-kms` or `vault-transit`, the keys are the
  `keys/` objects in the store. If you restore data without its keys, or
  with the keys of another deployment, memd refuses it with
  `KeyCustodyError`. memd never reads it as empty.
- **Erasure and backups.** A backup that you made before a crypto-shred
  still contains the wrapped key of the namespace. Keep the `keys/` prefix
  under the retention that you promise for erasure, or do not include it in
  backups. [Security: key custody](security.md#key-custody) gives the details.
- **Hosted mode:** memd cannot rebuild `admin/admin.sqlite3` (orgs, keys,
  the usage ledger). Include it in the backup with the data root.
- **A portable copy:** `memd export --out backup.jsonl` writes the records
  of a namespace as JSON lines. `memd import memd --export backup.jsonl`
  restores such a copy.
- Do not attach long-lived readers to the index cache files (for example, a
  backup tool that holds a read transaction, or an `sqlite3` shell). A
  hard-delete purge waits for them.

## Recovering from an unreadable log frame

{% include-markdown "../SECURITY.md" start="## Recovering from an unreadable log frame" end="## Hardening checklist for a real deployment" %}

## Hardening checklist

{% include-markdown "../SECURITY.md" start="## Hardening checklist for a real deployment" %}

## Hosted mode & billing

{% include-markdown "../README-engine.md" start="## Hosted mode & billing" end="## Doors (one engine)" %}

## Benchmarks and maintenance commands

{% include-markdown "../README-engine.md" start="## Ops" %}
