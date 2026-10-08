# HTTP API

`memd serve --http` serves the REST door on port 8700. Every route under
`/v1/ns/{ns}/` operates on one namespace.

```bash
export MEMD_ADMIN_KEY="$(python -c 'import secrets;print(secrets.token_urlsafe(32))')"
memd serve --http --host 127.0.0.1 --port 8700
memd key create --namespace acme        # {"key": "memd_acme_..."}: give this one to the app
```

**Authentication.** Send `Authorization: Bearer <key>`. A namespace key gives
access to its own namespace only. `MEMD_ADMIN_KEY` is the operator key and
gives access to every namespace. WARNING: Never give the operator key to an
application. `/health` does not need a key.

**Errors** have a JSON body with `detail` and a
machine-readable `code` (for example `not_found`, `rate_limited`,
`preview_mismatch`). The response to a rate-limited request also has
`Retry-After`.

**Read consistency.** Reads are strong (the writer of the namespace serves
them), unless a search or get sends `X-Memd-Read-Consistency: eventual` (or
`?consistency=eventual`), optionally with `X-Memd-Max-Staleness-Ms`. Then, in
a cluster, a read replica that is not more stale than that value can serve it.
Every search and get response has `X-Memd-Served-By: leader|replica`. A replica
also adds `X-Memd-Replica-Seq` (the seq that it applied) and
`X-Memd-Replica-Age-Ms`.
Refer to [Operations: read replicas](../operations.md#read-replicas-eventual-reads).

**Clients.** Two software development kits (SDKs) wrap every route below: the
[Python SDK](python.md#hosted-client) (`HostedMemory`) and
the [TypeScript SDK](typescript.md).

**The schema.** The docs build makes this reference from the OpenAPI schema of
the server itself. You can download the schema as [openapi.json](openapi.json).
A running server serves it at `/openapi.json` (and interactive docs at
`/docs`) only with `MEMD_ENABLE_DOCS=1`. They are off by default, because
they show every route of an API that otherwise needs authentication. Hosted
mode adds the billing routes, which
[Operations: hosted mode](../operations.md#hosted-mode-billing) documents.

## Routes

<!-- openapi-reference -->
