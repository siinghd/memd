# HTTP API

`memd serve --http` serves the REST door on port 8700. Every route under
`/v1/ns/{ns}/` acts on one namespace.

```bash
export MEMD_ADMIN_KEY="$(python -c 'import secrets;print(secrets.token_urlsafe(32))')"
memd serve --http --host 127.0.0.1 --port 8700
memd key create --namespace acme        # {"key": "memd_acme_..."}: give this one to the app
```

**Authentication.** `Authorization: Bearer <key>`. A namespace key reaches
its own namespace only; `MEMD_ADMIN_KEY` is the operator key and reaches
every namespace (never hand it to an application). `/health` needs no key.

**Errors** carry a JSON body with `detail` and a
machine-readable `code` (for example `not_found`, `rate_limited`,
`preview_mismatch`); a rate-limited request also gets `Retry-After`.

**Clients.** The [Python SDK](python.md#hosted-client) (`HostedMemory`) and
the [TypeScript SDK](typescript.md) wrap every route below.

**The schema.** This reference is rendered at build time from the OpenAPI
schema of the server itself: download it as [openapi.json](openapi.json).
A running server serves it at `/openapi.json` (and interactive docs at
`/docs`) only with `MEMD_ENABLE_DOCS=1`: they are off by default because
they list every route of an otherwise authenticated API. Hosted mode adds
the billing routes, documented under
[Operations: hosted mode](../operations.md#hosted-mode-billing).

## Routes

<!-- openapi-reference -->
