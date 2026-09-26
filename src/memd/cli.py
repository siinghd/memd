"""CLI: `memd serve --http|--mcp [--hosted] [--node-id N]`, `memd key create`,
`memd keys status|migrate|rotate`, `memd org`, `memd export`, `memd import
mem0`, `memd status`, `memd migrate --report`."""
from __future__ import annotations

import argparse
import json
import os
import sys


def _cmd_serve(args) -> int:
    if args.mcp:
        from memd.server.mcp_server import main as mcp_main

        mcp_main()
        return 0
    import uvicorn

    from memd.server.cluster import ClusterConfig

    host = args.host or os.environ.get("MEMD_HOST", "127.0.0.1")
    port = args.port or int(os.environ.get("MEMD_PORT", "8700"))
    # --node-id (or MEMD_NODE_ID): one node of a fleet on one s3:// data
    # root, routing each namespace to the node holding its lease (ADR-12)
    cluster = ClusterConfig.from_env(node_id=args.node_id, advertise=args.advertise,
                                     host=host, port=port)
    app = create_app_from_env(hosted=True if args.hosted else None, cluster=cluster)
    # graceful: SIGTERM stops accepting, drains in-flight requests, then the
    # lifespan hook deregisters the node and releases its leases
    uvicorn.run(app, host=host, port=port,
                timeout_graceful_shutdown=int(os.environ.get("MEMD_SHUTDOWN_GRACE_S", "30")))
    return 0


def create_app_from_env(hosted: bool | None = None, cluster=None):
    """`hosted` None: MEMD_HOSTED decides (default off)."""
    from memd.server.http import create_app

    return create_app(
        data_dir=os.environ.get("MEMD_DATA", "./memd-data"),
        admin_key=os.environ.get("MEMD_ADMIN_KEY"),
        hosted=hosted,
        cluster=cluster,
    )


def _admin_store(args):
    from memd.hosted.store import AdminStore

    data_dir = args.data or os.environ.get("MEMD_DATA", "./memd-data")
    os.makedirs(data_dir, exist_ok=True)
    return AdminStore.for_data_root(data_dir), data_dir


def _cmd_key_hosted(args) -> int:
    """Hosted mode: keys live in the admin store, belong to an org and are
    bound to one of its namespaces (claimed for the org on first use)."""
    from memd.hosted.store import OwnershipError

    store, data_dir = _admin_store(args)
    try:
        if args.sub == "create":
            if not args.org:
                print("hosted mode: --org is required (see `memd org create`)", file=sys.stderr)
                return 2
            scopes = (args.scopes or "memory").replace(",", " ").split()
            if args.scope_override and "override" not in scopes:
                scopes.append("override")
            try:
                full, kid = store.create_key(args.org, args.namespace, name=args.name or "",
                                             scopes=scopes, pinned_user=args.pin_user)
            except (KeyError, OwnershipError, ValueError) as ex:
                print(f"refused: {ex}", file=sys.stderr)
                return 1
            print(json.dumps({"key": full, "key_id": kid, "namespace": args.namespace, "org": args.org,
                              "scopes": sorted(set(scopes))}, indent=1))
            print("# store this now - it is not retrievable later", file=sys.stderr)
            return 0
        if args.sub == "list":
            for k in store.list_keys(org_id=args.org):
                print(json.dumps(k))
            return 0
        if args.sub == "revoke":
            ok = store.revoke_key(args.key_id)
            print("revoked" if ok else "not found")
            return 0 if ok else 1
        if args.sub == "migrate":
            return _migrate_legacy_keys(store, data_dir, args)
        return 2
    finally:
        store.close()


def _migrate_legacy_keys(store, data_dir: str, args) -> int:
    """Adopt self-hosted keys (keys.toml.json) into an org: same key string,
    same secret hash, now metered. Namespace-wide ('*') keys are skipped -
    in hosted mode only the operator's MEMD_ADMIN_KEY spans namespaces."""
    from memd.hosted.store import OwnershipError

    if not args.org:
        print("--org is required", file=sys.stderr)
        return 2
    path = os.path.join(data_dir, "keys.toml.json")
    try:
        with open(path) as f:
            recs = json.load(f)
    except FileNotFoundError:
        print(f"no legacy key file at {path}", file=sys.stderr)
        return 1
    moved = skipped = 0
    for r in recs:
        if (r.get("revoked") or not r.get("hash") or r.get("namespace") in (None, "*")
                or (args.namespace and r["namespace"] != args.namespace)):
            skipped += 1
            continue
        scopes = ["memory"] + (["override"] if r.get("scope_override") else [])
        try:
            store.import_key(args.org, r["namespace"], r["key_id"], r["hash"], name=r.get("name", ""),
                             scopes=scopes, pinned_user=r.get("pinned_user"), created=r.get("created"))
            moved += 1
        except (OwnershipError, KeyError) as ex:
            print(f"skipped {r['key_id']}: {ex}", file=sys.stderr)
            skipped += 1
    print(json.dumps({"migrated": moved, "skipped": skipped, "org": args.org}))
    return 0


def _cmd_org(args) -> int:
    """Hosted-mode orgs (the billing unit): create / list / set-plan."""
    store, _ = _admin_store(args)
    try:
        if args.sub == "create":
            from memd.hosted.plans import Plans

            if args.plan not in Plans.from_env():
                print(f"unknown plan {args.plan!r}", file=sys.stderr)
                return 2
            oid = store.create_org(args.name, plan=args.plan)
            print(json.dumps({"org": oid, "name": args.name, "plan": args.plan}))
            return 0
        if args.sub == "list":
            for o in store.list_orgs():
                print(json.dumps(o))
            return 0
        if args.sub == "set-plan":
            # operator override (comped / enterprise accounts); Stripe
            # webhooks keep syncing paying orgs
            from memd.hosted.plans import Plans

            if args.plan not in Plans.from_env():
                print(f"unknown plan {args.plan!r}", file=sys.stderr)
                return 2
            if store.get_org(args.org_id) is None:
                print("not found", file=sys.stderr)
                return 1
            store.update_org(args.org_id, plan=args.plan)
            print(json.dumps({"org": args.org_id, "plan": args.plan}))
            return 0
        return 2
    finally:
        store.close()


def _cmd_key(args) -> int:
    from memd.hosted import hosted_enabled

    if hosted_enabled(True if getattr(args, "hosted", False) else None):
        return _cmd_key_hosted(args)
    if args.sub == "migrate":
        print("`memd key migrate` adopts keys into a hosted org: pass --hosted (or MEMD_HOSTED=1)",
              file=sys.stderr)
        return 2
    from memd.server.auth import KeyStore

    data_dir = args.data or os.environ.get("MEMD_DATA", "./memd-data")
    os.makedirs(data_dir, exist_ok=True)
    ks = KeyStore(os.path.join(data_dir, "keys.toml.json"))
    if args.sub == "create":
        full, kid = ks.create(
            args.namespace,
            name=args.name or "",
            pinned_user=args.pin_user,
            scope_override=args.scope_override,
        )
        print(json.dumps({"key": full, "key_id": kid, "namespace": args.namespace}, indent=1))
        print("# store this now - it is not retrievable later", file=sys.stderr)
        return 0
    if args.sub == "list":
        for k in ks.list_keys():
            print(json.dumps(k))
        return 0
    if args.sub == "revoke":
        ok = ks.revoke(args.key_id)
        print("revoked" if ok else "not found")
        return 0 if ok else 1
    return 2


def _cmd_export(args) -> int:
    from memd.engine.memory import Memory

    mem = Memory(args.data, namespace=args.namespace)
    try:
        blob = mem.export_jsonl(namespace=args.namespace)
    finally:
        mem.close()
    out = open(args.out, "wb") if args.out else sys.stdout.buffer
    out.write(blob)
    if args.out:
        out.close()
        print(f"wrote {len(blob)} bytes to {args.out}", file=sys.stderr)
    return 0


def _import_native(records_in: list[dict], args, ns_name: str) -> int:
    """Full-fidelity restore of a memd export: kind, provenance tiers,
    bitemporal fields, scopes and entity keys preserved verbatim."""
    from memd.core.schema import MemoryRecord
    from memd.engine.memory import Memory

    recs = [MemoryRecord.from_dict(r) for r in records_in]
    for r in recs:
        r.namespace = ns_name  # retarget to the import namespace
    mem = Memory(args.data, namespace=ns_name)
    try:
        ns_store = mem._ns_for(ns_name)
        # preserve original ids: replay is idempotent by id
        ns_store.append(recs)
        count = len(recs)
    finally:
        mem.close()
    print(f"restored {count} records into namespace {ns_name!r} (native full-fidelity restore)")
    return 0


def _cmd_import(args) -> int:
    """Importers map onto the explicit lane with provenance.source=import
    and t_event preserved (gap accepted in D4 §4.5: no lineage)."""
    import os as _os

    from memd.core.schema import Kind, MemoryRecord, Scope, Source
    from memd.engine.memory import Memory

    max_bytes = 100 * 1024 * 1024
    fsize = _os.path.getsize(args.file)
    if fsize > max_bytes:
        print(f"refusing: export file {fsize} bytes exceeds {max_bytes} cap; split it first", file=sys.stderr)
        return 1
    data = None
    try:
        data = json.loads(raw := open(args.file).read())
    except json.JSONDecodeError:
        # memd native exports are JSONL: one record per line
        data = [json.loads(l) for l in raw.splitlines() if l.strip()]
    records: list[MemoryRecord] = []
    ns_name = args.namespace

    def norm_scope(item: dict) -> Scope:
        md = item.get("metadata") or item.get("meta") or {}
        return Scope(
            user=item.get("user_id") or md.get("user_id"),
            agent=md.get("agent_id"),
            session=item.get("session_id") or md.get("session_id") or md.get("run_id"),
            org=md.get("org_id"),
        )

    def t_event_of(item: dict) -> int | None:
        for k in ("created_at", "timestamp", "t", "time"):
            v = item.get(k)
            if v is None:
                continue
            if isinstance(v, (int, float)):
                return int(v * 1000 if v < 10**12 else v)
            try:
                import datetime as dt

                d = dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
                return int(d.timestamp() * 1000)
            except Exception:
                pass
        return None

    if isinstance(data, dict):
        # memd native export: full-fidelity restore (kind, tiers, times,
        # scopes preserved); foreign formats fall through to mem0 mapping
        if all(k in data for k in ("id", "namespace")):
            return _import_native(data["results"] if isinstance(data.get("results"), list) else [data], args, ns_name)
        if isinstance(data.get("events"), list) and isinstance(data.get("cases"), list):
            print("harness dataset detected - not an importable export", file=sys.stderr)
            return 1
        # mem0 export shapes: {"results":[...]} / {"events":[...]} / list under any key
        items = []
        for v in data.values():
            if isinstance(v, list):
                items.extend(x for x in v if isinstance(x, dict))
    elif isinstance(data, list):
        # could be memd JSONL-parsed list OR foreign rows: sniff first row
        first = next((x for x in data if isinstance(x, dict)), None)
        if first and "id" in first and "namespace" in first and "kind" in first:
            return _import_native(data, args, ns_name)
        items = data
    else:
        items = []

    if not items:
        print("no importable records found; expected mem0-style export", file=sys.stderr)
        return 1

    for it in items:
        text = it.get("memory") or it.get("text") or it.get("content") or it.get("data")
        if not text:
            continue
        te = t_event_of(it)
        rec = MemoryRecord.create(
            namespace=ns_name,
            kind=Kind.FACT,
            content=str(text),
            scope=norm_scope(it),
            source=Source.IMPORT,
            actor_id="import",
            entity_keys=[],
            t_event=te,
            meta={"imported_from": args.source, "raw_import": {k: str(v)[:200] for k, v in it.items() if k not in ("memory", "text", "content", "data")}},
        )
        records.append(rec)

    mem = Memory(args.data, namespace=ns_name)
    try:
        nstore = mem._ns_for(ns_name)
        nstore.append(records)
        count = len(records)
    finally:
        mem.close()
    print(f"imported {count} facts into namespace {ns_name!r} (source=import, supersedable, not re-runnable)")
    return 0


def _cmd_reindex(args) -> int:
    """Re-embedding batch job (ADR-8): rebuild the vector lane from raw."""
    from memd.engine.memory import Memory

    mem = Memory(args.data, namespace=args.namespace)
    try:
        rep = mem.reembed(namespace=args.namespace)
        print(json.dumps(rep))
        return 0
    finally:
        mem.close()


def _cmd_metrics(args) -> int:
    """Emit the in-process metrics snapshot (JSON). For live graphing use
    GET /metrics on a running server, or set MEMD_METRICS_PATH for dumps."""
    from memd.metrics import METRICS

    snap = METRICS.snapshot()
    import time as _t

    snap["_epoch_ms"] = int(_t.time() * 1000)
    print(json.dumps(snap, indent=1))
    return 0


def _cmd_status(args) -> int:
    from memd.engine.memory import Memory

    mem = Memory(args.data, namespace=args.namespace)
    try:
        print(json.dumps(mem.status(), indent=1))
        for nsmeta in mem.engine.list_namespaces():
            st = mem.stats(namespace=nsmeta)
            print(f"{nsmeta}: {json.dumps(st)}")
    finally:
        mem.close()
    return 0


def _cmd_migrate(args) -> int:
    """`memd migrate --report DATA`: per namespace, its store format, whether
    its format-1 migration is still pending (previewed, not run), the ledger
    deletes kept live on ambiguous evidence, the deletes recovered and
    applied, and the losses an older version left that cannot be recovered.
    Read-only: nothing is migrated, locked or written - run it before an
    upgrade and after it (see the CHANGELOG's upgrade notes)."""
    from memd.storage.crypto import LocalKeyEnvelope, NullKeyEnvelope
    from memd.storage.engine import StorageEngine

    data = args.report
    if str(data).startswith("s3://"):
        print("migrate --report reads a local data root; for S3, run it on the node that "
              "holds local_dir and the keys", file=sys.stderr)
        return 2
    store_dir = os.path.join(data, "store")
    if not os.path.isdir(store_dir):
        print(f"no memd data root at {data!r} (expected {store_dir})", file=sys.stderr)
        return 1
    keys = os.path.join(data, "keys")
    # an encrypted root has its keys here; never create them just to read
    env = (LocalKeyEnvelope(keys) if os.path.exists(os.path.join(keys, "root.key"))
           else NullKeyEnvelope())
    rep = StorageEngine(store_dir, envelope=env).migration_report()
    print(json.dumps(rep, indent=1, sort_keys=True))
    return 0


def _keys_store(data: str):
    """(object store, local dir) for a data root: a local path, or s3://
    with MEMD_LOCAL_DIR (the node-local dir that holds `local` key files)."""
    if str(data).startswith("s3://"):
        from memd.storage.s3store import S3ObjectStore

        bucket, _, prefix = str(data)[len("s3://"):].partition("/")
        store = S3ObjectStore(bucket=bucket, prefix=prefix,
                              endpoint_url=os.environ.get("MEMD_S3_ENDPOINT"),
                              region=os.environ.get("AWS_REGION"))
        local = os.environ.get("MEMD_LOCAL_DIR") or os.path.join(".memd-local", bucket, prefix or "_")
        return store, local
    from memd.storage.objectstore import LocalObjectStore

    return LocalObjectStore(os.path.join(data, "store")), data


def _cmd_keys(args) -> int:
    """`memd keys status|migrate|rotate`: data-key custody (ADR-12).

    migrate --to aws-kms|vault-transit re-wraps every namespace's LOCAL data
    key under the remote provider. The data key itself does not change, so
    no data is re-encrypted; what moves is who can unwrap it. Crash-safe by
    construction - every step is idempotent and ordered so that each
    namespace always has at least one usable wrapped key:
      1. per namespace: wrap under the provider, create keys/<ns>.dek
         (conditional create), read it back and unwrap it, compare;
      2. only when EVERY namespace verified: write the custody marker
         (keys/_custody.json) - the swap: from here on a node still on the
         `local` provider refuses to open the store instead of minting keys;
      3. then shred the local wrapped key files (root.key alone decrypts
         nothing). Until this step completes, crypto-shred of a migrated
         namespace does NOT cover its local copy - rerun to finish.
    Rerunning after a crash at any point converges to the same end state."""
    from memd.storage.crypto import (LocalKeyEnvelope, ObjectStoreKeyEnvelope, legacy_key_path,
                                     provider_from_config, read_custody,
                                     resolve_key_provider_name, wrapped_key_object)

    store, local_dir = _keys_store(args.data)
    keys_dir = os.path.join(local_dir, "keys")
    if args.sub == "status":
        custody = read_custody(store)
        out: dict = {"custody": custody or {"provider": "local"}, "namespaces": {}}
        local_env = (LocalKeyEnvelope(keys_dir)
                     if os.path.exists(os.path.join(keys_dir, "root.key")) else None)
        nss = set(n for n in (local_env.namespaces() if local_env else []))
        for k in store.list("keys/"):
            if k.endswith(".dek"):
                nss.add(k[len("keys/"):-len(".dek")])
        for ns in sorted(nss):
            row: dict = {"local_key": bool(local_env and os.path.exists(legacy_key_path(keys_dir, ns)))}
            raw = store.get(wrapped_key_object(ns))
            if raw:
                rec = json.loads(raw)
                row["wrapped"] = {k: rec.get(k) for k in ("provider", "key_id", "key_version", "created_ms")}
            out["namespaces"][ns] = row
        print(json.dumps(out, indent=1, sort_keys=True))
        return 0

    target = args.to if args.sub == "migrate" else resolve_key_provider_name({})
    if target == "local":
        print("the target must be a remote provider (aws-kms or vault-transit); moving keys back "
              "to local files is not supported", file=sys.stderr)
        return 2
    provider = provider_from_config(target, {})
    remote = ObjectStoreKeyEnvelope(provider, store)

    if args.sub == "rotate":
        done = []
        for k in store.list("keys/"):
            if k.endswith(".dek"):
                done.append(remote.rewrap(k[len("keys/"):-len(".dek")]))
        print(json.dumps({"rotated": len(done), "namespaces": done}, indent=1))
        return 0

    # ---- migrate
    if not os.path.exists(os.path.join(keys_dir, "root.key")):
        print(f"no local key directory at {keys_dir} (MEMD_LOCAL_DIR / --data)", file=sys.stderr)
        return 1
    custody = read_custody(store)
    if custody and custody.get("provider") not in (target,):
        print(f"refused: this store's keys are already held by {custody.get('provider')!r}",
              file=sys.stderr)
        return 1
    local = LocalKeyEnvelope(keys_dir)
    report: dict = {"to": target, "migrated": [], "already": [], "errors": {}}
    # Hold every namespace's single-writer lock for the whole migration: a
    # node still running on the `local` provider would otherwise MINT a new
    # local key for a namespace it opens after step 3 removed the old one -
    # and write data no key can read. Busy namespaces refuse the migration.
    held, busy = _hold_namespaces(store, local.namespaces())
    try:
        if busy:
            print(json.dumps({"to": target, "busy": busy}, indent=1))
            print("refused: these namespaces are open in a running memd - stop it first",
                  file=sys.stderr)
            return 1
        return _migrate_keys(args, store, keys_dir, local, provider, remote, target, report)
    finally:
        _release_namespaces(store, held)


def _hold_namespaces(store, names: list[str]) -> tuple[list, list[str]]:
    from memd.storage.engine import _acquire_owner, NamespaceBusyError

    held: list = []
    busy: list[str] = []
    for ns in names:
        leaser = getattr(store, "try_acquire_owner", None)
        try:
            if callable(leaser):
                if leaser(ns, f"memd-keys-migrate@{os.getpid()}"):
                    held.append(("lease", ns))
                else:
                    busy.append(ns)
            else:
                path = os.path.join(store.root, "ns", ns.replace("/", "__"), ".owner")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                _acquire_owner(path)
                held.append(("flock", path))
        except NamespaceBusyError:
            busy.append(ns)
    return held, busy


def _release_namespaces(store, held: list) -> None:
    from memd.storage.engine import _release_owner

    for kind, what in held:
        try:
            if kind == "lease":
                store.release_owner(what)
            else:
                _release_owner(what)
        except Exception:  # noqa: BLE001 - leases expire; flocks die with us
            pass


def _migrate_keys(args, store, keys_dir, local, provider, remote, target, report) -> int:
    from memd.storage.crypto import (KeyCustodyError, WrappedKey, _overwrite_unlink,
                                     legacy_key_path, wrapped_key_object, write_custody)

    for ns in local.namespaces():
        try:
            dk = local.peek_data_key(ns)
            if dk is None:
                continue
            rec = remote.read_record(ns)
            if rec is None:
                wk = provider.wrap(ns, dk)
                body = json.dumps(wk.to_record(ns), sort_keys=True).encode()
                store.put_if_absent(wrapped_key_object(ns), body)
                rec = remote.read_record(ns)
                bucket = report["migrated"]
            else:
                bucket = report["already"]
            if rec is None or rec.get("provider") != target:
                raise KeyCustodyError(f"wrapped key for {ns!r} is missing or under another provider")
            if provider.unwrap(ns, WrappedKey.from_record(rec)) != dk:
                # someone minted a DIFFERENT key for this namespace remotely:
                # never paper over that - both copies are kept for an operator
                raise KeyCustodyError(f"the remote key for {ns!r} differs from the local one")
            bucket.append(ns)
        except Exception as ex:  # noqa: BLE001 - report every namespace
            report["errors"][ns] = f"{type(ex).__name__}: {ex}"
    if report["errors"]:
        report["swapped"] = False
        print(json.dumps(report, indent=1, sort_keys=True))
        print("not swapped: fix the errors above and rerun (nothing local was removed)", file=sys.stderr)
        return 1
    write_custody(store, provider)           # the swap point
    report["swapped"] = True
    removed = []
    if not args.keep_local:
        for ns in report["migrated"] + report["already"]:
            p = legacy_key_path(keys_dir, ns)
            if os.path.exists(p):
                _overwrite_unlink(p)
                removed.append(ns)
    report["local_keys_removed"] = removed
    print(json.dumps(report, indent=1, sort_keys=True))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="memd", description="memd - agent memory engine")
    sub = p.add_subparsers(dest="cmd", required=True)

    serve = sub.add_parser("serve", help="start a server")
    serve.add_argument("--http", action="store_true", help="REST API on :8700")
    serve.add_argument("--mcp", action="store_true", help="MCP over stdio")
    serve.add_argument("--hosted", action="store_true",
                       help="hosted mode: orgs, metering, quotas, Stripe billing (also MEMD_HOSTED=1)")
    serve.add_argument("--port", type=int, default=None)
    serve.add_argument("--host", default=None)
    serve.add_argument("--node-id", default=None,
                       help="cluster mode (also MEMD_NODE_ID): this node's id; nodes sharing an "
                            "s3:// MEMD_DATA route each namespace to its leaseholder")
    serve.add_argument("--advertise", default=None,
                       help="cluster mode: the URL peers reach this node at (MEMD_ADVERTISE_URL; "
                            "default http://HOST:PORT)")
    serve.set_defaults(fn=_cmd_serve)

    key = sub.add_parser("key", help="manage API keys")
    ksub = key.add_subparsers(dest="sub", required=True)
    kc = ksub.add_parser("create")
    kc.add_argument("--ns", "--namespace", dest="namespace", required=True)
    kc.add_argument("--name", default="")
    kc.add_argument("--pin-user", default=None)
    kc.add_argument("--scope-override", action="store_true")
    kc.add_argument("--org", default=None, help="hosted mode: the org the key belongs to (required)")
    kc.add_argument("--scopes", default=None,
                    help="hosted mode: comma-separated subset of memory,billing,override (default memory)")
    kl = ksub.add_parser("list")
    kl.add_argument("--org", default=None, help="hosted mode: only this org's keys")
    kr = ksub.add_parser("revoke")
    kr.add_argument("key_id")
    km = ksub.add_parser("migrate", help="hosted mode: adopt keys.toml.json keys into an org")
    km.add_argument("--org", required=True)
    km.add_argument("--ns", "--namespace", dest="namespace", default=None, help="only this namespace's keys")
    for x in (kc, kl, kr, km):
        x.add_argument("--data", default=None)
        x.add_argument("--hosted", action="store_true", help="use the hosted admin store (also MEMD_HOSTED=1)")
    key.set_defaults(fn=_cmd_key)

    org = sub.add_parser("org", help="hosted mode: manage orgs (the billing unit)")
    osub = org.add_subparsers(dest="sub", required=True)
    oc = osub.add_parser("create")
    oc.add_argument("--name", required=True)
    oc.add_argument("--plan", default="free")
    ol = osub.add_parser("list")
    op_ = osub.add_parser("set-plan")
    op_.add_argument("org_id")
    op_.add_argument("plan")
    for x in (oc, ol, op_):
        x.add_argument("--data", default=None)
    org.set_defaults(fn=_cmd_org)

    exp = sub.add_parser("export", help="full JSONL export (anti-lock-in)")
    exp.add_argument("--namespace", default="default")
    exp.add_argument("--data", default="./memd-data")
    exp.add_argument("--out", default=None)
    exp.set_defaults(fn=_cmd_export)

    imp = sub.add_parser("import", help="import from another system (mem0) or restore a memd export")
    imp.add_argument("source", choices=["mem0", "memd"], help="source format")
    imp.add_argument("--export", dest="file", required=True, help="path to their export JSON")
    imp.add_argument("--namespace", default="default")
    imp.add_argument("--data", default="./memd-data")
    imp.set_defaults(fn=_cmd_import)

    rex = sub.add_parser("reindex", help="re-embed records missing/outdated vectors (ADR-8 batch job)")
    rex.add_argument("--namespace", default="default")
    rex.add_argument("--data", default="./memd-data")
    rex.set_defaults(fn=_cmd_reindex)

    st = sub.add_parser("status", help="engine + namespaces overview")
    st.add_argument("--namespace", default="default")
    st.add_argument("--data", default="./memd-data")
    st.set_defaults(fn=_cmd_status)

    mt = sub.add_parser("metrics", help="dump in-process metrics snapshot (JSON)")
    mt.set_defaults(fn=_cmd_metrics)

    ks = sub.add_parser("keys", help="data-key custody: status, migrate to a KMS, rotate (ADR-12)")
    kssub = ks.add_subparsers(dest="sub", required=True)
    kst = kssub.add_parser("status", help="custody marker + where each namespace's key is wrapped")
    ksm = kssub.add_parser("migrate", help="re-wrap local data keys under a remote provider")
    ksm.add_argument("--to", required=True, choices=["aws-kms", "vault-transit"])
    ksm.add_argument("--keep-local", action="store_true",
                     help="keep the local wrapped key files (crypto-shred will NOT cover them)")
    ksr = kssub.add_parser("rotate", help="re-wrap every data key under the provider's current key version")
    for x in (kst, ksm, ksr):
        x.add_argument("--data", default=os.environ.get("MEMD_DATA", "./memd-data"),
                       help="data root: a local path or s3://bucket/prefix (then MEMD_LOCAL_DIR)")
    ks.set_defaults(fn=_cmd_keys)

    mg = sub.add_parser("migrate", help="store-format upgrade: report what it did or would do")
    mg.add_argument("--report", metavar="DATA", required=True,
                    help="data root to report on (read-only; JSON on stdout)")
    mg.set_defaults(fn=_cmd_migrate)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
