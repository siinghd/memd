"""CLI: `memd serve --http|--mcp`, `memd key create`, `memd export`,
`memd import mem0`, `memd status`."""
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

    app = create_app_from_env()
    uvicorn.run(
        app,
        host=args.host or os.environ.get("MEMD_HOST", "127.0.0.1"),
        port=args.port or int(os.environ.get("MEMD_PORT", "8700")),
    )
    return 0


def create_app_from_env():
    from memd.server.http import create_app

    return create_app(
        data_dir=os.environ.get("MEMD_DATA", "./memd-data"),
        admin_key=os.environ.get("MEMD_ADMIN_KEY"),
    )


def _cmd_key(args) -> int:
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


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="memd", description="memd - agent memory engine")
    sub = p.add_subparsers(dest="cmd", required=True)

    serve = sub.add_parser("serve", help="start a server")
    serve.add_argument("--http", action="store_true", help="REST API on :8700")
    serve.add_argument("--mcp", action="store_true", help="MCP over stdio")
    serve.add_argument("--port", type=int, default=None)
    serve.add_argument("--host", default=None)
    serve.set_defaults(fn=_cmd_serve)

    key = sub.add_parser("key", help="manage API keys")
    ksub = key.add_subparsers(dest="sub", required=True)
    kc = ksub.add_parser("create")
    kc.add_argument("--ns", "--namespace", dest="namespace", required=True)
    kc.add_argument("--name", default="")
    kc.add_argument("--pin-user", default=None)
    kc.add_argument("--scope-override", action="store_true")
    kl = ksub.add_parser("list")
    kr = ksub.add_parser("revoke")
    kr.add_argument("key_id")
    for x in (kc, kl, kr):
        x.add_argument("--data", default=None)
    key.set_defaults(fn=_cmd_key)

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

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
