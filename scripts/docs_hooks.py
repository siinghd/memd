"""MkDocs hooks for the docs site (mkdocs.yml `hooks:`).

1. Links that leave docs/. Pages include the repository's own markdown
   (SECURITY.md, BENCHMARKS.md, sdk-ts/README.md, ...) instead of copying
   it, and the include plugin rebases their relative links onto the
   including page. A link that then points outside docs/ goes to the page
   that includes that file when there is one, else to the file on the
   repository host (`repo_url`, branch `extra.source_branch`). Runs after
   the include plugin: hooks run after every configured plugin.

2. Lists written for GitHub. The included files nest lists by 2 spaces
   and start a list right under a paragraph line; Python-Markdown needs 4
   and a blank line, and otherwise renders the items as running text. Such
   items are re-indented and given their blank line (outside code fences).

3. The HTTP reference. `<!-- openapi-reference -->` in a page is replaced
   by a reference rendered from the OpenAPI schema of the server itself
   (scripts/gen_openapi.py), and the schema is published beside it as
   reference/openapi.json - so the reference cannot drift from the routes.
"""
from __future__ import annotations

import json
import posixpath
import re
import sys
from pathlib import Path

from mkdocs.structure.files import File

ROOT = Path(__file__).resolve().parents[1]

# repository file -> the docs page that includes it whole
INCLUDED = {
    "SECURITY.md": "security.md",
    "BENCHMARKS.md": "benchmarks.md",
    "CHANGELOG.md": "changelog.md",
    "sdk-ts/README.md": "reference/typescript.md",
    "examples/README.md": "examples.md",
    "examples/mcp/README.md": "reference/mcp.md",
}

OPENAPI_MARKER = "<!-- openapi-reference -->"
OPENAPI_PATH = "reference/openapi.json"

_LINK = re.compile(r"(\]\(\s*<?)([^)\s>]+)(>?(?:\s+\"[^\"]*\")?\s*\))")
_REF_DEF = re.compile(r"^(\s{0,3}\[[^\]]+\]:\s*<?)(\S+?)(>?(?:\s.*)?)$")
_FENCE = re.compile(r"^\s*(```|~~~)")
_ITEM = re.compile(r"^(\s*)([-*+]|\d+[.)])\s+\S")

_schema: dict | None = None


def _openapi() -> dict:
    global _schema
    if _schema is None:
        sys.path.insert(0, str(ROOT / "scripts"))
        from gen_openapi import build_schema

        _schema = build_schema()
    return _schema


# -- 1, 2. markdown written for GitHub -----------------------------------

def _rewrite(target: str, page_src: str, config) -> str:
    if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I) or target.startswith(("#", "/")):
        return target   # absolute URL, mailto:, in-page anchor, site-absolute
    path, _, anchor = target.partition("#")
    if not path:
        return target
    in_docs = posixpath.normpath(posixpath.join(posixpath.dirname(page_src), path))
    if not in_docs.startswith("../"):
        return target   # stays inside docs/: MkDocs validates it
    repo_path = posixpath.normpath(posixpath.join("docs", in_docs))
    suffix = f"#{anchor}" if anchor else ""
    if repo_path in INCLUDED:
        rel = posixpath.relpath(INCLUDED[repo_path], posixpath.dirname(page_src) or ".")
        return rel + suffix
    branch = config.extra.get("source_branch", "master")
    kind = "tree" if (ROOT / repo_path).is_dir() else "blob"
    return f"{config.repo_url.rstrip('/')}/{kind}/{branch}/{repo_path}{suffix}"


def _from_github(markdown: str, page_src: str, config) -> str:
    out, fence = [], None
    for line in markdown.split("\n"):
        m = _FENCE.match(line)
        if m:
            fence = None if fence == m.group(1) else (fence or m.group(1))
            out.append(line)
            continue
        if fence is None:
            line = _LINK.sub(lambda g: g.group(1) + _rewrite(g.group(2), page_src, config) + g.group(3), line)
            line = _REF_DEF.sub(lambda g: g.group(1) + _rewrite(g.group(2), page_src, config) + g.group(3), line)
            item = _ITEM.match(line)
            if item and len(item.group(1)) in (2, 3):
                line = "    " + line.lstrip()   # a 2-space nested item
            elif item and not item.group(1) and out:
                prev = out[-1]
                if prev.strip() and not _ITEM.match(prev) and not prev.startswith((" ", "\t", "|", "#", "<")):
                    out.append("")              # a list right under a paragraph
        out.append(line)
    return "\n".join(out)


# -- 3. the HTTP reference ------------------------------------------------

def _type(schema: dict, components: dict) -> str:
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        return f"[{name}](#{name.lower()})" if name in components else name
    if "anyOf" in schema:
        parts = [_type(s, components) for s in schema["anyOf"] if s.get("type") != "null"]
        nullable = any(s.get("type") == "null" for s in schema["anyOf"])
        return " \\| ".join(parts) + (" \\| null" if nullable else "")
    t = schema.get("type", "any")
    if t == "array":
        return f"array of {_type(schema.get('items', {}), components)}"
    if t == "object" and schema.get("additionalProperties"):
        return "object"
    return t


def _constraints(schema: dict) -> str:
    bits = []
    for src in [schema, *schema.get("anyOf", [])]:
        for key, label in (("minLength", "min length"), ("maxLength", "max length"),
                           ("minimum", "≥"), ("maximum", "≤"), ("maxItems", "max items"),
                           ("pattern", "pattern")):
            if key in src:
                v = src[key]
                v = int(v) if isinstance(v, float) and v.is_integer() else v
                bits.append(f"{label} `{v}`" if key == "pattern" else f"{label} {v}")
    if "default" in schema:
        bits.append(f"default `{json.dumps(schema['default'])}`")
    return ", ".join(bits)


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def _render_reference(schema: dict) -> str:
    components = schema.get("components", {}).get("schemas", {})
    lines = []
    for path, ops in schema["paths"].items():
        for method, op in ops.items():
            lines += [f"### `{method.upper()} {path}`", ""]
            if op.get("description"):
                # the route's docstring: its first paragraph is the contract,
                # the rest is maintainer notes
                lines += [op["description"].strip().split("\n\n")[0], ""]
            params = op.get("parameters", [])
            if params:
                lines += ["| parameter | in | type | required | |", "|---|---|---|---|---|"]
                for p in params:
                    s = p.get("schema", {})
                    lines.append(f"| `{p['name']}` | {p['in']} | {_type(s, components)} | "
                                 f"{'yes' if p.get('required') else 'no'} | {_cell(_constraints(s))} |")
                lines.append("")
            body = op.get("requestBody", {}).get("content", {}).get("application/json", {}).get("schema")
            if body:
                lines += [f"Request body: {_type(body, components)}", ""]
            codes = ", ".join(f"`{c}`" for c in op.get("responses", {}))
            lines += [f"Responses: {codes}", ""]
    lines += ["## Request models", ""]
    for name, s in components.items():
        if name in ("HTTPValidationError", "ValidationError"):
            continue
        lines += [f"### {name}", ""]
        required = set(s.get("required", []))
        lines += ["| field | type | required | |", "|---|---|---|---|"]
        for field, fs in s.get("properties", {}).items():
            lines.append(f"| `{field}` | {_type(fs, components)} | {'yes' if field in required else 'no'} | "
                         f"{_cell(_constraints(fs))} |")
        lines.append("")
    return "\n".join(lines)


# -- mkdocs events --------------------------------------------------------

def on_files(files, config):
    files.append(File.generated(config, OPENAPI_PATH, content=json.dumps(_openapi(), indent=2) + "\n"))
    return files


def on_page_markdown(markdown, page, config, files):
    if OPENAPI_MARKER in markdown:
        markdown = markdown.replace(OPENAPI_MARKER, _render_reference(_openapi()))
    return _from_github(markdown, page.file.src_uri, config)
