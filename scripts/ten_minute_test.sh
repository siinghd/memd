#!/usr/bin/env bash
# The literal 10-minute story (D4 §4.4), CI-enforced:
# fresh directory -> working engine -> cross-session recall across restart.
set -euo pipefail

DIR=$(mktemp -d /tmp/memd-10min-XXXX)
PY=${PYTHON:-python}
trap 'rm -rf "$DIR"' EXIT
cd "$DIR"
export PYTHONPATH="${MEMD_SRC:-}"

echo "[1/4] engine up, write in session 1"
"$PY" - <<'EOF'
from memd import Memory
m = Memory("./memd-data")
m.add("We deploy with `make ship`, never CI", session_id="s1", user_id="u1", role="user")
m.add("The database is postgres 16", session_id="s1", user_id="u1", role="user")
m.close_session("s1")
m.close()
EOF

echo "[2/4] kill everything (process gone; data on disk)"
sleep 1

echo "[3/4] restart fresh process, ask what we decided"
"$PY" - <<'EOF'
from memd import Memory
m = Memory("./memd-data")
hits = m.search("how do we deploy?", user_id="u1", budget_tokens=1000)
assert "make ship" in hits.packed_context, f"cross-session recall failed: {hits.packed_context!r}"
hits2 = m.search("what database do we run?", user_id="u1", budget_tokens=1000)
assert "postgres" in hits2.packed_context, "second recall failed"
st = m.stats()
assert st["records"] >= 2
print(f"[4/4] cross-session recall OK ({st['records']} records, facts={st['facts']}) in a fresh process")
m.close()
EOF

echo "PASS: fresh-directory to cross-session recall, no external accounts, well under 10 minutes."
