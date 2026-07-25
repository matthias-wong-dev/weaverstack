#!/usr/bin/env bash
# Provision a Claude Code session so the full suite can run.
#
# Idempotent and quiet on the happy path: a warm container re-runs this in well
# under a second. Never fails the session — a broken setup should surface as a
# failing test with a real message, not as a session that refuses to start.
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
venv="$root/.venv"
stamp="$venv/.weaver-setup"
# Reinstall when the dependency declaration moves, not on every session.
key="$(cat "$root/pyproject.toml" | cksum | cut -d' ' -f1)"

if [ -f "$stamp" ] && [ "$(cat "$stamp")" = "$key" ]; then
    echo "weaverstack: environment ready ($venv)"
    exit 0
fi

if [ ! -x "$venv/bin/python" ]; then
    # A distribution-patched setuptools cannot build PySpark's sdist, so the
    # venv gets current packaging tools before anything else is installed.
    python3 -m venv "$venv" || { echo "weaverstack: could not create $venv"; exit 0; }
    "$venv/bin/pip" install -q -U pip setuptools wheel
fi

# Editable, so `weaver install` can still walk up to the checkout's pyproject.
if "$venv/bin/pip" install -q -e "$root[dev]"; then
    echo "$key" > "$stamp"
    echo "weaverstack: environment ready ($venv)"
    "$venv/bin/weaver" doctor || true
else
    echo "weaverstack: dependency install failed — run .venv/bin/pip install -e '.[dev]' to see why"
fi
exit 0
