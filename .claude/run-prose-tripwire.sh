#!/usr/bin/env bash
# Run the advisory prose check with the repository environment when available.
set -eu

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -x "$root/.venv/bin/python" ]; then
    exec "$root/.venv/bin/python" "$root/.claude/prose_tripwire.py"
fi
if [ -x "$root/.venv/Scripts/python.exe" ]; then
    exec "$root/.venv/Scripts/python.exe" "$root/.claude/prose_tripwire.py"
fi
if command -v python3 >/dev/null 2>&1; then
    exec python3 "$root/.claude/prose_tripwire.py"
fi
if command -v python >/dev/null 2>&1; then
    exec python "$root/.claude/prose_tripwire.py"
fi

echo "Prose review skipped: no Python interpreter was found."
