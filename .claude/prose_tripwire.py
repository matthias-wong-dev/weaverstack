#!/usr/bin/env python3
"""Prompt a review of high-signal prose patterns in changed source text."""

from __future__ import annotations

import re
import subprocess
import sys


PATTERNS = {
    "the whole point": re.compile(r"\bthe whole point\b", re.IGNORECASE),
    "the key point": re.compile(r"\bthe key point\b", re.IGNORECASE),
    "worth noting": re.compile(r"\bworth noting\b", re.IGNORECASE),
    "obviously": re.compile(r"\bobviously\b", re.IGNORECASE),
    "simply": re.compile(r"\bsimply\b", re.IGNORECASE),
    "merely": re.compile(r"\bmerely\b", re.IGNORECASE),
    "this is not X; it is Y": re.compile(r"\bthis is not\b.+\b(it is|it's)\b", re.IGNORECASE),
}
SKIP = {"AGENTS.md", "CLAUDE.md"}


def changed_lines() -> list[tuple[str, int, str]]:
    """Return added lines against origin/main, including uncommitted changes."""

    result = subprocess.run(
        ["git", "diff", "--unified=0", "origin/main"],
        check=False,
        capture_output=True,
    )
    if result.returncode:
        print("Prose review skipped: unable to read the Git diff.")
        return []

    output = result.stdout.decode("utf-8", errors="replace")
    lines: list[tuple[str, int, str]] = []
    path = ""
    line_number = 0
    for line in output.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:]
        elif line.startswith("@@"):
            match = re.search(r"\+(\d+)", line)
            line_number = int(match.group(1)) if match else 0
        elif line.startswith("+") and not line.startswith("+++"):
            lines.append((path, line_number, line[1:]))
            line_number += 1
        elif not line.startswith("-"):
            line_number += 1
    return lines


def main() -> int:
    """Print review prompts for changed prose; never fail the caller."""

    prompts = []
    for path, line_number, text in changed_lines():
        if path in SKIP:
            continue
        for label, pattern in PATTERNS.items():
            if pattern.search(text):
                prompts.append(f"{path}:{line_number}: review '{label}': {text.strip()}")

    if prompts:
        print("Prose review prompts (review context; these are not failures):")
        print("\n".join(prompts))
    else:
        print("Prose review: no high-signal phrases found in changed source text.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
