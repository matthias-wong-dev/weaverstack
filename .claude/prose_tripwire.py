#!/usr/bin/env python3
"""Prompt a review of the register checks in CLAUDE.md, over changed lines."""

from __future__ import annotations

import re
import subprocess
import sys

# The register checks from CLAUDE.md. Phrase bans alone do not hold: "this is not
# X; it is Y" was banned and the same move reappeared 870 times as "X rather than
# Y". These check the constructions instead.
PATTERNS = {
    "em dash": re.compile(r"\u2014"),
    "contrastive definition": re.compile(r"\b(rather than|instead of)\b", re.I),
    "counterfactual": re.compile(
        r"\bwould (have|be|then|leave|need|make|put|report|send|know)\b", re.I
    ),
    "code with mental states": re.compile(
        r"\b(knows|knew|wants|refuses|cares|needs to know|is meant to)\b", re.I
    ),
    "emphasis": re.compile(
        r"(?<![*\w])\*[A-Za-z][A-Za-z ]{1,25}\*(?![*\w])"
        r"|\b(genuinely|the whole point|the key point|the very|deliberately"
        r"|intentionally|quietly|worth noting|obviously|simply|merely)\b",
        re.I,
    ),
    "the reader in the text": re.compile(
        r"\b(a reader|the reader|someone reading)\b", re.I
    ),
}
# CLAUDE.md quotes these constructions to ban them.
SKIP = {"CLAUDE.md", ".claude/prose_tripwire.py"}


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
                prompts.append(
                    f"{path}:{line_number}: review '{label}': {text.strip()}"
                )

    if prompts:
        print("Prose review prompts (review context; these are not failures):")
        print("\n".join(prompts))
    else:
        print("Prose review: no high-signal phrases found in changed source text.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
