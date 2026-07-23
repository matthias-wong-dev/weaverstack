"""Weaver carries no product names and no data-architecture opinions.

Examples included. Folder, Delta and SQL are materialisation forms, so example
names must not imply a tiering scheme (T0/T1/T2, bronze/silver/gold) — a reader
should not be able to infer an architecture Weaver does not have.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCANNED = (ROOT / "src", ROOT / "tests")

FORBIDDEN = {
    "product name": re.compile(r"ilovegov|I Love Gov|\bILG\b|\bDWG\b", re.IGNORECASE),
    "tiering scheme": re.compile(r"\bT[0-9]_|\bbronze\b|\bsilver\b|\bgold\b", re.IGNORECASE),
}


def _sources() -> list[Path]:
    this_file = Path(__file__).resolve()
    return sorted(
        path
        for directory in SCANNED
        for path in directory.rglob("*.py")
        if path.resolve() != this_file
    )


def test_there_are_sources_to_scan():
    assert _sources()


@pytest.mark.parametrize("label", sorted(FORBIDDEN))
def test_no_forbidden_vocabulary(label):
    pattern = FORBIDDEN[label]
    offenders = [
        f"{path.relative_to(ROOT)}:{number}"
        for path in _sources()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if pattern.search(line)
    ]
    assert not offenders, f"{label} appears in: {offenders}"
