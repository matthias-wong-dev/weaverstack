"""Weaver carries no house jargon, examples included.

Folder, Delta and SQL are materialisation forms, so `T0`/`T1`/`T2` naming must
not appear: it is our own tiering convention and means nothing to a reader
outside these repositories. Widely-understood naming such as bronze/silver/gold
is fine — it illustrates without assuming.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCANNED = (ROOT / "src", ROOT / "tests")

HOUSE_TIERING = re.compile(r"\bT[0-9]_", re.IGNORECASE)


def _sources() -> list[Path]:
    return sorted(path for directory in SCANNED for path in directory.rglob("*.py"))


def test_there_are_sources_to_scan():
    assert _sources()


def test_no_house_tiering_vocabulary():
    offenders = [
        f"{path.relative_to(ROOT)}:{number}"
        for path in _sources()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if HOUSE_TIERING.search(line)
    ]
    assert not offenders, f"house tiering names appear in: {offenders}"
