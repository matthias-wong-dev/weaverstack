"""Mechanical invariants for the test suite's claim-oriented architecture."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"
CLAIMS = frozenset(
    {
        "declaration",
        "representation",
        "boundary",
        "install",
        "primitive",
        "cycle",
        "invariant",
        "journey",
    }
)
RETIRED_CLAIMS = frozenset({"render", "binding", "lifecycle"})
MODULE_NAME = re.compile(rf"^test_.+_(?P<claim>{'|'.join(sorted(CLAIMS))})\.py$")
RETIRED_NAME = re.compile(
    rf"^test_.+_(?P<claim>{'|'.join(sorted(RETIRED_CLAIMS))})\.py$"
)


def _test_modules() -> set[str]:
    return {path.relative_to(ROOT).as_posix() for path in TESTS.rglob("test_*.py")}


def test_every_test_module_names_a_claim():
    unnamed = sorted(
        path for path in _test_modules() if not MODULE_NAME.fullmatch(Path(path).name)
    )
    assert not unnamed, (
        "test modules must be named test_<subject>_<claim>.py with a claim from "
        f"{sorted(CLAIMS)}: {unnamed}"
    )


def test_no_module_takes_a_claim_the_taxonomy_retired():
    named = sorted(
        path for path in _test_modules() if RETIRED_NAME.fullmatch(Path(path).name)
    )
    assert not named, f"these claims were retired: {named}"
