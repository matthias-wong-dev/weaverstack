"""Coverage this refactor owes, and the tests that will pay it.

Deleting the local Spark tier removed proofs that were real. Where the claim
still matters and the only honest home is a Fabric test, the test was moved
rather than dropped — but a moved test that is skipped proves nothing, so each
one is registered here with the acceptance work that closes it.

The register is self-clearing. Every entry names a test that must exist and must
still be skipped; the moment one is implemented and unskipped, this invariant
fails until its entry is removed. That makes closing a gap a deliberate act and
makes forgetting one impossible.

**This register must be empty before the Fabric-only refactor merges.** A gap
recorded here is work in flight, not an accepted cost.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

TESTS = Path(__file__).resolve().parent

#: Each gap: the module, the tests it owes, and what closing it requires.
DEBT = {
    "fabric/test_run_dispatch_boundary.py": {
        "tests": (
            "test_an_all_succeeding_run_says_so",
            "test_one_broken_node_does_not_stop_the_others_from_running",
            "test_the_run_reports_failure_when_any_node_failed",
        ),
        "closes_with": (
            "a Fabric harness that deploys the thin artefacts into a real "
            "Lakehouse, so dispatch reaching a primitive and settling its "
            "result is proven where the modules are actually imported"
        ),
    },
    "fabric/test_validation_dispatch_boundary.py": {
        "tests": (
            "test_a_validation_reaches_its_artefact_the_same_way_a_load_does",
            "test_a_disagreement_is_a_failure_carrying_what_it_found",
            "test_a_validation_that_could_not_run_is_invalid_rather_than_failed",
        ),
        "closes_with": (
            "the same harness, plus a Test whose artefact returns a real Spark "
            "frame — the Spark half of the validation semantics that "
            "test_warehouse_validation_primitive proves for T-SQL"
        ),
    },
}


def _module(relative: str) -> ast.Module:
    path = TESTS / relative
    assert path.exists(), f"{relative} is registered as owing coverage and is absent"
    return ast.parse(path.read_text(encoding="utf-8"), str(path))


def _skip_marked(tree: ast.Module) -> bool:
    """Whether the module skips wholesale, through ``pytestmark``."""

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "pytestmark"
            for target in node.targets
        ):
            continue
        if "mark.skip" in ast.unparse(node.value):
            return True
    return False


def _functions(tree: ast.Module) -> dict[str, ast.FunctionDef]:
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }


@pytest.mark.parametrize("relative", sorted(DEBT))
def test_every_registered_gap_names_tests_that_exist(relative: str):
    """A register pointing at nothing would read as coverage."""

    functions = _functions(_module(relative))
    missing = [name for name in DEBT[relative]["tests"] if name not in functions]

    assert not missing, (
        f"{relative} is registered as owing {missing}, and does not define them. "
        "Either the test was renamed, or the register is stale."
    )


@pytest.mark.parametrize("relative", sorted(DEBT))
def test_a_gap_that_has_been_closed_is_removed_from_the_register(relative: str):
    """The self-clearing half.

    A registered test that is no longer skipped has been implemented, and the
    entry describing it as owed is now false. Fail here so the register is
    updated in the same change that closes the gap.
    """

    tree = _module(relative)
    if _skip_marked(tree):
        return

    functions = _functions(tree)
    live = [
        name
        for name in DEBT[relative]["tests"]
        if not any(
            "skip" in ast.unparse(decorator)
            for decorator in functions[name].decorator_list
        )
    ]

    assert not live, (
        f"{relative} now runs {live}, so the coverage it owed has been paid. "
        "Remove its entry from DEBT."
    )


@pytest.mark.parametrize("relative", sorted(DEBT))
def test_every_registered_gap_says_what_closes_it(relative: str):
    """A gap with no stated remedy is indistinguishable from an oversight."""

    assert DEBT[relative]["closes_with"].strip()


def test_the_register_is_visible_in_one_place():
    """Guard the guard: an empty register would pass every check above.

    When this fails because ``DEBT`` is empty, the refactor has paid what it
    owed — delete this module rather than weakening it.
    """

    assert DEBT, (
        "the coverage register is empty, which means every gap is closed: "
        "delete this module, which exists only to hold them open"
    )
