"""Every mirror journey drives the whole lifecycle, and reads every step.

A mirror is cheap to prove badly. Standing one up and listing what appeared says
the parts are there; what says they work is the lifecycle after it, and those
steps are the slow ones, so they are the ones a hurried change drops.

Each Fabric mirror journey names its steps from
:mod:`support.mirror_journey`. This reads both modules and fails when one stops
driving a step or stops making a claim about it. Pure: it parses the source and
runs nothing.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from support import mirror_journey
from support.weaver_test import weaver_test

FABRIC = Path(__file__).resolve().parent / "fabric"

#: The journeys this holds to the lifecycle, one per item kind. A mirror of a
#: new kind belongs here in the change that adds it.
JOURNEYS = (
    FABRIC / "test_warehouse_mirror_journey.py",
    FABRIC / "test_lakehouse_mirror_journey.py",
)

#: Steps that arrange the next transition rather than being one. Nothing reads
#: them, and a failure in one surfaces through the step it set up.
SETUP = frozenset({mirror_journey.BUILD_SOURCE, mirror_journey.CHANGE})

#: Each step's constant name, by the value it carries, so a step named in a
#: journey resolves back to the shared vocabulary.
BY_NAME = {
    name: value
    for name, value in vars(mirror_journey).items()
    if name.isupper() and isinstance(value, str)
}


def _called_with(tree: ast.AST, method: str) -> set[str]:
    """The step values passed to one method, resolved through the shared names."""

    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != method:
            continue
        for argument in node.args:
            if isinstance(argument, ast.Name) and argument.id in BY_NAME:
                found.add(BY_NAME[argument.id])
    return found


def _subscripted(tree: ast.AST) -> set[str]:
    """The steps a journey reads back, being ``journey[STEP]``."""

    return {
        BY_NAME[node.slice.id]
        for node in ast.walk(tree)
        if isinstance(node, ast.Subscript)
        and isinstance(node.slice, ast.Name)
        and node.slice.id in BY_NAME
    }


def _parsed(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


@weaver_test()
@pytest.mark.parametrize("path", JOURNEYS, ids=lambda path: path.stem)
def test_a_journey_drives_every_step_of_the_lifecycle(path):
    """Building and mirroring is half of it. The rest is what happens after."""

    driven = _called_with(_parsed(path), "step")

    assert set(mirror_journey.LIFECYCLE) <= driven, (
        f"{path.name} does not drive: "
        + ", ".join(sorted(set(mirror_journey.LIFECYCLE) - driven))
    )


@weaver_test()
@pytest.mark.parametrize("path", JOURNEYS, ids=lambda path: path.stem)
def test_a_journey_makes_a_claim_about_every_transition(path):
    """A step nothing reads is a transition nobody is watching.

    Setup steps are exempt. A journey does not cascade, so a failed one marks
    every later step failed, and the claims about those report it.
    """

    tree = _parsed(path)
    read = _called_with(tree, "require") | _subscripted(tree)
    wanted = set(mirror_journey.LIFECYCLE) - SETUP

    assert wanted <= read, f"{path.name} makes no claim about: " + ", ".join(
        sorted(wanted - read)
    )


@weaver_test()
def test_the_shared_vocabulary_is_what_the_journeys_import():
    """A journey spelling a step by hand would drift out of this check."""

    for path in JOURNEYS:
        source = path.read_text(encoding="utf-8")
        assert "from support.mirror_journey import" in source, (
            f"{path.name} names its steps outside the shared vocabulary"
        )
