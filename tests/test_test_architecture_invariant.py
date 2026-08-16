"""Mechanical invariants for the test suite's claim-oriented architecture."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from support.weaver_test import weaver_test

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


def _test_paths() -> tuple[Path, ...]:
    return tuple(sorted(TESTS.rglob("test_*.py")))


@weaver_test()
def test_every_test_module_names_a_claim():
    unnamed = sorted(
        path for path in _test_modules() if not MODULE_NAME.fullmatch(Path(path).name)
    )
    assert not unnamed, (
        "test modules must be named test_<subject>_<claim>.py with a claim from "
        f"{sorted(CLAIMS)}: {unnamed}"
    )


@weaver_test()
def test_no_module_takes_a_claim_the_taxonomy_retired():
    named = sorted(
        path for path in _test_modules() if RETIRED_NAME.fullmatch(Path(path).name)
    )
    assert not named, f"these claims were retired: {named}"


@weaver_test()
def test_every_test_function_has_one_weaver_declaration():
    missing = []
    duplicated = []
    for path in _test_paths():
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not node.name.startswith("test_"):
                continue
            declarations = [
                decorator
                for decorator in node.decorator_list
                if isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "weaver_test"
            ]
            location = f"{path.relative_to(ROOT)}:{node.lineno}:{node.name}"
            if not declarations:
                missing.append(location)
            if len(declarations) > 1:
                duplicated.append(location)
    assert not missing, f"missing @weaver_test declarations: {missing}"
    assert not duplicated, f"duplicate @weaver_test declarations: {duplicated}"


@weaver_test()
def test_managed_pytest_markers_are_generated_only_by_the_wrapper():
    handwritten = []
    managed = {"fabric", "remote", "hosted", "full_integration", "provision"}
    for path in TESTS.rglob("*.py"):
        if path == TESTS / "support" / "weaver_test.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Attribute) or node.attr not in managed:
                continue
            mark = node.value
            if (
                isinstance(mark, ast.Attribute)
                and mark.attr == "mark"
                and isinstance(mark.value, ast.Name)
                and mark.value.id == "pytest"
            ):
                handwritten.append(
                    f"{path.relative_to(ROOT)}:{node.lineno}:pytest.mark.{node.attr}"
                )
    assert not handwritten, f"managed markers must come from @weaver_test: {handwritten}"


@weaver_test()
def test_superseded_test_declaration_machinery_is_absent():
    retired_paths = {
        TESTS / "support" / "livy_telemetry.py",
        TESTS / "support" / "test_livy_telemetry_invariant.py",
    }
    assert not {path.relative_to(ROOT) for path in retired_paths if path.exists()}

    harness = (TESTS / "support" / "weaver_test.py").read_text()
    root_harness = (TESTS / "conftest.py").read_text()
    assert "_known_sessions" not in harness + root_harness
    assert "Session.__init__" not in harness + root_harness
