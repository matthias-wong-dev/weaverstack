"""What ships as an example is what gets copied.

`examples/` is documentation that runs: a notebook a user opens in Fabric, a
desktop script, a composition and a workspace configuration. A retired spelling
surviving in one of them is worse than in a docstring, because the reader's
first move is to paste it.

The suite does not execute these, the notebook needs Fabric and the script
needs a workspace, so nothing else would notice. This reads them.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"

#: Public spellings the refactor retired. Each was a keyword or a callable a
#: reader could plausibly have copied from an older example.
RETIRED_SPELLINGS = (
    "weaver_lakehouse",
    # The catalogue is a Warehouse, and a run's evidence is rows in `_.Log`.
    "Files/_/Log",
    "task_logging",
    "IndexDictionary",
    "workspace_type",
    "FabricWorkspace",
    "LocalWorkspace",
    "build_uploaded_item_repository",
    "weaverstack[cli]",
    "weaver push ",
    "weaver unbind ",
    "weaver capacity ",
    "weaver notebook ",
    "weaver doctor",
)


#: Every file an example presents. The Environment directory holds a built wheel and
#: Fabric's own metadata, neither of which anybody copies from.
def _example_files() -> list[Path]:
    return sorted(
        path
        for path in EXAMPLES.rglob("*")
        if path.is_file()
        and path.suffix in {".py", ".md", ".yml", ".yaml", ".sql"}
        and "weaver.Environment" not in path.parts
    )


@weaver_test()
def test_there_are_examples_to_check():
    """Guards the way the rest of this file could pass for the wrong reason."""

    assert _example_files()


@pytest.mark.parametrize("spelling", RETIRED_SPELLINGS)
@weaver_test()
def test_no_example_uses_a_retired_spelling(spelling):
    carrying = [
        path.relative_to(ROOT).as_posix()
        for path in _example_files()
        if spelling in path.read_text(encoding="utf-8")
    ]

    assert not carrying, (
        f"{spelling!r} was retired; these examples would teach the use of "
        f"it: {carrying}"
    )


@weaver_test()
def test_the_python_examples_parse():
    """A shipped example that does not parse has never been run by anybody."""

    broken = []
    for path in _example_files():
        if path.suffix != ".py":
            continue
        try:
            ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            broken.append(f"{path.relative_to(ROOT).as_posix()}: {exc}")

    assert not broken, broken


@weaver_test()
def test_the_catalogue_is_named_typed_wherever_an_example_names_one():
    """`catalogue=` takes `Warehouse/Weaver`, and an example must show that.

    The bare name parses as a configuration error rather than a Warehouse, so
    an example carrying one would fail for the reader on their first run.
    """

    # Literal values only. `catalogue=options.catalogue` passes whatever the
    # caller supplied, and what that turns out to be is the caller's business.
    quoted = re.compile(r"""catalogue=["'](?P<value>[^"']+)["']""")
    configured = re.compile(r"""^catalogue:\s*(?P<value>\S+)""")

    wrong = []
    for path in _example_files():
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            found = quoted.search(stripped) or configured.match(stripped)
            if found and not found.group("value").startswith("Warehouse/"):
                wrong.append(f"{path.relative_to(ROOT).as_posix()}: {stripped}")

    assert not wrong, wrong


# --- what an authored table returns -------------------------------------------

#: Where authored objects live outside `src`: the examples that get copied, and
#: the fixture repositories the Fabric journeys build and load.
AUTHORED_ROOTS = (EXAMPLES, ROOT / "tests" / "fixtures")


def _authored_tables():
    """Every ``Table`` subclass in the tree, with its ``read()`` and its document.

    Read rather than imported: a fixture module imports its siblings by package
    path and expects a Lakehouse, so parsing is what makes the whole tree
    answerable at all.
    """

    for root in AUTHORED_ROOTS:
        for path in sorted(root.rglob("*.py")):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - unused
                continue
            document = ast.get_docstring(tree) or ""
            for node in tree.body:
                if not isinstance(node, ast.ClassDef):
                    continue
                if not any(
                    isinstance(base, ast.Name) and base.id == "Table"
                    for base in node.bases
                ):
                    continue
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name == "read":
                        yield path, node.name, document, item


def _returns_a_pair(function) -> bool:
    return any(
        isinstance(node, ast.Return) and isinstance(node.value, ast.Tuple)
        for node in ast.walk(function)
    )


@weaver_test()
def test_there_are_authored_tables_to_check():
    """Guards the claim below from passing because it found nothing."""

    assert len(list(_authored_tables())) >= 5


@weaver_test()
def test_no_non_incremental_table_returns_a_delete_claim():
    """A non-incremental source is the whole truth, so it stages and no more.

    The rule is enforced at run time, which means a fixture or an example that
    breaks it fails inside a Fabric journey rather than here. Both spellings of
    the mistake have been made: an empty second frame, and a literal ``None``
    beside the staging one. This reads the tree, so neither survives a commit.
    """

    offenders = [
        f"{path.relative_to(ROOT)}: {name}"
        for path, name, document, function in _authored_tables()
        if _returns_a_pair(function) and "incremental: true" not in document.lower()
    ]

    assert not offenders, (
        "these tables return a delete claim their declaration does not permit; "
        f"return the staging frame on its own: {offenders}"
    )
