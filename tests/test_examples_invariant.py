"""What ships as an example is what a reader will copy.

`examples/` is documentation that runs: a notebook a user opens in Fabric, a
desktop script, a composition and a workspace configuration. A retired spelling
surviving in one of them is worse than in a docstring, because the reader's
first move is to paste it.

The suite does not execute these — the notebook needs Fabric and the script
needs a workspace — so nothing else would notice. This reads them.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"

#: Public spellings the refactor retired. Each was a keyword or a callable a
#: reader could plausibly have copied from an older example.
RETIRED_SPELLINGS = (
    "weaver_lakehouse",
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


#: Every file a reader reads. The Environment directory holds a built wheel and
#: Fabric's own metadata, neither of which anybody copies from.
def _example_files() -> list[Path]:
    return sorted(
        path
        for path in EXAMPLES.rglob("*")
        if path.is_file()
        and path.suffix in {".py", ".md", ".yml", ".yaml", ".sql"}
        and "weaver.Environment" not in path.parts
    )


def test_there_are_examples_to_check():
    """Guards the way the rest of this file could pass for the wrong reason."""

    assert _example_files()


@pytest.mark.parametrize("spelling", RETIRED_SPELLINGS)
def test_no_example_uses_a_retired_spelling(spelling):
    carrying = [
        path.relative_to(ROOT).as_posix()
        for path in _example_files()
        if spelling in path.read_text(encoding="utf-8")
    ]

    assert not carrying, (
        f"{spelling!r} was retired; these examples would teach a reader to use "
        f"it: {carrying}"
    )


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
