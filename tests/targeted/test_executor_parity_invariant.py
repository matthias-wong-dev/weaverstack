"""Executors must not branch on where they are running.

The parity argument rests on this and would be worthless without it: if an
executor took one path on a desktop and another in a session, testing it from the
checkout would prove the desktop branch while the wheel probes — which check
*acquisition*, not behaviour — would never touch the other.

AGENTS.md already states the rule: "an `if isinstance(workspace, …)` in core
operation code means the abstraction is being broken; the fix belongs in the
factories, or in the CLI that does the crossing." The split in
`tests/fabric/test_published_weaver_primitive.py` makes it load-bearing in a new way, so it
is asserted here rather than trusted.
"""

from __future__ import annotations

import ast
import pathlib

import pytest
from support.weaver_test import weaver_test

EXECUTORS = (
    pathlib.Path(__file__).resolve().parents[2]
    / "src"
    / "weaver"
    / "build_bundle"
    / "executors"
)

#: Names that would mean an executor is deciding *where* it is rather than doing
#: its job. Acquisition belongs to the factories; behaviour belongs here.
ENVIRONMENT_TELLS = {
    "Workspace",
    "FabricStore",
    "FilesystemStore",
    "OneLakeDfsClient",
    "FabricResolver",
    "LocalResolver",
    "FabricSessionResolver",
    "notebookutils",
    "mssparkutils",
}


def executor_modules():
    return sorted(
        path
        for path in EXECUTORS.glob("*.py")
        if path.name not in {"__init__.py", "base.py"}
    )


@weaver_test()
def test_there_are_executors_to_check():
    """A guard on the guard: an empty glob would pass every test below."""

    assert executor_modules()


@pytest.mark.parametrize("module", executor_modules(), ids=lambda path: path.stem)
@weaver_test()
def test_an_executor_never_names_a_transport(module):
    """No executor mentions a workspace kind, a store class or a resolver class.

    Naming one would be the executor choosing its own environment — the decision
    the factories exist to make once, above it.
    """

    tree = ast.parse(module.read_text())
    named = (
        {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        | {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        | {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        | {
            (node.module or "").split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
    )

    assert not (named & ENVIRONMENT_TELLS), (
        f"{module.name} names {sorted(named & ENVIRONMENT_TELLS)} — an executor "
        "that can tell where it is running can behave differently there, and the "
        "desktop tests would only ever prove one branch"
    )


# A blanket ban on `isinstance` was the first version of this file and it was
# wrong: `tsql` and `spark_sql_batch` both use it to check that a decoded payload
# is the array they expect, which is validating data rather than choosing a
# branch by environment. The names above are the real signal — an executor cannot
# behave differently in a session without being able to *tell* it is in one.
