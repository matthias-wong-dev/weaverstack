"""What the Fabric-only refactor removed, and must not come back.

A retirement stays retired when something fails if it returns. These are the
names, imports and payload shapes a second, non-Fabric workspace brought with
it: each was deleted, and each would be easy to reintroduce by
habit, a stray
``import pyspark`` in a module that runs on a desktop, a token in a payload that
is meant to be finished SQL, a second workspace kind.

The rule underneath all of them: **Fabric is the only workspace, Spark is only
a Fabric engine, and a build freezes the names it decided.**
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

SOURCE = Path(__file__).resolve().parents[1] / "src"
CORE = SOURCE / "weaver"
CLI = SOURCE / "weaver_cli"

#: The only modules that may import PySpark: they run inside a Fabric session,
#: where Spark is the host's own and authored runtime code holds it.
#: Everything else is desktop or shared code, and a desktop has no Spark.
HOSTED_MODULES = frozenset(
    {
        "weaver/runtime",
        "weaver/sessions/notebook.py",
        "weaver/sessions/host.py",
        "weaver/build_bundle/executors/spark_case.py",
    }
)

#: Names the refactor retired. A module reintroducing one is either restoring
#: the emulator or building a second naming layer beside FabricSparkTarget.
RETIRED = (
    "LocalWorkspace",
    "LocalResolver",
    "SparkNaming",
    "SparkDestination",
    "local_destination",
    "fabric_destination",
    "local_delta_session",
    "workspace_type",
    "WORKSPACE_TYPES",
    "spark_schema",
    "drop_local_destination_catalogue",
    # One workspace, so no name distinguishes it from another kind.
    "FabricWorkspace",
    "is_fabric",
    # One build, so no name distinguishes a position's own version of it.
    "_build_in_process",
    "_build_desktop_fabric",
    # Compatibility wrappers, deleted with the migration they spanned.
    "build_uploaded_item_repository",
    # What Static used to ask. The record of whether an object has been loaded is
    # its bookmark, not its contents: a table somebody populated by hand has not
    # been loaded, and a table a clean load emptied has been.
    "table_is_populated",
    "folder_is_populated",
    # Bookmark-specific state, carried and resolved beside the catalogue it is a
    # table of. The `_` schema is one thing, so one object reads and writes it
    # and one payload carries it: see `weaver.catalogue.state.Catalogue`.
    "BookmarkContext",
    "RunBookmarks",
    "with_bookmarks",
    "read_installed_bookmarks",
    "resolve_installed_object",
    # The switch that said who records. Which interface was called decides it
    # now, `_load()` against `load()`, `read()` against `run()`, an object's own
    # procedure against `_.Load`, so there is no flag a caller can get wrong.
    "update_catalogue",
    "catalogue_write",
    # Bookmark-specific build plumbing, generalised into the runtime-table
    # family: one reconciliation stage over every current-state table, and one
    # reference stage over every table a target presents.
    "bookmark_statements",
    "render_bookmark_reconciliation",
    "render_bookmark_reference",
    "bookmark_reference_views",
    "read_bookmark_source",
    "bookmark_reference",
    "reconcile_bookmarks",
    "create_bookmark_reference",
    # One record rather than evidence alone: a settled unit of work leaves the
    # state it produced beside the row that says it settled.
    "RunLog",
    "open_run_log",
    # Bespoke per-table writes. A table declares how its rows are maintained and
    # the generic verbs read that declaration.
    "advance_bookmark",
    "set_load_status",
    "record_test_status",
    # Per-operation reversals of the catalogue. One installed graph answers what
    # is installed and what reads what, and load, test and health read it.
    "InstalledEstate",
    "InstalledObject",
    "InstalledDependency",
    "ValidationDependencies",
    "_validation_dependencies",
    # Fabric repoints a shortcut in one request, so nothing waits for a deleted
    # shortcut's name to come free.
    "REPLACE_TIMEOUT",
    "REPLACE_POLL_INTERVAL",
)


def _modules(root: Path):
    return sorted(path for path in root.rglob("*.py") if "__pycache__" not in str(path))


def _relative(path: Path) -> str:
    return path.relative_to(SOURCE).as_posix()


def _is_hosted(path: Path) -> bool:
    relative = _relative(path)
    return any(
        relative == allowed or relative.startswith(f"{allowed}/")
        for allowed in HOSTED_MODULES
    )


def _imports(path: Path) -> set[str]:
    """Every module name this file imports, however it spells the import."""

    tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            found.add(node.module)
    return found


# --- Spark is a Fabric engine, not a desktop dependency -----------------------


@pytest.mark.parametrize(
    "path", _modules(CORE) + _modules(CLI), ids=lambda path: _relative(path)
)
@weaver_test()
def test_only_hosted_runtime_modules_import_pyspark(path: Path):
    """A desktop never holds a Spark session, so it never imports one.

    Stated as an import rule rather than a dependency one because the suite may
    run where PySpark happens to be installed: what must be true is that no
    desktop code path reaches for it.
    """

    spark_imports = {
        name
        for name in _imports(path)
        if name == "pyspark"
        or name.startswith("pyspark.")
        or name == "delta"
        or name.startswith("delta.")
    }
    if _is_hosted(path):
        return
    assert not spark_imports, (
        f"{_relative(path)} imports {sorted(spark_imports)}, and is not one of "
        "the hosted runtime modules that may. Spark belongs where Fabric runs "
        "it; a desktop reaches Spark through the Session instead."
    )


@weaver_test()
def test_the_hosted_allowlist_names_modules_that_exist():
    """Guard the guard: an allowlist of nothing would allow everything."""

    for allowed in HOSTED_MODULES:
        assert (SOURCE / allowed).exists(), f"{allowed} is allowlisted and absent"


@weaver_test()
def test_pyspark_is_not_a_declared_dependency():
    """``pip install weaverstack`` must not pull Spark or need a JVM."""

    text = (SOURCE.parent / "pyproject.toml").read_text(encoding="utf-8")
    declared = re.findall(r'^\s*"([^"]+)"', text, flags=re.MULTILINE)
    names = {entry.split(">=")[0].split("[")[0].strip().lower() for entry in declared}

    assert "pyspark" not in names
    assert "delta-spark" not in names


# --- one workspace, one naming rule -------------------------------------------


@pytest.mark.parametrize(
    "path", _modules(CORE) + _modules(CLI), ids=lambda path: _relative(path)
)
@weaver_test()
def test_no_module_reintroduces_a_retired_name(path: Path):
    text = path.read_text(encoding="utf-8")
    found = [name for name in RETIRED if re.search(rf"\b{re.escape(name)}\b", text)]

    assert not found, (
        f"{_relative(path)} names {found}, which the Fabric-only refactor "
        "removed. Fabric is the only workspace, and FabricSparkTarget is the "
        "only Spark naming."
    )


@weaver_test()
def test_the_retired_list_is_not_empty():
    """Guard the guard, again: reflection that found nothing passes everything."""

    assert len(RETIRED) > 5


# --- a build freezes the names it decided --------------------------------------


@pytest.mark.parametrize(
    "path", _modules(CORE) + _modules(CLI), ids=lambda path: _relative(path)
)
@weaver_test()
def test_no_module_emits_an_object_or_schema_token(path: Path):
    """Payloads carry finished SQL.

    ``{{object:Schema.Name}}`` was how a payload deferred a name to install
    time. The Builder decides it now, so a token in a payload would be a name
    nothing resolves, and unresolved token syntax is not valid Spark SQL.
    """

    text = path.read_text(encoding="utf-8")
    tokens = set(re.findall(r"\{\{(object|schema):", text))

    assert not tokens, (
        f"{_relative(path)} emits {sorted(tokens)} tokens. A build renders "
        "final Fabric names, so a payload has nothing left to resolve."
    )


@weaver_test()
def test_the_only_payload_token_left_is_the_publication_epoch():
    """One value cannot be frozen, and it is named.

    A rendered clock would make the same repository produce different payload
    bytes on every run, and a bundle's identity is its bytes.
    """

    from weaver import tokens

    public = {name for name in vars(tokens) if not name.startswith("_")}

    assert "BUILD_DATETIME_TOKEN" in public
    assert not {"OBJECT", "SCHEMA", "expand", "object_token", "schema_token"} & public
