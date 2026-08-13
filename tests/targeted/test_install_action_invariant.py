"""Every InstallAction kind has a test that executes it, or is deliberately deferred.

The list below *is* the checklist, and it is legible from the terminal:

```text
pytest --collect-only -q tests/targeted/test_install_action_invariant.py
```

Each parametrised case reads ``[<kind>-<the test that executes it>]``. Each InstallAction kind names the test that runs it
against a real engine and inspects what it made — and adding a kind without
adding that test fails here, naming what is missing rather than leaving it to be
noticed.

Two things this deliberately does not do.

It does not check that the named test *passes*, or that it asserts anything
useful. A test's own claim is its business; this only holds the estate to having
one per kind.

And it does not scan for tests by pattern. A convention that merely *hopes* every
kind is covered is the state this replaces — the point is a written list someone
had to change on purpose, so that deferring a kind is a decision with a name
attached rather than an omission.
"""

from __future__ import annotations

import pathlib
import re

import pytest

from weaver.build_bundle import models

REPOSITORY = pathlib.Path(__file__).resolve().parents[2]
TESTS = REPOSITORY / "tests"

#: Why a kind has no execution test yet. A deferral is a claim about the future,
#: so it carries a reason and is as visible as a covered kind.
DEFERRED = {
    "write_file": "load semantics land in a later branch; the executor is covered by test_actions_delta_install.py",
    "delete_file": "load semantics land in a later branch; the executor is covered by test_actions_delta_install.py",
    "build_procedure": "generated load procedures land in a later branch",
    "drop_procedure": "generated load procedures land in a later branch",
}

#: Kinds that change a target, and the test that executes each. A kind appearing
#: on both physical sides needs one test per side: the executors differ, and so
#: does what "the object is what it should be" means.
COVERED = {
    "create_schema": (
        "test_create_schema_action_creates_the_schema",
        "test_create_schema_action_creates_the_schema_in_the_warehouse",
    ),
    "build_table": (
        "test_build_table_action_creates_the_declared_columns",
        "test_build_table_action_is_accepted_by_fabric",
    ),
    "build_view": (
        "test_build_view_action_creates_a_view_over_the_table_it_reads",
    ),
    "build_folder": ("test_build_folder_action_creates_the_directory",),
    "create_alias": (
        "test_the_alias_lands_in_the_consumers_tables_area_without_copying_data",
        "test_the_alias_exists_as_a_onelake_shortcut",
        "test_a_warehouse_alias_is_a_view_over_the_bound_lakehouse",
    ),
    "drop_table": ("test_drop_table_action_clears_the_way_for_a_rebuild",),
    "drop_view": ("test_drop_table_action_clears_the_way_for_a_rebuild",),
    "drop_folder": ("test_drop_folder_action_removes_the_directory",),
    "prune_table": ("test_prune_table_action_removes_an_object_nothing_declares",),
    "prune_view": ("test_prune_table_action_removes_an_object_nothing_declares",),
    "prune_schema": ("test_prune_table_action_removes_an_object_nothing_declares",),
    "prune_folder": ("test_prune_table_action_removes_an_object_nothing_declares",),
    "refresh_sql_endpoint": (
        "test_each_mutated_lakehouse_had_its_endpoint_refreshed_for_real",
    ),
}

#: Kinds that write the catalogue rather than the estate. They are covered as a
#: catalogue round trip rather than per action, because what matters about them
#: is the rows they leave, not the statement that left them.
CATALOGUE_KINDS = frozenset(
    {"delete_catalogue_claims", "publish_catalogue", "publish_registry"}
)


def declared_kinds() -> set[str]:
    """Every action kind the product defines, read from the product.

    Taken from the module rather than listed here, which is what makes this a
    tripwire: a new kind arrives on its own and has to be placed.

    Omission reasons share the shape — lower-case strings on the same module —
    and are not actions, so they are subtracted from their own declared set
    rather than by guessing at their names.
    """

    return {
        value
        for name, value in vars(models).items()
        if name.isupper() and isinstance(value, str) and re.fullmatch(r"[a-z_]+", value)
    } - set(models.OMISSION_REASONS)


def test_the_product_defines_action_kinds_to_check():
    """Guard the guard: reflection that found nothing would pass everything."""

    assert len(declared_kinds()) > 10


def test_every_action_kind_is_covered_or_deliberately_deferred():
    """The checklist itself. A new kind must be placed before this passes."""

    placed = set(COVERED) | set(DEFERRED) | CATALOGUE_KINDS
    unplaced = declared_kinds() - placed

    assert not unplaced, (
        "these action kinds are neither covered by an execution test nor "
        f"deferred with a reason: {sorted(unplaced)}"
    )


def test_no_kind_is_both_covered_and_deferred():
    """A deferral that is also covered means one of the two is stale."""

    assert not set(COVERED) & set(DEFERRED)


@pytest.mark.parametrize(
    ("kind", "test_name"),
    sorted((kind, name) for kind, names in COVERED.items() for name in names),
)
def test_the_named_execution_test_exists(kind: str, test_name: str):
    """The list points at something real.

    Renaming a test without updating the checklist would otherwise leave the list
    describing an estate nobody checks — the failure mode a written list has and
    a pattern match does not.
    """

    pattern = re.compile(rf"^def {re.escape(test_name)}\(", re.MULTILINE)
    found = [
        path.relative_to(REPOSITORY)
        for path in TESTS.rglob("test_*.py")
        if pattern.search(path.read_text(encoding="utf-8"))
    ]

    assert found, f"{kind}: no test named {test_name} exists"


def test_every_deferral_says_why():
    for kind, reason in DEFERRED.items():
        assert reason and len(reason) > 20, kind


def test_no_executor_declares_where_it_has_to_run():
    """Every build action runs in the Installer, in whichever position that is.

    Executors used to declare ``needs_spark``, and the Installer read it to send
    those actions to a second Installer constructed inside a Fabric session.
    They now reach for the capability their work needs — storage, REST, TDS, or
    the Session's Spark SQL — and the Session knows what that means where it is.
    So there is no class of action that travels differently, and an executor
    that started declaring one again would bring the routing back with it.
    """

    from weaver.build_bundle.executors import default_executors

    declaring = sorted(
        name
        for name, executor in default_executors().items()
        if hasattr(executor, "needs_spark")
    )

    assert not declaring, (
        "these executors declare where they have to run, which the Installer no "
        f"longer routes on: {declaring}"
    )
