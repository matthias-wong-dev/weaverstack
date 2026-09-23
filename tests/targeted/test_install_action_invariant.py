"""An inventory of which Fabric test exercises each InstallAction kind.

Legible from the terminal:

```text
pytest --collect-only -q tests/targeted/test_install_action_invariant.py
```

Each covered kind names a Fabric test whose build emits that kind and which
inspects what it made. A kind with no such test is listed as uncovered with
the reason and its local coverage. A new kind fails here until it is placed.

This is an inventory, not execution evidence: it checks that a named test
exists, not that it runs the kind or passes.
"""

from __future__ import annotations

import pathlib
import re

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle import models

REPOSITORY = pathlib.Path(__file__).resolve().parents[2]
TESTS = REPOSITORY / "tests"

#: Kinds no Fabric test runs and inspects, with where they are covered locally.
UNCOVERED = {
    "delete_file": (
        "emitted only for a deployed module nothing claims, and no Fabric test "
        "removes a declaration; the executor is in test_actions_delta_install.py"
    ),
    "drop_procedure": (
        "emitted only for a procedure nothing claims, and no Fabric test removes "
        "a declaration; planning is in test_load_plan_install.py"
    ),
    "build_folder": (
        "the acceptance journey builds folders and reads them only through a "
        "load; the executor is in test_folder_executor_boundary.py"
    ),
    "drop_folder": (
        "no Fabric test changes an owned folder; the executor is in "
        "test_folder_executor_boundary.py"
    ),
}

#: Kinds that change a target, and a Fabric test that runs and inspects each.
#: ``drop_table`` is proved on the Lakehouse side only.
COVERED = {
    "create_schema": ("test_a_built_warehouse_reads_back_as_the_fixture_predicts",),
    "build_table": ("test_a_built_table_uses_the_declared_types",),
    "build_view": ("test_a_built_warehouse_reads_back_as_the_fixture_predicts",),
    "create_shortcut": (
        "test_the_shortcut_exists_as_a_onelake_shortcut",
        "test_a_warehouse_shortcut_is_a_view_over_the_bound_lakehouse",
    ),
    "prune_table": ("test_prune_table_action_removes_an_object_nothing_declares",),
    "prune_view": ("test_prune_table_action_removes_an_object_nothing_declares",),
    "prune_schema": ("test_prune_table_action_removes_an_object_nothing_declares",),
    "prune_folder": ("test_prune_table_action_removes_an_object_nothing_declares",),
    "refresh_sql_endpoint": (
        "test_each_mutated_lakehouse_had_its_endpoint_refreshed_for_real",
    ),
    "write_file": ("test_a_build_here_rewrites_this_items_runtime_module_alone",),
    "build_procedure": (
        "test_the_source_load_runs_and_settles_what_the_mirror_will_carry",
    ),
    "drop_table": ("test_a_declaration_change_rebuilds_exactly_what_it_must",),
    "drop_view": ("test_the_changed_object_becomes_a_local_table",),
    "drop_shortcut": ("test_the_changed_object_stops_being_a_shortcut",),
}

#: Kinds that write the catalogue rather than the estate. They are covered as a
#: catalogue round trip rather than per action, because what matters about them
#: is the rows they leave, not the statement that left them.
CATALOGUE_KINDS = frozenset(
    {
        "delete_catalogue_claims",
        "publish_catalogue",
        "publish_registry",
        "reconcile_runtime_state",
    }
)


def declared_kinds() -> set[str]:
    """Every action kind the product defines, read from the product.

    Taken from the module rather than listed here, which is what makes this a
    tripwire: a new kind arrives on its own and has to be placed.

    Omission reasons share the shape, lower-case strings on the same module,
    and are not actions, so they are subtracted from their own declared set
    rather than by guessing at their names.
    """

    return {
        value
        for name, value in vars(models).items()
        if name.isupper() and isinstance(value, str) and re.fullmatch(r"[a-z_]+", value)
    } - set(models.OMISSION_REASONS)


@weaver_test()
def test_the_product_defines_action_kinds_to_check():
    """Guard the guard: reflection that found nothing would pass everything."""

    assert len(declared_kinds()) > 10


@weaver_test()
def test_every_action_kind_is_covered_or_deliberately_deferred():
    """The checklist itself. A new kind must be placed before this passes."""

    placed = set(COVERED) | set(UNCOVERED) | CATALOGUE_KINDS
    unplaced = declared_kinds() - placed

    assert not unplaced, (
        "these action kinds are neither covered by a Fabric test nor listed as "
        f"uncovered: {sorted(unplaced)}"
    )


@weaver_test()
def test_no_kind_is_both_covered_and_uncovered():
    assert not set(COVERED) & set(UNCOVERED)


@pytest.mark.parametrize(
    ("kind", "test_name"),
    sorted((kind, name) for kind, names in COVERED.items() for name in names),
)
@weaver_test()
def test_the_named_execution_test_exists(kind: str, test_name: str):
    """The list points at something real.

    Renaming a test without updating the checklist would otherwise leave the list
    describing an estate nobody checks, the failure mode a written list has and
    a pattern match does not.
    """

    pattern = re.compile(rf"^def {re.escape(test_name)}\(", re.MULTILINE)
    found = [
        path.relative_to(REPOSITORY)
        for path in TESTS.rglob("test_*.py")
        if pattern.search(path.read_text(encoding="utf-8"))
    ]

    assert found, f"{kind}: no test named {test_name} exists"


@weaver_test()
def test_no_executor_declares_where_it_has_to_run():
    """Every build action runs in the Installer, in whichever position that is.

    Executors used to declare ``needs_spark``, and the Installer read it to send
    those actions to a second Installer constructed inside a Fabric session.
    They now reach for the capability their work needs, storage, REST, TDS, or
    the Session's Spark SQL, and the Session knows what that means where it is.
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
