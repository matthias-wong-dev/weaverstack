"""The ordinary build owns the package catalogue Item lifecycle."""

from __future__ import annotations

from pathlib import Path

import pytest
from factories import (
    item_bindings,
    lakehouse_table,
    single_document_repository,
    target_inventory,
)
from support.sessions import given_session
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE, given_resolver, given_workspace

from weaver.build_bundle import (
    ItemBinding,
    ItemBindings,
    WarehouseBinding,
    build_item_repository,
    effective_item_bindings,
)
from weaver.build_bundle.workflow import BuildState
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import CATALOGUE_TABLES
from weaver.declaration.model import WeaverItemId
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

BUILTIN = WeaverItemId.parse("Warehouse/_weaver")


class RecordingExecutor:
    def __init__(self, name: str) -> None:
        self.name = name
        self.seen: list[str] = []

    def execute(self, action, payload, context):
        self.seen.append(action.id)
        return {"executor": self.name}


@pytest.fixture
def estate(tmp_path):
    """One authored item, and an empty catalogue Warehouse with no `_` at all."""

    root = tmp_path / "repo"
    repository = single_document_repository(
        root,
        item="Lakehouse/Sales",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    workspace = given_workspace(catalogue="Warehouse/Weaver")
    store = FilesystemStore()
    resolver = given_resolver(workspace=workspace, root=tmp_path)
    for item in ("Weaver", "Sales_LH"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))

    executors = {
        name: RecordingExecutor(name)
        for name in (
            "spark_sql",
            "spark_sql_batch",
            "spark_schema",
            "spark_table",
            "folder",
            "alias",
            "sql_endpoint_refresh",
            "tsql",
            "tsql_batch",
            "load_file",
        )
    }
    return {
        "repository": repository,
        "store": store,
        "executors": executors,
        "session": given_session(workspace=workspace, store=store, resolver=resolver),
    }


def _build(estate):
    """One ordinary build against an entirely empty catalogue."""

    bindings = effective_item_bindings(
        item_bindings(("Lakehouse/Sales", "Sales_LH")),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )
    inventories = {
        binding.item: target_inventory(
            target_id=binding.to_bound_target().id,
            target_name=binding.to_bound_target().name,
        )
        for binding in bindings.entries
    }
    result = build_item_repository(
        estate["repository"],
        bindings=bindings,
        # Nothing persisted, nothing present: the bootstrap state, stated as
        # state rather than arranged by a privileged step.
        state=BuildState(catalogue=Catalogue(rows={}), target_inventories=inventories),
        session=estate["session"],
        executors=estate["executors"],
        source_store=estate["store"],
        catalogue_binding=WarehouseBinding(
            warehouse=ItemRef("Weaver"), workspace_name=WORKSPACE
        ),
    )
    return bindings, result


# --- the built-in item is injected and bound, without being asked for ---------


@weaver_test()
def test_the_repository_carries_the_builtin_item_without_it_being_authored(estate):
    identities = {item.identity for item in estate["repository"].items}

    assert BUILTIN in identities


@weaver_test()
def test_the_builtin_item_is_bound_to_the_catalogue_warehouse_automatically():
    bindings = effective_item_bindings(
        item_bindings(("Lakehouse/Sales", "Sales_LH")),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )

    binding = bindings.by_item[BUILTIN]
    assert isinstance(binding.target, WarehouseBinding)
    assert binding.target.warehouse.name == "Weaver"


@weaver_test()
def test_the_catalogue_warehouse_can_also_host_an_authored_item():
    curated = ItemBinding(
        WeaverItemId.parse("Warehouse/Curated"),
        WarehouseBinding(ItemRef("Curated"), workspace_name=WORKSPACE),
    )

    bindings = effective_item_bindings(
        ItemBindings((curated,)),
        control_item=ItemRef("Curated"),
        workspace_name=WORKSPACE,
    )

    assert bindings.by_item[BUILTIN].target.item == curated.target.item
    assert set(bindings.by_item) == {BUILTIN, curated.item}


# --- and the ordinary planner creates the catalogue from nothing --------------


@weaver_test()
def test_an_empty_catalogue_plans_every_catalogue_table_as_an_ordinary_action(estate):
    """The acceptance criterion the nested bootstrap made untestable."""

    _bindings, result = _build(estate)

    built = {
        action.resource_node_id
        for _sequence, _batch, action in result.plan.actions()
        if action.resource_node_id
    }
    for table in CATALOGUE_TABLES:
        assert f"{BUILTIN}/{table.qualified}" in built, (
            f"{table.qualified} was not planned by the ordinary build"
        )


@weaver_test()
def test_the_catalogue_tables_are_built_by_the_same_executors_as_authored_objects(
    estate,
):
    """One installer for built-in and authored artefacts, or the claim is false."""

    _bindings, result = _build(estate)

    catalogue_actions = [
        action
        for _sequence, _batch, action in result.plan.actions()
        if action.resource_node_id and action.resource_node_id.startswith(f"{BUILTIN}/")
    ]
    assert catalogue_actions
    used = {action.executor for action in catalogue_actions}
    assert used <= set(estate["executors"]), (
        f"catalogue work reached an executor authored objects do not use: {used}"
    )


@weaver_test()
def test_the_build_succeeds_against_an_empty_lakehouse_with_no_prior_preparation(
    estate,
):
    _bindings, result = _build(estate)

    assert result.report.status == "succeeded"


# --- no build path reaches a second initialisation lifecycle ------------------


@weaver_test()
def test_no_build_module_reaches_the_initialisation_wrapper():
    """`initialise` is a compatibility shell; build must not route through it.

    Source-level because the failure it guards is a *reintroduced call*, and the
    cheapest honest statement of "build does not do this" is that the build
    modules do not name it. A behavioural version would have to stand up each of
    the three platform paths to prove a negative about all of them.
    """

    core = Path(__file__).resolve().parents[2] / "src" / "weaver"
    build_modules = [
        *sorted((core / "operations").rglob("*.py")),
        *sorted((core / "build_bundle").rglob("*.py")),
    ]

    offenders = [
        module.name
        for module in build_modules
        if "initialise_catalogue" in module.read_text(encoding="utf-8")
        or "prepare_catalogue" in module.read_text(encoding="utf-8")
    ]

    assert not offenders, (
        "these build modules call the initialisation wrapper; the catalogue is "
        f"created by the ordinary plan instead: {offenders}"
    )


@weaver_test()
def test_the_initialisation_wrapper_owns_no_catalogue_ddl_or_publication():
    """Thin means thin: it selects the built-in item and delegates."""

    source = (
        Path(__file__).resolve().parents[2] / "src" / "weaver" / "initialise.py"
    ).read_text(encoding="utf-8")

    for forbidden in (
        "CREATE TABLE",
        "MERGE INTO",
        "DELETE FROM",
        "generate_item_build_bundle",
        "Installer(",
        "read_reconciled_catalogue",
        "read_target_inventories",
    ):
        assert forbidden not in source, (
            f"initialise.py names {forbidden!r}; it must delegate the whole "
            "lifecycle to the ordinary build path"
        )
