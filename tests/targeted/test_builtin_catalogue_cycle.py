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
    Builder,
    ItemBinding,
    ItemBindings,
    WarehouseBinding,
    build_item_repository,
    effective_item_bindings,
)
from weaver.build_bundle.models import BUILD_TABLE, DROP_TABLE
from weaver.build_bundle.workflow import BuildState
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import PROJECTED_TABLES
from weaver.declaration.model import WeaverItemId
from weaver.locations import Location
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
        documents={"Tables/DWG__Customer.py": lakehouse_table("DWG.Customer")},
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
            "shortcut",
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


# --- the built-in item is composed in and bound, without being asked for ------


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
    for table in PROJECTED_TABLES:
        assert f"{BUILTIN}/{table.qualified}" in built, (
            f"{table.qualified} was not planned by the ordinary build"
        )


@weaver_test()
def test_empty_registry_recovers_existing_protected_catalogue_tables(estate, tmp_path):
    """Inventory protects the physical catalogue even when Registry is empty."""

    repository = estate["repository"]
    bindings = effective_item_bindings(
        item_bindings(("Lakehouse/Sales", "Sales_LH")),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )
    bound = {entry.item: entry.to_bound_target() for entry in bindings.entries}
    catalogue_tables = tuple(
        sorted(
            identity.object_id.qualified
            for identity in repository.source_documents
            if identity.item == BUILTIN
        )
    )
    missing = catalogue_tables[-1]
    inventories = {
        item: target_inventory(
            target_id=target.id,
            kind=target.kind,
            target_name=target.name,
            tables=tuple(name for name in catalogue_tables if name != missing)
            if item == BUILTIN
            else (),
            schemas=("_",) if item == BUILTIN else (),
        )
        for item, target in bound.items()
    }
    bundle = Builder(
        repository=repository,
        state=BuildState(catalogue=Catalogue(rows={}), target_inventories=inventories),
        bindings=bindings,
        catalogue_binding=WarehouseBinding(
            warehouse=ItemRef("Weaver"), workspace_name=WORKSPACE
        ),
        source_store=estate["store"],
    ).build(output=Location(str(tmp_path / "recovery-bundle")))

    physical = [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.resource_node_id
        and action.resource_node_id.startswith(f"{BUILTIN}/")
        and action.kind in {DROP_TABLE, BUILD_TABLE}
    ]
    assert [action.resource_node_id for action in physical] == [f"{BUILTIN}/{missing}"]
    retained = {
        identity
        for identity in bundle.plan.selection.prohibited
        if identity.item == BUILTIN
    }
    assert {identity.object_id.qualified for identity in retained} == set(
        catalogue_tables
    ) - {missing}

    registry = next(
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "publish_registry"
    )
    payload = (
        estate["store"]
        .read(bundle.location.join(*registry.payload.split("/")))
        .decode()
    )
    assert all(
        f"N'_', N'{name.split('.', 1)[1]}'" in payload for name in catalogue_tables
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


# --- setting a project up owns no catalogue lifecycle -------------------------


@weaver_test()
def test_initialise_owns_no_catalogue_ddl_or_publication():
    """`weaver initialise` provisions the Warehouse; the build fills it.

    Source-level, because what it guards is a reintroduced shortcut, and the
    cheapest honest statement of "initialise does not do this" is that its module
    does not name it.
    """

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
            f"initialise.py names {forbidden!r}; the catalogue tables are the "
            "ordinary build's, and setting a project up creates the Warehouse "
            "they live in"
        )
