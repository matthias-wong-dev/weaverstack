"""The catalogue is built by the ordinary build, and by nothing else.

Weaver's own catalogue tables are Weaver objects. That claim is only worth
making if it is *load-bearing*: if the catalogue could also be created by a
privileged path that ran first, the ordinary path would never be exercised on an
empty estate and the claim would be decorative.

Build used to run a whole nested build before its own — `_ensure_control_plane`
called `initialise_weaver_lakehouse`, which read state, planned and installed a
bundle of its own, before the real build read anything. So every build built the
catalogue twice, and the second build's plan was never the one that created it.

These tests hold the seam shut from both sides: the ordinary planner really does
create the catalogue from nothing, and no build path reaches a second
initialisation lifecycle to do it for them.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from factories import (
    item_bindings,
    lakehouse_table,
    single_document_repository,
    target_inventory,
)

from weaver.build_bundle import (
    InstallationEnvironment,
    LakehouseBinding,
    build_item_repository,
    effective_item_bindings,
)
from weaver.catalogue.state import Catalogue, Reconciliation
from weaver.catalogue.tables import CATALOGUE_TABLES
from weaver.declaration.model import WeaverItemId
from weaver.resolution import LocalResolver
from weaver.store import FilesystemStore
from weaver.targets import ItemRef
from weaver.workspaces import LocalWorkspace

BUILTIN = WeaverItemId.parse("Lakehouse/_weaver")


class RecordingExecutor:
    def __init__(self, name: str) -> None:
        self.name = name
        self.seen: list[str] = []

    def execute(self, action, payload, context):
        self.seen.append(action.id)
        return {"executor": self.name}


@pytest.fixture
def estate(tmp_path):
    """One authored item, and an empty Weaver Lakehouse with no catalogue at all."""

    root = tmp_path / "repo"
    repository = single_document_repository(
        root,
        item="Lakehouse/Sales",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    workspace = LocalWorkspace(workspace=tmp_path / "ws", weaver_lakehouse="Weaver")
    store = FilesystemStore()
    resolver = LocalResolver(workspace)
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
        "environment": InstallationEnvironment(
            store=store,
            resolver=resolver,
            spark=None,
            workspace=workspace,
            executors=executors,
        ),
    }


def _build(estate):
    """One ordinary build against an entirely empty catalogue."""

    bindings = effective_item_bindings(
        item_bindings(("Lakehouse/Sales", "Sales_LH")),
        weaver_lakehouse="Weaver",
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
        target_inventories=inventories,
        # Nothing persisted, nothing present: the bootstrap state, stated as
        # state rather than arranged by a privileged step.
        reconciliation=Reconciliation(Catalogue(rows={}), stale_claims=()),
        environment=estate["environment"],
        source_store=estate["store"],
        control_lakehouse=LakehouseBinding(lakehouse=ItemRef("Weaver")),
    )
    return bindings, result


# --- the built-in item is injected and bound, without being asked for ---------


def test_the_repository_carries_the_builtin_item_without_it_being_authored(estate):
    identities = {item.identity for item in estate["repository"].items}

    assert BUILTIN in identities


def test_the_builtin_item_is_bound_to_the_control_lakehouse_automatically():
    bindings = effective_item_bindings(
        item_bindings(("Lakehouse/Sales", "Sales_LH")), weaver_lakehouse="Weaver"
    )

    binding = bindings.by_item[BUILTIN]
    assert isinstance(binding.target, LakehouseBinding)
    assert binding.target.lakehouse.name == "Weaver"


# --- and the ordinary planner creates the catalogue from nothing --------------


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


def test_the_catalogue_tables_are_built_by_the_same_executors_as_authored_objects(
    estate,
):
    """One installer for built-in and authored artefacts, or the claim is false."""

    _bindings, result = _build(estate)

    catalogue_actions = [
        action
        for _sequence, _batch, action in result.plan.actions()
        if action.resource_node_id
        and action.resource_node_id.startswith(f"{BUILTIN}/")
    ]
    assert catalogue_actions
    used = {action.executor for action in catalogue_actions}
    assert used <= set(estate["executors"]), (
        f"catalogue work reached an executor authored objects do not use: {used}"
    )


def test_the_build_succeeds_against_an_empty_lakehouse_with_no_prior_preparation(
    estate,
):
    _bindings, result = _build(estate)

    assert result.report.status == "succeeded"


# --- no build path reaches a second initialisation lifecycle ------------------


def test_no_build_module_reaches_the_initialisation_wrapper():
    """`initialise` is a compatibility shell; build must not route through it.

    Source-level because the failure it guards is a *reintroduced call*, and the
    cheapest honest statement of "build does not do this" is that the build
    modules do not name it. A behavioural version would have to stand up each of
    the three platform paths to prove a negative about all of them.
    """

    core = Path(__file__).resolve().parents[2] / "src" / "weaver"
    build_modules = [core / "operations.py", *sorted((core / "build_bundle").rglob("*.py"))]

    offenders = [
        module.name
        for module in build_modules
        if "initialise_weaver_lakehouse" in module.read_text(encoding="utf-8")
        or "prepare_weaver_lakehouse" in module.read_text(encoding="utf-8")
    ]

    assert not offenders, (
        "these build modules call the initialisation wrapper; the catalogue is "
        f"created by the ordinary plan instead: {offenders}"
    )


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
        "install_bundle",
        "read_reconciled_catalogue",
        "read_target_inventories",
    ):
        assert forbidden not in source, (
            f"initialise.py names {forbidden!r}; it must delegate the whole "
            "lifecycle to the ordinary build path"
        )
