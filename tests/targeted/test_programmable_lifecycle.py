"""The Programmable lifecycle, planned by the ordinary build machinery.

A stored procedure an item authors is managed repository content with the same
create, change, fixed-point and remove lifecycle as anything else Weaver
installs. These tests plan real bundles against real catalogue and inventory
state; nothing here models what an installer would do.
"""

from __future__ import annotations

import pytest
from factories import WAREHOUSE_ITEM, FixtureCatalogue, FixtureInventory
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.catalogue.state import Catalogue
from weaver.declaration import parse_item_repository
from weaver.declaration.metadata import SQL_TARGET
from weaver.declaration.model import ObjectId, WeaverDocumentId, WeaverItemId
from weaver.errors import DiscoveryError
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

ITEM = WeaverItemId.parse(WAREHOUSE_ITEM)
CATALOGUE_BINDING = WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE)

from factories import warehouse_table

TABLE_SQL = warehouse_table("DWG.Customer")
PROCEDURE_V1 = (
    "create or alter procedure dbo.RefreshSummary\nas\nbegin\n    select 1;\nend;\n"
)
PROCEDURE_V2 = (
    "create or alter procedure dbo.RefreshSummary\nas\nbegin\n    select 2;\nend;\n"
)


def _write(root, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _repository(
    tmp_path,
    *,
    procedure: str | None,
    name: str = "repo",
) -> object:
    root = tmp_path / name
    _write(root, f"{WAREHOUSE_ITEM}/schemas/dbo.yml", "Schema ID: dbo\nDescription: dbo.\n")
    _write(root, f"{WAREHOUSE_ITEM}/schemas/DWG.yml", "Schema ID: DWG\nDescription: DWG.\n")
    _write(root, f"{WAREHOUSE_ITEM}/DWG.Customer.sql", TABLE_SQL)
    if procedure is not None:
        _write(root, f"{WAREHOUSE_ITEM}/programmables/dbo.RefreshSummary.sql", procedure)
    return parse_item_repository(Location(str(root)))


def _identity() -> WeaverDocumentId:
    return WeaverDocumentId(ITEM, ObjectId(schema="dbo", object="RefreshSummary"), shape="procedure")


def _plan(repository, tmp_path, *, catalogue):
    """One bundle for this repository against the given catalogue state."""

    bindings = _bindings()
    target = next(iter(bindings.by_item.values())).to_bound_target()
    inventory = FixtureInventory.from_repository(
        repository,
        item=WAREHOUSE_ITEM,
        target_kind=SQL_TARGET,
        target_id=target.id,
        kind="warehouse",
        target_name=target.name,
    )
    return generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories={ITEM: inventory},
        catalogue=catalogue,
        catalogue_binding=CATALOGUE_BINDING,
    )


def _bindings():
    from factories import item_bindings

    return item_bindings((WAREHOUSE_ITEM, "Reporting_WH"))


def _actions(bundle, *kinds: str) -> list:
    return [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind in kinds
    ]


# --- discovery -----------------------------------------------------------------


@weaver_test()
def test_a_programmable_is_discovered_with_its_procedure_identity(tmp_path):
    repository = _repository(tmp_path, procedure=PROCEDURE_V1)

    assert list(repository.programmables) == [_identity()]
    programmable = repository.programmables[_identity()]
    assert programmable.role == "programmable"
    assert programmable.relative_path == f"{WAREHOUSE_ITEM}/programmables/dbo.RefreshSummary.sql"


@weaver_test()
def test_the_statement_and_the_filename_must_agree(tmp_path):
    root = tmp_path / "repo"
    _write(root, f"{WAREHOUSE_ITEM}/schemas/DWG.yml", "Schema ID: DWG\nDescription: x\n")
    _write(root, f"{WAREHOUSE_ITEM}/DWG.Customer.sql", TABLE_SQL)
    _write(
        root,
        f"{WAREHOUSE_ITEM}/programmables/dbo.RefreshSummary.sql",
        "create or alter procedure dbo.SomethingElse\nas\nbegin\nend;",
    )

    with pytest.raises(DiscoveryError, match="must agree"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_a_plain_create_is_refused(tmp_path):
    """Replace needs create-or-alter: the installer runs the text verbatim."""

    root = tmp_path / "repo"
    _write(root, f"{WAREHOUSE_ITEM}/schemas/DWG.yml", "Schema ID: DWG\nDescription: x\n")
    _write(root, f"{WAREHOUSE_ITEM}/DWG.Customer.sql", TABLE_SQL)
    _write(
        root,
        f"{WAREHOUSE_ITEM}/programmables/dbo.RefreshSummary.sql",
        "create procedure dbo.RefreshSummary\nas\nbegin\nend;",
    )

    with pytest.raises(DiscoveryError, match="create or alter"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_an_authored_programmable_may_not_claim_weavers_schema(tmp_path):
    root = tmp_path / "repo"
    _write(root, f"{WAREHOUSE_ITEM}/schemas/DWG.yml", "Schema ID: DWG\nDescription: x\n")
    _write(root, f"{WAREHOUSE_ITEM}/DWG.Customer.sql", TABLE_SQL)
    _write(
        root,
        f"{WAREHOUSE_ITEM}/programmables/_.Load.sql",
        "create or alter procedure _.Load\nas\nbegin\nend;",
    )

    with pytest.raises(DiscoveryError, match="reserved"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_a_lakehouse_item_has_no_programmables_directory(tmp_path):
    from factories import ITEM as LAKEHOUSE_ITEM

    root = tmp_path / "repo"
    _write(root, f"{LAKEHOUSE_ITEM}/schemas/DWG.yml", "Schema ID: DWG\nDescription: x\n")
    _write(root, f"{LAKEHOUSE_ITEM}/DWG__Customer.py", '"""\nTable ID: DWG.Customer\n"""\n')
    _write(
        root,
        f"{LAKEHOUSE_ITEM}/programmables/dbo.Refresh.sql",
        "create or alter procedure dbo.Refresh\nas\nbegin\nend;",
    )

    with pytest.raises(DiscoveryError):
        parse_item_repository(Location(str(root)))


# --- the lifecycle ---------------------------------------------------------------


@weaver_test()
def test_a_new_programmable_is_created(tmp_path):
    repository = _repository(tmp_path, procedure=PROCEDURE_V1)

    bundle = _plan(repository, tmp_path, catalogue=Catalogue({}))

    # The item's generated procedures ride in the same stage; this item's own
    # declaration is what this test claims, so it is picked out by identity.
    created = [
        action
        for action in _actions(bundle, "build_procedure")
        if action.resource_node_id == str(_identity())
    ]
    assert [action.resource_node_id for action in created] == [str(_identity())]


@weaver_test()
def test_an_unchanged_programmable_plans_no_work(tmp_path):
    repository = _repository(tmp_path, procedure=PROCEDURE_V1)

    correct = Catalogue.from_repository(repository)
    bundle = _plan(repository, tmp_path, catalogue=correct)

    assert _actions(bundle, "build_procedure", "drop_procedure") == []


@weaver_test()
def test_a_changed_programmable_is_replaced(tmp_path):
    old = _repository(tmp_path, procedure=PROCEDURE_V1, name="old")
    new = _repository(tmp_path, procedure=PROCEDURE_V2, name="new")

    bundle = _plan(new, tmp_path, catalogue=Catalogue.from_repository(old))

    replaced = _actions(bundle, "build_procedure")
    assert [action.resource_node_id for action in replaced] == [str(_identity())]


@weaver_test()
def test_a_removed_programmable_is_dropped(tmp_path):
    before = _repository(tmp_path, procedure=PROCEDURE_V1, name="before")
    after = _repository(tmp_path, procedure=None, name="after")

    bundle = _plan(after, tmp_path, catalogue=Catalogue.from_repository(before))

    dropped = _actions(bundle, "drop_procedure")
    assert [action.resource_node_id for action in dropped] == [str(_identity())]


@weaver_test()
def test_the_registry_records_what_a_programmable_is_for(tmp_path):
    """Its own role, not one inferred from its physical shape."""

    from weaver.etl import item_runtime_artefacts

    repository = _repository(tmp_path, procedure=PROCEDURE_V1)
    artefacts = {
        str(artefact.identity): artefact
        for artefact in item_runtime_artefacts(repository, item=ITEM)
    }
    assert artefacts[str(_identity())].role == "programmable"
    assert artefacts[str(_identity())].source_path.endswith("dbo.RefreshSummary.sql")


__all__: tuple = ()
