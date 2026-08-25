"""Runtime references survive build publication into public load planning.

The physical primitive is doubled because the tenant-free suite cannot execute
T-SQL. Everything around it is production: repository preparation, wiped-state
build planning, catalogue projection, public ``weaver.load``, graph construction,
run recording and bookmark state carried into the second load.
"""

from __future__ import annotations

from datetime import datetime, timezone

from factories import (
    _write,
    installed_catalogue,
    item_bindings,
    lakehouse_table,
    schema_document,
    target_inventory,
    warehouse_table,
)
from support.sessions import given_session
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE, given_workspace

import weaver
from weaver.build_bundle import Builder, WarehouseBinding, effective_item_bindings
from weaver.build_bundle.workflow import BuildState
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    BOOKMARK_SENTINEL,
    SHORTCUT,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.load_report import TASK_SUCCEEDED, LoadResult
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

LANDING = WeaverItemId.parse("Lakehouse/Landing")
CURATED = WeaverItemId.parse("Warehouse/Curated")
CATALOGUE = WeaverItemId.parse("Warehouse/_weaver")
CUSTOMER = WeaverDocumentId.parse("Warehouse/Curated/Wh.Customer")
FIRST = datetime(2026, 8, 24, 1, 2, 3, tzinfo=timezone.utc)
SECOND = datetime(2026, 8, 24, 1, 3, 4, tzinfo=timezone.utc)


def _repository(root):
    _write(root, f"{LANDING}/schemas/Raw.yml", schema_document("Raw"))
    _write(
        root,
        f"{LANDING}/Raw__Customer.py",
        lakehouse_table("Raw.Customer"),
    )
    _write(root, f"{CURATED}/schemas/Wh.yml", schema_document("Wh"))
    _write(
        root,
        f"{CURATED}/Wh.Customer.sql",
        warehouse_table(
            "Wh.Customer",
            select="""declare @bookmark_datetime datetime2(6);

set @bookmark_datetime = (
    select [Bookmark datetime]
    from _.Bookmark
    where [Item type] = N'Warehouse'
      and [Item name] = N'Curated'
      and [Schema name] = N'Wh'
      and [Object name] = N'Customer'
);

select v.CustomerId
from (values
    (1, cast('2026-01-01' as datetime2(6))),
    (2, cast('2026-01-02' as datetime2(6)))
) as v (CustomerId, Modified)
where v.Modified > coalesce(
    @bookmark_datetime, cast('1900-01-01' as datetime2(6))
)""",
        ),
    )
    return parse_item_repository(Location(str(root)))


def _bindings():
    return effective_item_bindings(
        item_bindings((str(LANDING), "Landing"), (str(CURATED), "Curated")),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )


def _wiped_state(bindings):
    inventories = {}
    for binding in bindings.entries:
        target = binding.to_bound_target()
        inventories[binding.item] = target_inventory(
            target_id=target.id,
            kind=target.kind,
            target_name=target.name,
        )
    return BuildState(
        catalogue=Catalogue(rows={}),
        target_inventories=inventories,
    )


@weaver_test()
def test_public_load_reconstructs_a_built_runtime_reference_without_manual_dependencies(
    tmp_path, monkeypatch
):
    repository = _repository(tmp_path / "repository")
    bindings = _bindings()
    store = FilesystemStore()

    bundle = Builder(
        repository=repository,
        state=_wiped_state(bindings),
        bindings=bindings,
        catalogue_binding=WarehouseBinding(
            warehouse=ItemRef("Weaver"), workspace_name=WORKSPACE
        ),
        source_store=store,
    ).build(output=Location(str(tmp_path / "bundle")))

    procedure = next(
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.resource_node_id == "Warehouse/Curated/procedure:_/Load Wh.Customer"
    )
    generated = store.read(bundle.location.join(*procedure.payload.split("/"))).decode()
    assert "from _.Bookmark" in generated

    workspace = given_workspace(catalogue="Warehouse/Weaver")
    session = given_session(
        workspace=workspace,
        lakehouses=("Landing",),
        warehouses=("Weaver", "Curated"),
    )
    catalogue = installed_catalogue(repository, bindings, session=session)

    curated_rows = catalogue.rows[CURATED]
    bookmark_shortcut = next(
        row for row in curated_rows[SHORTCUT.name] if row["shortcut_id"] == "_.Bookmark"
    )
    assert bookmark_shortcut["target_item_name"] == "_weaver"
    assert (
        catalogue.registered[
            WeaverDocumentId.parse("Warehouse/Curated/_.Bookmark")
        ].object_role
        == "shortcut"
    )

    import weaver.run as run_module
    import weaver.run.state as state_module

    monkeypatch.setattr(
        state_module,
        "read_installed_catalogue",
        lambda **_asked: catalogue,
    )

    def dispatch(node, **_asked):
        if node.logical_id != CUSTOMER:
            return LoadResult(succeeded=True)
        first = catalogue.bookmark(CUSTOMER) == BOOKMARK_SENTINEL
        return LoadResult(
            succeeded=True,
            rows_read=2 if first else 0,
            rows_inserted=2 if first else 0,
            bookmark_datetime=FIRST if first else SECOND,
        )

    monkeypatch.setattr(run_module, "dispatch_primitive", dispatch)

    first = weaver.load(("Lakehouse/Landing", "Warehouse/Curated"), session=session)
    assert first.status == TASK_SUCCEEDED
    assert catalogue.bookmark(CUSTOMER) == FIRST

    second = weaver.load(("Lakehouse/Landing", "Warehouse/Curated"), session=session)
    curated = next(node for node in second.nodes if node.logical_id == str(CUSTOMER))
    assert second.status == TASK_SUCCEEDED
    assert curated.result.rows_read == 0
    assert catalogue.bookmark(CUSTOMER) == SECOND
