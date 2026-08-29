"""Catalogue checks at the load boundary."""

from __future__ import annotations

from pathlib import Path

import pytest
from factories import (
    ITEM,
    installed_catalogue,
    item_bindings,
    load_estate,
    load_estate_bindings,
    single_document_repository,
)
from support.sessions import given_session
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient, given_workspace

from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.fabric.resolution import FabricResolver
from weaver.load_report import TASK_SUCCEEDED
from weaver.operations.load import run_load
from weaver.run import RunState
from weaver.store import FilesystemStore

MISTYPED = WeaverItemId.parse("Lakehouse/Rwa")
VIEWS = WeaverItemId.parse(ITEM)


class Refreshing(FabricResolver):
    def refresh_sql_endpoint(self, item):
        return None


def _session(tmp_path, *, items=("Weaver_LH", "Raw_LH")):
    workspace = given_workspace(catalogue="Warehouse/Weaver_LH")
    resolver = Refreshing(
        workspace,
        client=InventoryClient(
            workspace.workspace,
            [("Lakehouse", name) for name in items],
        ),
        base_url=Path(tmp_path).as_posix(),
    )
    return workspace, given_session(
        workspace=workspace, resolver=resolver, store=FilesystemStore()
    )


@weaver_test()
def test_an_item_the_catalogue_does_not_know_is_refused(tmp_path):
    """Named as the caller named it, and told where to look for the answer.

    A typo is refused as the logical item it was written as. The old phrasing
    named a physical target the caller never wrote.
    """

    repository = load_estate(tmp_path / "repository")
    catalogue = installed_catalogue(repository, load_estate_bindings())
    workspace, session = _session(tmp_path)

    with pytest.raises(CommandError, match="has no installation") as raised:
        run_load(
            session,
            workspace=workspace,
            state=RunState(catalogue=catalogue),
            items=(MISTYPED,),
            dry_run=True,
        )

    message = str(raised.value)
    assert "Lakehouse/Rwa" in message
    # Which catalogue was asked, and what it does hold.
    assert "Warehouse/Weaver_LH" in message
    assert "Lakehouse/Raw" in message


VIEW_ONLY = """/*
View ID: DWG.Nothing

Description: A view over a literal.

Lineage: Declared for a test.

Dependencies: []
*/
select 1 as CustomerId;
"""


@weaver_test()
def test_an_installed_item_with_no_load_work_is_a_successful_no_op(tmp_path):
    repository = single_document_repository(
        tmp_path / "views", documents={"DWG.Nothing.sql": VIEW_ONLY}
    )
    catalogue = installed_catalogue(repository, item_bindings((ITEM, "Views_LH")))
    workspace, session = _session(tmp_path, items=("Weaver_LH", "Views_LH"))

    report = run_load(
        session,
        workspace=workspace,
        state=RunState(catalogue=catalogue),
        items=(VIEWS,),
        fault_tolerant=False,
    )

    assert report.status == TASK_SUCCEEDED
    assert report.nodes == ()


@weaver_test()
def test_load_state_has_no_physical_inventory_api():
    assert not hasattr(RunState, "inventory")
    assert "target_inventories" not in RunState.__dataclass_fields__
