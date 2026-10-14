"""Hand-installed Warehouse fixtures publish their resolved physical identity."""

from fabric.sql_support import (
    PROCEDURE_ITEM,
    prepare_hand_installed,
    record_installation,
)
from support.weaver_test import weaver_test
from test_mirror_identity_boundary import ITEMS, WORKSPACE_ID, RecordedInventory

from weaver.catalogue.render import InstallationScope, render_merge
from weaver.catalogue.tables import INSTALLATION
from weaver.fabric.resolution import FabricResolver
from weaver.targets import ItemRef
from weaver.workspaces import Workspace


class RecordingSql:
    def __init__(self):
        self.scripts = []

    def execute_script(self, statement):
        self.scripts.append(statement)


@weaver_test()
def test_hand_installation_merges_all_required_columns_from_resolved_item():
    inventory = RecordedInventory()
    resolver = FabricResolver(Workspace(workspace="Analytics"), client=inventory)
    physical = resolver.resolve(ItemRef("Model"), item_type="Warehouse")
    executor = RecordingSql()
    record_installation(
        executor, physical_item=physical, workspace_name=resolver.workspace.name
    )
    expected = {
        "item_type": PROCEDURE_ITEM[0],
        "item_name": PROCEDURE_ITEM[1],
        "workspace_name": "Analytics",
        "workspace_id": WORKSPACE_ID,
        "target_name": "Model",
        "item_id": ITEMS[2][2],
        "weaver_version": "test",
        "signature": "",
    }
    assert executor.scripts[-1] == render_merge(
        INSTALLATION, [expected], scope=InstallationScope(*PROCEDURE_ITEM)
    )
    assert "db_name()" not in executor.scripts[-1]


@weaver_test()
def test_hand_installation_quotes_resolved_names_and_forwards_identity():
    from weaver.fabric.resources import Item

    executor = RecordingSql()
    physical = Item(ITEMS[2][2], "Model's ] Warehouse", "Warehouse", WORKSPACE_ID)
    prepare_hand_installed(
        executor,
        "Sales",
        "Weaver",
        physical_item=physical,
        workspace_name="Team's workspace",
    )
    merge = executor.scripts[-1]
    assert "Model''s ] Warehouse" in merge
    assert "Team''s workspace" in merge
    assert physical.id in merge and physical.workspace_id in merge
    assert "[Workspace GUID]" in merge and "[Target item GUID]" in merge
