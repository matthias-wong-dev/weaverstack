"""Repository/item catalogue proof with Weaver running inside Fabric."""

from __future__ import annotations

from support.weaver_test import weaver_test

_SCHEMA = """Schema ID: Sales
Description: Sales objects for the item prune proof.
"""

_TABLE = """/*
Table ID: Sales.Customer
Description: One row per customer.
Lineage: A declared source.
Dependencies: []
Primary key: Id
Schema:
  Id: string
*/
select cast(null as string) as Id where 1 = 0
"""

_FOLDER = '''\
"""
Folder ID: Sales.Customer
Description: Customer source files.
Lineage: A declared source.
File key: "*.csv"
"""
from weaver import Folder

class Sales__Customer(Folder):
    def read(self):
        return self.staging_folder(), []
'''


@weaver_test(hosted=True)
def test_installed_weaver_builds_and_catalogues_its_builtin_item(
    livy_session, fabric_workspace, fabric_target_lakehouse
):
    """Discovery, generation, install and reads all happen in the Fabric session."""

    body = (
        "import weaver\n"
        "from weaver.workspaces import Workspace\n"
        "from weaver.targets import ItemRef\n"
        "from weaver.declaration.model import WeaverItemId\n"
        "from weaver.build_bundle import (ItemBinding, ItemBindings, "
        "WarehouseBinding, build_item_repository_source, "
        "effective_item_bindings)\n"
        "from weaver.sessions import NotebookSession\n"
        "from weaver.catalogue.tables import CATALOGUE_TABLES\n"
        "from weaver.catalogue.connection import catalogue_connection\n"
        "from weaver.resolution import resolver_for, store_for\n"
        f"workspace = Workspace(workspace={fabric_workspace.workspace!r}, "
        f"catalogue={fabric_workspace.catalogue!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        f"root = resolver.files_root(ItemRef({fabric_target_lakehouse.name!r}))"
        " / 'weaver_items'\n"
        "store.make_directory(root)\n"
        "control = WarehouseBinding(workspace.catalogue_item, "
        "workspace_name=workspace.workspace)\n"
        "session = NotebookSession(workspace=workspace, spark=spark)\n"
        "result = build_item_repository_source(\n"
        "    root,\n"
        "    source_store=store,\n"
        "    bindings=ItemBindings((ItemBinding(\n"
        "        WeaverItemId.parse('Warehouse/_weaver'), control),)),\n"
        "    session=session,\n"
        "    catalogue_binding=control)\n"
        "report = result.report\n"
        # The catalogue is a Warehouse, so it is read back over TDS, in this
        # session, on its own identity, exactly as the build wrote it.
        "catalogue = catalogue_connection(session, workspace)\n"
        "catalogue.forget_shape()\n"
        "installation = catalogue.rows(\n"
        '    "select [Target name] from [_].[Installation] "\n'
        "    \"where [Item type] = N'Warehouse' and [Item name] = N'_weaver'\")\n"
        "registry = catalogue.rows(\n"
        '    "select count(*) as n from [_].[Registry] "\n'
        "    \"where [Item type] = N'Warehouse' and [Item name] = N'_weaver'\")\n"
        "emit({\n"
        "    'version': weaver.__version__,\n"
        "    'status': report.status,\n"
        "    'errors': [\n"
        "        {'id': action.action_id, 'type': action.error_type, "
        "         'message': action.error_message}\n"
        "        for action in report.action_results() if action.status == 'failed'],\n"
        "    'tables': sorted(catalogue.shape()),\n"
        "    'expected': sorted(\n"
        "        table.name.casefold() for table in CATALOGUE_TABLES),\n"
        "    'target_names': [dict(row)['Target name'] for row in installation],\n"
        "    'registry_count': dict(registry[0])['n'],\n"
        "    'table_count': len(CATALOGUE_TABLES),\n"
        "})\n"
    )

    payload = livy_session.run(body).payload
    assert payload["status"] == "succeeded", payload["errors"]
    # Every catalogue table, physically present in the Warehouse.
    assert payload["tables"] == payload["expected"]
    # The Installation row records the item: which Warehouse holds `_`.
    assert payload["target_names"] == [str(fabric_workspace.catalogue_item)]
    # One Registry row per catalogue table it built.
    assert payload["registry_count"] == payload["table_count"]
    assert payload["version"]
