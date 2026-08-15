"""Repository/item catalogue proof with Weaver running inside Fabric."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.fabric, pytest.mark.hosted]

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


def test_installed_weaver_builds_and_catalogues_its_builtin_item(
    livy_session, fabric_workspace
):
    """Discovery, generation, install and reads all happen in the Fabric session."""

    installation_filter = "item_type = 'Lakehouse' AND item_name = '_weaver'"
    registry_filter = installation_filter
    body = (
        "import weaver\n"
        "from weaver.workspaces import Workspace\n"
        "from weaver.targets import ItemRef\n"
        "from weaver.declaration.model import WeaverItemId\n"
        "from weaver.build_bundle import (ItemBinding, ItemBindings, "
        "LakehouseBinding, build_item_repository_source, "
        "effective_item_bindings)\n"
        "from weaver.sessions import NotebookSession\n"
        "from weaver.catalogue.tables import CATALOGUE_TABLES, INSTALLATION, REGISTRY\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.spark import SparkCatalogue\n"
        f"workspace = Workspace(workspace={fabric_workspace.workspace!r}, "
        f"catalogue={fabric_workspace.catalogue!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "root = resolver.weaver_items_root\n"
        "store.make_directory(root)\n"
        "control = LakehouseBinding(workspace.catalogue_item, "
        "workspace_name=workspace.workspace)\n"
        "result = build_item_repository_source(\n"
        "    root,\n"
        "    source_store=store,\n"
        "    bindings=ItemBindings((ItemBinding(\n"
        "        WeaverItemId.parse('Warehouse/_weaver'), control),)),\n"
        "    session=NotebookSession(workspace=workspace, spark=spark),\n"
        "    catalogue_binding=control)\n"
        "report = result.report\n"
        "catalogue = SparkCatalogue(spark, resolver.spark_destination(\n"
        "    workspace.catalogue_item))\n"
        "installation = spark.table(catalogue.qualify('_', INSTALLATION.name)).where(\n"
        f"    {installation_filter!r}).select('target_name').collect()\n"
        "registry_count = spark.table(catalogue.qualify('_', REGISTRY.name)).where(\n"
        f"    {registry_filter!r}).count()\n"
        "emit({\n"
        "    'version': weaver.__version__,\n"
        "    'status': report.status,\n"
        "    'errors': [\n"
        "        {'id': action.action_id, 'type': action.error_type, "
        "         'message': action.error_message}\n"
        "        for action in report.action_results() if action.status == 'failed'],\n"
        "    'tables': sorted(catalogue.tables('_')),\n"
        "    'expected': sorted(table.name for table in CATALOGUE_TABLES),\n"
        "    'physical_tables': sorted(\n"
        "        entry.name for entry in store.list(\n"
        "            resolver.tables_root(workspace.catalogue_item) / '_')\n"
        "        if entry.name.casefold() != 'schema.json.gz'\n"
        "    ),\n"
        "    'target_names': [row['target_name'] for row in installation],\n"
        "    'registry_count': registry_count,\n"
        "    'table_count': len(CATALOGUE_TABLES),\n"
        "})\n"
    )

    payload = livy_session.run(body, label="generate and install").payload
    assert payload["status"] == "succeeded", payload["errors"]
    assert payload["tables"] == payload["expected"]
    assert {name.casefold() for name in payload["physical_tables"]} == {
        name.casefold() for name in payload["expected"]
    }
    # The Installation row records the *item*, not the workspace's typed
    # catalogue value: what it says is which Lakehouse holds the catalogue.
    assert payload["target_names"] == [str(fabric_workspace.catalogue_item)]
    # Every catalogue table, plus the one Folder the control plane declares:
    # `_.Log`, the task log every top-level Weaver task writes beneath.
    assert payload["registry_count"] == payload["table_count"] + 1
    assert payload["version"]
