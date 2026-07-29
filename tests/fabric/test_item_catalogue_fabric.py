"""Repository/item catalogue proof with Weaver running inside Fabric."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.fabric

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
        "from weaver import FabricWorkspace, ItemRef, WeaverItemId\n"
        "from weaver.build_bundle import (InstallationEnvironment, ItemBinding, "
        "ItemBindings, LakehouseBinding, build_uploaded_item_repository, "
        "effective_item_bindings)\n"
        "from weaver.catalogue.tables import CATALOGUE_TABLES, INSTALLATION, REGISTRY\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.spark import SparkCatalogue\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "root = resolver.weaver_items_root\n"
        "store.make_directory(root)\n"
        "control = LakehouseBinding(ItemRef(workspace.weaver_lakehouse))\n"
        "result = build_uploaded_item_repository(\n"
        "    root,\n"
        "    bindings=ItemBindings((ItemBinding(\n"
        "        WeaverItemId.parse('Lakehouse/_weaver'), control),)),\n"
        "    environment=InstallationEnvironment(\n"
        "        store=store, resolver=resolver, spark=spark, workspace=workspace),\n"
        "    prune=False, control_lakehouse=control)\n"
        "report = result.report\n"
        "catalogue = SparkCatalogue(spark, resolver.spark_destination(\n"
        "    ItemRef(workspace.weaver_lakehouse)))\n"
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
        "            resolver.tables_root(ItemRef(workspace.weaver_lakehouse)) / '_')\n"
        "        if entry.name.casefold() != 'schema.json.gz'\n"
        "    ),\n"
        "    'target_names': [row['target_name'] for row in installation],\n"
        "    'registry_count': registry_count,\n"
        "    'table_count': len(CATALOGUE_TABLES),\n"
        "})\n"
    )

    payload = livy_session.run(body).payload
    assert payload["status"] == "succeeded", payload["errors"]
    assert payload["tables"] == payload["expected"]
    assert {name.casefold() for name in payload["physical_tables"]} == {
        name.casefold() for name in payload["expected"]
    }
    assert payload["target_names"] == [fabric_workspace.weaver_lakehouse]
    assert payload["registry_count"] == payload["table_count"]
    assert payload["version"]


def test_item_build_prunes_and_full_lakehouse_wipe_clears_both_areas(
    livy_session, fabric_workspace, fabric_target_lakehouse
):
    body = (
        "from weaver import (FabricWorkspace, ItemRef, WeaverItemId, "
        "wipe_lakehouse)\n"
        "from weaver.build_bundle import (InstallationEnvironment, ItemBinding, "
        "ItemBindings, LakehouseBinding, build_uploaded_item_repository, "
        "effective_item_bindings)\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.spark import SparkCatalogue\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        f"target = ItemRef({fabric_target_lakehouse.name!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "root = resolver.weaver_items_root\n"
        "files = {\n"
        f"    'Lakehouse/Domain/schemas/Sales.yml': {_SCHEMA.encode()!r},\n"
        f"    'Lakehouse/Domain/Sales.Customer.sql': {_TABLE.encode()!r},\n"
        f"    'Lakehouse/Domain/Files/Sales__Customer.py': {_FOLDER.encode()!r},\n"
        "}\n"
        "for relative, data in files.items():\n"
        "    store.write(root.join(*relative.split('/')), data)\n"
        "catalogue = SparkCatalogue(spark, resolver.spark_destination(target))\n"
        "catalogue.create_schema('Sales')\n"
        "catalogue.sql(\"CREATE TABLE {{object:Sales.Gworkspace}} (`Id` string) USING delta\")\n"
        "old_folder = resolver.files_root(target) / 'Sales' / 'OldFolder'\n"
        "store.make_directory(old_folder)\n"
        "binding = LakehouseBinding(target)\n"
        "control = LakehouseBinding(ItemRef(workspace.weaver_lakehouse))\n"
        "selected = ItemBindings((ItemBinding(\n"
        "    WeaverItemId.parse('Lakehouse/Domain'), binding),))\n"
        "result = build_uploaded_item_repository(\n"
        "    root, bindings=effective_item_bindings(\n"
        "        selected, weaver_lakehouse=workspace.weaver_lakehouse),\n"
        "    environment=InstallationEnvironment(\n"
        "        store=store, resolver=resolver, spark=spark, workspace=workspace),\n"
        "    prune=True, control_lakehouse=control)\n"
        "report = result.report\n"
        "tables_after_build = sorted(name.lower() for name in catalogue.tables('Sales'))\n"
        "customer_folder = resolver.files_root(target) / 'Sales' / 'Customer'\n"
        "folders_after_build = {\n"
        "    'customer': store.exists(customer_folder),\n"
        "    'old': store.exists(old_folder),\n"
        "}\n"
        "wipe_reports = wipe_lakehouse(target, workspace, store=store)\n"
        "remaining = {\n"
        "    'Files': [entry.name for entry in store.list(resolver.files_root(target))],\n"
        "    'Tables': [entry.name for entry in store.list(resolver.tables_root(target))],\n"
        "}\n"
        "emit({\n"
        "    'status': report.status,\n"
        "    'errors': [\n"
        "        {'id': action.action_id, 'type': action.error_type, "
        "         'message': action.error_message}\n"
        "        for action in report.action_results() if action.status == 'failed'],\n"
        "    'tables_after_build': tables_after_build,\n"
        "    'folders_after_build': folders_after_build,\n"
        "    'wipe_targets': sorted(item.target.split(':', 1)[0] "
        "                           for item in wipe_reports),\n"
        "    'remaining': remaining,\n"
        "})\n"
    )

    payload = livy_session.run(body).payload
    assert payload["status"] == "succeeded", payload["errors"]
    assert payload["tables_after_build"] == ["customer"]
    assert payload["folders_after_build"] == {"customer": True, "old": False}
    assert payload["wipe_targets"] == ["delta", "folder"]
    assert payload["remaining"] == {"Files": [], "Tables": []}
