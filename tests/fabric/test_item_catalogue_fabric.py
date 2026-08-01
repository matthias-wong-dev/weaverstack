"""Repository/item catalogue proof with Weaver running inside Fabric."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.published_weaver

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
        "    control_lakehouse=control)\n"
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

    payload = livy_session.run(body, label="generate and install").payload
    assert payload["status"] == "succeeded", payload["errors"]
    assert payload["tables"] == payload["expected"]
    assert {name.casefold() for name in payload["physical_tables"]} == {
        name.casefold() for name in payload["expected"]
    }
    assert payload["target_names"] == [fabric_workspace.weaver_lakehouse]
    assert payload["registry_count"] == payload["table_count"]
    assert payload["version"]


def test_a_wipe_clears_both_onelake_areas(
    livy_session, fabric_workspace, fabric_target_lakehouse
):
    """What only OneLake can answer about a wipe: it clears Tables *and* Files.

    This used to run a whole generate-and-install to arrange a table and a
    folder, then prune, then wipe — a build paid for entirely to create state.
    The build is proven at the boundary, the prune decision is pure Python, and
    what is left is the wipe itself against real OneLake storage, which has two
    areas and a directory layout nothing local has to get right.

    So the state is seeded directly, in the same submission that wipes it.
    """

    body = (
        "from weaver import FabricWorkspace, ItemRef, wipe_lakehouse\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.spark import SparkCatalogue\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        f"target = ItemRef({fabric_target_lakehouse.name!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "catalogue = SparkCatalogue(spark, resolver.spark_destination(target))\n"
        "catalogue.create_schema('Sales')\n"
        "catalogue.sql(\"CREATE TABLE IF NOT EXISTS {{object:Sales.Customer}} "
        "(`Id` string) USING delta\")\n"
        "store.make_directory(resolver.files_root(target) / 'Sales' / 'Customer')\n"
        "seeded = {\n"
        "    'Files': [e.name for e in store.list(resolver.files_root(target))],\n"
        "    'Tables': [e.name for e in store.list(resolver.tables_root(target))],\n"
        "}\n"
        "wipe_reports = wipe_lakehouse(target, workspace, store=store)\n"
        "emit({\n"
        "    'seeded': seeded,\n"
        "    'wipe_targets': sorted(item.target.split(':', 1)[0] for item in wipe_reports),\n"
        "    'remaining': {\n"
        "        'Files': [e.name for e in store.list(resolver.files_root(target))],\n"
        "        'Tables': [e.name for e in store.list(resolver.tables_root(target))],\n"
        "    },\n"
        "})\n"
    )

    payload = livy_session.run(body, label="seed and wipe").payload

    # The seed has to have landed, or an empty wipe would pass for a working one.
    assert payload["seeded"]["Files"], payload["seeded"]
    assert payload["seeded"]["Tables"], payload["seeded"]

    assert payload["wipe_targets"] == ["delta", "folder"]
    assert payload["remaining"] == {"Files": [], "Tables": []}
