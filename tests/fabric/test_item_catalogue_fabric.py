"""Repository/item catalogue proof with Weaver running inside Fabric."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.fabric


def test_installed_weaver_builds_and_catalogues_its_builtin_item(
    livy_session, fabric_host
):
    """Discovery, generation, install and reads all happen in the Fabric session."""

    installation_filter = (
        "repository = 'ItemCatalogueProbe' AND item_type = 'Lakehouse' "
        "AND item_name = '_weaver'"
    )
    registry_filter = installation_filter + " AND object_namespace = 'Tables'"
    body = (
        "from weaver import FabricHost, ItemRef, RepositoryRef, WeaverItemId\n"
        "from weaver.build_bundle import (InstallationEnvironment, ItemBinding, "
        "ItemBindings, LakehouseBinding, generate_item_build_bundle, install_bundle)\n"
        "from weaver.catalogue.item_tables import CATALOGUE_TABLES, INSTALLATION, REGISTRY\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.ses import read_weaver_repository\n"
        "from weaver.spark import SparkCatalogue\n"
        f"host = FabricHost(workspace={fabric_host.workspace!r}, "
        f"weaver_lakehouse={fabric_host.weaver_lakehouse!r}, "
        f"fabric_environment={fabric_host.fabric_environment!r})\n"
        "store = store_for(host)\n"
        "resolver = resolver_for(host)\n"
        "root = resolver.repository(RepositoryRef('ItemCatalogueProbe'))\n"
        "store.make_directory(root)\n"
        "repository = read_weaver_repository(root, store=store)\n"
        "control = LakehouseBinding(ItemRef(host.weaver_lakehouse))\n"
        "bundle = generate_item_build_bundle(\n"
        "    repository,\n"
        "    bindings=ItemBindings((ItemBinding(\n"
        "        WeaverItemId.parse('Lakehouse/_weaver'), control),)),\n"
        "    output=resolver.build_bundle('item-catalogue-probe'),\n"
        "    store=store, prune=False, catalogue=True, control_lakehouse=control)\n"
        "report = install_bundle(bundle, environment=InstallationEnvironment(\n"
        "    store=store, resolver=resolver, spark=spark))\n"
        "catalogue = SparkCatalogue(spark, resolver.spark_destination(\n"
        "    ItemRef(host.weaver_lakehouse)))\n"
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
        "    'tables': sorted(name.lower() for name in catalogue.tables('_')),\n"
        "    'expected': sorted(table.name.lower() for table in CATALOGUE_TABLES),\n"
        "    'target_names': [row['target_name'] for row in installation],\n"
        "    'registry_count': registry_count,\n"
        "    'table_count': len(CATALOGUE_TABLES),\n"
        "})\n"
    )

    payload = livy_session.run(body).payload
    assert payload["status"] == "succeeded", payload["errors"]
    assert payload["tables"] == payload["expected"]
    assert payload["target_names"] == [fabric_host.weaver_lakehouse]
    assert payload["registry_count"] == payload["table_count"]
    assert payload["version"]
