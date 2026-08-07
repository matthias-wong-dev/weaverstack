"""The notebook case: an object with no ``lakehouse=``, in a real Fabric session.

Everything else about authored objects — identity from the class name, the paths
a table and a folder resolve to — is asserted against the estate in
``test_lakehouse_journey``, where it costs nothing extra. This one cannot join it:
inference reads the Lakehouse the session has attached, only Fabric attaches one,
and it needs no built estate at all.
"""

from __future__ import annotations

import pytest

from weaver.targets import ItemRef

pytestmark = [pytest.mark.fabric, pytest.mark.hosted]


def test_an_object_resolves_the_lakehouse_the_session_attached(
    livy_session, fabric_workspace, fabric_client
):
    """``Sales__Order(spark)`` — no destination named anywhere, as in a notebook.

    A Livy session is opened beneath the Weaver Lakehouse, so that is the
    attachment, and it is what inference must produce: the same root the desktop
    resolver names over REST. If this fails, the session reports its attachment
    under keys other than the ones :func:`weaver.lakehouse.default_lakehouse`
    reads — which is the one thing about it that no local test can settle.

    Nothing here touches ``/lakehouse/default``. An inferred Lakehouse is reached
    exactly as a resolved one is, which is what keeps a notebook and a detached
    load running the same authored code.
    """

    from weaver.fabric import FabricResolver

    weaver_lakehouse = ItemRef(fabric_workspace.weaver_lakehouse)
    expected_root = FabricResolver(fabric_workspace, client=fabric_client).spark_root(
        weaver_lakehouse
    )

    payload = livy_session.run(
        "from weaver import Folder, Table, default_lakehouse\n"
        "class Sales__Order(Table):\n"
        "    def read(self):\n"
        "        return [], []\n"
        "class Sales__OrderExport(Folder):\n"
        "    def read(self):\n"
        "        return self.staging_folder(), []\n"
        "order = Sales__Order(spark)\n"
        "export = Sales__OrderExport(order)\n"
        "emit({\n"
        "  'name': order.lakehouse.name,\n"
        "  'spark_root': order.spark_root,\n"
        "  'table_path': order.lakehouse.table_path(*order.identity),\n"
        "  'folder_path': str(export.path()),\n"
        "  'folder_spark_path': export.spark_path(),\n"
        # The staging *path*, not an issued StagingFolder: one is only issued
        # inside a load, and nothing here runs one.
        "  'staging_path': str(export._staging_path()),\n"
        "  'inferred': default_lakehouse(spark).spark_root,\n"
        "})\n"
    ).payload

    assert payload["spark_root"] == expected_root
    assert payload["inferred"] == expected_root
    assert payload["table_path"] == f"{expected_root}/Tables/Sales/Order"
    # Spark gets the abfss:// form of the root inference produced.
    assert payload["folder_spark_path"] == f"{expected_root}/Files/Sales/OrderExport"
    # Python gets the mount of that same root — an inferred Lakehouse is reached
    # exactly as a resolved one is, which is what keeps a notebook and a detached
    # load running the same authored code.
    assert payload["folder_path"].startswith("/synfs/notebook/")
    assert payload["folder_path"].endswith("/Files/Sales/OrderExport")
    assert payload["staging_path"] == f"{payload['folder_path']}_Staging"
