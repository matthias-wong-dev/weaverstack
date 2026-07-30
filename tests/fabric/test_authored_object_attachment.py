"""The notebook case: an object with no ``lakehouse=``, in a real Fabric session.

Everything else about authored objects is transport-neutral and lives in
``test_authored_object_access.py``. This one cannot be: inference reads the
Lakehouse the session has attached, and only Fabric attaches one.
"""

from __future__ import annotations

import pytest

from weaver import ItemRef

pytestmark = pytest.mark.fabric


def test_an_object_resolves_the_lakehouse_the_session_attached(
    livy_session, fabric_workspace, fabric_client
):
    """``Sales__Order(spark)`` — no destination named anywhere, as in a notebook.

    A Livy session is opened beneath the Weaver Lakehouse, so that is the
    attachment, and it is what inference must produce: the same root the desktop
    resolver names over REST, and the ``/lakehouse/default`` mount a folder object
    writes through. If this fails on the roots, the session reports its attachment
    under keys other than the ones :func:`weaver.lakehouse.default_lakehouse`
    reads — which is the one thing about it that no local test can settle.
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
        "  'fuse_root': order.fuse_root,\n"
        "  'table_path': order.lakehouse.table_path(*order.identity),\n"
        "  'folder_path': export.path(),\n"
        "  'staging_folder': export.staging_folder(),\n"
        "  'inferred': default_lakehouse(spark).spark_root,\n"
        "})\n"
    ).payload

    assert payload["spark_root"] == expected_root
    assert payload["inferred"] == expected_root
    assert payload["table_path"] == f"{expected_root}/Tables/Sales/Order"
    assert payload["fuse_root"] == "/lakehouse/default"
    assert payload["folder_path"] == "/lakehouse/default/Files/Sales/OrderExport"
    assert payload["staging_folder"] == payload["folder_path"] + "_Staging"
