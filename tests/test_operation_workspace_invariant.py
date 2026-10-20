"""Workspace arguments preserve an already opened Session's bindings."""

from types import SimpleNamespace

from support.weaver_test import weaver_test

from weaver.operations.workspace import _operation_workspace
from weaver.workspaces import Workspace


@weaver_test()
def test_repeating_a_session_workspace_name_keeps_its_configuration():
    configured = Workspace(
        workspace="Parcel Development",
        catalogue="Warehouse/Weaver",
        data_sources={"Sales": "Warehouse/Sales"},
    )

    resolved = _operation_workspace(
        workspace="Parcel Development",
        workspace_config=None,
        session=SimpleNamespace(workspace=configured),
    )

    assert resolved is configured
