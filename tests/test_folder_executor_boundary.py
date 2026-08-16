"""Strict managed-folder transitions and idempotent prune execution."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_resolver, given_workspace

from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.executors.folder import FolderExecutor
from weaver.build_bundle.models import (
    BUILD_FOLDER,
    DROP_FOLDER,
    PRUNE_FOLDER,
    InstallAction,
)
from weaver.build_bundle.targets import BoundTarget
from weaver.errors import InstallError
from weaver.store import FilesystemStore
from weaver.targets import ItemRef


def _context(tmp_path):
    workspace = given_workspace(catalogue="Warehouse/Control")
    resolver = given_resolver(
        workspace=workspace, lakehouses=("Control", "Sales"), root=tmp_path
    )
    store = FilesystemStore()
    lakehouse = ItemRef("Sales")
    bound = BoundTarget(
        id="lakehouse-Sales",
        kind="lakehouse",
        item_id=lakehouse.name,
    )
    return InstallationContext(
        resolver=resolver,
        store=store,
        target=ResolvedTarget(bound=bound, lakehouse=lakehouse),
    )


def _action(kind: str) -> InstallAction:
    return InstallAction(
        id=f"{kind}-sales-export",
        kind=kind,
        resource_node_id="Lakehouse/Raw/Files/Sales.Export",
        executor="folder",
        payload=None,
        payload_sha256=None,
    )


@weaver_test()
def test_managed_folder_create_and_drop_are_strict(tmp_path):
    context = _context(tmp_path)
    executor = FolderExecutor()

    executor.execute(_action(BUILD_FOLDER), None, context)
    with pytest.raises(InstallError, match="already exists"):
        executor.execute(_action(BUILD_FOLDER), None, context)

    executor.execute(_action(DROP_FOLDER), None, context)
    with pytest.raises(InstallError, match="does not exist"):
        executor.execute(_action(DROP_FOLDER), None, context)


@weaver_test()
def test_folder_prune_remains_idempotent(tmp_path):
    context = _context(tmp_path)
    result = FolderExecutor().execute(_action(PRUNE_FOLDER), None, context)
    # Named by where it resolved to, which is keyed by item id rather than by
    # the display name the caller typed.
    assert result["pruned"].endswith("/Files/Sales/Export")
