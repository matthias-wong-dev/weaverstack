"""Strict managed-folder transitions and idempotent prune execution."""

from __future__ import annotations

import pytest

from weaver.targets import ItemRef
from weaver.resolution import LocalResolver
from weaver.store import FilesystemStore
from weaver.workspaces import LocalWorkspace
from weaver.locations import Location
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


def _context(tmp_path):
    workspace = LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Control")
    resolver = LocalResolver(workspace)
    store = FilesystemStore()
    lakehouse = ItemRef("Sales")
    bound = BoundTarget(
        id="lakehouse-Sales",
        kind="lakehouse",
        item_id=lakehouse.name,
    )
    return InstallationContext(
        spark=None,
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


def test_managed_folder_create_and_drop_are_strict(tmp_path):
    context = _context(tmp_path)
    executor = FolderExecutor()

    executor.execute(_action(BUILD_FOLDER), None, context)
    with pytest.raises(InstallError, match="already exists"):
        executor.execute(_action(BUILD_FOLDER), None, context)

    executor.execute(_action(DROP_FOLDER), None, context)
    with pytest.raises(InstallError, match="does not exist"):
        executor.execute(_action(DROP_FOLDER), None, context)


def test_folder_prune_remains_idempotent(tmp_path):
    context = _context(tmp_path)
    result = FolderExecutor().execute(_action(PRUNE_FOLDER), None, context)
    assert result == {
        "pruned": str(tmp_path / "Sales" / "Files" / "Sales" / "Export")
    }
