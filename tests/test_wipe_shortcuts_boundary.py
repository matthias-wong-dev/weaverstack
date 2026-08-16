"""Wiping a Lakehouse that holds shortcuts.

A shortcut is the one thing in a Lakehouse that is not the Lakehouse's own data:
it is a name this item holds for data another item owns. So the interesting
assertion is not that the shortcut goes — it is that the thing it pointed at
*stays*. A wipe of one Lakehouse must never reach through a pointer into
another, which is why the shortcut is taken away through the workspace before
any storage is swept.
"""

from __future__ import annotations

import sys

import pytest

from weaver.fabric.shortcuts import Shortcut
from weaver.physical_wipe import wipe_delta_target, wipe_folder_target, wipe_lakehouse
from weaver.targets import DeltaTarget, FolderTarget

WIPE_MODULE = sys.modules["weaver.physical_wipe"]


# --- the shortcut is taken away through the workspace -------------------------


class _ShortcutResolver:
    """A resolver that holds shortcuts, and records what a wipe takes away.

    Wraps the resolver the test already has, so every path still resolves the
    way Fabric resolves it.
    """

    def __init__(self, inner, shortcuts):
        self._inner = inner
        self._shortcuts = list(shortcuts)
        self.removed: list[str] = []

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def onelake_shortcuts(self, item):
        return tuple(self._shortcuts)

    def remove_onelake_shortcut(self, item, *, path, name):
        self.removed.append(f"{path}/{name}")
        self._shortcuts = [
            shortcut
            for shortcut in self._shortcuts
            if (shortcut.path, shortcut.name) != (path, name)
        ]


@pytest.fixture
def shortcut_workspace(lakehouses, monkeypatch):
    """The workspace, answering as though it held two Fabric shortcuts."""

    shortcuts = (
        Shortcut(path="Tables/Sales", name="Portable", target_item_id="producer"),
        Shortcut(path="Files/Sales", name="Landed", target_item_id="producer"),
    )
    resolver = _ShortcutResolver(lakehouses.resolver, shortcuts)
    monkeypatch.setattr(WIPE_MODULE, "resolver_for", lambda workspace: resolver)
    monkeypatch.setattr(WIPE_MODULE, "store_for", lambda workspace: lakehouses.store)
    return resolver


def test_a_table_shortcut_is_taken_away_before_storage_is_swept(
    lakehouses, shortcut_workspace
):
    report = wipe_delta_target(
        DeltaTarget(lakehouse=lakehouses.target), lakehouses.workspace
    )

    assert shortcut_workspace.removed == ["Tables/Sales/Portable"]
    assert report.removed[0] == "shortcut:Tables/Sales/Portable"


def test_only_the_area_being_wiped_loses_its_shortcuts(lakehouses, shortcut_workspace):
    wipe_folder_target(FolderTarget(lakehouse=lakehouses.target), lakehouses.workspace)

    assert shortcut_workspace.removed == ["Files/Sales/Landed"]


def test_a_dry_run_reports_the_shortcut_without_taking_it_away(
    lakehouses, shortcut_workspace
):
    report = wipe_delta_target(
        DeltaTarget(lakehouse=lakehouses.target), lakehouses.workspace, dry_run=True
    )

    assert report.dry_run is True
    assert "shortcut:Tables/Sales/Portable" in report.removed
    assert shortcut_workspace.removed == []


def test_a_whole_lakehouse_wipe_clears_shortcuts_in_both_areas(
    lakehouses, shortcut_workspace
):
    wipe_lakehouse(lakehouses.target, lakehouses.workspace)

    assert sorted(shortcut_workspace.removed) == [
        "Files/Sales/Landed",
        "Tables/Sales/Portable",
    ]
