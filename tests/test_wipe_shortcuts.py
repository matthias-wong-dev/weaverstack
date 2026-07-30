"""Wiping a Lakehouse that holds shortcuts.

A shortcut is the one thing in a Lakehouse that is not the Lakehouse's own data:
it is a name this item holds for data another item owns. So the interesting
assertion is not that the shortcut goes — it is that the thing it pointed at
*stays*. A wipe of one Lakehouse must never reach through a pointer into another.

The two environments reach that guarantee differently, which is why both are
tested here: Fabric removes the shortcut through the workspace before any storage
is swept, and the emulator's link is unlinked rather than followed.
"""

from __future__ import annotations

import sys

import pytest

from weaver import DeltaTarget, FolderTarget, ItemRef, wipe_delta_target, wipe_folder_target
from weaver.fabric.shortcuts import Shortcut
from weaver.wipe import wipe_lakehouse

#: The package exports a ``wipe`` *function*, so ``weaver.wipe`` as an attribute
#: path names that rather than this module. Patch the module object itself.
WIPE_MODULE = sys.modules["weaver.wipe"]


# --- the emulator: a link is unlinked, never followed --------------------------


def _linked_table(lakehouses, *, producer="Producer_LH"):
    """A Delta table in one Lakehouse, aliased into the target's Tables area."""

    store, resolver = lakehouses.store, lakehouses.resolver
    produced = resolver.tables_root(ItemRef(producer)) / "Sales" / "Customer"
    store.write(produced / "part.parquet", b"rows")
    aliased = resolver.tables_root(lakehouses.target) / "Sales" / "Portable"
    store.link(produced, aliased)
    return produced, aliased


def test_wiping_tables_removes_the_link_and_not_what_it_points_at(lakehouses):
    produced, aliased = _linked_table(lakehouses)

    report = wipe_delta_target(
        DeltaTarget(lakehouse=lakehouses.target), lakehouses.workspace
    )

    assert not aliased.path.exists()
    assert report.count == 1  # the Sales schema directory that held the link
    assert lakehouses.store.read(produced / "part.parquet") == b"rows"


def test_wiping_files_removes_a_folder_link_and_not_its_source(lakehouses):
    store, resolver = lakehouses.store, lakehouses.resolver
    produced = resolver.files_root(ItemRef("Producer_LH")) / "Sales" / "Export"
    store.write(produced / "orders.csv", b"id\n1\n")
    aliased = resolver.files_root(lakehouses.target) / "Sales" / "Portable"
    store.link(produced, aliased)

    wipe_folder_target(
        FolderTarget(lakehouse=lakehouses.target), lakehouses.workspace
    )

    assert not aliased.path.exists()
    assert store.read(produced / "orders.csv") == b"id\n1\n"


def test_a_whole_lakehouse_wipe_leaves_every_producer_intact(lakehouses):
    produced, _aliased = _linked_table(lakehouses)

    wipe_lakehouse(lakehouses.target, lakehouses.workspace)

    assert lakehouses.store.read(produced / "part.parquet") == b"rows"
    assert lakehouses.resolver.tables_root(lakehouses.target).path.is_dir()


# --- Fabric: the shortcut is taken away through the workspace ------------------


class _ShortcutResolver:
    """A resolver that holds shortcuts, as the Fabric ones do.

    Wraps the local resolver so paths still resolve, and records what a wipe asks
    it to take away.
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
    """The local workspace, answering as though it held Fabric shortcuts."""

    shortcuts = (
        Shortcut(path="Tables/Sales", name="Portable", target_item_id="producer"),
        Shortcut(path="Files/Sales", name="Landed", target_item_id="producer"),
    )
    resolver = _ShortcutResolver(lakehouses.resolver, shortcuts)
    monkeypatch.setattr(WIPE_MODULE, "resolver_for", lambda workspace: resolver)
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
    wipe_folder_target(
        FolderTarget(lakehouse=lakehouses.target), lakehouses.workspace
    )

    assert shortcut_workspace.removed == ["Files/Sales/Landed"]


def test_a_subpath_target_leaves_shortcuts_it_never_reached(lakehouses, monkeypatch):
    """A folder target may be a root within Files, and scope has to follow it.

    ``Sales_LH/Files/Extracts`` clears only beneath itself, so taking away a
    pointer under ``Files/Sales`` would be removing something the wipe never
    touched.
    """

    shortcuts = (
        Shortcut(path="Files/Extracts", name="Inbound", target_item_id="producer"),
        Shortcut(path="Files/Sales", name="Landed", target_item_id="producer"),
    )
    resolver = _ShortcutResolver(lakehouses.resolver, shortcuts)
    monkeypatch.setattr(WIPE_MODULE, "resolver_for", lambda workspace: resolver)

    wipe_folder_target(
        FolderTarget.parse(f"{lakehouses.target.name}/Files/Extracts"),
        lakehouses.workspace,
    )

    assert resolver.removed == ["Files/Extracts/Inbound"]


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
