"""A Wipe runs as one MutationPlan: targets overlap, shortcuts detach first."""

from __future__ import annotations

import pytest
from support.sessions import given_session
from support.weaver_test import weaver_test
from support.workspaces import given_resolver, given_workspace

import weaver.build_bundle.executors.wipe as wipe_executor
from weaver import plan_wipe
from weaver import wipe as public_wipe
from weaver.errors import CommandError
from weaver.fabric.shortcuts import Shortcut
from weaver.store import FilesystemStore
from weaver.targets import ItemRef
from weaver.wipe_plan import wipe_mutation_plan

LAKEHOUSE = "Sales_LH"


class _Shortcuts:
    """The production resolver, holding shortcuts and recording their removal."""

    def __init__(self, inner, shortcuts, events, *, failing=False):
        self._inner = inner
        self._shortcuts = list(shortcuts)
        self.events = events
        self.failing = failing

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def onelake_shortcuts(self, item):
        return tuple(self._shortcuts)

    def remove_onelake_shortcut(self, item, *, path, name):
        if self.failing:
            raise CommandError(f"Fabric refused to remove {path}/{name}")
        self.events.append(f"detach {path}/{name}")
        self._shortcuts = [
            s for s in self._shortcuts if (s.path, s.name) != (path, name)
        ]


class _Recording(FilesystemStore):
    """A local store recording deletions; ``lingering`` paths answer for a while."""

    def __init__(self, events, lingering=()):
        self.events = events
        self.lingering = {path: 2 for path in lingering}

    def exists(self, location):
        for path, remaining in self.lingering.items():
            if location.value.endswith(path) and remaining:
                self.lingering[path] -= 1
                return True
        return super().exists(location)

    def delete(self, location, *, recursive=False):
        area = "Files" if "/Files/" in location.value else "Tables"
        self.events.append(f"delete {area}/{location.name}")
        super().delete(location, recursive=recursive)


@pytest.fixture
def estate(tmp_path, monkeypatch):
    monkeypatch.setattr(wipe_executor, "NAME_RELEASE_POLL_INTERVAL", 0.0)
    workspace = given_workspace(catalogue="Warehouse/Weaver")
    resolver = given_resolver(
        workspace=workspace,
        lakehouses=(LAKEHOUSE,),
        warehouses=("Weaver", "Curated_WH"),
        root=tmp_path,
    )
    events: list[str] = []

    def build(*, lingering=(), failing=False):
        store = _Recording(events, lingering)
        lakehouse = ItemRef(LAKEHOUSE)
        for area, root in (
            ("Files", resolver.files_root(lakehouse)),
            ("Tables", resolver.tables_root(lakehouse)),
        ):
            store.make_directory(root / "Sales" / f"{area}Owned")
        store.make_directory(resolver.tables_root(lakehouse) / "dbo")
        held = _Shortcuts(
            resolver,
            (
                Shortcut(path="Tables/Sales", name="Portable", target_item_id="other"),
                Shortcut(path="Files/Sales", name="Landed", target_item_id="other"),
            ),
            events,
            failing=failing,
        )
        session = given_session(workspace=workspace, resolver=held, store=store)
        return session, store, events

    return workspace, build


def _plan(session, *targets):
    return plan_wipe(
        targets, workspace="Demo", catalogue="Warehouse/Weaver", session=session
    )


@weaver_test()
def test_targets_share_no_edge_and_the_catalogue_follows_them_all(estate):
    _workspace, build = estate
    session, _store, _events = build()
    plan = _plan(session, f"Lakehouse/{LAKEHOUSE}", "Warehouse/Curated_WH")

    mutation, _payloads = wipe_mutation_plan(plan)

    actions = {a.id: a for _s, _b, a in mutation.actions()}
    lakehouse = [a for a in actions.values() if a.target_id == f"lakehouse-{LAKEHOUSE}"]
    assert {a.kind for a in lakehouse} == {
        "detach_file_shortcuts",
        "clear_files",
        "detach_table_shortcuts",
        "clear_tables",
    }
    assert actions["clear-files-lakehouse-Sales_LH"].depends_on == (
        "detach-file-shortcuts-lakehouse-Sales_LH",
    )
    assert actions["wipe-warehouse-Curated_WH"].depends_on == ()
    assert set(actions["wipe-warehouse-Weaver"].depends_on) == {
        "clear-files-lakehouse-Sales_LH",
        "clear-tables-lakehouse-Sales_LH",
        "wipe-warehouse-Curated_WH",
    }
    assert mutation.execution.spark_home_target_id is None


@weaver_test()
def test_shortcuts_are_detached_and_released_before_their_area_is_swept(estate):
    _workspace, build = estate
    session, store, events = build(lingering=("Tables/Sales/Portable",))

    result = public_wipe(plan=_plan(session, f"Lakehouse/{LAKEHOUSE}"), session=session)

    for area, shortcut in (("Tables", "Sales/Portable"), ("Files", "Sales/Landed")):
        assert events.index(f"detach {area}/{shortcut}") < events.index(
            f"delete {area}/Sales"
        )
    assert result.items[0].counts == {"entries": 2, "shortcuts": 2}
    lakehouse = ItemRef(LAKEHOUSE)
    resolver = session.resolver(_workspace)
    assert not store.exists(resolver.files_root(lakehouse) / "Sales")
    assert not store.exists(resolver.tables_root(lakehouse) / "Sales")
    assert store.exists(resolver.tables_root(lakehouse) / "dbo")


@weaver_test()
def test_an_area_whose_shortcuts_could_not_be_detached_is_not_swept(estate):
    _workspace, build = estate
    session, store, events = build(failing=True)

    with pytest.raises(CommandError, match="did not complete"):
        public_wipe(plan=_plan(session, f"Lakehouse/{LAKEHOUSE}"), session=session)

    assert not any(event.startswith("delete") for event in events)
