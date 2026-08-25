"""What a physical shortcut may point at, and how its address is resolved.

Intent: A repository can land a foreign Warehouse's tables, not only a foreign
Lakehouse's, because a Fabric Warehouse publishes its tables into OneLake the
same way.

Proof: resolution asks the workspace for the item type the declaration named,
and refuses only the combination that has no physical form.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.shortcut_sources import read_shortcut_sources
from weaver.declaration.model import ShortcutDeclaration, WeaverItemId
from weaver.errors import BuildError
from weaver.locations import Location

EXTERNAL_WORKSPACE = "Upstream"


class Item:
    """An already-resolved Fabric item, as a resolver hands one over."""

    def __init__(self, name: str, item_type: str) -> None:
        self.name = name
        self.id = "11111111-2222-3333-4444-555555555555"
        self.workspace_id = "66666666-7777-8888-9999-000000000000"
        self.item_type = item_type


class Resolver:
    """Records what type each name was asked about."""

    def __init__(self) -> None:
        self.asked: list[tuple[str, str, str | None]] = []

    def external_item(self, name, *, item_type, workspace=None):
        self.asked.append((name, item_type, workspace))
        return Item(name, item_type)

    def external_root(self, item) -> Location:
        return Location(f"https://onelake/{item.workspace_id}/{item.id}")


class Store:
    """A store where every path the caller asks about is present."""

    def exists(self, location) -> bool:
        return True

    def list(self, location):  # pragma: no cover - exists() answers first
        return ()


def declaration(
    *, item: str, shortcut_type: str, tail: str, name: str = "Land__Source"
):
    return ShortcutDeclaration(
        owner=WeaverItemId.parse("Lakehouse/Landing"),
        name=name,
        shortcut_type=shortcut_type,
        target_type="physical",
        target=f"{item}/{tail}",
        workspace=EXTERNAL_WORKSPACE,
        relative_path="shortcuts.py",
    )


@weaver_test()
def test_a_table_shortcut_resolves_a_warehouse_by_its_declared_type():
    resolver = Resolver()
    sources = read_shortcut_sources(
        [
            declaration(
                item="Warehouse/Upstream_WH",
                shortcut_type="table",
                tail="Source.Transaction",
            )
        ],
        resolver=resolver,
        store=Store(),
    )

    assert resolver.asked == [("Upstream_WH", "Warehouse", EXTERNAL_WORKSPACE)]
    source = sources["Lakehouse/Landing/Land__Source"]
    assert source.path == "Tables/Source/Transaction"
    assert source.item_name == "Upstream_WH"


@weaver_test()
def test_a_table_shortcut_still_resolves_a_lakehouse_by_its_declared_type():
    resolver = Resolver()
    read_shortcut_sources(
        [
            declaration(
                item="Lakehouse/Upstream_LH",
                shortcut_type="table",
                tail="Source.Customer",
            )
        ],
        resolver=resolver,
        store=Store(),
    )

    assert resolver.asked == [("Upstream_LH", "Lakehouse", EXTERNAL_WORKSPACE)]


@weaver_test()
def test_a_schema_shortcut_resolves_a_warehouse_schema():
    sources = read_shortcut_sources(
        [
            declaration(
                item="Warehouse/Upstream_WH",
                shortcut_type="schema",
                tail="Source",
                name="Source",
            )
        ],
        resolver=Resolver(),
        store=Store(),
    )

    assert sources["Lakehouse/Landing/Source"].path == "Tables/Source"


@weaver_test()
def test_a_files_shortcut_refuses_a_warehouse_because_it_has_no_files_area():
    with pytest.raises(BuildError) as raised:
        read_shortcut_sources(
            [
                declaration(
                    item="Warehouse/Upstream_WH",
                    shortcut_type="folder",
                    tail="Files/Source",
                )
            ],
            resolver=Resolver(),
            store=Store(),
        )

    assert "has none" in str(raised.value)
