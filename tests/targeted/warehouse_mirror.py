"""The mirrored Warehouse item the Warehouse mirror cycles build over."""

from __future__ import annotations

import json
from dataclasses import replace

from factories import FixtureInventory, item_bindings, item_id
from support.workspaces import WORKSPACE

from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import MIRROR
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId
from weaver.store import FilesystemStore

ITEM = "Warehouse/Model"
#: The Warehouse the mirror is built in, and the one it borrows rows from.
TARGET = "Model_Dev"
SOURCE_TARGET = "Model"


def mirror_object(name: str) -> WeaverDocumentId:
    return WeaverDocumentId(item_id(ITEM), ObjectId("Sales", name))


def mirror_bindings():
    return item_bindings((ITEM, TARGET))


def with_borrowed(catalogue: Catalogue, *names: str) -> Catalogue:
    """The same catalogue, with those objects recorded as borrowed."""

    item = item_id(ITEM)
    rows = {each: dict(tables) for each, tables in catalogue.rows.items()}
    rows[item][MIRROR.name] = tuple(
        {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "schema_name": "Sales",
            "object_name": name,
            "source_workspace_name": WORKSPACE,
            "source_target_name": SOURCE_TARGET,
            "source_schema_name": "Sales",
            "source_object_name": name,
            "physical_type": "view",
        }
        for name in names
    )
    return Catalogue(rows=rows, materialised=catalogue.materialised | {MIRROR.name})


def mirror_inventory(repository, *, borrowed: tuple[str, ...]):
    """The Warehouse as a mirror leaves it: a View at each borrowed address."""

    bound = {b.item: b.to_bound_target() for b in mirror_bindings().entries}
    inventory = FixtureInventory.from_repository(
        repository,
        item=ITEM,
        target_id=bound[item_id(ITEM)].id,
        kind="warehouse",
        target_name=TARGET,
    )
    names = {f"Sales.{name}" for name in borrowed}
    return replace(
        inventory,
        tables=tuple(name for name in inventory.tables if name not in names),
        views=tuple(sorted(set(inventory.views) | names)),
    )


def batch_statements(bundle, action) -> str:
    """One T-SQL batch action's statements, as one string to search."""

    content = FilesystemStore().read(bundle.location.join(*action.payload.split("/")))
    return "\n".join(json.loads(content.decode("utf-8")))
