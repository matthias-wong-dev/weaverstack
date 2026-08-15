"""A real FabricResolver over a workspace inventory the test declares.

The double is the HTTP client, which is a genuine boundary: it answers the two
listings resolution asks for and reaches nothing. Everything above it is the
production resolver, so a core test proves the arithmetic Fabric will use —
which is the whole reason not to hand-write a resolver here.

.. code-block:: python

    resolver = given_resolver(lakehouses=["Weaver", "Sales_LH"])
    resolver.tables_root(ItemRef("Sales_LH"))
"""

from __future__ import annotations

import pathlib
import uuid
from typing import Iterable

from weaver.fabric.resolution import FabricResolver
from weaver.workspaces import FabricWorkspace

WORKSPACE = "Demo"
#: The Lakehouse the catalogue lives in, as an item name.
WEAVER_LAKEHOUSE = "Weaver"
#: And as the workspace's typed catalogue value.
CATALOGUE = f"Lakehouse/{WEAVER_LAKEHOUSE}"
TARGET_LAKEHOUSE = "Sales_LH"

LAKEHOUSE_TYPE = "Lakehouse"
WAREHOUSE_TYPE = "Warehouse"
#: Fabric generates one of these per Lakehouse, sharing its display name.
SQL_ENDPOINT_TYPE = "SQLEndpoint"


def _identifier(kind: str, name: str) -> str:
    """A stable id for one named item, so two resolvers agree about it.

    Derived rather than random: a test that resolves the same Lakehouse twice
    compares locations, and a fresh GUID each time would make them differ for
    no reason the test is about.
    """

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"weaver-test/{kind}/{name}"))


class InventoryClient:
    """Answers workspace and item listings from a fixed inventory."""

    def __init__(self, workspace: str, items: Iterable[tuple[str, str]]) -> None:
        self.workspace = workspace
        self.items = list(items)
        #: Every path asked for, so a test can claim what was and was not called.
        self.requested: list[str] = []

    def paged(self, path: str, **_):
        self.requested.append(path)
        if path == "workspaces":
            return [
                {
                    "id": _identifier("workspace", self.workspace),
                    "displayName": self.workspace,
                }
            ]
        route, _, query = path.partition("?")
        if route.endswith("/items"):
            wanted = query.partition("type=")[2] or None
            return [
                {
                    "id": _identifier(kind, name),
                    "displayName": name,
                    "type": kind,
                }
                for kind, name in self.items
                if wanted is None or kind == wanted
            ]
        if route.endswith("/shortcuts"):
            # A test that means to hold shortcuts wraps this resolver; an
            # inventory on its own holds none.
            return []
        raise AssertionError(f"this inventory was not asked to answer {path!r}")

    def get(self, path: str, **_):
        self.requested.append(path)
        raise AssertionError(f"this inventory was not asked to answer {path!r}")

    def request(self, method: str, path: str, **_):
        """A write this inventory accepts and records, rather than performs.

        The shape matches what the REST client returns — a response carrying
        headers — because the caller reads an operation id off it.
        """

        self.requested.append(f"{method} {path}")
        return _Response()

    def wait_for_operation(self, response, **_):
        return {"status": "Succeeded"}


class _Response:
    """The little of a REST response the resolver reads."""

    status_code = 202
    headers = {"x-ms-operation-id": "operation-for-a-test"}

    def json(self):
        return {"status": "Succeeded"}


def given_workspace(
    *,
    workspace: str = WORKSPACE,
    catalogue: str | None = CATALOGUE,
    environment: str | None = None,
    **rest,
) -> FabricWorkspace:
    """One Fabric workspace configuration, with neutral names."""

    return FabricWorkspace(
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        **rest,
    )


def given_resolver(
    *,
    workspace: FabricWorkspace | str = WORKSPACE,
    lakehouses: Iterable[str] = (WEAVER_LAKEHOUSE, TARGET_LAKEHOUSE),
    warehouses: Iterable[str] = (),
    root: object = None,
) -> FabricResolver:
    """The production resolver, over an inventory this test declares.

    ``root`` moves what it resolves onto a real filesystem, so a test about a
    store can write what it resolves and read it back. That is the resolver's
    own ``base_url`` parameter, not an emulator: the arithmetic above it is
    unchanged, and what differs is only where OneLake is.
    """

    configuration = (
        workspace
        if isinstance(workspace, FabricWorkspace)
        else given_workspace(workspace=workspace)
    )
    items = [(LAKEHOUSE_TYPE, name) for name in lakehouses]
    # A Lakehouse's generated endpoint shares its display name, which is why
    # resolution is typed: the two are different items.
    items += [(SQL_ENDPOINT_TYPE, name) for name in lakehouses]
    items += [(WAREHOUSE_TYPE, name) for name in warehouses]
    client = InventoryClient(configuration.workspace, items)
    if root is None:
        return FabricResolver(configuration, client=client)
    return FabricResolver(
        configuration, client=client, base_url=pathlib.Path(root).as_posix()
    )


__all__ = [
    "InventoryClient",
    "TARGET_LAKEHOUSE",
    "CATALOGUE",
    "WEAVER_LAKEHOUSE",
    "WORKSPACE",
    "given_resolver",
    "given_workspace",
]
