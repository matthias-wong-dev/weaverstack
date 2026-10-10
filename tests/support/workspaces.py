"""A real FabricResolver over a workspace inventory the test declares.

The double is the HTTP client, which is a genuine boundary: it answers the two
listings resolution asks for and reaches nothing. Everything above it is the
production resolver, so a core test proves the arithmetic Fabric will use,
which is the whole reason not to hand-write a resolver here.

.. code-block:: python

    resolver = given_resolver(lakehouses=["Weaver", "Sales_LH"])
    resolver.tables_root(ItemRef("Sales_LH"))
"""

from __future__ import annotations

import pathlib
import uuid
from typing import TYPE_CHECKING, Iterable

from weaver.fabric.resolution import FabricResolver
from weaver.workspaces import Workspace

if TYPE_CHECKING:  # names used only in annotations
    from weaver.lakehouse import Lakehouse

WORKSPACE = "Demo"
#: The Warehouse the Weaver catalogue lives in, as an item name.
WEAVER_WAREHOUSE = "Weaver"
#: And as the workspace's typed catalogue value.
CATALOGUE = f"Warehouse/{WEAVER_WAREHOUSE}"
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

        The shape matches what the REST client returns, a response carrying
        headers, because the caller reads an operation id off it.
        """

        self.requested.append(f"{method} {path}")
        return _Response()

    def wait_for_operation(self, response, **_):
        return {"status": "Succeeded"}

    def poll_operation(self, operation, **_):
        from weaver.fabric.client import Operation

        self.requested.append(f"GET {operation.location}")
        return Operation(
            location=operation.location,
            operation_id=operation.operation_id,
            done=True,
            body={"status": "Succeeded"},
        )


class CreatingInventoryClient(InventoryClient):
    """An inventory that holds each SemanticModel and Report created in it.

    ``created`` records ``(type, name, definition)`` in creation order.
    """

    _CREATES = {"semanticModels": "SemanticModel", "reports": "Report"}

    def __init__(self, workspace: str, items: Iterable[tuple[str, str]]) -> None:
        super().__init__(workspace, items)
        self.created: list[tuple[str, str, dict]] = []

    def request(self, method: str, path: str, **options):
        kind = self._CREATES.get(path.rsplit("/", 1)[-1]) if method == "POST" else None
        if kind is None:
            return super().request(method, path, **options)
        self.requested.append(f"{method} {path}")
        payload = options["payload"]
        self.items.append((kind, payload["displayName"]))
        self.created.append((kind, payload["displayName"], payload["definition"]))
        return _Created(_identifier(kind, payload["displayName"]))


class _Created:
    status_code = 201

    def __init__(self, item_id: str) -> None:
        self._item_id = item_id

    def json(self):
        return {"id": self._item_id}


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
) -> Workspace:
    """One Fabric workspace configuration, with neutral names."""

    return Workspace(
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        **rest,
    )


def given_resolver(
    *,
    workspace: Workspace | str = WORKSPACE,
    lakehouses: Iterable[str] = (WEAVER_WAREHOUSE, TARGET_LAKEHOUSE),
    warehouses: Iterable[str] = (),
    root: object = None,
) -> FabricResolver:
    """The production resolver, over an inventory this test declares.

    ``root`` moves what it resolves onto a real filesystem, so a test about a
    store can write what it resolves and read it back. That is the resolver's
    own ``base_url`` parameter: the arithmetic above it is unchanged, and what
    differs is only where OneLake is.
    """

    configuration = (
        workspace
        if isinstance(workspace, Workspace)
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
    "WEAVER_WAREHOUSE",
    "WORKSPACE",
    "given_resolver",
    "given_workspace",
]


def mounted_lakehouse(
    name: str, directory, *, deleted: Iterable[str] = ()
) -> "Lakehouse":
    """A Fabric Lakehouse whose Files area is this directory.

    A Lakehouse lives in OneLake. Authored Python reaches its Files through a
    Fabric mount, and Weaver lists and changes them through the session's
    store. Those are the two boundaries a fast test may stand in for: the
    directory is registered as the mount point and served as the store, and
    every other property of the Lakehouse stays what it is in production.

    ``deleted`` names paths beneath the item, such as ``Files/Sales/a.csv``,
    that OneLake no longer has while the mount still lists them.

    Nothing here makes a directory into a Lakehouse. `spark_root` is a real
    OneLake address, and Spark paths are composed from it as they always are.
    """

    from weaver.lakehouse import _MOUNTS, Lakehouse

    root = f"abfss://ws@onelake.dfs.fabric.microsoft.com/{name}"
    _MOUNTS[root] = str(directory)
    return Lakehouse(
        name=name,
        spark_root=root,
        store=OneLakeDirectory(root, directory, deleted=deleted),
    )


class OneLakeDirectory:
    """A Lakehouse's OneLake storage, held in a directory.

    The store a Fabric session reaches its Files through, addressed by the same
    ``abfss://`` locations. Paths named in ``deleted`` stay in the directory,
    which is what the mount shows, and are absent from every answer this store
    gives, which is what OneLake says.
    """

    def __init__(self, root: str, directory, *, deleted: Iterable[str] = ()) -> None:
        from weaver.store import FilesystemStore

        self.root = root.rstrip("/")
        self.directory = pathlib.Path(directory)
        self.deleted = {path.strip("/") for path in deleted}
        self._local = FilesystemStore()

    def _relative(self, location) -> str:
        value = location.value
        if value != self.root and not value.startswith(f"{self.root}/"):
            raise AssertionError(f"{value} is outside {self.root}")
        return value[len(self.root) :].strip("/")

    def _gone(self, location) -> bool:
        relative = self._relative(location)
        return any(
            relative == path or relative.startswith(f"{path}/") for path in self.deleted
        )

    def _here(self, location):
        from weaver.locations import Location

        relative = self._relative(location)
        path = self.directory / relative if relative else self.directory
        return Location(str(path))

    def exists(self, location) -> bool:
        return not self._gone(location) and self._local.exists(self._here(location))

    def is_directory(self, location) -> bool:
        return not self._gone(location) and self._local.is_directory(
            self._here(location)
        )

    def list(self, location, *, recursive: bool = False):
        from dataclasses import replace

        from weaver.store import StoreNotFoundError

        if self._gone(location):
            raise StoreNotFoundError(f"cannot list missing location {location.value}")
        local = self._here(location).value
        entries = []
        for entry in self._local.list(self._here(location), recursive=recursive):
            translated = location.join(
                *entry.location.value[len(local) :].strip("/").split("/")
            )
            if not self._gone(translated):
                entries.append(replace(entry, location=translated))
        return entries

    def read(self, location) -> bytes:
        from weaver.store import StoreError

        if self._gone(location):
            raise StoreError(f"cannot read a location that does not exist: {location}")
        return self._local.read(self._here(location))

    def write(self, location, data: bytes) -> None:
        self._local.write(self._here(location), data)

    def delete(self, location, *, recursive: bool = False) -> None:
        self._local.delete(self._here(location), recursive=recursive)

    def make_directory(self, location) -> None:
        self._local.make_directory(self._here(location))

    def copy(self, source, destination) -> None:
        self._local.copy(self._here(source), self._here(destination))

    def move(self, source, destination) -> None:
        self._local.move(self._here(source), self._here(destination))

    def copy_file_to_local(self, source, destination: pathlib.Path) -> None:
        from weaver.store import StoreError

        if self._gone(source):
            raise StoreError(f"cannot read a location that does not exist: {source}")
        self._local.copy_file_to_local(self._here(source), destination)
