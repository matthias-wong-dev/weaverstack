"""Build orchestration from source snapshot through installation.

Source parsing and request validation finish before target state is read.
"""

from __future__ import annotations

import stat
import tempfile
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Iterator, Mapping

from ..catalogue.state import (
    Catalogue,
    Reconciliation,
    read_catalogue_state,
    reconcile_catalogue_state,
)
from ..declaration.model import WeaverItemId, WeaverRepository
from ..declaration.repository import parse_item_repository
from ..errors import BuildError
from ..locations import Location
from ..store import FilesystemStore, Store
from ..targets import ItemRef
from .builder import Builder
from .bundle import BuildBundle, load_bundle
from .installer import Installer
from .models import BuildPlan
from .prune import (
    TargetInventory,
    read_lakehouse_inventory,
    read_warehouse_inventory,
)
from .report import InstallationReport
from .shortcut_sources import (
    physical_shortcuts,
    read_shortcut_sources,
)
from .shortcuts import ResolvedShortcutSource
from .targets import (
    WAREHOUSE_TARGET,
    ItemBindings,
    WarehouseBinding,
)

ARCHIVE_SUFFIX = ".weaver.zip"


@dataclass(frozen=True)
class MaterialisedTree:
    location: Location
    store: FilesystemStore


@dataclass(frozen=True)
class PreparedRepository:
    repository: WeaverRepository
    store: FilesystemStore


@dataclass(frozen=True)
class ItemBuildResult:
    plan: BuildPlan
    report: InstallationReport
    repository_signature: str
    item_signatures: Mapping[WeaverItemId, str]

    @property
    def bundle_id(self) -> str:
        return self.plan.bundle_id


@dataclass(frozen=True)
class BuildState:
    catalogue: Catalogue
    target_inventories: Mapping[WeaverItemId, TargetInventory]
    #: Direct shortcut destinations, keyed by ``<owner>/<name>``.
    shortcut_sources: Mapping[str, ResolvedShortcutSource] = field(default_factory=dict)

    def to_mapping(self) -> dict[str, object]:
        return {
            "format_version": 1,
            "catalogue": self.catalogue.to_mapping(),
            "shortcut_sources": {
                key: vars(source)
                for key, source in sorted(self.shortcut_sources.items())
            },
            "target_inventories": [
                {
                    "item": str(item),
                    "inventory": inventory.to_mapping(),
                }
                for item, inventory in sorted(
                    self.target_inventories.items(), key=lambda pair: str(pair[0])
                )
            ],
        }

    @classmethod
    def from_mapping(cls, mapping) -> "BuildState":
        version = mapping.get("format_version")
        if version != 1:
            raise BuildError(
                f"unsupported build state format_version {version!r}; expected 1"
            )
        return cls(
            catalogue=Catalogue.from_mapping(mapping["catalogue"]),
            target_inventories={
                WeaverItemId.parse(entry["item"]): TargetInventory.from_mapping(
                    entry["inventory"]
                )
                for entry in mapping.get("target_inventories", ())
            },
            shortcut_sources={
                key: ResolvedShortcutSource(**value)
                for key, value in (mapping.get("shortcut_sources") or {}).items()
            },
        )


def catalogue_items_for_build(
    repository: WeaverRepository, bindings: ItemBindings
) -> tuple[WeaverItemId, ...]:
    """Include external shortcut producers needed for freshness checks."""

    bound = set(bindings.by_item)
    items = bound | {
        shortcut.source.item
        for shortcut in repository.logical_shortcuts
        if shortcut.destination.item in bound and shortcut.source.item not in bound
    }
    return tuple(sorted(items, key=str))


def validate_build_request(
    repository: WeaverRepository,
    bindings: ItemBindings,
    *,
    catalogue_binding: WarehouseBinding,
) -> tuple[WeaverItemId, ...]:
    if catalogue_binding is None:
        raise BuildError("Select a catalogue Warehouse before building")
    if not bindings.entries:
        raise BuildError("Select at least one Weaver item to build")
    known = {item.identity for item in repository.items}
    unknown = set(bindings.by_item) - known
    if unknown:
        raise BuildError(
            "Item(s) not found in the project: " + ", ".join(sorted(map(str, unknown)))
        )
    placed = {item for layer in repository.item_layers for item in layer}
    missing = set(bindings.by_item) - placed
    if missing:
        raise BuildError(
            "Cannot determine build order for item(s): "
            + ", ".join(sorted(map(str, missing)))
        )
    from ..catalogue.builtin import BUILTIN_ITEM

    binding = bindings.by_item.get(BUILTIN_ITEM)
    if binding is not None and binding.target.kind != WAREHOUSE_TARGET:
        raise BuildError("The Weaver catalogue must target a Warehouse")
    if binding is not None and binding.target.item.name != catalogue_binding.item.name:
        raise BuildError(
            "The Weaver catalogue must use the selected catalogue Warehouse"
        )
    return catalogue_items_for_build(repository, bindings)


def read_build_state(
    bindings: ItemBindings,
    *,
    required_catalogue_items,
    session,
    workspace=None,
    sql_by_item=None,
    shortcuts=(),
) -> BuildState:
    """Read the catalogue and selected target state for build planning."""

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise BuildError("every build needs a Workspace with a Weaver catalogue")

    # Check occupancy before the slower per-target inventory and Spark reads.
    with session.step("Check target occupancy"):
        _refuse_occupied_targets(bindings, session=session, workspace=workspace)
    with session.step("Read target inventories"):
        inventories = read_target_inventories(
            bindings, session=session, workspace=workspace, sql_by_item=sql_by_item
        )
    with session.step("Read catalogue"):
        catalogue = _read_catalogue(
            session=session,
            workspace=workspace,
            required=tuple(required_catalogue_items),
        )
    sources = {}
    physical = physical_shortcuts(shortcuts, bindings=bindings)
    if physical:
        with session.step("Resolve physical shortcut targets"):
            sources = read_shortcut_sources(
                physical,
                resolver=session.resolver(workspace),
                store=session.transport_store(workspace),
            )
    return BuildState(
        catalogue=catalogue,
        target_inventories=inventories,
        shortcut_sources=sources,
    )


def _refuse_occupied_targets(bindings: ItemBindings, *, session, workspace) -> None:
    """Refuse a target already installed to by an item outside this build.

    Occupancy is read unscoped, because a build's own catalogue read is scoped to
    its selected items. Item-specific pruning compares one item's keep-set with
    the whole target inventory and would otherwise prune another item's objects.

    ``Warehouse/_weaver`` is exempt: its inventory contains only ``_``, which
    every other item's inventory excludes. See
    :func:`weaver.build_bundle.prune.read_warehouse_inventory`.
    """

    from ..catalogue.builtin import BUILTIN_ITEM
    from ..catalogue.connection import catalogue_connection
    from ..catalogue.state import read_target_occupancy

    ordinary = [binding for binding in bindings.entries if binding.item != BUILTIN_ITEM]
    if not ordinary:
        return
    occupancy = read_target_occupancy(catalogue_connection(session, workspace))
    for binding in ordinary:
        kind, name = binding.target.physical_kind, binding.target.item.name
        others = sorted(
            str(item)
            for item in occupancy.get((kind.casefold(), name.casefold()), ())
            if item != binding.item and item != BUILTIN_ITEM
        )
        if others:
            raise BuildError(
                f"{kind}/{name} is installed to by "
                + ", ".join(others)
                + f", so {binding.item} cannot be built into it. Empty and "
                f"unbind it first, or give {binding.item} a physical target of "
                "its own"
            )


def _read_catalogue(*, session, workspace, required):
    """Read the Warehouse catalogue over TDS without starting Spark."""

    from ..catalogue.connection import catalogue_connection

    return read_catalogue_state(catalogue_connection(session, workspace), required)


def session_catalogue(session, workspace, item: ItemRef):
    """Access a Lakehouse's Spark-only views through the Session."""

    from ..spark import SparkCatalogue

    destination = session.resolver(workspace).spark_destination(item)
    return SparkCatalogue.over_sql(
        lambda statement: session.execute_spark_sql(statement, workspace=workspace),
        destination,
    )


@contextmanager
def materialise_tree(
    source: Location,
    *,
    store: Store,
    prefix: str = "weaver-source-",
) -> Iterator[MaterialisedTree]:
    """Copy a store tree to a temporary local directory.

    Stores may provide a recursive local copy; others use listing and file reads.
    """

    if not store.exists(source):
        raise BuildError(f"source does not exist: {source.value}")
    if not store.is_directory(source):
        raise BuildError(f"source is not a directory: {source.value}")

    with tempfile.TemporaryDirectory(prefix=prefix) as temporary:
        destination = Path(temporary) / _snapshot_name(source)
        copier = getattr(store, "copy_to_local", None)
        if callable(copier):
            copier(source, destination)
        else:
            _copy_tree_through_store(source, store, destination)
        if not destination.is_dir():
            raise BuildError(
                f"materialising {source.value} did not create {destination}"
            )
        yield MaterialisedTree(Location(destination.as_posix()), FilesystemStore())


def _snapshot_name(source: Location) -> str:
    """Return a child-directory name even when ``source`` is ``.`` or ``..``."""

    name = source.name if source.is_url else source.path.resolve().name
    return name if name and name not in (".", "..") else "repository"


@contextmanager
def prepare_repository(
    source: Location,
    *,
    source_store: Store,
) -> Iterator[PreparedRepository]:
    with _temp_copy(source, source_store, prefix="weaver-repository-") as root:
        store = FilesystemStore()
        repository = parse_item_repository(Location(root.as_posix()), store=store)
        yield PreparedRepository(repository=repository, store=store)


def _copy_tree_through_store(source: Location, store: Store, destination: Path) -> None:
    destination.mkdir(parents=True)
    prefix = source.value.rstrip("/") + "/"
    entries = store.list(source, recursive=True)
    for entry in entries:
        relative = entry.location.value[len(prefix) :]
        target = destination.joinpath(*relative.split("/"))
        if entry.is_directory:
            target.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        if entry.is_directory:
            continue
        relative = entry.location.value[len(prefix) :]
        target = destination.joinpath(*relative.split("/"))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(store.read(entry.location))


def timestamped_archive_name(at: datetime | None = None) -> str:
    at = at or datetime.now(timezone.utc)
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    stamp = at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{stamp}{ARCHIVE_SUFFIX}"


def persist_bundle_archive(
    bundle: BuildBundle,
    destination: Location,
    *,
    store: Store,
) -> Location:
    """Persist a bundle as a deterministic ZIP file."""

    if not destination.name.endswith(ARCHIVE_SUFFIX):
        raise BuildError(
            f"bundle archive must end with {ARCHIVE_SUFFIX!r}: {destination.value}"
        )
    bundle_store = bundle.store or store
    with _temp_copy(
        bundle.location, bundle_store, prefix="weaver-bundle-source-"
    ) as root:
        with tempfile.TemporaryDirectory(prefix="weaver-bundle-archive-") as temporary:
            archive = Path(temporary) / destination.name
            _write_archive(root, archive)
            parent_value, separator, _ = destination.value.rpartition("/")
            if separator:
                parent = Location(parent_value)
                if not store.exists(parent):
                    store.make_directory(parent)
            copier = getattr(store, "copy_from_local", None)
            if callable(copier):
                copier(archive, destination)
            else:
                store.write(destination, archive.read_bytes())
    return destination


@contextmanager
def materialise_bundle_archive(
    archive: Location,
    *,
    store: Store,
) -> Iterator[BuildBundle]:
    """Copy an archive locally and safely extract its validated bundle."""

    if not archive.name.endswith(ARCHIVE_SUFFIX):
        raise BuildError(f"not a Weaver bundle archive: {archive.value}")
    with tempfile.TemporaryDirectory(prefix="weaver-bundle-install-") as temporary:
        temporary_path = Path(temporary)
        local_archive = temporary_path / archive.name
        copier = getattr(store, "copy_to_local", None)
        if callable(copier):
            copier(archive, local_archive)
        else:
            local_archive.write_bytes(store.read(archive))
        root = temporary_path / "bundle"
        root.mkdir()
        _extract_archive(local_archive, root)
        local_store = FilesystemStore()
        yield load_bundle(Location(root.as_posix()), store=local_store)


def install_bundle_archive(
    archive: Location,
    *,
    archive_store: Store,
    session,
    workspace=None,
    executors=None,
) -> InstallationReport:
    with materialise_bundle_archive(archive, store=archive_store) as bundle:
        return Installer(session, workspace=workspace, executors=executors).install(
            bundle
        )


def build_item_repository(
    repository: WeaverRepository,
    *,
    bindings: ItemBindings,
    state: BuildState,
    session,
    workspace=None,
    source_store: Store,
    catalogue_binding: WarehouseBinding,
    output: Location | None = None,
    executors=None,
) -> ItemBuildResult:
    """Build and install, optionally retaining the generated bundle at ``output``."""

    installer = Installer(session, workspace=workspace, executors=executors)

    with tempfile.TemporaryDirectory(prefix="weaver-build-") as temporary:
        bundle = build_repository_bundle(
            repository,
            state=state,
            bindings=bindings,
            catalogue_binding=catalogue_binding,
            source_store=source_store,
            output=output or Location((Path(temporary) / "bundle").as_posix()),
        )
        report = installer.install(bundle)
        return ItemBuildResult(
            plan=bundle.plan,
            report=report,
            repository_signature=repository.signature,
            item_signatures={
                item.identity: item.signature for item in repository.items
            },
        )


def build_repository_bundle(
    repository: WeaverRepository,
    *,
    state: BuildState,
    bindings: ItemBindings,
    catalogue_binding: WarehouseBinding,
    source_store: Store,
    output: Location,
) -> BuildBundle:
    """Build a bundle without target access or mutation."""

    return Builder(
        repository=repository,
        state=state,
        bindings=bindings,
        catalogue_binding=catalogue_binding,
        source_store=source_store,
    ).build(output=output)


def build_item_repository_source(
    source: Location,
    *,
    source_store: Store,
    bindings: ItemBindings,
    session,
    workspace=None,
    catalogue_binding: WarehouseBinding,
    output: Location | None = None,
    sql_by_item=None,
    executors=None,
) -> ItemBuildResult:
    with prepare_repository(source, source_store=source_store) as prepared:
        repository = prepared.repository
        validate_build_request(
            repository, bindings, catalogue_binding=catalogue_binding
        )
        # The Builder needs stale claims, so reconciliation must happen there.
        state = read_build_state(
            bindings,
            required_catalogue_items=catalogue_items_for_build(repository, bindings),
            session=session,
            workspace=workspace,
            sql_by_item=sql_by_item,
            shortcuts=repository.shortcuts,
        )
        return build_item_repository(
            repository,
            bindings=bindings,
            state=state,
            session=session,
            workspace=workspace,
            source_store=prepared.store,
            catalogue_binding=catalogue_binding,
            output=output,
            executors=executors,
        )


def read_reconciled_catalogue(
    bindings: ItemBindings,
    *,
    inventories,
    session,
    workspace=None,
    repository=None,
) -> Reconciliation:
    """Read the Weaver catalogue and prove selected claims physically.

    External shortcut producers are read for freshness comparison (see
    :func:`~weaver.build_bundle.incremental.stale_through_shortcuts`).
    They have no inventory here, so their claims are not reconciled or written.
    """

    items = {binding.item for binding in bindings.entries}
    if repository is not None:
        items |= {
            shortcut.source.item
            for shortcut in repository.logical_shortcuts
            if shortcut.destination.item in items and shortcut.source.item not in items
        }

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise BuildError("every build needs a Workspace with a Weaver catalogue")
    from ..catalogue.connection import catalogue_connection

    state = read_catalogue_state(
        catalogue_connection(session, workspace), sorted(items, key=str)
    )
    return reconcile_catalogue_state(state, inventories=inventories)


def read_target_inventories(
    bindings: ItemBindings,
    *,
    session,
    workspace=None,
    sql_by_item=None,
) -> dict:
    supplied_sql = sql_by_item or {}
    workspace = workspace if workspace is not None else session.workspace
    inventories = {}
    delta = []

    # Warehouse inventories are separate TDS reads; Lakehouses share one substep.
    for binding in bindings.entries:
        target = binding.to_bound_target()
        if target.kind == WAREHOUSE_TARGET:
            with session.substep(f"Read {target.display} inventory"):
                sql = supplied_sql.get(binding.item)
                if sql is None:
                    if workspace is None:
                        raise BuildError(
                            f"Cannot inspect the Warehouse target for {binding.item} "
                            "without a Workspace. Pass the Workspace and retry."
                        )
                    from ..targets import WarehouseTarget

                    sql = session.sql_executor(
                        WarehouseTarget.parse(target.item_id), workspace=workspace
                    )
                inventories[binding.item] = read_warehouse_inventory(target, sql=sql)
        else:
            delta.append((binding.item, target))

    if delta:
        named = ", ".join(target.display for _item, target in delta)
        plural = "inventories" if len(delta) > 1 else "inventory"
        with session.substep(f"Read {named} {plural}"):
            observed = _lakehouse_inventories(
                [target for _item, target in delta],
                session=session,
                workspace=workspace,
            )
        for item, target in delta:
            inventories[item] = observed[target.id]
    return inventories


def _lakehouse_inventories(targets, *, session, workspace) -> dict:
    """Read Delta objects from storage and views from the Spark catalogue."""

    resolver = session.resolver(workspace)
    store = session.transport_store(workspace)
    return {
        target.id: read_lakehouse_inventory(
            target,
            resolver=resolver,
            store=store,
            catalogue=session_catalogue(session, workspace, ItemRef(target.item_id)),
        )
        for target in targets
    }


@contextmanager
def _temp_copy(
    source: Location,
    store: Store,
    *,
    prefix: str,
) -> Iterator[Path]:
    """Snapshot every source, including local ones, before parsing.

    The build reads only the copy, so caller edits cannot change the repository
    between parsing and bundle generation.
    """

    with materialise_tree(source, store=store, prefix=prefix) as tree:
        yield tree.location.path


def _write_archive(root: Path, destination: Path) -> None:
    with zipfile.ZipFile(
        destination, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as zipped:
        for path in sorted(
            candidate for candidate in root.rglob("*") if candidate.is_file()
        ):
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            zipped.writestr(info, path.read_bytes(), compresslevel=9)


def _extract_archive(archive: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive) as zipped:
        for info in zipped.infolist():
            path = PurePosixPath(info.filename)
            mode = info.external_attr >> 16
            if (
                path.is_absolute()
                or not path.parts
                or any(part in ("", ".", "..") for part in path.parts)
                or stat.S_ISLNK(mode)
            ):
                raise BuildError(f"unsafe path in bundle archive: {info.filename!r}")
        zipped.extractall(destination)
