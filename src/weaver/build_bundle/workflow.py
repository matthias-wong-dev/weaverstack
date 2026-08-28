"""Source preparation, state handover, bundle generation, and installation.

A repository source is independent of the target estate: remote sources are
materialised onto the local filesystem, and parsing and request validation
finish before any target state is read. The four stages then run in one process
whichever position that is, reaching the estate through Session capabilities.
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
    """A source tree copied onto the current process's local filesystem."""

    location: Location
    store: FilesystemStore


@dataclass(frozen=True)
class PreparedRepository:
    """A parsed repository and the process-local store that owns its files."""

    repository: WeaverRepository
    store: FilesystemStore


@dataclass(frozen=True)
class ItemBuildResult:
    """Durable in-memory result of a temporary in-environment build."""

    plan: BuildPlan
    report: InstallationReport
    repository_signature: str
    item_signatures: Mapping[WeaverItemId, str]

    @property
    def bundle_id(self) -> str:
        return self.plan.bundle_id


@dataclass(frozen=True)
class BuildState:
    """Authoritative target state handed from Fabric to a local planner."""

    catalogue: Catalogue
    target_inventories: Mapping[WeaverItemId, TargetInventory]
    #: Where each direct shortcut points, resolved while the estate was
    #: readable. Keyed by ``<owner>/<name>``.
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
    """Catalogue scope needed for selection and cross-item shortcut freshness."""

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
    """Validate repository-dependent input before any target is contacted."""

    if catalogue_binding is None:
        raise BuildError("every build needs an explicit catalogue Warehouse")
    if not bindings.entries:
        raise BuildError("at least one Weaver item must be bound")
    known = {item.identity for item in repository.items}
    unknown = set(bindings.by_item) - known
    if unknown:
        raise BuildError(
            "binding names item(s) absent from the repository: "
            + ", ".join(sorted(map(str, unknown)))
        )
    placed = {item for layer in repository.item_layers for item in layer}
    missing = set(bindings.by_item) - placed
    if missing:
        raise BuildError(
            "bound item(s) absent from the repository item graph: "
            + ", ".join(sorted(map(str, missing)))
        )
    from ..catalogue.builtin import BUILTIN_ITEM

    binding = bindings.by_item.get(BUILTIN_ITEM)
    if binding is not None and binding.target.kind != WAREHOUSE_TARGET:
        raise BuildError("Warehouse/_weaver requires a Warehouse binding")
    if binding is not None and binding.target.item.name != catalogue_binding.item.name:
        raise BuildError("Warehouse/_weaver must be bound to the catalogue Warehouse")
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
    """Read only the authoritative state a source-independent planner needs.

    The boundary between the physical estate and the Builder: the catalogue and
    every selected target, assembled into one handover a Builder takes directly
    and a test can construct without an estate.
    """

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise BuildError("every build needs a Workspace with a Weaver catalogue")

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
    _refuse_occupied_targets(bindings, catalogue)
    return BuildState(
        catalogue=catalogue,
        target_inventories=inventories,
        shortcut_sources=sources,
    )


def _refuse_occupied_targets(bindings: ItemBindings, catalogue) -> None:
    """Refuse a target the catalogue already installs a different item to.

    :func:`weaver.build_bundle.physical.item_prune_stage` diffs one item's
    keep-set against the whole target inventory, so building into a target
    holding another item's objects would prune them.

    ``Warehouse/_weaver`` is exempt both ways. Its inventory is read as the ``_``
    schema and nothing else, and every other item's excludes ``_``, so the
    catalogue shares a host with proven isolation. See
    :func:`weaver.build_bundle.prune.read_warehouse_inventory`.
    """

    from ..catalogue.builtin import BUILTIN_ITEM

    for binding in bindings.entries:
        if binding.item == BUILTIN_ITEM:
            continue
        target = binding.target.item.name
        others = sorted(
            str(item)
            for item in catalogue.bound_to(target)
            if item != binding.item and item != BUILTIN_ITEM
        )
        if others:
            raise BuildError(
                f"{binding.target.physical_kind}/{target} is installed to by "
                + ", ".join(others)
                + f", so {binding.item} cannot be built into it. Empty and "
                f"unbind it first, or give {binding.item} a physical target of "
                "its own"
            )


def _read_catalogue(*, session, workspace, required):
    """The catalogue a build plans against.

    The catalogue is Warehouse tables under ``_``, so reading it is T-SQL over
    TDS. The statements go through the Session and the rows are assembled here,
    in whichever position that is. Neither needs Spark.
    """

    from ..catalogue.connection import catalogue_connection

    return read_catalogue_state(catalogue_connection(session, workspace), required)


def session_catalogue(session, workspace, item: ItemRef):
    """Spark catalogue operations against one Lakehouse, through the Session.

    A destination Lakehouse's views live only in the Spark catalogue, so
    reading its inventory needs Spark. The Weaver catalogue does not come
    through here: it is a Warehouse, read over TDS.

    The one construction, both positions: in a session the statements run
    against its Spark, from a desktop they cross. Nothing above it can tell.
    """

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
    """Copy a store tree once to a temporary local directory.

    FabricStore uses one recursive ``notebookutils.fs.cp``. A generic Store
    falls back to one listing and one read per file, so the same contract is
    testable without Fabric.
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
    """A usable directory name for the snapshot of ``source``.

    ``.`` and ``..`` are ordinary ways to name a repository and neither can name
    a directory: joined onto the temporary root, either addresses the root
    itself. A filesystem source is resolved first, so the snapshot is named for
    the directory it copies. Anything leaving no final segment falls back to a
    fixed name; a snapshot's identity is its contents.
    """

    name = source.name if source.is_url else source.path.resolve().name
    return name if name and name not in (".", "..") else "repository"


@contextmanager
def prepare_repository(
    source: Location,
    *,
    source_store: Store,
) -> Iterator[PreparedRepository]:
    """Snapshot a repository to a temporary copy, then parse it completely."""

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
    """A sortable, collision-resistant physical name for an optional record."""

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
    """Persist a complete bundle as one deterministic ZIP file."""

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
    """Copy one archive locally, extract safely, and load its validated bundle."""

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
    """Install a handover archive entirely from its temporary local extraction."""

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
    """Decide, then install: a convenience over the two doers, not a third one.

    .. code-block:: text

        Repository + BuildState → Builder → BuildBundle → Installer

    Both halves are separately callable and testable; this exists so the common
    case reads as one call, and adds no decisions of its own.

    ``output`` places the generated bundle tree somewhere durable instead of the
    temporary directory. Only a caller that needs the bundle afterwards passes
    it.
    """

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
    """Build one durable bundle from a parsed repository and observed state.

    This is the boundary between planning and installation. It has
    no Session: target state is already represented by ``state`` and mutation
    belongs to :class:`Installer`.
    """

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
    """Prepare an explicit source independently from the target, then build it."""

    with prepare_repository(source, source_store=source_store) as prepared:
        repository = prepared.repository
        validate_build_request(
            repository, bindings, catalogue_binding=catalogue_binding
        )
        # The unreconciled catalogue. Reconciliation is a
        # decision and belongs to the Builder; handing it an already-reconciled
        # catalogue would hand it one whose stale claims had already been
        # removed, so the bundle would never be told to prune them.
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

    The read covers the bound items and, when a ``repository`` is given, the
    items that produce what those items shortcut. Those producers are not being
    built and nothing about them will be written. Their Registry rows carry the
    build that published them, and comparing that against the shortcut's own row
    is the only way to learn that a producer moved on while this consumer was not
    looking (see
    :func:`~weaver.build_bundle.incremental.stale_shortcut_destinations`).

    They are read without an inventory, so nothing about them is reconciled away:
    a build has no business proving claims about a target it was not pointed at.
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
    """Read every selected physical target into one planning snapshot."""

    supplied_sql = sql_by_item or {}
    workspace = workspace if workspace is not None else session.workspace
    inventories = {}
    delta = []

    # A Warehouse is its own read over TDS and gets its own Sub-step; the
    # Lakehouses are named together, as the one read they are. Each line says
    # what it is doing rather than only what it is doing it to, because children
    # print above the parent they belong to.
    for binding in bindings.entries:
        target = binding.to_bound_target()
        if target.kind == WAREHOUSE_TARGET:
            with session.substep(f"Read {target.display} inventory"):
                sql = supplied_sql.get(binding.item)
                if sql is None:
                    if workspace is None:
                        raise BuildError(
                            f"reading Warehouse inventory for {binding.item} needs a Workspace"
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
    """Every named Lakehouse's inventory.

    Mostly storage, since a Delta table is a directory, and Spark SQL for the
    views, which exist only in the catalogue.
    """

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
    """Always copy ``source`` to a temporary tree, whatever store holds it.

    There is no shortcut for a source that is already on this
    filesystem. A build that parsed the caller's own directory would be reading a
    tree the caller can still edit, so a repository could change between parsing
    and bundle generation, and the bundle would describe a source that never
    existed as a whole. Copying every source makes the snapshot the only thing
    the build ever reads, and makes that true identically in a notebook, on a
    desktop and over OneLake rather than only where the transport happened to
    force it.
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
