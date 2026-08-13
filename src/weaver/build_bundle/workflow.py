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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Iterator, Mapping

from ..errors import BuildError
from ..locations import Location
from ..declaration.model import WeaverItemId, WeaverRepository
from ..declaration.repository import parse_item_repository
from ..store import FilesystemStore, Store
from ..targets import ItemRef
from .bundle import BuildBundle, load_bundle
from .builder import Builder
from .installer import Installer
from .planner import generate_item_build_bundle
from .models import BuildPlan
from .report import InstallationReport
from .targets import ItemBindings, LakehouseBinding
from .targets import WAREHOUSE_TARGET
from .prune import (
    TargetInventory,
    read_lakehouse_inventory,
    read_warehouse_inventory,
)
from ..catalogue.state import (
    Catalogue,
    Reconciliation,
    read_catalogue_state,
    reconcile_catalogue_state,
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
    archive: Location | None = None

    @property
    def bundle_id(self) -> str:
        return self.plan.bundle_id


@dataclass(frozen=True)
class BuildState:
    """Authoritative target state handed from Fabric to a local planner."""

    catalogue: Catalogue
    target_inventories: Mapping[WeaverItemId, TargetInventory]

    def to_mapping(self) -> dict[str, object]:
        return {
            "format_version": 1,
            "catalogue": self.catalogue.to_mapping(),
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
        )


def catalogue_items_for_build(
    repository: WeaverRepository, bindings: ItemBindings
) -> tuple[WeaverItemId, ...]:
    """Catalogue scope needed for selection and cross-item alias freshness."""

    bound = set(bindings.by_item)
    items = bound | {
        alias.source.item
        for alias in repository.aliases
        if alias.destination.item in bound and alias.source.item not in bound
    }
    return tuple(sorted(items, key=str))


def validate_build_request(
    repository: WeaverRepository,
    bindings: ItemBindings,
    *,
    control_lakehouse: LakehouseBinding,
) -> tuple[WeaverItemId, ...]:
    """Validate repository-dependent input before any target is contacted."""

    if control_lakehouse is None:
        raise BuildError("every build needs an explicit control-plane Lakehouse")
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
    builtin = WeaverItemId.parse("Lakehouse/_weaver")
    binding = bindings.by_item.get(builtin)
    if binding is not None and not isinstance(binding.target, LakehouseBinding):
        raise BuildError("Lakehouse/_weaver requires a Lakehouse binding")
    if (
        binding is not None
        and binding.target.lakehouse.name != control_lakehouse.lakehouse.name
    ):
        raise BuildError(
            "Lakehouse/_weaver must be bound to the explicit control-plane Lakehouse"
        )
    return catalogue_items_for_build(repository, bindings)


def read_build_state(
    bindings: ItemBindings,
    *,
    required_catalogue_items,
    session,
    workspace=None,
    sql_by_item=None,
) -> BuildState:
    """Read only the authoritative state a source-independent planner needs.

    The boundary between the physical estate and the Builder: two reads — the
    catalogue and every selected target — assembled into one Python handover
    that a Builder can be given directly, and that a test can construct without
    an estate at all.
    """

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.weaver_lakehouse:
        raise BuildError("every build needs a Workspace with a Weaver Lakehouse")

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
    return BuildState(catalogue=catalogue, target_inventories=inventories)


def _read_catalogue(*, session, workspace, required):
    """The catalogue a build plans against.

    The catalogue is Delta tables in the Weaver Lakehouse, so reading it is
    Spark SQL. The statements go through the Session and the rows are assembled
    here, in whichever position that is.
    """

    return read_catalogue_state(
        session_catalogue(
            session, workspace, ItemRef(workspace.weaver_lakehouse)
        ),
        required,
    )


def session_catalogue(session, workspace, item: ItemRef):
    """Catalogue operations against one Lakehouse, run through the Session.

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

    FabricStore uses one recursive ``notebookutils.fs.cp`` operation. A generic
    Store falls back to one listing and exactly one read per file, which makes the
    same contract testable without Fabric.
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

    ``.`` and ``..`` are ordinary ways to name a repository on a desktop and
    neither can name a directory: joining either onto the temporary root would
    address the root itself, and the copy would fail or land in the wrong place.
    A filesystem source is therefore resolved first, so the snapshot is named for
    the directory it actually copies rather than for the way the caller spelled
    it. A filesystem root, and anything else that leaves no final segment, falls
    back to a fixed name — the snapshot's identity is its contents, not its name.
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
    with _temp_copy(bundle.location, bundle_store, prefix="weaver-bundle-source-") as root:
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
    control_lakehouse: LakehouseBinding,
    archive: Location | None = None,
    archive_store: Store | None = None,
    output: Location | None = None,
    executors=None,
) -> ItemBuildResult:
    """Decide, then install — a convenience over the two doers, not a third one.

    .. code-block:: text

        Repository + BuildState → Builder → BuildBundle → Installer

    Both halves are separately callable and separately testable; this exists so
    the common case reads as one call. It adds no decisions of its own, which is
    what stops it becoming a hidden third architecture.

    ``output`` places the generated bundle tree somewhere durable instead of the
    temporary directory this otherwise uses. Only a caller that wants the bundle
    afterwards passes it; the build itself does not care where it sat.
    """

    builder = Builder(
        repository=repository,
        state=state,
        bindings=bindings,
        control_lakehouse=control_lakehouse,
        source_store=source_store,
    )
    installer = Installer(session, workspace=workspace, executors=executors)

    with tempfile.TemporaryDirectory(prefix="weaver-build-") as temporary:
        bundle = builder.build(
            output=output or Location((Path(temporary) / "bundle").as_posix())
        )
        report = installer.install(bundle)
        persisted = None
        if archive is not None:
            persisted = persist_bundle_archive(
                bundle,
                archive,
                store=archive_store or installer.store,
            )
        return ItemBuildResult(
            plan=bundle.plan,
            report=report,
            repository_signature=repository.signature,
            item_signatures={item.identity: item.signature for item in repository.items},
            archive=persisted,
        )


def build_uploaded_item_repository(
    repository_root: Location,
    *,
    bindings: ItemBindings,
    session,
    workspace=None,
    control_lakehouse: LakehouseBinding,
    archive: Location | None = None,
    archive_store: Store | None = None,
    sql_by_item=None,
    executors=None,
) -> ItemBuildResult:
    """Compatibility wrapper for a repository stored with the target estate."""

    return build_item_repository_source(
        repository_root,
        source_store=session.store(workspace or session.workspace),
        bindings=bindings,
        session=session,
        workspace=workspace,
        control_lakehouse=control_lakehouse,
        archive=archive,
        archive_store=archive_store,
        sql_by_item=sql_by_item,
        executors=executors,
    )


def build_item_repository_source(
    source: Location,
    *,
    source_store: Store,
    bindings: ItemBindings,
    session,
    workspace=None,
    control_lakehouse: LakehouseBinding,
    archive: Location | None = None,
    archive_store: Store | None = None,
    output: Location | None = None,
    sql_by_item=None,
    executors=None,
) -> ItemBuildResult:
    """Prepare an explicit source independently from the target, then build it."""

    with prepare_repository(source, source_store=source_store) as prepared:
        repository = prepared.repository
        validate_build_request(
            repository, bindings, control_lakehouse=control_lakehouse
        )
        # The *unreconciled* catalogue, deliberately. Reconciliation is a
        # decision and belongs to the Builder; handing it an already-reconciled
        # catalogue would hand it one whose stale claims had already been
        # removed, so the bundle would never be told to prune them.
        state = read_build_state(
            bindings,
            required_catalogue_items=catalogue_items_for_build(repository, bindings),
            session=session,
            workspace=workspace,
            sql_by_item=sql_by_item,
        )
        return build_item_repository(
            repository,
            bindings=bindings,
            state=state,
            session=session,
            workspace=workspace,
            source_store=prepared.store,
            control_lakehouse=control_lakehouse,
            archive=archive,
            archive_store=archive_store,
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
    """Read the Weaver Lakehouse catalogue and prove selected claims physically.

    The read covers the bound items and, when a ``repository`` is given, the
    items that *produce* what those items alias. Those producers are not being
    built and nothing about them will be written — but their Registry rows carry
    the build that published them, and comparing that against the alias's own row
    is the only way to learn that a producer moved on while this consumer was not
    looking (see
    :func:`~weaver.build_bundle.incremental.stale_alias_destinations`).

    They are read without an inventory, so nothing about them is reconciled away:
    a build has no business proving claims about a target it was not pointed at.
    """

    items = {binding.item for binding in bindings.entries}
    if repository is not None:
        items |= {
            alias.source.item
            for alias in repository.aliases
            if alias.destination.item in items and alias.source.item not in items
        }

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.weaver_lakehouse:
        raise BuildError("every build needs a Workspace with a Weaver Lakehouse")
    from ..spark import SparkCatalogue
    from ..targets import ItemRef

    catalogue = SparkCatalogue(
        session.spark(workspace),
        session.resolver(workspace).spark_destination(
            ItemRef(workspace.weaver_lakehouse)
        ),
    )
    state = read_catalogue_state(catalogue, sorted(items, key=str))
    return reconcile_catalogue_state(state, inventories=inventories)


def read_target_inventories(
    bindings: ItemBindings,
    *,
    session,
    workspace=None,
    sql_by_item=None,
) -> dict:
    """Read every selected physical target before planning begins.

    A boundary read: physical target to :class:`TargetInventory`, once, so that
    everything above it decides against a snapshot rather than against state
    that is still moving. Its capabilities come from the Session, which owns
    them and closes them — this used to open a TDS connection per Warehouse and
    close it again, which a build following a wipe paid for twice.
    """

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

    Mostly storage — a Delta table is a directory — and Spark SQL for the views,
    which exist only in the catalogue.
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

    There is deliberately no shortcut for a source that is already on this
    filesystem. A build that parsed the caller's own directory would be reading a
    tree the caller can still edit — so a repository could change between parsing
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
        for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
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
