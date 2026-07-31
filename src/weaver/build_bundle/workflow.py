"""One in-environment item build, with optional archive handover.

The ordinary product path starts with a repository in OneLake and Weaver already
running inside Fabric. The session copies that repository once onto its driver,
then discovery, planning, snapshotting and installation use driver-local files.
Persisting a ZIP is optional and happens after generation or installation; it is
not a prerequisite for a development build.
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
from ..store import LocalStore, Store
from .bundle import BuildBundle, load_bundle
from .installer import InstallationEnvironment, install_bundle
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
    Reconciliation,
    read_catalogue_state,
    reconcile_catalogue_state,
)

ARCHIVE_SUFFIX = ".weaver.zip"


@dataclass(frozen=True)
class MaterialisedTree:
    """A source tree copied onto the current process's local filesystem."""

    location: Location
    store: LocalStore


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
        destination = Path(temporary) / source.name
        copier = getattr(store, "copy_to_local", None)
        if callable(copier):
            copier(source, destination)
        else:
            _copy_tree_through_store(source, store, destination)
        if not destination.is_dir():
            raise BuildError(
                f"materialising {source.value} did not create {destination}"
            )
        yield MaterialisedTree(Location(destination.as_posix()), LocalStore())


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
    with _local_tree(bundle.location, bundle_store, prefix="weaver-bundle-source-") as root:
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
        local_store = LocalStore()
        yield load_bundle(Location(root.as_posix()), store=local_store)


def install_bundle_archive(
    archive: Location,
    *,
    archive_store: Store,
    environment: InstallationEnvironment,
) -> InstallationReport:
    """Install a handover archive entirely from its temporary local extraction."""

    with materialise_bundle_archive(archive, store=archive_store) as bundle:
        return install_bundle(bundle, environment=environment)


def build_item_repository(
    repository: WeaverRepository,
    *,
    bindings: ItemBindings,
    target_inventories: Mapping[WeaverItemId, TargetInventory],
    reconciliation: Reconciliation,
    environment: InstallationEnvironment,
    source_store: Store,
    control_lakehouse: LakehouseBinding,
    archive: Location | None = None,
    archive_store: Store | None = None,
) -> ItemBuildResult:
    """Generate and install from already parsed source and already read state.

    This is the planner/executor seam.  It deliberately cannot materialise or
    parse authored files, inspect a Workspace, or discover target state.
    """

    with tempfile.TemporaryDirectory(prefix="weaver-build-") as temporary:
        bundle = generate_item_build_bundle(
            repository,
            bindings=bindings,
            output=Location((Path(temporary) / "bundle").as_posix()),
            store=source_store,
            target_inventories=target_inventories,
            catalogue=reconciliation.catalogue,
            stale_claims=reconciliation.stale_claims,
            control_lakehouse=control_lakehouse,
        )
        report = install_bundle(bundle, environment=environment)
        persisted = None
        if archive is not None:
            persisted = persist_bundle_archive(
                bundle,
                archive,
                store=archive_store or environment.store,
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
    environment: InstallationEnvironment,
    control_lakehouse: LakehouseBinding,
    archive: Location | None = None,
    archive_store: Store | None = None,
    sql_by_item=None,
) -> ItemBuildResult:
    """Materialise and prepare one uploaded repository, then build it."""

    with materialise_tree(repository_root, store=environment.store) as materialised:
        repository = parse_item_repository(materialised.location, store=materialised.store)
        inventories = read_target_inventories(
            bindings, environment=environment, sql_by_item=sql_by_item
        )
        reconciled = read_reconciled_catalogue(
            bindings,
            inventories=inventories,
            environment=environment,
            repository=repository,
        )
        return build_item_repository(
            repository,
            bindings=bindings,
            target_inventories=inventories,
            reconciliation=reconciled,
            environment=environment,
            source_store=materialised.store,
            control_lakehouse=control_lakehouse,
            archive=archive,
            archive_store=archive_store,
        )


def read_reconciled_catalogue(
    bindings: ItemBindings,
    *,
    inventories,
    environment: InstallationEnvironment,
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

    if environment.spark is None:
        raise BuildError("every build needs Spark to read and publish the catalogue")
    workspace = environment.workspace
    if workspace is None or not workspace.weaver_lakehouse:
        raise BuildError("every build needs a Workspace with a Weaver Lakehouse")
    from ..spark import SparkCatalogue
    from ..targets import ItemRef

    catalogue = SparkCatalogue(
        environment.spark,
        environment.resolver.spark_destination(ItemRef(workspace.weaver_lakehouse)),
    )
    state = read_catalogue_state(catalogue, sorted(items, key=str))
    return reconcile_catalogue_state(state, inventories=inventories)


def read_target_inventories(
    bindings: ItemBindings,
    *,
    environment: InstallationEnvironment,
    sql_by_item=None,
) -> dict:
    """Read every selected physical target before planning begins."""

    supplied_sql = sql_by_item or {}
    inventories = {}
    owned = []
    try:
        for binding in bindings.entries:
            target = binding.to_bound_target()
            if target.kind == WAREHOUSE_TARGET:
                sql = supplied_sql.get(binding.item)
                if sql is None:
                    if environment.workspace is None:
                        raise BuildError(
                            f"reading Warehouse inventory for {binding.item} needs a Workspace"
                        )
                    from ..fabric.sql import fabric_sql_executor
                    from ..targets import WarehouseTarget

                    sql = fabric_sql_executor(
                        WarehouseTarget.parse(target.item_id), environment.workspace
                    )
                    owned.append(sql)
                inventories[binding.item] = read_warehouse_inventory(target, sql=sql)
            else:
                inventories[binding.item] = read_lakehouse_inventory(
                    target,
                    resolver=environment.resolver,
                    store=environment.store,
                    spark=environment.spark,
                )
        return inventories
    finally:
        for sql in owned:
            if hasattr(sql, "close"):
                sql.close()


@contextmanager
def _local_tree(
    source: Location,
    store: Store,
    *,
    prefix: str,
) -> Iterator[Path]:
    if isinstance(store, LocalStore) and not source.is_url:
        yield source.path
        return
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
