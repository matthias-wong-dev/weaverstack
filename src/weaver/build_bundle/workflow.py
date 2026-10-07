"""Build orchestration from source snapshot through installation.

Source parsing and request validation finish before target state is read.
"""

from __future__ import annotations

import shutil
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
from ..declaration.model import SEMANTIC_MODEL, WeaverItemId, WeaverRepository
from ..declaration.repository import parse_item_repository
from ..errors import BuildError, DiscoveryError
from ..locations import Location
from ..mutation.models import MutationPlan
from ..store import FilesystemStore, Store
from ..targets import ItemRef
from .builder import Builder
from .bundle import BuildBundle, load_bundle
from .execution import ExecutionIdentity, resolve_execution_identity
from .execution_plan import execute_bundle
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
    LAKEHOUSE_TARGET,
    SEMANTIC_MODEL_TARGET,
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
    plan: MutationPlan
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
    semantic_sources: Mapping[str, dict] = field(default_factory=dict)
    semantic_expressions: Mapping[str, dict] = field(default_factory=dict)

    def to_mapping(self) -> dict[str, object]:
        return {
            "format_version": 1,
            "catalogue": self.catalogue.to_mapping(),
            "semantic_sources": dict(self.semantic_sources),
            "semantic_expressions": dict(self.semantic_expressions),
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
            semantic_sources=mapping.get("semantic_sources", {}),
            semantic_expressions=mapping.get("semantic_expressions", {}),
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
    from ..semantic_models.references import source_identity

    items.update(
        source_identity(reference).item
        for item, contribution in repository.semantic_models.items()
        if item in bound
        for reference in contribution.source_references.values()
    )
    items.update(
        c.model
        for item, c in repository.reports.items()
        if item in bound and c.model is not None
    )
    return tuple(sorted(items, key=str))


def require_catalogue_for(bindings: ItemBindings) -> None:
    """Refuse items that need a catalogue; semantic models build without one."""

    others = sorted(
        str(item)
        for item in bindings.by_item
        if item.item_type not in {SEMANTIC_MODEL, "Report"}
    )
    if others or not bindings.entries:
        raise BuildError(
            "Building "
            + (", ".join(others) or "this selection")
            + " needs a Weaver catalogue: pass catalogue='Warehouse/Weaver', or "
            "give one in workspace configuration. Semantic models build without one"
        )


def _require_separate_semantic_build(bindings: ItemBindings) -> None:
    from ..catalogue.builtin import BUILTIN_ITEM

    powerbi = sorted(
        str(item)
        for item in bindings.by_item
        if item.item_type in {SEMANTIC_MODEL, "Report"}
    )
    sources = sorted(
        str(item)
        for item in bindings.by_item
        if item.item_type not in {SEMANTIC_MODEL, "Report"} and item != BUILTIN_ITEM
    )
    if powerbi and sources:
        raise BuildError(
            "Power BI items must be built as a separate step. Build "
            f"{', '.join(sources)}, then {', '.join(powerbi)}"
        )


def validate_build_request(
    repository: WeaverRepository,
    bindings: ItemBindings,
    *,
    catalogue_binding: WarehouseBinding | None,
) -> tuple[WeaverItemId, ...]:
    _require_separate_semantic_build(bindings)

    if catalogue_binding is None:
        require_catalogue_for(bindings)
        validated = sorted(
            str(item.identity)
            for item in repository.items
            if item.identity in bindings.by_item and item.validations
        )
        if validated:
            raise BuildError(
                f"{', '.join(validated)} declares tests or assumptions, which need "
                "a Weaver catalogue: pass catalogue='Warehouse/Weaver', or give one "
                "in workspace configuration"
            )
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
    repository=None,
) -> BuildState:
    """Read the catalogue and selected target state for build planning."""

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None:
        raise BuildError("every build needs a Workspace")
    catalogued = bool(workspace.catalogue)
    if not catalogued:
        require_catalogue_for(bindings)
        from ..semantic_models.fragments import source_table

        references = sorted(
            str(item)
            for item, contribution in (
                repository.semantic_models if repository else {}
            ).items()
            if item in bindings.by_item
            and any(
                not source_table(contribution.parts, name).get("partitions")
                for name in contribution.source_references
            )
        )
        if references:
            raise BuildError(
                f"{references[0]} generates tables from managed sources, which needs "
                "a Weaver catalogue: pass catalogue='Warehouse/Weaver', or give one "
                "in workspace configuration"
            )

    # Check occupancy before the slower per-target inventory and Spark reads.
    occupancy = {}
    if catalogued:
        with session.step("Check target occupancy"):
            occupancy = _refuse_occupied_targets(
                bindings, session=session, workspace=workspace
            )
    semantic_expressions = {}
    required = set(required_catalogue_items)
    if repository is not None:
        from ..semantic_models.expressions import read_expression_sources

        with session.step("Resolve semantic data sources"):
            semantic_expressions = read_expression_sources(
                repository, bindings, session=session, workspace=workspace
            )
        for expressions in semantic_expressions.values():
            for source in expressions.values():
                key = (source["item_type"].casefold(), source["item_name"].casefold())
                aliases = set(occupancy.get(key, ()))
                aliases.update(
                    binding.item
                    for binding in bindings.entries
                    if (
                        binding.target.physical_kind.casefold(),
                        binding.target.item.name.casefold(),
                    )
                    == key
                )
                required.update(aliases)
                source["logical_items"] = sorted(str(item) for item in aliases)
    # Without a catalogue nothing is installed, so every selected model deploys.
    catalogue = Catalogue({})
    if catalogued:
        with session.step("Read catalogue"):
            catalogue = _read_catalogue(
                session=session,
                workspace=workspace,
                required=tuple(sorted(required, key=str)),
            )
    if repository is not None:
        from ..catalogue.tables import INSTALLATION

        for model in sorted(
            {
                c.model
                for i, c in repository.reports.items()
                if i in bindings.by_item
                and c.model is not None
                and c.model not in bindings.by_item
            },
            key=str,
        ):
            rows = catalogue.rows.get(model, {}).get(INSTALLATION.name, ())
            if len(rows) != 1:
                raise BuildError(f"{model} has no installed binding; build it first")
            row = rows[0]
            resolved = session.resolve_item(
                row["target_name"], item_type="SemanticModel", workspace=workspace
            )
            if (resolved.workspace_id, resolved.id) != (
                row.get("workspace_id"),
                row.get("item_id"),
            ):
                raise BuildError(
                    f"{model} binding has changed; rebuild the model first"
                )
    with session.step("Read target inventories"):
        inventories = read_target_inventories(
            bindings,
            session=session,
            workspace=workspace,
            sql_by_item=sql_by_item,
            catalogue=catalogue,
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
    semantic_sources = {}
    if repository is not None:
        from ..semantic_models.lineage import managed_relations
        from .semantic_sources import read_semantic_sources

        for expressions in semantic_expressions.values():
            for source in expressions.values():
                source["relations"] = managed_relations(
                    repository, catalogue, bindings, source
                )
        with session.step("Read semantic sources"):
            semantic_sources = read_semantic_sources(
                repository,
                bindings,
                catalogue,
                session=session,
                workspace=workspace,
                inventories=inventories,
            )
    return BuildState(
        catalogue=catalogue,
        target_inventories=inventories,
        shortcut_sources=sources,
        semantic_sources=semantic_sources,
        semantic_expressions=semantic_expressions,
    )


def _refuse_occupied_targets(bindings: ItemBindings, *, session, workspace):
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
        return {}
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

    return occupancy


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
        # Only the copy. Wrapping the yield as well would report a failure in
        # whatever reads the snapshot as a failure to take it.
        try:
            copier = getattr(store, "copy_to_local", None)
            if callable(copier):
                copier(source, destination)
            else:
                _copy_tree_through_store(source, store, destination)
        # A source file that cannot be read is a source failure, so it is
        # rendered and retried like every other one an offline check reports.
        except shutil.Error as exc:
            raise DiscoveryError(_copy_failures(source, exc)) from exc
        except OSError as exc:
            raise DiscoveryError(
                f"{source.value} could not be read: {_os_failure(source, exc)}"
            ) from exc
        if not destination.is_dir():
            raise BuildError(
                f"materialising {source.value} did not create {destination}"
            )
        yield MaterialisedTree(Location(destination.as_posix()), FilesystemStore())


#: How many failing files a copy error names before it stops listing them.
COPY_FAILURE_LIMIT = 5


def _copy_failures(source: Location, exc: shutil.Error) -> str:
    """Name the files a recursive copy could not take, bounded.

    ``shutil`` collects every failure of a tree copy into one error. Rendering
    all of them would bury the first, which is usually the one to fix.
    """

    failures = exc.args[0] if exc.args and isinstance(exc.args[0], list) else []
    if not failures:
        return f"{source.value} could not be copied: {exc}"
    named = [
        f"  {_relative(source, str(entry[0]))}: {entry[-1]}"
        for entry in failures[:COPY_FAILURE_LIMIT]
    ]
    omitted = len(failures) - len(named)
    if omitted > 0:
        named.append(f"  ... and {omitted} more file(s)")
    return f"{source.value} could not be copied:\n" + "\n".join(named)


def _os_failure(source: Location, exc: OSError) -> str:
    named = getattr(exc, "filename", None)
    detail = exc.strerror or str(exc)
    return f"{_relative(source, named)}: {detail}" if named else detail


def _relative(source: Location, path: str | None) -> str:
    """A failing file as the project names it, never as the snapshot does."""

    if not path:
        return source.value
    root = source.value.rstrip("/")
    text = str(path).replace("\\", "/")
    if text.startswith(root + "/"):
        return text[len(root) + 1 :]
    return text


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
    executors=None,
) -> InstallationReport:
    """Install an archive against the execution context it froze."""

    with materialise_bundle_archive(archive, store=archive_store) as bundle:
        return execute_bundle(bundle, session, executors=executors)


def build_item_repository(
    repository: WeaverRepository,
    *,
    bindings: ItemBindings,
    state: BuildState,
    session,
    workspace=None,
    source_store: Store,
    catalogue_binding: WarehouseBinding,
    execution: ExecutionIdentity | None = None,
    output: Location | None = None,
    executors=None,
) -> ItemBuildResult:
    """Build and install, optionally retaining the generated bundle at ``output``."""

    with tempfile.TemporaryDirectory(prefix="weaver-build-") as temporary:
        bundle = build_repository_bundle(
            repository,
            state=state,
            bindings=bindings,
            catalogue_binding=catalogue_binding,
            execution=execution
            if execution is not None
            else resolve_execution_identity(
                workspace if workspace is not None else session.workspace,
                session=session,
            ),
            source_store=source_store,
            output=output or Location((Path(temporary) / "bundle").as_posix()),
        )
        report = execute_bundle(bundle, session, executors=executors)
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
    execution: ExecutionIdentity,
    source_store: Store,
    output: Location,
) -> BuildBundle:
    """Build a bundle without target access or mutation."""

    return Builder(
        repository=repository,
        state=state,
        bindings=bindings,
        catalogue_binding=catalogue_binding,
        execution=execution,
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
        from ..semantic_models.binding import begin_semantic_sources

        repository = begin_semantic_sources(prepared.repository, bindings.by_item)
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
            repository=repository,
        )
        return build_item_repository(
            repository,
            bindings=bindings,
            state=state,
            session=session,
            workspace=workspace,
            source_store=prepared.store,
            catalogue_binding=catalogue_binding,
            execution=resolve_execution_identity(
                workspace if workspace is not None else session.workspace,
                session=session,
            ),
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
    catalogue=None,
) -> dict:
    """Read each bound target's physical inventory.

    ``catalogue`` names the views Weaver recorded installing in each Lakehouse,
    so the inventory resolves only the relations it does not account for.
    """

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
        elif target.kind in {SEMANTIC_MODEL_TARGET, "report"}:
            with session.substep(f"Resolve {target.display}"):
                resolved = session.resolve_item(
                    target.name,
                    item_type="Report" if target.kind == "report" else "SemanticModel",
                    workspace=workspace,
                )
                inventories[binding.item] = TargetInventory(
                    target.id,
                    target.kind,
                    target.name,
                    workspace_id=resolved.workspace_id,
                    item_id=resolved.id,
                )
        elif target.kind == LAKEHOUSE_TARGET:
            delta.append((binding.item, target))
        else:
            raise BuildError(f"Unsupported target kind {target.kind!r}")

    if delta:
        named = ", ".join(target.display for _item, target in delta)
        plural = "inventories" if len(delta) > 1 else "inventory"
        with session.substep(f"Read {named} {plural}"):
            observed = _lakehouse_inventories(
                [target for _item, target in delta],
                session=session,
                workspace=workspace,
                known_views={
                    target.id: recorded_views(catalogue, item) for item, target in delta
                },
            )
        for item, target in delta:
            inventories[item] = observed[target.id]
    return inventories


def _lakehouse_inventories(targets, *, session, workspace, known_views) -> dict:
    """Read Delta objects from storage and views from the Spark catalogue."""

    resolver = session.resolver(workspace)
    store = session.transport_store(workspace)
    return {
        target.id: read_lakehouse_inventory(
            target,
            resolver=resolver,
            store=store,
            catalogue=session_catalogue(session, workspace, ItemRef(target.item_id)),
            known_views=known_views[target.id],
        )
        for target in targets
    }


def recorded_views(catalogue, item) -> frozenset[str]:
    """``schema.name`` of each view the catalogue records for ``item``."""

    if catalogue is None:
        return frozenset()
    registered = (
        identity
        for identity, document in catalogue.registered.items()
        if document.object_type == "view"
    )
    borrowed = (
        identity
        for identity, mirrored in catalogue.mirrors.items()
        if mirrored.physical_type == "view"
    )
    return frozenset(
        identity.object_id.qualified
        for identity in (*registered, *borrowed)
        if identity.item == item
    )


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
