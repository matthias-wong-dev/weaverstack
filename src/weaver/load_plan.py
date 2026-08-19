"""Build the physical load graph from installed catalogue state.

The graph contains selected targets, load dependencies, and required endpoint
refresh barriers.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Mapping, Sequence

from .catalogue.state import Catalogue
from .catalogue.tables import DEPENDENCY, INSTALLATION, SHORTCUT
from .declaration.item_dependencies import SHORTCUTS_MODULE
from .declaration.metadata import ObjectId
from .declaration.model import (
    FILE_SHAPE,
    LAKEHOUSE,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
)
from .errors import LoadError
from .etl import LOAD_ROOT, load_procedure_id
from .load_report import DEPENDENCY_EXTERNAL, LoadMessage, info
from .targets import LAKEHOUSE_KIND, WAREHOUSE_KIND

# --- the primitive kinds ------------------------------------------------------
#
# What an installed load *is*, in the vocabulary dispatch branches on. Four
# values, three of them a real installed artefact and one a barrier the planner
# inserts. They are strings rather than a class hierarchy because they cross into
# a plan file and a task log, where a reader needs to see the word.
#
# There is no kind for a Spark-SQL-authored table, deliberately. It installs as
# a deployed ``SparkSqlTable`` module and dispatches as ``python_table``, so the
# language it was authored in is a fact about its *declaration* — recorded in the
# catalogue, where a reader can ask — and not about how it runs. A kind that said
# otherwise would be orchestration knowing something it must not act on.

WAREHOUSE_PROCEDURE = "warehouse_procedure"
PYTHON_TABLE = "python_table"
PYTHON_FOLDER = "python_folder"
ENDPOINT_REFRESH = "endpoint_refresh"

PRIMITIVE_KINDS = (
    WAREHOUSE_PROCEDURE,
    PYTHON_TABLE,
    PYTHON_FOLDER,
    ENDPOINT_REFRESH,
)

#: What the catalogue calls each physical target kind. The same two words the
#: build's :mod:`weaver.build_bundle.targets` uses, because a load plan and a
#: build bundle describe the same estate.
LAKEHOUSE_TARGET = "lakehouse"
WAREHOUSE_TARGET = "warehouse"

_TARGET_KIND_FOR_ITEM = {LAKEHOUSE: LAKEHOUSE_TARGET, WAREHOUSE: WAREHOUSE_TARGET}
_GRAMMAR_KIND = {LAKEHOUSE_TARGET: LAKEHOUSE_KIND, WAREHOUSE_TARGET: WAREHOUSE_KIND}


@dataclass(frozen=True)
class PhysicalTargetRef:
    """One physical item, as the public grammar names it."""

    kind: str
    name: str

    @classmethod
    def of(cls, target) -> "PhysicalTargetRef":
        """The reference one typed physical target makes.

        The single conversion from the typed vocabulary — ``DeltaTarget`` and
        ``WarehouseTarget`` — into the two words a plan and a catalogue row
        carry. Every operation that names a target goes through it.
        """

        from .targets import DeltaTarget, physical_item

        return cls(
            kind=LAKEHOUSE_TARGET
            if isinstance(target, DeltaTarget)
            else WAREHOUSE_TARGET,
            name=physical_item(target).name,
        )

    def __str__(self) -> str:
        return f"{_GRAMMAR_KIND[self.kind]}/{self.name}"

    @property
    def is_lakehouse(self) -> bool:
        return self.kind == LAKEHOUSE_TARGET


def lakehouse_names(targets) -> tuple[str, ...]:
    """The Lakehouse names among some :class:`PhysicalTargetRef`, in order given.

    What a Livy session needs a Lakehouse for is somewhere to attach, so which
    of them is picked does not matter: every generated statement names its own
    target in full.
    """

    return tuple(target.name for target in targets if target.is_lakehouse)


@dataclass(frozen=True)
class PhysicalObjectRef:
    """One installed object, addressed as its physical target holds it.

    ``schema`` is the catalogue's ``schema_name`` unchanged: for a folder that
    carries its ``Files/`` prefix, and for a deployed file it is the path
    beneath ``Files``. Keeping the stored spelling lets a reference go straight
    to :meth:`weaver.build_bundle.prune.TargetInventory.has_object`.
    """

    target_id: str
    target_kind: str
    schema: str
    object: str
    object_type: str
    shape: str | None = None

    def __str__(self) -> str:
        return f"{self.schema}.{self.object}"


@dataclass(frozen=True)
class InstalledObject:
    """One certified Registry row, as load planning reads it."""

    identity: WeaverDocumentId
    object_type: str
    target: PhysicalTargetRef

    @property
    def physical(self) -> PhysicalObjectRef:
        from .catalogue.claims import catalogue_schema

        return PhysicalObjectRef(
            target_id=self.target.name,
            target_kind=self.target.kind,
            schema=catalogue_schema(self.identity),
            object=self.identity.object_id.object,
            object_type=self.object_type,
            shape=self.identity.shape,
        )


@dataclass(frozen=True)
class InstalledShortcut:
    """One logical shortcut a consuming item installed, as the catalogue kept it.

    Only the logical ones. A load reads this to learn which producer a name
    stands for, and a physical shortcut has no producer in the estate: it points
    at an item Weaver does not manage.
    """

    destination: WeaverDocumentId
    source: WeaverDocumentId


@dataclass(frozen=True)
class InstalledDependency:
    """One dependency edge, with the reference exactly as its author wrote it."""

    consumer: WeaverDocumentId
    reference: str
    is_within_item: bool


@dataclass(frozen=True)
class InstalledEstate:
    """The installed catalogue, reversed into what load planning asks of it.

    Built from a :class:`Catalogue`, which a test can hand-write and production
    reads over Spark. Everything below this class is arithmetic on these five
    mappings.
    """

    installations: Mapping[WeaverItemId, PhysicalTargetRef]
    objects: Mapping[WeaverDocumentId, InstalledObject]
    primitives: Mapping[WeaverDocumentId, InstalledObject]
    dependencies: tuple[InstalledDependency, ...]
    shortcuts: tuple[InstalledShortcut, ...]
    #: Physical addresses two logical objects both claim, by the target they are
    #: in. Recorded rather than raised — see :meth:`from_catalogue`.
    ambiguous: Mapping[PhysicalTargetRef, tuple[str, ...]] = field(default_factory=dict)

    @classmethod
    def from_catalogue(cls, catalogue: Catalogue) -> "InstalledEstate":
        """Reverse the whole catalogue, recording ambiguity rather than refusing it.

        Two logical objects at one physical address is a real fault, but an
        estate accumulates Registry rows from every item ever bound to a target,
        so a stale duplicate claim can outlive its binding. Refusing here would
        let it stop a load of an unrelated target.

        The finding is kept instead, and :func:`load_dag` refuses when it
        touches the request.
        """

        installations = _installations(catalogue)
        objects: dict[WeaverDocumentId, InstalledObject] = {}
        primitives: dict[WeaverDocumentId, InstalledObject] = {}
        physical_owner: dict[tuple, WeaverDocumentId] = {}
        ambiguous: dict[PhysicalTargetRef, list[str]] = {}
        for identity, document in sorted(
            catalogue.registered.items(), key=lambda pair: str(pair[0])
        ):
            target = installations.get(identity.item)
            if target is None:
                # Registry without Installation: the estate says an object is
                # certified but not where it lives. Refused here rather than
                # skipped, because skipping it would silently shrink the graph.
                raise LoadError(
                    f"{identity} is registered but {identity.item} has no "
                    "installation row, so its physical target is unknown"
                )
            installed = InstalledObject(identity, document.object_type, target)
            where = installed.physical
            key = (
                where.target_kind,
                where.target_id.casefold(),
                where.schema.casefold(),
                where.object.casefold(),
                where.object_type,
            )
            owner = physical_owner.get(key)
            if owner is not None:
                ambiguous.setdefault(target, []).append(
                    f"{owner} and {identity} both resolve to {where}"
                )
            else:
                physical_owner[key] = identity
            # What an installed artefact is *for*, from the Registry row that
            # said so — never from its physical shape. A Test compiles to a file
            # or a procedure exactly as a load does, so shape inference would
            # walk validation straight into the load DAG.
            if document.is_runtime_artefact:
                primitives[identity] = installed
            else:
                objects[identity] = installed
        return cls(
            installations=MappingProxyType(installations),
            objects=MappingProxyType(objects),
            primitives=MappingProxyType(primitives),
            dependencies=_dependencies(catalogue),
            shortcuts=_shortcuts(catalogue),
            ambiguous=MappingProxyType(
                {target: tuple(found) for target, found in ambiguous.items()}
            ),
        )

    def target_for(self, item: WeaverItemId) -> PhysicalTargetRef:
        target = self.installations.get(item)
        if target is None:
            raise LoadError(f"{item} has no installation row in the catalogue")
        return target

    @property
    def targets(self) -> tuple[PhysicalTargetRef, ...]:
        return tuple(
            sorted(
                set(self.installations.values()), key=lambda ref: (ref.kind, ref.name)
            )
        )


def _installations(catalogue: Catalogue) -> dict[WeaverItemId, PhysicalTargetRef]:
    """Each logical item's bound physical target, keyed for reverse lookup.

    Several logical items may name one physical target, which is not an error: a
    request names a target and means everything installed there, answerable so
    long as no two objects claim one address.
    :meth:`InstalledEstate.from_catalogue` asks that narrower question per
    object. Refusing at the item level instead would let a binding that has
    since moved on stop a load of a target it no longer has an object in.
    """

    bound: dict[WeaverItemId, PhysicalTargetRef] = {}
    for item, tables in catalogue.rows.items():
        for row in tables.get(INSTALLATION.name, ()):
            name = str(row.get("target_name") or "")
            if not name:
                raise LoadError(
                    f"the installation row for {item} names no physical target"
                )
            kind = _TARGET_KIND_FOR_ITEM.get(item.item_type)
            if kind is None:
                raise LoadError(
                    f"{item} has item type {item.item_type!r}, which names no "
                    "physical target kind"
                )
            bound[item] = PhysicalTargetRef(kind=kind, name=name)
    return bound


def _dependencies(catalogue: Catalogue) -> tuple[InstalledDependency, ...]:
    found = []
    for item, tables in catalogue.rows.items():
        for row in tables.get(DEPENDENCY.name, ()):
            consumer = _registry_identity(
                catalogue,
                item,
                row,
                columns=("referencing_schema_name", "referencing_object_name"),
            )
            if consumer is None:
                continue
            # Within-item is the edge's own answer rather than a stored flag:
            # a resolved edge names the item it reached, and an unresolved one
            # left the item by definition.
            referenced_type = row.get("referenced_item_type")
            referenced_name = row.get("referenced_item_name")
            found.append(
                InstalledDependency(
                    consumer=consumer,
                    reference=str(row.get("dependency_reference") or ""),
                    is_within_item=(
                        referenced_type == item.item_type
                        and referenced_name == item.item_name
                    ),
                )
            )
    return tuple(sorted(found, key=lambda edge: (str(edge.consumer), edge.reference)))


def _shortcuts(catalogue: Catalogue) -> tuple[InstalledShortcut, ...]:
    """The logical shortcuts the catalogue holds, as producer pairs.

    A physical shortcut is skipped: it names an item outside the estate, so
    there is no producer for a load to order against.
    """

    found = []
    for item, tables in catalogue.rows.items():
        for row in tables.get(SHORTCUT.name, ()):
            if str(row.get("target_type") or "").casefold() != "logical":
                continue
            destination_object = str(row.get("destination_object_name") or "")
            target_object = str(row.get("target_object_name") or "")
            if not destination_object or not target_object:
                continue
            destination = _document_id(
                item,
                str(row.get("destination_schema_name") or ""),
                destination_object,
            )
            target_item = WeaverItemId(
                str(row.get("target_item_type") or ""),
                str(row.get("target_item_name") or ""),
            )
            source = _document_id(
                target_item,
                str(row.get("target_schema_name") or ""),
                target_object,
            )
            found.append(InstalledShortcut(destination=destination, source=source))
    return tuple(sorted(found, key=lambda each: str(each.destination)))


_FILES_PREFIX = "Files/"


def _document_id(item: WeaverItemId, schema: str, name: str) -> WeaverDocumentId:
    """One stored ``schema_name``/``object_name`` pair back as an identity."""

    is_files = schema.startswith(_FILES_PREFIX)
    return WeaverDocumentId(
        item,
        ObjectId(schema[len(_FILES_PREFIX) :] if is_files else schema, name),
        is_files=is_files,
    )


#: What separates schema from object in a Python module name. A module name
#: cannot carry a dot, so ``Sales.Seed`` is spelled ``Sales__Seed``.
_PYTHON_ID_SEPARATOR = "__"

#: The one directory beneath an item that holds runtime source rather than
#: declarations. An import of it names a helper, never a Weaver object.
_LIB = "lib"

#: The item-relative directory a Folder document lives in.
_FILES = "Files"


def _is_python_module_reference(reference: str) -> bool:
    """Whether a stored dependency names a Python module rather than an object.

    The catalogue records a dependency as its author wrote it, and for a Python
    object that is an import — ``.Files.Sales__Seed``, or ``Files.Sales__Seed``.

    A leading dot is a relative import. Otherwise the tell is the separator: a
    module name cannot carry a dot, so a Python object module spells
    ``Schema.Object`` as ``Schema__Object``. A shortcut import is named for the
    module it comes from, because a schema shortcut carries no separator.
    """

    if not reference:
        return False
    if reference.startswith("."):
        return True
    if reference.startswith(f"{SHORTCUTS_MODULE}."):
        return True
    return _PYTHON_ID_SEPARATOR in reference.rsplit(".", 1)[-1]


def _python_module_identity(
    item: WeaverItemId, reference: str
) -> WeaverDocumentId | None:
    """The object one written import names, or ``None`` if it names none.

    The mirror of
    :func:`weaver.declaration.item_dependencies._python_references`: the two
    share one rule for the ``__`` split, including a schema that is itself
    underscores.
    """

    from .declaration.source import python_id_parts

    components = [part for part in reference.split(".") if part]
    if not components or components[0] == _LIB:
        return None
    if components[0] == SHORTCUTS_MODULE and _PYTHON_ID_SEPARATOR not in components[-1]:
        # A schema shortcut presents a namespace, so it is registered under the
        # schema it establishes and names no object of its own.
        return WeaverDocumentId(item, ObjectId(components[-1], components[-1]))
    parts = python_id_parts(components[-1])
    if len(parts) != 2 or not all(part.strip() for part in parts):
        return None
    return WeaverDocumentId(
        item,
        ObjectId(parts[0].strip(), parts[1].strip()),
        is_files=components[0] == _FILES,
    )


def _registry_identity(
    catalogue,
    item,
    row,
    *,
    columns: tuple[str, str] = ("schema_name", "object_name"),
) -> WeaverDocumentId | None:
    """The document a dictionary row describes, when the Registry certifies it.

    A dictionary row no Registry row certifies describes something declared and
    not installed, so it contributes no edge: the graph is of what is there.

    ``columns`` names the pair to read, because a relationship table spells the
    owning object under the side it declares.
    """

    schema, name = columns
    identity = _document_id(item, str(row.get(schema) or ""), str(row.get(name) or ""))
    return identity if identity in catalogue.registered else None


# --- primitives ---------------------------------------------------------------


def primitive_candidates(
    identity: WeaverDocumentId, object_type: str
) -> tuple[tuple[str, WeaverDocumentId], ...]:
    """Where an object's installed load primitive would be, and what kind it is.

    Derived from identity and object type alone, which is all a build has when
    it decides where to put one: the naming is the contract. Candidates rather
    than an answer, because one case has two — a Warehouse table's load is a
    procedure, a Lakehouse object's a deployed module.

    A Lakehouse table has one candidate whatever it was authored in: a Spark SQL
    table compiles to a ``SparkSqlTable`` module under the module's own name, so
    it and a hand-written ``Sales__OrderSummary.py`` install to one path.
    """

    item = identity.item
    schema, name = identity.object_id.schema, identity.object_id.object
    if item.item_type == WAREHOUSE:
        if object_type != "table":
            return ()
        return ((WAREHOUSE_PROCEDURE, load_procedure_id(item, identity.object_id)),)
    if object_type == "folder":
        return (
            (
                PYTHON_FOLDER,
                _deployed_file(item, f"{_FILES_PREFIX}{schema}__{name}.py"),
            ),
        )
    if object_type != "table":
        return ()
    return ((PYTHON_TABLE, _deployed_file(item, f"{schema}__{name}.py")),)


def _deployed_file(item: WeaverItemId, relative: str) -> WeaverDocumentId:
    """A file in the deployed runtime tree, as the Registry stores it."""

    path = f"{LOAD_ROOT}/{relative}"
    directory, _, name = path.rpartition("/")
    return WeaverDocumentId(
        item, ObjectId(schema=directory, object=name), shape=FILE_SHAPE
    )


# --- the graph ----------------------------------------------------------------


@dataclass(frozen=True)
class LoadNode:
    """One unit of physical load work, or the barrier between two of them."""

    node_id: str
    logical_id: WeaverDocumentId | None
    physical_target: PhysicalTargetRef
    primitive_kind: str
    physical_object: PhysicalObjectRef | None = None
    #: The installed primitive itself — the procedure or the deployed file.
    #: ``None`` for a refresh, which is a capability rather than an artefact.
    primitive_id: WeaverDocumentId | None = None
    primitive_object: PhysicalObjectRef | None = None

    @property
    def sort_key(self) -> tuple[str, str, str, str]:
        """What orders two nodes that became ready at the same moment.

        Target kind, then target, then logical identity, then primitive kind,
        so a plan's order is a property of the estate rather than of dictionary
        iteration.
        """

        return (
            self.physical_target.kind,
            self.physical_target.name,
            str(self.logical_id or ""),
            self.primitive_kind,
        )


@dataclass(frozen=True)
class LoadDag:
    """The selected physical load graph: nodes, edges and what was requested.

    An edge means the upstream node must complete successfully before the
    downstream node may execute, and nothing else — not a data-flow statement,
    and not a claim about what the downstream node reads.
    """

    nodes: tuple[LoadNode, ...]
    edges: tuple[tuple[str, str], ...]
    requested: tuple[PhysicalTargetRef, ...] = ()
    messages: tuple[LoadMessage, ...] = ()

    @classmethod
    def from_catalogue(
        cls,
        catalogue: Catalogue,
        *,
        targets: Sequence[PhysicalTargetRef],
        names: Sequence[str] = (),
    ) -> "LoadDag":
        return load_dag(
            InstalledEstate.from_catalogue(catalogue), targets=targets, names=names
        )

    @property
    def by_id(self) -> Mapping[str, LoadNode]:
        return {node.node_id: node for node in self.nodes}

    def upstream(self, node_id: str) -> frozenset[str]:
        return frozenset(
            upstream for upstream, downstream in self.edges if downstream == node_id
        )

    def descendants(self, node_id: str) -> frozenset[str]:
        """Every node that may not run once ``node_id`` has failed."""

        reached: set[str] = set()
        frontier = [node_id]
        while frontier:
            current = frontier.pop()
            for upstream, downstream in self.edges:
                if upstream == current and downstream not in reached:
                    reached.add(downstream)
                    frontier.append(downstream)
        return frozenset(reached)

    def order(self) -> tuple[LoadNode, ...]:
        """The deterministic topological order, or a refusal if there is a cycle.

        Ready nodes are sorted rather than taken as they come, so a dry run is
        inspectable and a log reproducible. The cycle check is here because the
        sort is the one place that can see one.
        """

        remaining = {node.node_id: node for node in self.nodes}
        pending = {
            node_id: set(self.upstream(node_id)) & set(remaining)
            for node_id in remaining
        }
        ordered: list[LoadNode] = []
        while pending:
            ready = sorted(
                (
                    remaining[node_id]
                    for node_id, waiting in pending.items()
                    if not waiting
                ),
                key=lambda node: node.sort_key,
            )
            if not ready:
                cycle = ", ".join(sorted(pending))
                raise LoadError(
                    f"the load graph contains a cycle among: {cycle}",
                )
            for node in ready:
                ordered.append(node)
                del pending[node.node_id]
            done = {node.node_id for node in ready}
            for waiting in pending.values():
                waiting -= done
        return tuple(ordered)


def load_dag(
    estate: InstalledEstate,
    *,
    targets: Sequence[PhysicalTargetRef],
    names: Sequence[str] = (),
) -> LoadDag:
    """The physical load graph for one set of requested physical targets.

    With no name filter, every installed loadable object hosted in the requested
    targets. Dependencies order them but never enlarge the target scope: two
    targets are crossed only when the caller named both.

    With ``names``, exactly those ``Schema.Object`` loadables within the
    requested targets — an operator override, so dependencies add neither nodes
    nor ordering edges.
    """

    requested = tuple(dict.fromkeys(targets))
    planner = _Planner(estate)
    return planner.plan(requested, names=tuple(names))


class _Planner:
    """One planning run's working state.

    A class rather than free functions because the traversal, the barrier
    placement and the message stream all read the same three lookups.
    """

    def __init__(self, estate: InstalledEstate) -> None:
        self.estate = estate
        self.messages: list[LoadMessage] = []
        self.nodes: dict[str, LoadNode] = {}
        self.edges: set[tuple[str, str]] = set()
        self.refresh_nodes: dict[str, LoadNode] = {}
        #: Which physical targets a refresh barrier must wait for, by refresh id.
        self.refresh_sources: dict[str, PhysicalTargetRef] = {}
        self._shortcut_by_destination = {
            each.destination: each for each in estate.shortcuts
        }
        self._dependencies: dict[WeaverDocumentId, list[InstalledDependency]] = {}
        for edge in estate.dependencies:
            self._dependencies.setdefault(edge.consumer, []).append(edge)
        self._loadable = self._installed_primitives()

    # --- what owns load work --------------------------------------------------

    def _installed_primitives(
        self,
    ) -> dict[WeaverDocumentId, tuple[str, InstalledObject]]:
        """Every data object the estate installed a load primitive for."""

        found: dict[WeaverDocumentId, tuple[str, InstalledObject]] = {}
        for identity, installed in self.estate.objects.items():
            for kind, primitive_id in primitive_candidates(
                identity, installed.object_type
            ):
                primitive = self.estate.primitives.get(primitive_id)
                if primitive is not None:
                    found[identity] = (kind, primitive)
                    break
        return found

    # --- planning -------------------------------------------------------------

    def plan(
        self,
        requested: tuple[PhysicalTargetRef, ...],
        *,
        names: tuple[str, ...] = (),
    ) -> LoadDag:
        self._refuse_ambiguity(requested)
        seeds = self._seeds(requested, names=names)
        if names:
            # An exact-name request is deliberately not a partial DAG request.
            # The caller chose the nodes and asked Weaver not to infer more work
            # or readiness constraints from their dependencies.
            for identity in seeds:
                self._load_node(identity)
        else:
            allowed_targets = frozenset(requested)
            visited: set[WeaverDocumentId] = set()
            for identity in seeds:
                self._select(identity, visited, allowed_targets=allowed_targets)
            self._place_refresh_barriers()
        dag = LoadDag(
            nodes=tuple(sorted(self.nodes.values(), key=lambda node: node.sort_key)),
            edges=tuple(sorted(self.edges)),
            requested=requested,
            messages=tuple(self.messages),
        )
        # Ordering is what proves acyclicity, so it is done here rather than left
        # to whoever consumes the graph — a planner that returned a cyclic graph
        # would have made a decision it could not defend.
        dag.order()
        return dag

    def _seeds(
        self,
        requested: tuple[PhysicalTargetRef, ...],
        *,
        names: tuple[str, ...],
    ) -> tuple[WeaverDocumentId, ...]:
        """The loadables the caller selected, before any ordering is applied."""

        available = tuple(
            sorted(
                (
                    identity
                    for identity in self._loadable
                    if self.estate.objects[identity].target in requested
                ),
                key=str,
            )
        )
        if not names:
            return available

        selected: list[WeaverDocumentId] = []
        seen: set[str] = set()
        for written in names:
            name = str(written).strip()
            if not name:
                raise LoadError("a load name must be a non-empty Schema.Object")
            folded = name.casefold()
            if folded in seen:
                continue
            seen.add(folded)
            candidates = [
                identity
                for identity in available
                if identity.object_id.qualified.casefold() == folded
            ]
            if not candidates:
                known = ", ".join(
                    sorted({identity.object_id.qualified for identity in available})
                )
                raise LoadError(
                    f"no loadable object named {name!r} is installed in the "
                    f"requested target(s). Installed: {known or 'none'}"
                )
            if len(candidates) > 1:
                found = ", ".join(str(identity) for identity in candidates)
                raise LoadError(
                    f"{name!r} names more than one installed loadable object "
                    f"({found}) — qualify the request with a single target"
                )
            selected.append(candidates[0])
        return tuple(selected)

    def _refuse_ambiguity(self, targets: tuple[PhysicalTargetRef, ...]) -> None:
        """Stop if any target this request touches holds a duplicated address.

        Only requested targets can be touched: dependency traversal is bounded
        by this same set, so ambiguity anywhere else is irrelevant to this run.
        """

        for target in targets:
            found = self.estate.ambiguous.get(target)
            if found:
                raise LoadError(
                    f"{target} holds two logical objects at one physical "
                    f"address, so a load of it is ambiguous: {found[0]}"
                )

    def _select(
        self,
        identity: WeaverDocumentId,
        visited: set,
        *,
        allowed_targets: frozenset[PhysicalTargetRef],
    ) -> str:
        """Add one in-scope loadable and its in-scope ordering constraints."""

        node = self._load_node(identity)
        if identity in visited:
            return node.node_id
        visited.add(identity)
        for producer, crossed in self._upstream_loadable(
            identity, allowed_targets=allowed_targets
        ):
            upstream_id = self._select(
                producer, visited, allowed_targets=allowed_targets
            )
            if crossed is None:
                self.edges.add((upstream_id, node.node_id))
            else:
                # A shortcut read as SQL: the producer's endpoint has to catch up
                # before the consumer can see it, so the barrier replaces the
                # direct edge rather than sitting beside it.
                refresh_id = self._refresh_node(crossed).node_id
                self.edges.add((refresh_id, node.node_id))
        return node.node_id

    def _load_node(self, identity: WeaverDocumentId) -> LoadNode:
        kind, primitive = self._loadable[identity]
        installed = self.estate.objects[identity]
        node_id = f"load:{installed.target}/{identity.object_id.qualified}"
        node = self.nodes.get(node_id)
        if node is None:
            node = LoadNode(
                node_id=node_id,
                logical_id=identity,
                physical_target=installed.target,
                primitive_kind=kind,
                physical_object=installed.physical,
                primitive_id=primitive.identity,
                primitive_object=primitive.physical,
            )
            self.nodes[node_id] = node
        return node

    def _refresh_node(self, target: PhysicalTargetRef) -> LoadNode:
        """The one refresh barrier for this Lakehouse, made once per run."""

        node_id = f"refresh:{target}"
        node = self.refresh_nodes.get(node_id)
        if node is None:
            node = LoadNode(
                node_id=node_id,
                logical_id=None,
                physical_target=target,
                primitive_kind=ENDPOINT_REFRESH,
            )
            self.refresh_nodes[node_id] = node
            self.nodes[node_id] = node
            self.refresh_sources[node_id] = target
        return node

    def _place_refresh_barriers(self) -> None:
        """Every selected load in a refreshed Lakehouse runs before its barrier.

        Broad by necessity: one barrier per affected Lakehouse, behind all of
        its selected loads rather than only those a shortcut names. A narrower
        placement would need to know which tables a consumer's query touches,
        and the catalogue records the shortcut rather than the read.
        """

        for node_id, target in self.refresh_sources.items():
            for node in list(self.nodes.values()):
                if node.primitive_kind == ENDPOINT_REFRESH:
                    continue
                if node.physical_target == target:
                    self.edges.add((node.node_id, node_id))

    # --- dependency resolution -------------------------------------------------

    def _upstream_loadable(
        self,
        identity: WeaverDocumentId,
        *,
        allowed_targets: frozenset[PhysicalTargetRef],
    ) -> tuple[tuple[WeaverDocumentId, PhysicalTargetRef | None], ...]:
        """The in-scope loadable ancestors, and where each hop crossed.

        Passing through non-loadable producers is what makes a view a conduit:
        it owns no load work, so it is not a node, but a consumer still depends
        on whatever fills the tables behind it. The traversal stops at the
        requested-target boundary even so.
        """

        found: dict[WeaverDocumentId, PhysicalTargetRef | None] = {}
        seen: set[WeaverDocumentId] = set()
        frontier: list[tuple[WeaverDocumentId, PhysicalTargetRef | None]] = [
            (identity, None)
        ]
        while frontier:
            current, crossing = frontier.pop()
            for producer, hop in self._direct_producers(current):
                if self.estate.objects[producer].target not in allowed_targets:
                    continue
                crossed = crossing or hop
                if producer in self._loadable:
                    # A closer crossing wins: the barrier belongs to the hop that
                    # actually left the consumer's engine.
                    if producer not in found or found[producer] is None:
                        found[producer] = crossed
                    continue
                if (producer, crossed) in seen:
                    continue
                seen.add((producer, crossed))
                frontier.append((producer, crossed))
        return tuple(sorted(found.items(), key=lambda pair: str(pair[0])))

    def _direct_producers(
        self, consumer: WeaverDocumentId
    ) -> tuple[tuple[WeaverDocumentId, PhysicalTargetRef | None], ...]:
        """What one object reads directly, and the barrier each read crosses."""

        producers: list[tuple[WeaverDocumentId, PhysicalTargetRef | None]] = []
        consumer_target = self.estate.objects[consumer].target
        for edge in self._dependencies.get(consumer, ()):
            resolved = self._resolve_reference(consumer, edge)
            if resolved is None:
                continue
            producer, through_shortcut = resolved
            producer_target = self.estate.objects[producer].target
            crossing = None
            if (
                through_shortcut
                and producer_target.is_lakehouse
                and not consumer_target.is_lakehouse
            ):
                # Lakehouse to Warehouse is the one crossing read through a SQL
                # analytics endpoint. A Lakehouse-to-Lakehouse shortcut is a OneLake
                # shortcut — Delta on both sides, and nothing to synchronise.
                crossing = producer_target
            producers.append((producer, crossing))
        return tuple(producers)

    def _resolve_reference(
        self, consumer: WeaverDocumentId, edge: InstalledDependency
    ) -> tuple[WeaverDocumentId, bool] | None:
        """What one written reference names, in the consumer's own namespace.

        Shortcuts are consulted before native objects, and the order matters: an
        shortcut destination is registered in the consuming item like any other, so
        a native lookup would find it, stop there, and lose the crossing.
        """

        reference = edge.reference
        if _is_python_module_reference(reference):
            producer = _python_module_identity(consumer.item, reference)
            if producer is None:
                # A `lib/` helper, or an import that names no object at all. It
                # is real source and it is not a Weaver object, so it orders
                # nothing.
                return None
            if producer not in self.estate.objects:
                # A shortcut import says nothing about which area its
                # destination is in, so a folder shortcut resolves to the table
                # spelling first. The estate is what knows.
                beneath_files = replace(producer, is_files=True)
                if beneath_files in self.estate.objects:
                    producer = beneath_files
            if producer not in self.estate.objects:
                raise LoadError(
                    f"{consumer} imports {reference!r}, which resolves to "
                    f"{producer} — not an installed object"
                )
            return producer, False
        parts = reference.split(".")
        if len(parts) > 2:
            # A fully qualified physical read. It names something outside the
            # estate's logical graph, so there is nothing here to order against.
            self.messages.append(
                info(
                    DEPENDENCY_EXTERNAL,
                    f"{consumer} reads {reference}, which names a physical object "
                    "directly and is not part of the load graph",
                    source="load_plan",
                )
            )
            return None
        if len(parts) != 2:
            raise LoadError(
                f"{consumer} declares dependency {reference!r}, which is not a "
                "Schema.Object reference"
            )
        candidate = WeaverDocumentId(consumer.item, ObjectId(parts[0], parts[1]))
        shortcut = self._shortcut_by_destination.get(candidate)
        if shortcut is not None:
            if shortcut.source not in self.estate.objects:
                raise LoadError(
                    f"{consumer} reads shortcut {reference}, which points at "
                    f"{shortcut.source} — not an installed object"
                )
            return shortcut.source, True
        if candidate in self.estate.objects:
            return candidate, False
        folder = replace(candidate, is_files=True)
        if folder in self.estate.objects:
            return folder, False
        raise LoadError(
            f"{consumer} declares dependency {reference!r}, which resolves to "
            "neither an installed object nor a shortcut in its own item"
        )


__all__ = [
    "ENDPOINT_REFRESH",
    "InstalledShortcut",
    "InstalledDependency",
    "InstalledEstate",
    "InstalledObject",
    "LAKEHOUSE_TARGET",
    "LoadDag",
    "LoadNode",
    "PRIMITIVE_KINDS",
    "PYTHON_FOLDER",
    "PYTHON_TABLE",
    "PhysicalObjectRef",
    "PhysicalTargetRef",
    "WAREHOUSE_PROCEDURE",
    "WAREHOUSE_TARGET",
    "lakehouse_names",
    "load_dag",
    "primitive_candidates",
]
