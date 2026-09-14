"""The installed managed estate projected from an in-memory catalogue.

This is the one place that interprets persisted dependency references. Topology
belongs to :class:`weaver.graph.Graph`; this module owns installed node metadata
and selection.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from functools import cached_property
from types import MappingProxyType
from typing import Iterable, Mapping, Sequence

from .catalogue.claims import catalogue_columns, stored_area
from .catalogue.state import Catalogue, InstalledMirror
from .catalogue.tables import (
    DEPENDENCY,
    FOLDER_DICTIONARY,
    INSTALLATION,
    ROLE_ASSUMPTION,
    ROLE_DATA,
    ROLE_TEST,
    SHORTCUT,
    TABLE_DICTIONARY,
    TEST_DICTIONARY,
    VALIDATION_ROLES,
)
from .declaration.metadata import ASSUMPTION, TEST, ObjectId
from .declaration.model import (
    AREAS,
    FILE_SHAPE,
    FILES,
    LAKEHOUSE,
    LOGICAL_TARGET,
    OBJECT_SHAPE,
    SCHEMA_SHORTCUT,
    TABLES,
    WAREHOUSE,
    WeaverDocumentId,
    WeaverItemId,
    WeaverSchemaId,
)
from .errors import CatalogueStateError
from .etl import LOAD_ROOT, load_procedure_id, validation_artefact_id
from .graph import Graph
from .targets import (
    LAKEHOUSE_TARGET,
    WAREHOUSE_TARGET,
    PhysicalObjectRef,
    PhysicalTargetRef,
)

#: Stable dispatch values persisted in plans and task logs. A Spark-SQL-authored
#: table installs as a module and therefore dispatches as ``python_table``.
WAREHOUSE_PROCEDURE = "warehouse_procedure"
PYTHON_TABLE = "python_table"
PYTHON_FOLDER = "python_folder"

#: Item type determines the kind of its named physical target.
_TARGET_KIND_FOR_ITEM = {LAKEHOUSE: LAKEHOUSE_TARGET, WAREHOUSE: WAREHOUSE_TARGET}

#: The catalogue uses lower case; declarations use title case.
KIND_FOR_TEST_TYPE = {"test": TEST, "assumption": ASSUMPTION}
TEST_TYPE_FOR_KIND = {kind: name for name, kind in KIND_FOR_TEST_TYPE.items()}

_ROLE_FOR_VALIDATION_KIND = {TEST: ROLE_TEST, ASSUMPTION: ROLE_ASSUMPTION}

#: A module spells ``Sales.Seed`` as ``Sales__Seed``.
_PYTHON_ID_SEPARATOR = "__"

#: Imports beneath this directory name helpers, not Weaver objects.
LIB = "lib"


# --- nodes --------------------------------------------------------------------


@dataclass(frozen=True)
class InstalledNode:
    """A managed logical identity and its separate runnable artefact.

    Validations have runnable artefacts but do not materialise objects under
    their logical identities.
    """

    identity: WeaverDocumentId | WeaverSchemaId
    target: PhysicalTargetRef
    role: str
    #: ``None`` for a validation, which materialises nothing under its own ID.
    object_type: str | None = None
    #: ``None`` for a View or schema shortcut, which runs nothing.
    artefact: WeaverDocumentId | None = None
    #: Dispatch kind for a loadable, or Test/Assumption for a validation.
    artefact_kind: str | None = None
    #: ``None`` when the runnable artefact is not installed.
    artefact_type: str | None = None
    #: A validation's declared key and description; empty for other nodes.
    primary_key: tuple[str, ...] = ()
    description: str | None = None
    #: True for an object loaded once.
    is_static: bool = False
    #: Source ownership when another target supplies this object's data.
    mirror: InstalledMirror | None = None

    @property
    def node_id(self) -> str:
        return str(self.identity)

    @property
    def item(self) -> WeaverItemId:
        return self.identity.item

    @property
    def is_validation(self) -> bool:
        return self.role in VALIDATION_ROLES

    @property
    def expects_artefact(self) -> bool:
        return self.artefact is not None

    @property
    def is_installed(self) -> bool:
        return self.artefact_type is not None

    @property
    def is_mirrored(self) -> bool:
        return self.mirror is not None

    @property
    def effective_object_type(self) -> str | None:
        return self.mirror.physical_type if self.mirror else self.object_type

    @property
    def can_load(self) -> bool:
        """Whether Weaver may run a load against this node in this estate.

        A mirrored node can retain an installed load primitive, but the source
        target owns its data. Load-state participation is a separate concern.
        """

        return self.role == ROLE_DATA and self.is_installed and not self.is_mirrored

    @property
    def load_name(self) -> str | None:
        """The request spelling, which may identify both a Folder and a table."""

        object_id = getattr(self.identity, "object_id", None)
        return None if object_id is None else object_id.qualified

    @property
    def load_key(self) -> str:
        """The target-local identity, including a Lakehouse object's area."""

        schema, name = catalogue_columns(self.identity)
        return f"{schema}.{name}"

    @property
    def physical(self) -> PhysicalObjectRef:
        schema, name = catalogue_columns(self.identity)
        return PhysicalObjectRef(
            target_id=self.target.name,
            target_kind=self.target.kind,
            schema=schema,
            object=name,
            object_type=self.effective_object_type or "",
            # A schema identity carries no shape: it names a namespace, and
            # nothing is installed inside it that this estate owns.
            shape=getattr(self.identity, "shape", OBJECT_SHAPE),
        )

    def artefact_physical(self, artefact_type: str) -> PhysicalObjectRef:
        schema, name = catalogue_columns(self.artefact)
        return PhysicalObjectRef(
            target_id=self.target.name,
            target_kind=self.target.kind,
            schema=schema,
            object=name,
            object_type=artefact_type,
            shape=self.artefact.shape,
        )


@dataclass(frozen=True)
class InstalledEdge:
    """A resolved read from ``downstream`` to ``upstream``.

    ``reference`` is the dependency exactly as its author wrote it, and
    ``through`` is the shortcut destination the read passed through, when it
    passed through one.
    """

    upstream: WeaverDocumentId | WeaverSchemaId
    downstream: WeaverDocumentId | WeaverSchemaId
    reference: str = ""
    through: WeaverDocumentId | None = None
    #: Shortcut destinations are materialised after their sources even though
    #: nothing declares that read.
    is_shortcut: bool = False


@dataclass(frozen=True)
class InstalledShortcut:
    """An installed shortcut.

    A logical shortcut carries its managed source. A physical shortcut has no
    source node. ``shortcut_type`` distinguishes destinations that share a
    ``Schema.Object`` identity across Lakehouse areas.
    """

    destination: WeaverDocumentId | WeaverSchemaId
    source: WeaverDocumentId | None = None
    shortcut_type: str = ""
    target_type: str = ""
    #: A logical target names a Weaver item; a physical target names a Fabric item.
    target_item: WeaverItemId | None = None
    #: Target schema or path, and the optional object beneath it.
    target_schema: str = ""
    target_object: str | None = None
    #: ``None`` for logical targets and physical targets in this workspace.
    target_workspace: str | None = None

    @property
    def is_logical(self) -> bool:
        return self.target_type == LOGICAL_TARGET

    @property
    def symbol(self) -> str:
        """The imported name, preserving schema shortcuts as namespaces."""

        if isinstance(self.destination, WeaverSchemaId):
            return self.destination.schema
        object_id = self.destination.object_id
        return f"{object_id.schema}{_PYTHON_ID_SEPARATOR}{object_id.object}"


# --- the graph ----------------------------------------------------------------


@dataclass(frozen=True)
class InstalledDag:
    """The complete, immutable and deterministic installed managed estate."""

    nodes: tuple[InstalledNode, ...]
    edges: tuple[InstalledEdge, ...]
    graph: Graph
    installations: Mapping[WeaverItemId, PhysicalTargetRef] = field(
        default_factory=dict
    )
    shortcuts: tuple[InstalledShortcut, ...] = ()
    #: Conflicting physical addresses by target. Retained so a conflict does not
    #: stop operations on unrelated targets.
    ambiguous: Mapping[PhysicalTargetRef, tuple[str, ...]] = field(default_factory=dict)
    #: Runnable artefact identities and their installed types.
    artefacts: Mapping[WeaverDocumentId, str] = field(default_factory=dict)
    #: Direct physical reads are reported but do not create managed edges.
    external_references: Mapping[WeaverDocumentId, tuple[str, ...]] = field(
        default_factory=dict
    )
    #: Unresolved reads by consumer. Deferred so unrelated targets remain usable.
    unresolved: Mapping[WeaverDocumentId, tuple[str, ...]] = field(default_factory=dict)

    # --- lookup ---------------------------------------------------------------

    def unresolved_for(self, node) -> tuple[str, ...]:
        return self.unresolved.get(getattr(node, "identity", node), ())

    @cached_property
    def by_id(self) -> Mapping[str, InstalledNode]:
        return MappingProxyType({node.node_id: node for node in self.nodes})

    @cached_property
    def _reads(self) -> Mapping[str, tuple[InstalledEdge, ...]]:
        """Declared reads indexed by consumer; shortcut ordering is excluded."""

        found: dict[str, list[InstalledEdge]] = {}
        for edge in self.edges:
            if edge.is_shortcut:
                continue
            found.setdefault(str(edge.downstream), []).append(edge)
        return MappingProxyType(
            {node_id: tuple(edges) for node_id, edges in found.items()}
        )

    def reads(self, node) -> tuple[InstalledEdge, ...]:
        return self._reads.get(str(node), ())

    def node(self, identity) -> InstalledNode:
        node = self.by_id.get(str(identity))
        if node is None:
            raise CatalogueStateError(
                f"{identity} is not installed in this catalogue. Check the name or "
                "build its item."
            )
        return node

    def __contains__(self, identity) -> bool:
        return str(identity) in self.by_id

    def __len__(self) -> int:
        return len(self.nodes)

    @property
    def targets(self) -> tuple[PhysicalTargetRef, ...]:
        """Every physical target the catalogue binds an item to, in name order."""

        return tuple(
            sorted(
                set(self.installations.values()), key=lambda ref: (ref.kind, ref.name)
            )
        )

    def target_for(self, item: WeaverItemId) -> PhysicalTargetRef:
        target = self.installations.get(item)
        if target is None:
            raise CatalogueStateError(
                f"The catalogue does not identify a target for {item}. "
                f"Build {item} again."
            )
        return target

    # --- navigation -----------------------------------------------------------

    def parents(self, node) -> tuple[InstalledNode, ...]:
        return self._nodes_for(self.graph.upstream_of(str(node)))

    def children(self, node) -> tuple[InstalledNode, ...]:
        return self._nodes_for(self.graph.downstream_of(str(node)))

    def ancestors(self, node) -> tuple[InstalledNode, ...]:
        return self._nodes_for(self.graph.ancestors(str(node)))

    def descendants(self, node) -> tuple[InstalledNode, ...]:
        return self._nodes_for(self.graph.descendants(str(node)))

    def order(self) -> tuple[InstalledNode, ...]:
        return self._nodes_for(self.graph.order())

    def _nodes_for(self, node_ids: Iterable[str]) -> tuple[InstalledNode, ...]:
        found = self.by_id
        return tuple(found[node_id] for node_id in node_ids)

    # --- selection ------------------------------------------------------------

    def select(
        self,
        *,
        targets: Sequence[PhysicalTargetRef] | None = None,
        items: Sequence[WeaverItemId] | None = None,
        roles: Sequence[str] | None = None,
        object_types: Sequence[str] | None = None,
        can_load: bool | None = None,
        validation: bool | None = None,
        load_names: Sequence[str] | None = None,
    ) -> tuple[InstalledNode, ...]:
        """Nodes matching every filter, in identity order.

        Filters combine: a node satisfies all of them or none of it is selected.
        ``load_names`` folds case, as a request naming ``Schema.Object`` does.
        """

        wanted_targets = None if targets is None else set(targets)
        wanted_items = None if items is None else set(items)
        wanted_roles = None if roles is None else set(roles)
        wanted_types = None if object_types is None else set(object_types)
        wanted_names = (
            None
            if load_names is None
            else {str(name).strip().casefold() for name in load_names}
        )
        selected = []
        for node in self.nodes:
            if wanted_targets is not None and node.target not in wanted_targets:
                continue
            if wanted_items is not None and node.item not in wanted_items:
                continue
            if wanted_roles is not None and node.role not in wanted_roles:
                continue
            if wanted_types is not None and node.object_type not in wanted_types:
                continue
            if can_load is not None and node.can_load is not can_load:
                continue
            if validation is not None and node.is_validation is not validation:
                continue
            if wanted_names is not None:
                name = node.load_name
                if name is None or name.casefold() not in wanted_names:
                    continue
            selected.append(node)
        return tuple(selected)

    def loadables(self, **filters) -> tuple[InstalledNode, ...]:
        return self.select(can_load=True, **filters)

    def validations(self, **filters) -> tuple[InstalledNode, ...]:
        return self.select(validation=True, **filters)

    def nodes_for_item(self, item: WeaverItemId) -> tuple[InstalledNode, ...]:
        return self.select(items=(item,))

    def subgraph(self, selection, *, with_ancestors: bool = False) -> Graph:
        return self.graph.subgraph(
            [str(each) for each in selection], with_ancestors=with_ancestors
        )

    # --- construction ---------------------------------------------------------

    @classmethod
    def from_catalogue(cls, catalogue: Catalogue) -> "InstalledDag":
        return _build(catalogue)


def installed_dag(catalogue: Catalogue) -> InstalledDag:
    return _build(catalogue)


# --- artefact identities ------------------------------------------------------


def primitive_candidates(
    identity: WeaverDocumentId, object_type: str
) -> tuple[tuple[str, WeaverDocumentId], ...]:
    """Candidate load artefact identities and dispatch kinds.

    Naming depends only on identity and object type. Lakehouse tables always use
    a deployed module, including tables authored in Spark SQL.
    """

    # A schema identity names a namespace, so there is no object to load and no
    # primitive to install for it.
    if not hasattr(identity, "object_id"):
        return ()
    item = identity.item
    schema, name = identity.object_id.schema, identity.object_id.object
    if item.item_type == WAREHOUSE:
        if object_type != "table":
            return ()
        return ((WAREHOUSE_PROCEDURE, load_procedure_id(item, identity.object_id)),)
    if object_type == "folder":
        return ((PYTHON_FOLDER, _deployed_file(item, f"{FILES}/{schema}__{name}.py")),)
    if object_type != "table":
        return ()
    return ((PYTHON_TABLE, _deployed_file(item, f"{TABLES}/{schema}__{name}.py")),)


def _deployed_file(item: WeaverItemId, relative: str) -> WeaverDocumentId:
    path = f"{LOAD_ROOT}/{relative}"
    directory, _, name = path.rpartition("/")
    return WeaverDocumentId(
        item, ObjectId(schema=directory, object=name), shape=FILE_SHAPE
    )


# --- reading the catalogue ----------------------------------------------------


def _build(catalogue: Catalogue) -> InstalledDag:
    installations = installed_targets(catalogue)
    data, artefacts, ambiguous = _registered(catalogue, installations)
    validations = _validations(catalogue, installations)
    nodes: dict[str, InstalledNode] = {}
    for node in (*data.values(), *validations.values()):
        prior = nodes.get(node.node_id)
        if prior is not None:
            raise CatalogueStateError(
                f"The catalogue describes {node.node_id} as both {prior.role} and "
                f"{node.role}, so its installed identity is ambiguous."
            )
        nodes[node.node_id] = node

    shortcuts = installed_shortcuts(catalogue)
    resolver = _References(objects=data, shortcuts=shortcuts)
    edges = resolver.resolve(_dependency_rows(catalogue, nodes))
    edges += _shortcut_edges(shortcuts, nodes)
    graph = Graph(
        nodes,
        [(str(edge.upstream), str(edge.downstream)) for edge in edges],
    )
    return InstalledDag(
        nodes=tuple(nodes[node_id] for node_id in sorted(nodes)),
        edges=edges,
        graph=graph,
        installations=MappingProxyType(installations),
        shortcuts=shortcuts,
        ambiguous=MappingProxyType(ambiguous),
        artefacts=MappingProxyType(artefacts),
        external_references=MappingProxyType(
            {
                consumer: tuple(references)
                for consumer, references in resolver.external.items()
            }
        ),
        unresolved=MappingProxyType(
            {
                consumer: tuple(messages)
                for consumer, messages in resolver.unresolved.items()
            }
        ),
    )


def installed_targets(
    catalogue: Catalogue,
) -> dict[WeaverItemId, PhysicalTargetRef]:
    """Physical targets by logical item; several items may share one target."""

    bound: dict[WeaverItemId, PhysicalTargetRef] = {}
    for item, tables in catalogue.rows.items():
        for row in tables.get(INSTALLATION.name, ()):
            name = str(row.get("target_name") or "")
            if not name:
                raise CatalogueStateError(
                    f"The catalogue does not identify a target for {item}. "
                    f"Build {item} again."
                )
            kind = _TARGET_KIND_FOR_ITEM.get(item.item_type)
            if kind is None:
                raise CatalogueStateError(
                    f"The catalogue contains unsupported item type "
                    f"{item.item_type!r} for {item}."
                )
            bound[item] = PhysicalTargetRef(kind=kind, name=name)
    return bound


def _registered(catalogue: Catalogue, installations):
    data: dict[WeaverDocumentId, InstalledNode] = {}
    artefacts: dict[WeaverDocumentId, str] = {}
    physical_owner: dict[tuple, WeaverDocumentId] = {}
    ambiguous: dict[PhysicalTargetRef, list[str]] = {}
    for identity, document in sorted(
        catalogue.registered.items(), key=lambda pair: str(pair[0])
    ):
        target = installations.get(identity.item)
        if target is None:
            # Missing ownership cannot be skipped because the graph must be complete.
            raise CatalogueStateError(
                f"The catalogue contains {identity}, but does not identify a target "
                f"for {identity.item}. Build {identity.item} again."
            )
        # Role, not physical shape, separates runnable artefacts from data. Tests
        # and loads can compile to the same shape.
        if document.is_runtime_artefact:
            artefacts[identity] = document.object_type
            continue
        node = InstalledNode(
            identity=identity,
            target=target,
            role=document.object_role,
            object_type=document.object_type,
            mirror=catalogue.mirrors.get(identity),
        )
        where = node.physical
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
        data[identity] = node

    # Join declarations and artefacts only after all installed identities are known.
    static = _static_declarations(catalogue)
    for identity, node in list(data.items()):
        if node.role != ROLE_DATA:
            continue
        node = replace(node, is_static=identity in static)
        data[identity] = node
        for kind, candidate in primitive_candidates(identity, node.object_type):
            data[identity] = replace(
                node,
                artefact=candidate,
                artefact_kind=kind,
                artefact_type=artefacts.get(candidate),
            )
            break
    return (
        data,
        artefacts,
        {target: tuple(found) for target, found in ambiguous.items()},
    )


def _static_declarations(catalogue: Catalogue) -> frozenset[WeaverDocumentId]:
    found = set()
    for table in (TABLE_DICTIONARY, FOLDER_DICTIONARY):
        for row in catalogue.table_rows(table):
            if not row.get("is_static"):
                continue
            item = WeaverItemId(
                str(row.get("item_type") or ""), str(row.get("item_name") or "")
            )
            found.add(
                stored_identity(
                    item,
                    str(row.get("schema_name") or ""),
                    str(row.get("object_name") or ""),
                )
            )
    return frozenset(found)


def _validations(catalogue: Catalogue, installations):
    """Validation nodes joined to artefacts by the build's identity rule.

    A missing artefact remains a declared but uninstalled node.
    """

    found: dict[WeaverDocumentId, InstalledNode] = {}
    for item, tables in catalogue.rows.items():
        target = installations.get(item)
        for row in tables.get(TEST_DICTIONARY.name, ()):
            logical = WeaverDocumentId.validation(
                item,
                ObjectId(
                    schema=str(row.get("schema_name") or ""),
                    object=str(row.get("object_name") or ""),
                ),
            )
            kind = _validation_kind(row, logical)
            if target is None:
                raise CatalogueStateError(
                    f"The catalogue contains {logical}, but does not identify a "
                    f"target for {item}. Build {item} again."
                )
            artefact = validation_artefact_id(item, kind, logical.object_id)
            registered = catalogue.registered.get(artefact)
            found[logical] = InstalledNode(
                identity=logical,
                target=target,
                role=_ROLE_FOR_VALIDATION_KIND[kind],
                artefact=artefact,
                artefact_kind=kind,
                artefact_type=registered.object_type if registered else None,
                primary_key=_column_set(row.get("primary_key")),
                description=_text(row.get("description")),
            )
    return found


def _validation_kind(row: Mapping[str, object], logical: WeaverDocumentId) -> str:
    test_type = str(row.get("test_type") or "").strip().casefold()
    try:
        return KIND_FOR_TEST_TYPE[test_type]
    except KeyError:
        expected = ", ".join(sorted(KIND_FOR_TEST_TYPE))
        raise CatalogueStateError(
            f"The catalogue records unsupported validation kind {test_type!r} for "
            f"{logical}; expected one of {expected}. Build {logical.item} again."
        ) from None


def installed_shortcuts(catalogue: Catalogue) -> tuple[InstalledShortcut, ...]:
    """Installed shortcuts, including managed and external sources."""

    found = []
    for item, tables in catalogue.rows.items():
        for row in tables.get(SHORTCUT.name, ()):
            shortcut_type = str(row.get("shortcut_type") or "").casefold()
            target_type = str(row.get("target_type") or "").casefold()
            schema_name = str(row.get("schema_name") or "")
            object_name = str(row.get("object_name") or "")
            if shortcut_type == SCHEMA_SHORTCUT:
                destination = WeaverSchemaId(item, schema_name)
            elif object_name:
                destination = stored_identity(item, schema_name, object_name)
            else:
                continue
            target_item = WeaverItemId(
                str(row.get("target_item_type") or ""),
                str(row.get("target_item_name") or ""),
            )
            target_schema = str(row.get("target_schema_name") or "")
            target_object = str(row.get("target_object_name") or "") or None
            source = None
            if target_type == LOGICAL_TARGET:
                if not target_object:
                    continue
                source = stored_identity(target_item, target_schema, target_object)
            found.append(
                InstalledShortcut(
                    destination=destination,
                    source=source,
                    shortcut_type=shortcut_type,
                    target_type=target_type,
                    target_item=target_item,
                    target_schema=target_schema,
                    target_object=target_object,
                    target_workspace=str(row.get("target_workspace_name") or "")
                    or None,
                )
            )
    return tuple(sorted(found, key=lambda each: str(each.destination)))


def stored_identity(item: WeaverItemId, schema: str, name: str) -> WeaverDocumentId:
    """Project stored catalogue columns back into an object identity.

    The stored schema may include a Lakehouse area; Warehouse relations do not.
    Validation identities use a separate projection.
    """

    area, relational = stored_area(schema)
    return WeaverDocumentId(item, ObjectId(relational, name), is_files=area == FILES)


def _shortcut_edges(shortcuts, nodes) -> tuple[InstalledEdge, ...]:
    """Order installed logical shortcut destinations after their sources."""

    return tuple(
        InstalledEdge(
            upstream=shortcut.source,
            downstream=shortcut.destination,
            is_shortcut=True,
        )
        for shortcut in shortcuts
        if shortcut.is_logical
        and str(shortcut.source) in nodes
        and str(shortcut.destination) in nodes
    )


@dataclass(frozen=True)
class _DependencyRow:
    consumer: WeaverDocumentId
    reference: str


def _dependency_rows(catalogue: Catalogue, nodes) -> tuple[_DependencyRow, ...]:
    """Dependencies whose consumers are installed nodes.

    Object and validation dependencies share one stored shape. Area usually
    distinguishes them; otherwise membership in ``nodes`` does.
    """

    found = []
    for item, tables in catalogue.rows.items():
        for row in tables.get(DEPENDENCY.name, ()):
            schema = str(row.get("referencing_schema_name") or "")
            name = str(row.get("referencing_object_name") or "")
            consumer = stored_identity(item, schema, name)
            if str(consumer) not in nodes:
                area, relational = stored_area(schema)
                if area is not None:
                    continue
                consumer = WeaverDocumentId.validation(item, ObjectId(relational, name))
                if str(consumer) not in nodes:
                    continue
            found.append(
                _DependencyRow(
                    consumer=consumer,
                    reference=str(row.get("dependency_reference") or ""),
                )
            )
    return tuple(
        sorted(dict.fromkeys(found), key=lambda row: (str(row.consumer), row.reference))
    )


class _References:
    """Resolve persisted imports and ``Schema.Object`` dependency references."""

    def __init__(self, *, objects, shortcuts) -> None:
        self._objects = objects
        self._shortcut_by_destination = {
            each.destination: each for each in shortcuts if each.is_logical
        }
        # Shortcut imports resolve by declaring item and symbol, not by whichever
        # installed destination happens to share the spelling.
        self._shortcut_by_symbol = {
            (each.destination.item, each.symbol): each for each in shortcuts
        }
        self.external: dict[WeaverDocumentId, list[str]] = {}
        self.unresolved: dict[WeaverDocumentId, list[str]] = {}

    def resolve(self, rows) -> tuple[InstalledEdge, ...]:
        edges: dict[tuple[str, str, str], InstalledEdge] = {}
        for row in rows:
            try:
                found = self._one(row.consumer, row.reference)
            except CatalogueStateError as exc:
                # Defer failure until an operation reaches this consumer so an
                # unrelated target remains usable.
                self.unresolved.setdefault(row.consumer, []).append(str(exc))
                continue
            if found is None:
                continue
            producer, through = found
            if producer == row.consumer:
                self.unresolved.setdefault(row.consumer, []).append(
                    f"{row.consumer} depends on {row.reference!r}, which resolves "
                    "to itself. Remove the dependency and build the item again."
                )
                continue
            edge = InstalledEdge(
                upstream=producer,
                downstream=row.consumer,
                reference=row.reference,
                through=None if through is None else through.destination,
            )
            edges.setdefault((str(producer), str(row.consumer), row.reference), edge)
        return tuple(edges.values())

    def _one(self, consumer: WeaverDocumentId, reference: str):
        """Resolve shortcuts before native objects to preserve the crossing."""

        if _is_python_module_reference(reference):
            return self._python(consumer, reference)
        return self._relation(consumer, reference)

    def _python(self, consumer: WeaverDocumentId, reference: str):
        symbol = _shortcut_symbol(reference)
        if symbol is not None:
            return self._shortcut(consumer, reference, symbol)
        producer = _python_module_identity(consumer, reference)
        if producer is None:
            # A `lib/` helper, or an import that names no object at all. It is
            # real source and it is not a Weaver object, so it orders nothing.
            return None
        if producer not in self._objects:
            raise CatalogueStateError(
                f"{consumer} imports {reference!r}, but {producer} is not installed. "
                f"Install {producer} or remove the import, then build "
                f"{consumer.item} again."
            )
        return producer, None

    def _shortcut(self, consumer: WeaverDocumentId, reference: str, symbol: str):
        shortcut = self._shortcut_by_symbol.get((consumer.item, symbol))
        if shortcut is None:
            raise CatalogueStateError(
                f"{consumer} imports {reference!r}, but {consumer.item} has no "
                f"shortcut named {symbol!r}. Declare the shortcut or remove the "
                f"import, then build {consumer.item} again."
            )
        if not shortcut.is_logical:
            self.external.setdefault(consumer, []).append(reference)
            return None
        return self._through(consumer, reference, shortcut)

    def _relation(self, consumer: WeaverDocumentId, reference: str):
        parts = reference.split(".")
        if len(parts) > 2:
            # A fully qualified physical read. It names something outside the
            # estate's logical graph, so there is nothing here to order against.
            self.external.setdefault(consumer, []).append(reference)
            return None
        if len(parts) != 2:
            raise CatalogueStateError(
                f"{consumer} has invalid dependency {reference!r}; expected "
                f"Schema.Object. Fix the dependency and build {consumer.item} again."
            )
        candidate = WeaverDocumentId(consumer.item, ObjectId(parts[0], parts[1]))
        shortcut = self._shortcut_by_destination.get(candidate)
        if shortcut is not None:
            return self._through(consumer, reference, shortcut)
        if candidate in self._objects:
            return candidate, None
        folder = replace(candidate, is_files=True)
        if folder in self._objects:
            return folder, None
        raise CatalogueStateError(
            f"{consumer} depends on {reference!r}, but no installed object or "
            f"shortcut with that name exists in {consumer.item}. Install the object "
            f"or declare the shortcut, then build {consumer.item} again."
        )

    def _through(self, consumer, reference, shortcut: InstalledShortcut):
        if shortcut.source not in self._objects:
            raise CatalogueStateError(
                f"{consumer} reads shortcut {reference}, but its source "
                f"{shortcut.source} is not installed. Install the source before "
                f"building {consumer.item} again."
            )
        return shortcut.source, shortcut


def _is_python_module_reference(reference: str) -> bool:
    """Distinguish persisted Python imports from relation references."""

    from .declaration.item_dependencies import SHORTCUTS_MODULE

    if not reference:
        return False
    if reference.startswith("."):
        return True
    if reference.startswith(f"{SHORTCUTS_MODULE}."):
        return True
    return _PYTHON_ID_SEPARATOR in reference.rsplit(".", 1)[-1]


def _shortcut_symbol(reference: str) -> str | None:
    from .declaration.item_dependencies import SHORTCUTS_MODULE

    prefix = f"{SHORTCUTS_MODULE}."
    if not reference.startswith(prefix):
        return None
    return reference[len(prefix) :]


def _python_module_identity(
    consumer: WeaverDocumentId, reference: str
) -> WeaverDocumentId | None:
    """Project a persisted import back into an object identity.

    This mirrors declaration parsing's ``__`` split and relative-import rule.
    Documents beneath ``Files`` begin one package deeper than item-root modules.
    """

    from .declaration.source import python_id_parts

    level = len(reference) - len(reference.lstrip("."))
    components = tuple(part for part in reference.split(".") if part)
    if level:
        area = consumer.area
        base = (area,) if area else ()
        parents = level - 1
        if parents > len(base):
            # Imports outside the item do not name Weaver objects.
            return None
        components = base[: len(base) - parents] + components
    if not components or components[0] == LIB:
        # Helper modules do not participate in installed ordering.
        return None
    parts = python_id_parts(components[-1])
    if len(parts) != 2 or not all(part.strip() for part in parts):
        return None
    if len(components) != 2 or components[0] not in AREAS:
        raise CatalogueStateError(
            f"{consumer} has an incompatible dependency reference {reference!r}. "
            f"Build {consumer.item} again."
        )
    return WeaverDocumentId(
        consumer.item,
        ObjectId(parts[0].strip(), parts[1].strip()),
        is_files=components[0] == FILES,
    )


def _column_set(value: object) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(part.strip() for part in str(value).split(",") if part.strip())


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "InstalledDag",
    "InstalledEdge",
    "InstalledNode",
    "InstalledShortcut",
    "KIND_FOR_TEST_TYPE",
    "PYTHON_FOLDER",
    "PYTHON_TABLE",
    "TEST_TYPE_FOR_KIND",
    "WAREHOUSE_PROCEDURE",
    "installed_dag",
    "installed_shortcuts",
    "installed_targets",
    "primitive_candidates",
    "stored_identity",
]
