"""The installed managed estate, as one logical graph.

Derived from a :class:`weaver.catalogue.state.Catalogue` already in memory:
``_.Registry`` and ``_.Installation`` say what is installed and where,
``_.TestDictionary`` says what validates it, and ``_.Dependency`` and
``_.Shortcut`` say what reads what. Nothing here reads a repository, queries
Fabric, or opens a physical target.

The one place the persisted dependency representation is interpreted. A stored
``dependency_reference`` is what its author wrote, being a Python import or a
``Schema.Object``, and resolving it needs the item's shortcuts and the Registry
alongside. Load planning, test planning and health read the resolved graph.

Topology is :class:`weaver.graph.Graph`. This module owns the installed node
metadata and the selections planning makes; ordering, layers, ancestry and
subgraphs are the graph's.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from functools import cached_property
from types import MappingProxyType
from typing import Iterable, Mapping, Sequence

from .catalogue.claims import catalogue_columns
from .catalogue.state import Catalogue
from .catalogue.tables import (
    DEPENDENCY,
    INSTALLATION,
    ROLE_ASSUMPTION,
    ROLE_DATA,
    ROLE_TEST,
    SHORTCUT,
    TEST_DICTIONARY,
    VALIDATION_ROLES,
)
from .declaration.metadata import ASSUMPTION, TEST, ObjectId
from .declaration.model import (
    FILE_SHAPE,
    LAKEHOUSE,
    OBJECT_SHAPE,
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

#: What an installed load is, in the vocabulary dispatch branches on. Three
#: installed artefacts. They are strings rather than a class hierarchy because
#: they cross into a plan file and a task log, where the word itself is what
#: appears.
#:
#: There is no kind for a Spark-SQL-authored table. It installs as a deployed
#: ``SparkSqlTable`` module and dispatches as ``python_table``. The authoring
#: language is recorded in the catalogue, not in the kind.
WAREHOUSE_PROCEDURE = "warehouse_procedure"
PYTHON_TABLE = "python_table"
PYTHON_FOLDER = "python_folder"

#: Which physical target kind an item type installs into. An ``Installation``
#: row names the target, and the item type says what kind of target that is.
_TARGET_KIND_FOR_ITEM = {LAKEHOUSE: LAKEHOUSE_TARGET, WAREHOUSE: WAREHOUSE_TARGET}

#: How ``TestDictionary.test_type`` spells each kind, and back. The catalogue's
#: vocabulary is lower case and a declaration's kind is title case, so the
#: translation is pinned in one place rather than guessed at each reader.
KIND_FOR_TEST_TYPE = {"test": TEST, "assumption": ASSUMPTION}
TEST_TYPE_FOR_KIND = {kind: name for name, kind in KIND_FOR_TEST_TYPE.items()}

#: Which role a validation kind carries in the Registry.
_ROLE_FOR_VALIDATION_KIND = {TEST: ROLE_TEST, ASSUMPTION: ROLE_ASSUMPTION}

#: What a Folder's stored schema carries, so a table and a folder of the same
#: name stay apart.
_FILES_PREFIX = "Files/"

#: What separates schema from object in a Python module name. A module name
#: cannot carry a dot, so ``Sales.Seed`` is spelled ``Sales__Seed``.
_PYTHON_ID_SEPARATOR = "__"

#: The one directory beneath an item that holds runtime source rather than
#: declarations. An import of it names a helper, never a Weaver object.
_LIB = "lib"

#: The item-relative directory a Folder document lives in.
_FILES = "Files"


# --- nodes --------------------------------------------------------------------


@dataclass(frozen=True)
class InstalledNode:
    """One managed logical node: what it is, where it lives, what runs it.

    ``identity`` is the logical identity a caller names and the graph keys on.
    ``artefact`` is the separate installed thing that runs it, being a load
    procedure, a deployed module or a compiled validation. The two are kept
    apart because a Test has an artefact and no Registry row of its own.
    """

    identity: WeaverDocumentId | WeaverSchemaId
    target: PhysicalTargetRef
    role: str
    #: What Registry records this object as: table, view, folder or schema.
    #: ``None`` for a validation, which materialises nothing under its own ID.
    object_type: str | None = None
    #: Where this node's runnable artefact belongs, derived from the identity
    #: and the role as the build derived it. ``None`` for a View or a schema
    #: shortcut, which run nothing.
    artefact: WeaverDocumentId | None = None
    #: What dispatch calls the artefact: a load primitive kind for a loadable,
    #: the Test or Assumption kind for a validation.
    artefact_kind: str | None = None
    #: What Registry records the artefact as, or ``None`` when Registry has no
    #: row for it, which is a missing installation.
    artefact_type: str | None = None
    #: A Test's declared key, and its description, as ``_.TestDictionary`` kept
    #: them. Empty for anything else.
    primary_key: tuple[str, ...] = ()
    description: str | None = None

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
        """Whether this node's role names a runnable artefact at all."""

        return self.artefact is not None

    @property
    def is_installed(self) -> bool:
        """Whether Registry certifies the artefact this node names."""

        return self.artefact_type is not None

    @property
    def is_loadable(self) -> bool:
        """Whether this node has an installed load primitive to dispatch."""

        return self.role == ROLE_DATA and self.is_installed

    @property
    def load_name(self) -> str | None:
        """``Schema.Object``, as a request selecting one node spells it."""

        object_id = getattr(self.identity, "object_id", None)
        return None if object_id is None else object_id.qualified

    @property
    def physical(self) -> PhysicalObjectRef:
        """Where this node's own object sits in its physical target."""

        schema, name = catalogue_columns(self.identity)
        return PhysicalObjectRef(
            target_id=self.target.name,
            target_kind=self.target.kind,
            schema=schema,
            object=name,
            object_type=self.object_type or "",
            # A schema identity carries no shape: it names a namespace, and
            # nothing is installed inside it that this estate owns.
            shape=getattr(self.identity, "shape", OBJECT_SHAPE),
        )

    def artefact_physical(self, artefact_type: str) -> PhysicalObjectRef:
        """Where this node's runnable artefact sits in its physical target."""

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
    """One resolved managed edge: ``upstream`` is read by ``downstream``.

    ``reference`` is the dependency exactly as its author wrote it, and
    ``through`` is the shortcut destination the read passed through, when it
    passed through one.
    """

    upstream: WeaverDocumentId | WeaverSchemaId
    downstream: WeaverDocumentId | WeaverSchemaId
    reference: str = ""
    through: WeaverDocumentId | None = None
    #: Whether this is a shortcut's own edge rather than a declared read. A
    #: shortcut destination is materialised from its source, so it is ordered
    #: behind it, and nothing declared the edge.
    is_shortcut: bool = False


@dataclass(frozen=True)
class InstalledShortcut:
    """One logical shortcut a consuming item installed, as the catalogue kept it.

    Only the logical ones. A physical shortcut points at an item Weaver does not
    manage, so it has no producer in this graph.
    """

    destination: WeaverDocumentId
    source: WeaverDocumentId


# --- the graph ----------------------------------------------------------------


@dataclass(frozen=True)
class InstalledDag:
    """The installed managed estate: its nodes, its edges and their topology.

    Immutable, complete and deterministic. Every managed logical node the
    catalogue records is here, whether or not any operation selects it, so a
    filtered view is a subgraph of it.
    """

    nodes: tuple[InstalledNode, ...]
    edges: tuple[InstalledEdge, ...]
    graph: Graph
    installations: Mapping[WeaverItemId, PhysicalTargetRef] = field(
        default_factory=dict
    )
    shortcuts: tuple[InstalledShortcut, ...] = ()
    #: Physical addresses two logical objects both claim, by the target they are
    #: in. Recorded rather than raised: an estate accumulates Registry rows from
    #: every item ever bound to a target, so a stale duplicate claim can outlive
    #: its binding, and refusing here would stop a load of an unrelated target.
    ambiguous: Mapping[PhysicalTargetRef, tuple[str, ...]] = field(default_factory=dict)
    #: Registry rows whose artefact role makes them runnable rather than data.
    #: Held so a caller can ask what an artefact identity is registered as.
    artefacts: Mapping[WeaverDocumentId, str] = field(default_factory=dict)
    #: Reads that name a physical object directly, by the node that declared
    #: them. A three-part reference names something outside the managed estate,
    #: so it is no edge, and an operation reports it rather than ordering it.
    external_references: Mapping[WeaverDocumentId, tuple[str, ...]] = field(
        default_factory=dict
    )
    #: Reads that name nothing installed, by the node that declared them, as the
    #: message each would raise. Recorded rather than raised for the reason
    #: :attr:`ambiguous` is: one item's dangling read must not stop an operation
    #: on an unrelated target.
    unresolved: Mapping[WeaverDocumentId, tuple[str, ...]] = field(default_factory=dict)

    # --- lookup ---------------------------------------------------------------

    def unresolved_for(self, node) -> tuple[str, ...]:
        """This node's declared reads that name nothing installed."""

        return self.unresolved.get(getattr(node, "identity", node), ())

    @cached_property
    def by_id(self) -> Mapping[str, InstalledNode]:
        return MappingProxyType({node.node_id: node for node in self.nodes})

    @cached_property
    def _reads(self) -> Mapping[str, tuple[InstalledEdge, ...]]:
        """The declared reads into each node, indexed once.

        A shortcut edge is left out: a shortcut destination is materialised from
        its source rather than reading it.
        """

        found: dict[str, list[InstalledEdge]] = {}
        for edge in self.edges:
            if edge.is_shortcut:
                continue
            found.setdefault(str(edge.downstream), []).append(edge)
        return MappingProxyType(
            {node_id: tuple(edges) for node_id, edges in found.items()}
        )

    def reads(self, node) -> tuple[InstalledEdge, ...]:
        """The declared reads into this node, in resolution order."""

        return self._reads.get(str(node), ())

    def node(self, identity) -> InstalledNode:
        """One node by identity or node id, or a refusal naming what is missing."""

        node = self.by_id.get(str(identity))
        if node is None:
            raise CatalogueStateError(
                f"{identity} is not a node of the installed graph"
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
                f"{item} has no installation row in the catalogue"
            )
        return target

    # --- navigation -----------------------------------------------------------

    def parents(self, node) -> tuple[InstalledNode, ...]:
        """What this node reads directly."""

        return self._nodes_for(self.graph.upstream_of(str(node)))

    def children(self, node) -> tuple[InstalledNode, ...]:
        """What reads this node directly."""

        return self._nodes_for(self.graph.downstream_of(str(node)))

    def ancestors(self, node) -> tuple[InstalledNode, ...]:
        """Everything reachable upstream, in dependency order."""

        return self._nodes_for(self.graph.ancestors(str(node)))

    def descendants(self, node) -> tuple[InstalledNode, ...]:
        """Everything reachable downstream, in dependency order."""

        return self._nodes_for(self.graph.descendants(str(node)))

    def order(self) -> tuple[InstalledNode, ...]:
        """Every node, upstream before downstream, ties broken by identity."""

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
        loadable: bool | None = None,
        validation: bool | None = None,
        load_names: Sequence[str] | None = None,
    ) -> tuple[InstalledNode, ...]:
        """The nodes matching every filter given, in identity order.

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
            if loadable is not None and node.is_loadable is not loadable:
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
        """The nodes with an installed load primitive, in identity order."""

        return self.select(loadable=True, **filters)

    def validations(self, **filters) -> tuple[InstalledNode, ...]:
        """The Test and Assumption nodes, in identity order."""

        return self.select(validation=True, **filters)

    def nodes_for_item(self, item: WeaverItemId) -> tuple[InstalledNode, ...]:
        return self.select(items=(item,))

    def subgraph(self, selection, *, with_ancestors: bool = False) -> Graph:
        """The topology over a selection, optionally widened to its ancestry."""

        return self.graph.subgraph(
            [str(each) for each in selection], with_ancestors=with_ancestors
        )

    # --- construction ---------------------------------------------------------

    @classmethod
    def from_catalogue(cls, catalogue: Catalogue) -> "InstalledDag":
        return _build(catalogue)


def installed_dag(catalogue: Catalogue) -> InstalledDag:
    """The installed managed graph one catalogue describes."""

    return _build(catalogue)


# --- artefact identities ------------------------------------------------------


def primitive_candidates(
    identity: WeaverDocumentId, object_type: str
) -> tuple[tuple[str, WeaverDocumentId], ...]:
    """Where an object's installed load primitive would be, and what kind it is.

    Derived from identity and object type alone, which is all a build has when
    it decides where to put one: the naming is the contract. Candidates rather
    than an answer, because one case has two: a Warehouse table's load is a
    procedure and a Lakehouse object's is a deployed module.

    A Lakehouse table has one candidate whatever it was authored in: a Spark SQL
    table compiles to a ``SparkSqlTable`` module under the module's own name, so
    it and a hand-written ``Sales__OrderSummary.py`` install to one path.
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


# --- reading the catalogue ----------------------------------------------------


def _build(catalogue: Catalogue) -> InstalledDag:
    """Read every node, resolve every edge, then hand the topology to Graph."""

    installations = installed_targets(catalogue)
    data, artefacts, ambiguous = _registered(catalogue, installations)
    validations = _validations(catalogue, installations)
    nodes: dict[str, InstalledNode] = {}
    for node in (*data.values(), *validations.values()):
        prior = nodes.get(node.node_id)
        if prior is not None:
            raise CatalogueStateError(
                f"{node.node_id} is both a {prior.role} and a {node.role} in the "
                "catalogue, so it names two installed nodes at one identity"
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
    """Each logical item's bound physical target, keyed for reverse lookup.

    Several logical items may name one physical target, which is not an error: a
    request names a target and means everything installed there, answerable so
    long as no two objects claim one address. :func:`_registered` asks that
    narrower question per object.
    """

    bound: dict[WeaverItemId, PhysicalTargetRef] = {}
    for item, tables in catalogue.rows.items():
        for row in tables.get(INSTALLATION.name, ()):
            name = str(row.get("target_name") or "")
            if not name:
                raise CatalogueStateError(
                    f"the installation row for {item} names no physical target"
                )
            kind = _TARGET_KIND_FOR_ITEM.get(item.item_type)
            if kind is None:
                raise CatalogueStateError(
                    f"{item} has item type {item.item_type!r}, which names no "
                    "physical target kind"
                )
            bound[item] = PhysicalTargetRef(kind=kind, name=name)
    return bound


def _registered(catalogue: Catalogue, installations):
    """The data nodes, the runtime artefacts, and the addresses two both claim."""

    data: dict[WeaverDocumentId, InstalledNode] = {}
    artefacts: dict[WeaverDocumentId, str] = {}
    physical_owner: dict[tuple, WeaverDocumentId] = {}
    ambiguous: dict[PhysicalTargetRef, list[str]] = {}
    for identity, document in sorted(
        catalogue.registered.items(), key=lambda pair: str(pair[0])
    ):
        target = installations.get(identity.item)
        if target is None:
            # Registry without Installation: the estate says an object is
            # certified but not where it lives. Refused rather than skipped,
            # because skipping it would silently shrink the graph.
            raise CatalogueStateError(
                f"{identity} is registered but {identity.item} has no "
                "installation row, so its physical target is unknown"
            )
        # What an installed artefact is for, from the Registry row that said so,
        # and never from its physical shape. A Test compiles to a file or a
        # procedure exactly as a load does, so shape inference would walk
        # validation straight into the load graph.
        if document.is_runtime_artefact:
            artefacts[identity] = document.object_type
            continue
        node = InstalledNode(
            identity=identity,
            target=target,
            role=document.object_role,
            object_type=document.object_type,
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

    # The load artefact is attached second, because it is a Registry row of its
    # own and both rows have to be read before either can be joined to the other.
    for identity, node in list(data.items()):
        if node.role != ROLE_DATA:
            continue
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


def _validations(catalogue: Catalogue, installations):
    """The Test and Assumption nodes ``_.TestDictionary`` declares.

    A validation has no Registry row under its logical ID, so its installed
    primitive is found by computing the artefact identity with
    :func:`weaver.etl.validation_artefact_id`, the function the build claimed it
    with. A row whose computed artefact is absent from Registry is a missing
    installation, kept as a node that is not installed.
    """

    found: dict[WeaverDocumentId, InstalledNode] = {}
    for item, tables in catalogue.rows.items():
        target = installations.get(item)
        for row in tables.get(TEST_DICTIONARY.name, ()):
            logical = WeaverDocumentId(
                item,
                ObjectId(
                    schema=str(row.get("schema_name") or ""),
                    object=str(row.get("object_name") or ""),
                ),
            )
            kind = _validation_kind(row, logical)
            if target is None:
                raise CatalogueStateError(
                    f"{logical} is declared but {item} has no installation "
                    "row, so its physical target is unknown"
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
            f"{logical} has unsupported test_type {test_type!r}; expected one of "
            f"{expected}"
        ) from None


def installed_shortcuts(catalogue: Catalogue) -> tuple[InstalledShortcut, ...]:
    """The logical shortcuts the catalogue holds, as producer pairs.

    A physical shortcut is skipped: it names an item outside the estate, so
    there is no producer for the graph to order against.
    """

    found = []
    for item, tables in catalogue.rows.items():
        for row in tables.get(SHORTCUT.name, ()):
            if str(row.get("target_type") or "").casefold() != "logical":
                continue
            object_name = str(row.get("object_name") or "")
            target_object = str(row.get("target_object_name") or "")
            if not object_name or not target_object:
                continue
            destination = stored_identity(
                item, str(row.get("schema_name") or ""), object_name
            )
            target_item = WeaverItemId(
                str(row.get("target_item_type") or ""),
                str(row.get("target_item_name") or ""),
            )
            source = stored_identity(
                target_item,
                str(row.get("target_schema_name") or ""),
                target_object,
            )
            found.append(InstalledShortcut(destination=destination, source=source))
    return tuple(sorted(found, key=lambda each: str(each.destination)))


def stored_identity(item: WeaverItemId, schema: str, name: str) -> WeaverDocumentId:
    """One stored ``schema_name``/``object_name`` pair back as an identity."""

    is_files = schema.startswith(_FILES_PREFIX)
    return WeaverDocumentId(
        item,
        ObjectId(schema[len(_FILES_PREFIX) :] if is_files else schema, name),
        is_files=is_files,
    )


def _shortcut_edges(shortcuts, nodes) -> tuple[InstalledEdge, ...]:
    """Each logical shortcut's own edge, from its source to its destination.

    A shortcut destination is materialised after the object it points at, so it
    is ordered behind it whether or not anything reads it yet. A destination
    Registry does not certify is not a node, and contributes no edge.
    """

    return tuple(
        InstalledEdge(
            upstream=shortcut.source,
            downstream=shortcut.destination,
            is_shortcut=True,
        )
        for shortcut in shortcuts
        if str(shortcut.source) in nodes and str(shortcut.destination) in nodes
    )


@dataclass(frozen=True)
class _DependencyRow:
    """One ``_.Dependency`` row, joined to the node that declared it."""

    consumer: WeaverDocumentId
    reference: str


def _dependency_rows(catalogue: Catalogue, nodes) -> tuple[_DependencyRow, ...]:
    """Every dependency row whose declaring object is a node of this graph.

    A row whose declaring object is neither registered nor a declared validation
    describes something declared and not installed, so it contributes no edge:
    the graph is of what is there.
    """

    found = []
    for item, tables in catalogue.rows.items():
        for row in tables.get(DEPENDENCY.name, ()):
            consumer = stored_identity(
                item,
                str(row.get("referencing_schema_name") or ""),
                str(row.get("referencing_object_name") or ""),
            )
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
    """The one interpretation of a persisted ``dependency_reference``.

    The catalogue records a dependency as its author wrote it, being a Python
    import for a Python object and a ``Schema.Object`` for a SQL one. Resolving
    one needs the consuming item's shortcuts and the installed objects
    alongside, so the three are held together here and nowhere else.
    """

    def __init__(self, *, objects, shortcuts) -> None:
        self._objects = objects
        self._shortcut_by_destination = {each.destination: each for each in shortcuts}
        #: Reads that name a physical object directly, by declaring node.
        self.external: dict[WeaverDocumentId, list[str]] = {}
        #: Reads that name nothing installed, by declaring node.
        self.unresolved: dict[WeaverDocumentId, list[str]] = {}

    def resolve(self, rows) -> tuple[InstalledEdge, ...]:
        edges: dict[tuple[str, str, str], InstalledEdge] = {}
        for row in rows:
            try:
                found = self._one(row.consumer, row.reference)
            except CatalogueStateError as exc:
                # Recorded rather than raised. An estate accumulates rows from
                # every item ever built, so one item's dangling read must not
                # stop an operation on an unrelated target. An operation that
                # reaches this node raises; see
                # :meth:`InstalledDag.require_resolved`.
                self.unresolved.setdefault(row.consumer, []).append(str(exc))
                continue
            if found is None:
                continue
            producer, through = found
            if producer == row.consumer:
                self.unresolved.setdefault(row.consumer, []).append(
                    f"{row.consumer} declares dependency {row.reference!r}, "
                    "which resolves to itself"
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
        """What one written reference names, in the consumer's own namespace.

        Shortcuts are consulted before native objects, and the order matters: a
        shortcut destination is registered in the consuming item like any other,
        so a native lookup would find it, stop there, and lose the crossing.
        """

        if _is_python_module_reference(reference):
            return self._python(consumer, reference)
        return self._relation(consumer, reference)

    def _python(self, consumer: WeaverDocumentId, reference: str):
        producer = _python_module_identity(consumer.item, reference)
        if producer is None:
            # A `lib/` helper, or an import that names no object at all. It is
            # real source and it is not a Weaver object, so it orders nothing.
            return None
        if producer not in self._objects:
            # A shortcut import says nothing about which area its destination is
            # in, so a folder shortcut resolves to the table spelling first. The
            # estate is what answers.
            beneath_files = replace(producer, is_files=True)
            if beneath_files in self._objects:
                producer = beneath_files
        shortcut = self._shortcut_by_destination.get(producer)
        if shortcut is not None:
            return self._through(consumer, reference, shortcut)
        if producer not in self._objects:
            # A schema shortcut is keyed by the namespace it presents, so a
            # two-part spelling of it finds nothing. It orders nothing either:
            # what appears inside belongs to the item it points at.
            namespace = WeaverSchemaId(producer.item, producer.object_id.schema)
            if namespace in self._objects:
                return namespace, None
            raise CatalogueStateError(
                f"{consumer} imports {reference!r}, which resolves to "
                f"{producer}, which is not an installed object"
            )
        return producer, None

    def _relation(self, consumer: WeaverDocumentId, reference: str):
        parts = reference.split(".")
        if len(parts) > 2:
            # A fully qualified physical read. It names something outside the
            # estate's logical graph, so there is nothing here to order against.
            self.external.setdefault(consumer, []).append(reference)
            return None
        if len(parts) != 2:
            raise CatalogueStateError(
                f"{consumer} declares dependency {reference!r}, which is not a "
                "Schema.Object reference"
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
            f"{consumer} declares dependency {reference!r}, which resolves to "
            "neither an installed object nor a shortcut in its own item"
        )

    def _through(self, consumer, reference, shortcut: InstalledShortcut):
        if shortcut.source not in self._objects:
            raise CatalogueStateError(
                f"{consumer} reads shortcut {reference}, which points at "
                f"{shortcut.source}, which is not an installed object"
            )
        return shortcut.source, shortcut


def _is_python_module_reference(reference: str) -> bool:
    """Whether a stored dependency names a Python module rather than an object.

    The catalogue records a dependency as its author wrote it, and for a Python
    object that is an import, such as ``.Files.Sales__Seed`` or
    ``Files.Sales__Seed``.

    A leading dot is a relative import. Otherwise the tell is the separator: a
    module name cannot carry a dot, so a Python object module spells
    ``Schema.Object`` as ``Schema__Object``. A shortcut import is named for the
    module it comes from, because a schema shortcut carries no separator.
    """

    from .declaration.item_dependencies import SHORTCUTS_MODULE

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

    from .declaration.item_dependencies import SHORTCUTS_MODULE
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
