"""Generate and qualify representative Warehouse Build estates."""

from __future__ import annotations

import random
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

DECLARATIONS_PER_MOTIF = 25
MOTIFS_PER_COMPONENT = 5
SCHEMA = "Scale"
PROFILE = "representative_warehouse_build"


@dataclass(frozen=True)
class RepresentativeWarehouseSpec:
    """Stable inputs for a representative Warehouse declaration estate."""

    motifs: int = 10
    seed: int = 20260924

    def __post_init__(self) -> None:
        if self.motifs < 1:
            raise ValueError("motifs must be positive")

    @property
    def declarations(self) -> int:
        return self.motifs * DECLARATIONS_PER_MOTIF

    @classmethod
    def from_declarations(
        cls, declarations: int, *, seed: int = 20260924
    ) -> "RepresentativeWarehouseSpec":
        if declarations < 2 * DECLARATIONS_PER_MOTIF:
            raise ValueError("a representative estate needs at least 50 declarations")
        if declarations % DECLARATIONS_PER_MOTIF:
            raise ValueError(
                f"declarations must be divisible by {DECLARATIONS_PER_MOTIF}"
            )
        return cls(motifs=declarations // DECLARATIONS_PER_MOTIF, seed=seed)


@dataclass(frozen=True)
class RepresentativeNode:
    """One declaration and the relation names its executable SQL reads."""

    identity: str
    object_id: str
    relative_path: str
    kind: str
    motif: int
    role: str
    sql_shape: str
    references: tuple[str, ...] = ()
    root_source: bool = False

    @property
    def reference_sql(self) -> tuple[str, ...]:
        return tuple(_sql_name(_object_id(reference)) for reference in self.references)


@dataclass(frozen=True)
class RepresentativeShortcut:
    """One cross-item Warehouse view shortcut and its consumers."""

    source: str
    destination: str
    consumers: tuple[str, ...]
    kind: str = "view"
    target_type: str = "logical"

    @property
    def source_item(self) -> str:
        return _item_of(self.source)

    @property
    def destination_item(self) -> str:
        return _item_of(self.destination)

    @property
    def reference_sql(self) -> str:
        return _sql_name(_object_id(self.destination))

    @property
    def consumed(self) -> bool:
        return bool(self.consumers)


@dataclass(frozen=True)
class RepresentativeOracle:
    """Expected estate derived only from generator inputs."""

    spec: RepresentativeWarehouseSpec
    nodes: tuple[RepresentativeNode, ...]
    shortcuts: tuple[RepresentativeShortcut, ...]
    source_rows: tuple[tuple[int, str, tuple[tuple[int | None, ...], ...]], ...]

    @property
    def by_identity(self) -> dict[str, RepresentativeNode]:
        return {node.identity: node for node in self.nodes}

    @property
    def identities(self) -> tuple[str, ...]:
        return tuple(node.identity for node in self.nodes)

    @property
    def graph_nodes(self) -> frozenset[str]:
        return frozenset(
            (*self.identities, *(shortcut.destination for shortcut in self.shortcuts))
        )

    @property
    def ordinary_edges(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (reference, node.identity)
            for node in self.nodes
            for reference in node.references
        )

    @property
    def shortcut_edges(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (shortcut.source, shortcut.destination) for shortcut in self.shortcuts
        )

    @property
    def graph_edges(self) -> tuple[tuple[str, str], ...]:
        return self.ordinary_edges + self.shortcut_edges

    @property
    def kind_counts(self) -> dict[str, int]:
        counts = Counter(node.kind for node in self.nodes)
        return {kind: counts[kind] for kind in ("table", "view", "test", "assumption")}

    @property
    def statistics(self) -> dict[str, int]:
        return _graph_metrics(self.graph_nodes, self.graph_edges)

    @property
    def roots(self) -> tuple[str, ...]:
        return _roots(self.graph_nodes, self.graph_edges)

    @property
    def leaves(self) -> tuple[str, ...]:
        return _leaves(self.graph_nodes, self.graph_edges)


_TABLE_ROLES = (
    ("SourceEntity", "source_rows"),
    ("SourceAdjustment", "source_rows"),
    ("Joined", "materialised_join"),
    ("Aggregate", "materialised_aggregate"),
    ("EntityProjection", "calculation"),
    ("EnrichedEntities", "join"),
    ("GroupedEntities", "grouped_aggregate"),
    ("RankedEntities", "filter"),
    ("AggregateProjection", "projection"),
    ("SharedSummary", "join"),
    ("SummaryAll", "calculation"),
    ("SummaryJoin", "join"),
    ("TerminalSummary", "grouped_aggregate"),
    ("TerminalAudit", "calculation"),
)
_VIEW_ROLES = (
    ("ActiveEntities", "filter"),
    ("AdjustmentProjection", "projection"),
    ("UnifiedEntities", "union"),
    ("WindowedEntities", "window"),
    ("SummaryPositive", "filter"),
    ("SummaryUnion", "union"),
)
_TEST_ROLES = (
    ("AggregateReconciles", "reconciliation"),
    ("JoinedMeasuresMatch", "reconciliation"),
    ("TerminalSummaryMatches", "reconciliation"),
)
_ASSUMPTION_ROLES = (
    ("NoOrphans", "assumption"),
    ("NonNegative", "assumption"),
)

_ENTITY_KEY_TABLES = {
    "SourceEntity",
    "SourceAdjustment",
    "Joined",
    "EntityProjection",
    "EnrichedEntities",
    "RankedEntities",
}

_LOCAL_DEPENDENCIES = {
    "Joined": ("SourceEntity", "SourceAdjustment"),
    "Aggregate": ("Joined",),
    "ActiveEntities": ("Joined",),
    "EntityProjection": ("ActiveEntities",),
    "AdjustmentProjection": ("SourceAdjustment",),
    "EnrichedEntities": ("EntityProjection", "AdjustmentProjection"),
    "UnifiedEntities": ("EntityProjection", "AdjustmentProjection"),
    "GroupedEntities": ("EnrichedEntities", "UnifiedEntities"),
    "WindowedEntities": ("EnrichedEntities",),
    "RankedEntities": ("WindowedEntities",),
    "AggregateProjection": ("Aggregate",),
    "SharedSummary": ("GroupedEntities", "AggregateProjection"),
    "SummaryPositive": ("SharedSummary",),
    "SummaryAll": ("SharedSummary",),
    "SummaryUnion": ("SummaryPositive", "SummaryAll"),
    "SummaryJoin": ("SummaryPositive", "SummaryAll"),
    "TerminalSummary": ("SummaryUnion", "SummaryJoin", "RankedEntities"),
    "TerminalAudit": ("TerminalSummary",),
    "AggregateReconciles": ("Joined", "Aggregate"),
    "JoinedMeasuresMatch": ("SourceEntity", "SourceAdjustment", "Joined"),
    "TerminalSummaryMatches": ("TerminalSummary", "TerminalAudit"),
    "NoOrphans": ("SourceAdjustment", "SourceEntity"),
    "NonNegative": ("TerminalSummary",),
}


def _item(motif: int) -> str:
    item_number = 0 if motif == 0 else 1
    return f"Warehouse/Representative{item_number:03d}"


def _object(motif: int, role: str) -> str:
    return f"{SCHEMA}.M{motif:03d}{role}"


def _identity(motif: int, role: str) -> str:
    return f"{_item(motif)}/{_object(motif, role)}"


def _shortcut_identity(motif: int) -> str:
    return f"{_item(motif)}/{SCHEMA}.UpstreamSummary"


def _item_of(identity: str) -> str:
    return identity.rsplit("/", 1)[0]


def _object_id(identity: str) -> str:
    return identity.rsplit("/", 1)[1]


def _sql_name(object_id: str) -> str:
    schema, name = object_id.split(".", 1)
    return f"[{schema}].[{name}]"


def _source_rows(
    spec: RepresentativeWarehouseSpec, motif: int, stream: int
) -> tuple[tuple[int | None, ...], ...]:
    rng = random.Random(spec.seed * 1_000_003 + motif * 101 + stream * 17)
    rows = []
    for offset in range(4):
        entity = motif * 100 + offset + 1
        group = motif * 10 + (offset % 2) + 1
        amount = 100 + rng.randrange(1, 80) + stream * 5
        active = 0 if offset == 3 else 1
        parent = entity if stream else (None if offset == 0 else motif * 100 + 1)
        rows.append((motif, entity, group, amount, active, parent))
    return tuple(rows)


def make_representative_oracle(
    spec: RepresentativeWarehouseSpec,
) -> RepresentativeOracle:
    """Derive declarations and graph evidence without importing Weaver."""

    nodes: list[RepresentativeNode] = []
    shortcuts: list[RepresentativeShortcut] = []
    all_rows: list[tuple[int, str, tuple[tuple[int | None, ...], ...]]] = []

    for motif in range(spec.motifs):
        bridge = motif % MOTIFS_PER_COMPONENT != 0
        bridge_reference = None
        if bridge:
            bridge_reference = (
                _shortcut_identity(motif)
                if motif == 1
                else _identity(motif - 1, "TerminalSummary")
            )

        def add(role: str, kind: str, sql_shape: str) -> None:
            references = tuple(
                _identity(motif, dependency)
                for dependency in _LOCAL_DEPENDENCIES.get(role, ())
            )
            if role == "Joined" and bridge_reference is not None:
                references += (bridge_reference,)
            object_id = _object(motif, role)
            directory = (
                "tests/"
                if kind == "test"
                else "assumptions/"
                if kind == "assumption"
                else ""
            )
            nodes.append(
                RepresentativeNode(
                    identity=_identity(motif, role),
                    object_id=object_id,
                    relative_path=f"{_item(motif)}/{directory}{object_id}.sql",
                    kind=kind,
                    motif=motif,
                    role=role,
                    sql_shape=sql_shape,
                    references=references,
                    root_source=role in {"SourceEntity", "SourceAdjustment"},
                )
            )

        for role, shape in _TABLE_ROLES:
            add(role, "table", shape)
        for role, shape in _VIEW_ROLES:
            add(role, "view", shape)
        for role, shape in _TEST_ROLES:
            add(role, "test", shape)
        for role, shape in _ASSUMPTION_ROLES:
            add(role, "assumption", shape)

        entity_rows = _source_rows(spec, motif, 0)
        adjustment_rows = _source_rows(spec, motif, 1)
        all_rows.extend(
            (
                (motif, "SourceEntity", entity_rows),
                (motif, "SourceAdjustment", adjustment_rows),
            )
        )
        if motif == 1:
            shortcuts.append(
                RepresentativeShortcut(
                    source=_identity(motif - 1, "TerminalSummary"),
                    destination=_shortcut_identity(motif),
                    consumers=(_identity(motif, "Joined"),),
                )
            )

    return RepresentativeOracle(
        spec=spec,
        nodes=tuple(nodes),
        shortcuts=tuple(shortcuts),
        source_rows=tuple(all_rows),
    )


def _schema_source() -> str:
    return "Schema ID: Scale\nDescription: Representative benchmark objects.\n"


def _metadata(node: RepresentativeNode) -> str:
    kind = node.kind.title()
    if node.root_source:
        description = "Representative root source rows."
    else:
        description = f"Representative {node.sql_shape.replace('_', ' ')} declaration."
    lines = [f"{kind} ID: {node.object_id}", f"Description: {description}"]
    if node.kind in {"table", "view"}:
        lines.append("Lineage: Representative benchmark data flow.")
    if node.kind == "table":
        primary = "EntityKey" if node.role in _ENTITY_KEY_TABLES else "GroupKey"
        lines.append(f"Primary key: {primary}")
    if node.kind == "test":
        primary = "GroupKey" if node.role != "JoinedMeasuresMatch" else "EntityKey"
        lines.append(f"Primary key: {primary}")
    return "/*\n" + "\n".join(lines) + "\n*/\n"


def _values_body(rows: tuple[tuple[int | None, ...], ...]) -> str:
    rendered = []
    for motif, entity, group, amount, active, parent in rows:
        parent_sql = "null" if parent is None else str(parent)
        rendered.append(
            f"        ({motif}, {entity}, {group}, {amount}.00, {active}, {parent_sql})"
        )
    values = ",\n".join(rendered)
    return f"""select
    cast(v.MotifKey as int) as MotifKey,
    cast(v.EntityKey as bigint) as EntityKey,
    cast(v.GroupKey as int) as GroupKey,
    cast(v.Amount as decimal(18, 2)) as Amount,
    cast(v.ActiveFlag as bit) as ActiveFlag,
    cast(v.ParentKey as bigint) as ParentKey
from (values
{values}
) as v(MotifKey, EntityKey, GroupKey, Amount, ActiveFlag, ParentKey)"""


def _local(motif: int, role: str) -> str:
    return _sql_name(_object(motif, role))


def _sql_body(node: RepresentativeNode, rows) -> str:
    motif = node.motif

    def name(role: str) -> str:
        return _local(motif, role)

    if node.role in {"SourceEntity", "SourceAdjustment"}:
        return _values_body(rows)
    if node.role == "Joined":
        upstream_select = "cast(0 as decimal(18, 2))"
        upstream_join = ""
        if len(node.references) == 3:
            upstream_select = "coalesce(u.TotalAmount, cast(0 as decimal(18, 2)))"
            upstream_join = (
                f"\nleft join {_sql_name(_object_id(node.references[-1]))} as u"
                " on u.GroupKey = e.GroupKey"
            )
        return f"""select
    e.MotifKey,
    e.EntityKey,
    e.GroupKey,
    cast(e.Amount + a.Amount + {upstream_select} as decimal(18, 2)) as EffectiveAmount,
    cast({upstream_select} as decimal(18, 2)) as UpstreamAmount,
    e.ActiveFlag,
    e.ParentKey
from {name("SourceEntity")} as e
join {name("SourceAdjustment")} as a on a.EntityKey = e.EntityKey{upstream_join}"""
    if node.role == "Aggregate":
        return f"""select MotifKey, GroupKey,
    cast(sum(EffectiveAmount) as decimal(18, 2)) as TotalAmount,
    count_big(*) as EntityCount
from {name("Joined")}
group by MotifKey, GroupKey"""
    if node.role == "ActiveEntities":
        return f"select * from {name('Joined')} where ActiveFlag = 1"
    if node.role == "EntityProjection":
        return f"""select MotifKey, EntityKey, GroupKey,
    cast(EffectiveAmount * cast(1.10 as decimal(4, 2)) as decimal(18, 2)) as AdjustedAmount,
    EffectiveAmount, UpstreamAmount
from {name("ActiveEntities")}"""
    if node.role == "AdjustmentProjection":
        return f"""select MotifKey, EntityKey, GroupKey,
    cast(Amount as decimal(18, 2)) as AdjustmentAmount
from {name("SourceAdjustment")}"""
    if node.role == "EnrichedEntities":
        return f"""select p.MotifKey, p.EntityKey, p.GroupKey,
    cast(p.AdjustedAmount + a.AdjustmentAmount as decimal(18, 2)) as EnrichedAmount
from {name("EntityProjection")} as p
join {name("AdjustmentProjection")} as a on a.EntityKey = p.EntityKey"""
    if node.role == "UnifiedEntities":
        return f"""select MotifKey, EntityKey, GroupKey, AdjustedAmount as EffectiveAmount
from {name("EntityProjection")}
union all
select MotifKey, EntityKey, GroupKey, AdjustmentAmount as EffectiveAmount
from {name("AdjustmentProjection")}"""
    if node.role == "GroupedEntities":
        return f"""select e.MotifKey, e.GroupKey,
    cast(sum(e.EnrichedAmount + u.EffectiveAmount) as decimal(18, 2)) as TotalAmount,
    count_big(*) as EntityCount
from {name("EnrichedEntities")} as e
join {name("UnifiedEntities")} as u on u.EntityKey = e.EntityKey
group by e.MotifKey, e.GroupKey"""
    if node.role == "WindowedEntities":
        return f"""select MotifKey, EntityKey, GroupKey, EnrichedAmount,
    sum(EnrichedAmount) over (
        partition by MotifKey, GroupKey order by EntityKey rows unbounded preceding
    ) as RunningAmount,
    row_number() over (partition by MotifKey, GroupKey order by EntityKey) as RankNumber
from {name("EnrichedEntities")}"""
    if node.role == "RankedEntities":
        return f"select * from {name('WindowedEntities')} where RankNumber <= 3"
    if node.role == "AggregateProjection":
        return f"""select MotifKey, GroupKey, TotalAmount,
    EntityCount, cast(TotalAmount / nullif(EntityCount, 0) as decimal(18, 2)) as AverageAmount
from {name("Aggregate")}"""
    if node.role == "SharedSummary":
        return f"""select g.MotifKey, g.GroupKey,
    cast(g.TotalAmount + a.TotalAmount as decimal(18, 2)) as TotalAmount,
    g.EntityCount + a.EntityCount as EntityCount
from {name("GroupedEntities")} as g
join {name("AggregateProjection")} as a on a.GroupKey = g.GroupKey"""
    if node.role == "SummaryPositive":
        return f"select * from {name('SharedSummary')} where TotalAmount >= 0"
    if node.role == "SummaryAll":
        return f"""select MotifKey, GroupKey,
    cast(TotalAmount + cast(0 as decimal(18, 2)) as decimal(18, 2)) as TotalAmount,
    EntityCount
from {name("SharedSummary")}"""
    if node.role == "SummaryUnion":
        return f"""select MotifKey, GroupKey, TotalAmount, EntityCount
from {name("SummaryPositive")}
union all
select MotifKey, GroupKey, TotalAmount, EntityCount
from {name("SummaryAll")}"""
    if node.role == "SummaryJoin":
        return f"""select p.MotifKey, p.GroupKey,
    cast(p.TotalAmount + a.TotalAmount as decimal(18, 2)) as TotalAmount,
    p.EntityCount + a.EntityCount as EntityCount
from {name("SummaryPositive")} as p
join {name("SummaryAll")} as a on a.GroupKey = p.GroupKey"""
    if node.role == "TerminalSummary":
        return f"""select u.MotifKey, u.GroupKey,
    cast(u.TotalAmount + j.TotalAmount + coalesce(sum(r.EnrichedAmount), 0)
        as decimal(18, 2)) as TotalAmount,
    max(u.EntityCount + j.EntityCount) as EntityCount
from {name("SummaryUnion")} as u
join {name("SummaryJoin")} as j on j.GroupKey = u.GroupKey
left join {name("RankedEntities")} as r on r.GroupKey = u.GroupKey
group by u.MotifKey, u.GroupKey, u.TotalAmount, j.TotalAmount"""
    if node.role == "TerminalAudit":
        return f"""select MotifKey, GroupKey, TotalAmount, EntityCount,
    cast(TotalAmount * EntityCount as decimal(28, 2)) as AuditValue
from {name("TerminalSummary")}"""
    if node.role == "AggregateReconciles":
        return f"""select MotifKey, GroupKey,
    cast(sum(EffectiveAmount) as decimal(18, 2)) as TotalAmount,
    count_big(*) as EntityCount
from {name("Joined")}
group by MotifKey, GroupKey;

select MotifKey, GroupKey, TotalAmount, EntityCount
from {name("Aggregate")}"""
    if node.role == "JoinedMeasuresMatch":
        return f"""select e.EntityKey,
    cast(e.Amount + a.Amount as decimal(18, 2)) as EffectiveAmount
from {name("SourceEntity")} as e
join {name("SourceAdjustment")} as a on a.EntityKey = e.EntityKey;

select EntityKey,
    cast(EffectiveAmount - UpstreamAmount as decimal(18, 2)) as EffectiveAmount
from {name("Joined")}"""
    if node.role == "TerminalSummaryMatches":
        return f"""select GroupKey, TotalAmount, EntityCount
from {name("TerminalSummary")};

select GroupKey, TotalAmount, EntityCount
from {name("TerminalAudit")}"""
    if node.role == "NoOrphans":
        return f"""select a.EntityKey, a.ParentKey
from {name("SourceAdjustment")} as a
left join {name("SourceEntity")} as e on e.EntityKey = a.ParentKey
where e.EntityKey is null"""
    if node.role == "NonNegative":
        return f"select GroupKey, TotalAmount from {name('TerminalSummary')} where TotalAmount < 0"
    raise ValueError(f"unknown representative role: {node.role}")


def write_representative_estate(root: Path, oracle: RepresentativeOracle) -> None:
    """Write one representative estate without touching an existing tree."""

    root = Path(root)
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"estate directory is not empty: {root}")
    rows = {(motif, role): values for motif, role, values in oracle.source_rows}
    by_item: dict[str, list[RepresentativeNode]] = defaultdict(list)
    for node in oracle.nodes:
        by_item[_item_of(node.identity)].append(node)

    for item, nodes in sorted(by_item.items()):
        schema = root / item / "schemas" / f"{SCHEMA}.yml"
        schema.parent.mkdir(parents=True, exist_ok=True)
        schema.write_text(_schema_source(), encoding="utf-8")
        for node in nodes:
            path = root / node.relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            source_rows = rows.get((node.motif, node.role))
            source = _metadata(node) + "\n" + _sql_body(node, source_rows) + ";\n"
            path.write_text(source, encoding="utf-8")

    by_destination_item: dict[str, list[RepresentativeShortcut]] = defaultdict(list)
    for shortcut in oracle.shortcuts:
        by_destination_item[shortcut.destination_item].append(shortcut)
    for item, shortcuts in sorted(by_destination_item.items()):
        lines = ["logical:"]
        for shortcut in shortcuts:
            lines.append(f"  {shortcut.destination}: {shortcut.source}")
        (root / item / "shortcuts.yml").write_text(
            "\n".join(lines) + "\n", encoding="utf-8"
        )


def parse_representative_estate(root: Path):
    """Parse generated files through Weaver's repository boundary."""

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    return parse_item_repository(Location(str(root)))


def _roots(nodes, edges) -> tuple[str, ...]:
    downstream = {downstream for _upstream, downstream in edges}
    return tuple(sorted(set(nodes) - downstream))


def _leaves(nodes, edges) -> tuple[str, ...]:
    upstream = {upstream for upstream, _downstream in edges}
    return tuple(sorted(set(nodes) - upstream))


def _graph_metrics(nodes, edges) -> dict[str, int]:
    nodes = set(nodes)
    edges = set(edges)
    incoming: dict[str, set[str]] = defaultdict(set)
    outgoing: dict[str, set[str]] = defaultdict(set)
    undirected: dict[str, set[str]] = defaultdict(set)
    for upstream, downstream in edges:
        incoming[downstream].add(upstream)
        outgoing[upstream].add(downstream)
        undirected[upstream].add(downstream)
        undirected[downstream].add(upstream)

    depths: dict[str, int] = {}
    pending = set(nodes)
    while pending:
        ready = sorted(
            node
            for node in pending
            if not incoming[node] or incoming[node] <= depths.keys()
        )
        if not ready:
            raise AssertionError("representative oracle contains a dependency cycle")
        for node in ready:
            depths[node] = max(
                (depths[parent] + 1 for parent in incoming[node]), default=0
            )
            pending.remove(node)

    components = 0
    unseen = set(nodes)
    while unseen:
        components += 1
        pending_component = [next(iter(unseen))]
        while pending_component:
            node = pending_component.pop()
            if node not in unseen:
                continue
            unseen.remove(node)
            pending_component.extend(undirected[node] & unseen)

    return {
        "nodes": len(nodes),
        "edges": len(edges),
        "roots": len(_roots(nodes, edges)),
        "leaves": len(_leaves(nodes, edges)),
        "maximum_depth": max(depths.values(), default=0),
        "maximum_fan_in": max((len(incoming[node]) for node in nodes), default=0),
        "maximum_fan_out": max((len(outgoing[node]) for node in nodes), default=0),
        "connected_components": components,
    }


def _representative_repository_evidence(
    repository, oracle: RepresentativeOracle
) -> dict:
    expected_identities = set(oracle.identities)
    expected_nodes = set(oracle.graph_nodes)
    actual_documents = {
        str(identity): source
        for identity, source in repository.source_documents.items()
        if str(identity) in expected_identities
    }
    actual_identities = {
        str(identity)
        for identity in repository.source_documents
        if identity.item.item_type == "Warehouse"
        and identity.item.item_name.startswith("Representative")
        and identity.object_id.schema == SCHEMA
    }
    actual_kinds = {
        identity: str(source.kind).casefold()
        for identity, source in actual_documents.items()
    }
    expected_kinds = {node.identity: node.kind for node in oracle.nodes}

    graph = repository.dependency_graph
    actual_graph_edges = {
        (edge.upstream, edge.downstream)
        for edge in graph.edges
        if edge.upstream in expected_nodes or edge.downstream in expected_nodes
    }
    expected_shortcut_edges = set(oracle.shortcut_edges)
    actual_shortcut_edges = actual_graph_edges & expected_shortcut_edges
    actual_ordinary_edges = actual_graph_edges - expected_shortcut_edges
    actual_metrics = _graph_metrics(expected_nodes, actual_graph_edges)

    expected_shortcuts = {
        (shortcut.source, shortcut.destination) for shortcut in oracle.shortcuts
    }
    actual_shortcuts = {
        (str(pair.source), str(pair.destination))
        for pair in repository.logical_shortcuts
        if str(pair.destination) in expected_nodes
    }
    authored_shortcuts = [
        shortcut
        for shortcut in repository.shortcuts
        if str(shortcut.destination) in expected_nodes
    ]

    physical_references = True
    non_root_queries = True
    for node in oracle.nodes:
        source = actual_documents.get(node.identity)
        if source is None:
            physical_references = False
            non_root_queries = False
            continue
        body = source.sql_body or ""
        if any(reference not in body for reference in node.reference_sql):
            physical_references = False
        if not node.root_source and (
            not node.references
            or re.search(r"\b(from|join)\b", body, flags=re.IGNORECASE) is None
        ):
            non_root_queries = False

    expected_statistics = oracle.statistics
    item_counts = Counter(_item_of(node.identity) for node in oracle.nodes)
    actual_item_counts = Counter(_item_of(identity) for identity in actual_documents)
    validation_counts = Counter(
        source.kind.casefold()
        for source in actual_documents.values()
        if source.is_validation
    )

    matches = {
        "declaration_identities": actual_identities == expected_identities,
        "declaration_kinds": actual_kinds == expected_kinds,
        "ordinary_edges": actual_ordinary_edges == set(oracle.ordinary_edges),
        "shortcut_edges": actual_shortcut_edges == expected_shortcut_edges,
        "roots": _roots(expected_nodes, actual_graph_edges) == oracle.roots,
        "leaves": _leaves(expected_nodes, actual_graph_edges) == oracle.leaves,
        "depth": actual_metrics["maximum_depth"]
        == expected_statistics["maximum_depth"],
        "fan_in": actual_metrics["maximum_fan_in"]
        == expected_statistics["maximum_fan_in"],
        "fan_out": actual_metrics["maximum_fan_out"]
        == expected_statistics["maximum_fan_out"],
        "connected_components": actual_metrics["connected_components"]
        == expected_statistics["connected_components"],
        "item_engine_distribution": actual_item_counts == item_counts,
        "validation_counts": validation_counts
        == Counter(
            {
                "test": oracle.kind_counts["test"],
                "assumption": oracle.kind_counts["assumption"],
            }
        ),
        "shortcut_census": actual_shortcuts == expected_shortcuts
        and len(authored_shortcuts) == len(oracle.shortcuts)
        and all(shortcut.shortcut_type == "view" for shortcut in authored_shortcuts),
        "physical_references": physical_references,
        "non_root_queries": non_root_queries,
    }
    return {"matches": matches, "metrics": actual_metrics}


def qualify_representative_estate(
    root: Path, spec: RepresentativeWarehouseSpec
) -> dict:
    """Generate, parse and compare a representative estate with its oracle."""

    oracle = make_representative_oracle(spec)
    write_representative_estate(root, oracle)
    repository = parse_representative_estate(root)
    evidence = _representative_repository_evidence(repository, oracle)
    matches = evidence["matches"]
    failed = [name for name, matched in matches.items() if not matched]
    if failed:
        raise AssertionError(
            "parsed representative estate differs from the independent oracle: "
            + ", ".join(failed)
        )

    counts = oracle.kind_counts
    declarations_by_item = Counter(_item_of(node.identity) for node in oracle.nodes)
    shortcut_census = [
        {
            "kind": shortcut.kind,
            "target_type": shortcut.target_type,
            "source": shortcut.source,
            "destination": shortcut.destination,
            "source_item": shortcut.source_item,
            "destination_item": shortcut.destination_item,
            "consumers": list(shortcut.consumers),
            "downstream_reads_destination": shortcut.consumed,
        }
        for shortcut in oracle.shortcuts
    ]
    return {
        "profile": PROFILE,
        "generator": {"seed": spec.seed, "motifs": spec.motifs},
        "declarations": {"total": len(oracle.nodes), "by_kind": counts},
        "items": {
            "engine": {"warehouse": len(oracle.nodes)},
            "count": len(declarations_by_item),
            "declarations_by_item": dict(sorted(declarations_by_item.items())),
        },
        "validations": {
            "total": counts["test"] + counts["assumption"],
            "test": counts["test"],
            "assumption": counts["assumption"],
            "executed": 0,
        },
        "shortcuts": {
            "count": len(oracle.shortcuts),
            "consumed": sum(shortcut.consumed for shortcut in oracle.shortcuts),
            "census": shortcut_census,
        },
        "graph": evidence["metrics"],
        "oracle_matches_repository": matches,
    }
