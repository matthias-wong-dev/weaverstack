"""Generate and qualify realistic Lakehouse Build estates."""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

DECLARATIONS_PER_MOTIF = 25
MOTIFS_PER_COMPONENT = 5
PROFILE = "representative_lakehouse_build"
SCHEMA = "Scale"


@dataclass(frozen=True)
class RepresentativeLakehouseSpec:
    """Stable inputs for one representative declaration estate."""

    motifs: int
    seed: int = 20260924

    def __post_init__(self) -> None:
        if self.motifs < 2:
            raise ValueError("a representative Lakehouse estate needs two motifs")

    @property
    def declarations(self) -> int:
        return self.motifs * DECLARATIONS_PER_MOTIF

    @classmethod
    def from_declarations(
        cls, declarations: int, *, seed: int = 20260924
    ) -> "RepresentativeLakehouseSpec":
        if declarations < 2 * DECLARATIONS_PER_MOTIF:
            raise ValueError("a representative estate needs at least 50 declarations")
        if declarations % DECLARATIONS_PER_MOTIF:
            raise ValueError(
                f"declarations must be divisible by {DECLARATIONS_PER_MOTIF}"
            )
        return cls(motifs=declarations // DECLARATIONS_PER_MOTIF, seed=seed)


@dataclass(frozen=True)
class RepresentativeDeclaration:
    """One generated declaration and its declared dependencies."""

    identity: str
    item: str
    object_id: str
    relative_path: str
    kind: str
    language: str
    motif: int
    role: str
    references: tuple[str, ...]


@dataclass(frozen=True)
class RepresentativeShortcut:
    """One shortcut and every declaration that reads its destination."""

    source: str
    destination: str
    consumers: tuple[str, ...]
    kind: str = "table"
    target_type: str = "logical"

    @property
    def source_item(self) -> str:
        return _item_of(self.source)

    @property
    def destination_item(self) -> str:
        return _item_of(self.destination)

    @property
    def consumed(self) -> bool:
        return bool(self.consumers)


@dataclass(frozen=True)
class RepresentativeLakehousePlan:
    """Expected source and graph derived without importing Weaver."""

    spec: RepresentativeLakehouseSpec
    declarations: tuple[RepresentativeDeclaration, ...]
    shortcuts: tuple[RepresentativeShortcut, ...]

    @property
    def nodes(self) -> tuple[RepresentativeDeclaration, ...]:
        return self.declarations

    @property
    def identities(self) -> tuple[str, ...]:
        return tuple(node.identity for node in self.declarations)

    @property
    def graph_nodes(self) -> frozenset[str]:
        return frozenset(
            (*self.identities, *(shortcut.destination for shortcut in self.shortcuts))
        )

    @property
    def ordinary_edges(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (reference, node.identity)
            for node in self.declarations
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
        counts = Counter(node.kind for node in self.declarations)
        return {kind: counts[kind] for kind in ("table", "view", "test", "assumption")}

    @property
    def statistics(self) -> dict[str, int]:
        return _graph_metrics(self.graph_nodes, self.graph_edges)


_TABLE_ROLES = (
    "SourceEntity",
    "SourceAdjustment",
    "Joined",
    "Aggregate",
)
_VIEW_ROLES = (
    "ActiveEntities",
    "EntityProjection",
    "AdjustmentProjection",
    "EnrichedEntities",
    "UnifiedEntities",
    "GroupedEntities",
    "WindowedEntities",
    "RankedEntities",
    "AggregateProjection",
    "SharedSummary",
    "SummaryPositive",
    "SummaryAll",
    "SummaryUnion",
    "SummaryJoin",
    "TerminalSummary",
    "PublishedBridge",
)
_TEST_ROLES = (
    "AggregateReconciles",
    "JoinedMeasuresMatch",
    "TerminalSummaryMatches",
)
_ASSUMPTION_ROLES = ("NoOrphans", "NonNegative")
_ROLE_KIND = {
    **dict.fromkeys(_TABLE_ROLES, "table"),
    **dict.fromkeys(_VIEW_ROLES, "view"),
    **dict.fromkeys(_TEST_ROLES, "test"),
    **dict.fromkeys(_ASSUMPTION_ROLES, "assumption"),
}
_PYTHON_ROLES = {
    "SourceEntity",
    "SourceAdjustment",
    "Joined",
    "AggregateReconciles",
    "JoinedMeasuresMatch",
    "NoOrphans",
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
    "PublishedBridge": ("TerminalSummary",),
    "AggregateReconciles": ("Joined", "Aggregate"),
    "JoinedMeasuresMatch": ("SourceEntity", "SourceAdjustment", "Joined"),
    "TerminalSummaryMatches": (
        "SummaryUnion",
        "SummaryJoin",
        "RankedEntities",
        "PublishedBridge",
    ),
    "NoOrphans": ("SourceAdjustment", "SourceEntity"),
    "NonNegative": ("TerminalSummary",),
}


def _item(motif: int) -> str:
    suffix = "000" if motif == 0 else "001"
    return f"Lakehouse/Representative{suffix}"


def _object(motif: int, role: str) -> str:
    return f"{SCHEMA}.M{motif:03d}{role}"


def _identity(motif: int, role: str) -> str:
    kind = _ROLE_KIND[role]
    object_id = _object(motif, role)
    if kind in {"table", "view"}:
        return f"{_item(motif)}/Tables/{object_id}"
    return f"{_item(motif)}/{object_id}"


def _shortcut_destination() -> str:
    return "Lakehouse/Representative001/Tables/Scale.UpstreamAggregate"


def _item_of(identity: str) -> str:
    if "/Tables/" in identity:
        return identity.split("/Tables/", 1)[0]
    return identity.rsplit("/", 1)[0]


def _object_id(identity: str) -> str:
    if "/Tables/" in identity:
        return identity.split("/Tables/", 1)[1]
    return identity.rsplit("/", 1)[1]


def _class_name(object_id: str) -> str:
    return object_id.replace(".", "__")


def _relative_path(motif: int, role: str, language: str) -> str:
    kind = _ROLE_KIND[role]
    directory = (
        "Tables"
        if kind in {"table", "view"}
        else "tests"
        if kind == "test"
        else "assumptions"
    )
    object_id = _object(motif, role)
    filename = _class_name(object_id) if language == "python" else object_id
    suffix = ".py" if language == "python" else ".sql"
    return f"{_item(motif)}/{directory}/{filename}{suffix}"


def make_representative_lakehouse_plan(
    spec: RepresentativeLakehouseSpec,
) -> RepresentativeLakehousePlan:
    """Create deterministic declarations and a bounded-depth dependency graph."""

    declarations: list[RepresentativeDeclaration] = []
    shortcut_destination = _shortcut_destination()
    for motif in range(spec.motifs):
        bridge_reference = None
        if motif and motif % MOTIFS_PER_COMPONENT:
            bridge_reference = (
                shortcut_destination
                if motif == 1
                else _identity(motif - 1, "Aggregate")
            )
        for role, kind in _ROLE_KIND.items():
            language = "python" if role in _PYTHON_ROLES else "sql"
            references = tuple(
                _identity(motif, dependency)
                for dependency in _LOCAL_DEPENDENCIES.get(role, ())
            )
            if role in {"Aggregate", "AggregateReconciles"} and bridge_reference:
                references += (bridge_reference,)
            declarations.append(
                RepresentativeDeclaration(
                    identity=_identity(motif, role),
                    item=_item(motif),
                    object_id=_object(motif, role),
                    relative_path=_relative_path(motif, role, language),
                    kind=kind,
                    language=language,
                    motif=motif,
                    role=role,
                    references=references,
                )
            )

    consumers = tuple(
        sorted(
            node.identity
            for node in declarations
            if shortcut_destination in node.references
        )
    )
    shortcut = RepresentativeShortcut(
        source=_identity(0, "Aggregate"),
        destination=shortcut_destination,
        consumers=consumers,
    )
    return RepresentativeLakehousePlan(
        spec=spec,
        declarations=tuple(declarations),
        shortcuts=(shortcut,),
    )


def _schema_source() -> str:
    return "Schema ID: Scale\nDescription: Representative benchmark objects.\n"


def _schema_lines(role: str) -> tuple[str, ...]:
    if role in {"SourceEntity", "SourceAdjustment"}:
        return (
            "  MotifKey: integer",
            "  EntityKey: long",
            "  GroupKey: integer",
            "  Amount: decimal(18, 2)",
            "  ActiveFlag: boolean",
            "  ParentKey: long",
        )
    if role == "Joined":
        return (
            "  MotifKey: integer",
            "  EntityKey: long",
            "  GroupKey: integer",
            "  EffectiveAmount: decimal(18, 2)",
            "  ActiveFlag: boolean",
            "  ParentKey: long",
        )
    if role == "Aggregate":
        return (
            "  MotifKey: integer",
            "  GroupKey: integer",
            "  TotalAmount: decimal(18, 2)",
            "  EntityCount: long",
        )
    raise ValueError(f"no table schema for role: {role}")


def _metadata_lines(node: RepresentativeDeclaration) -> list[str]:
    lines = [
        f"{node.kind.title()} ID: {node.object_id}",
        f"Description: Representative {node.role} benchmark declaration.",
    ]
    if node.kind in {"table", "view"}:
        lines.append("Lineage: Representative benchmark data flow.")
    if node.references:
        lines.append("Dependencies:")
        lines.extend(f"  - {_object_id(reference)}" for reference in node.references)
    else:
        lines.append("Dependencies: []")
    if node.kind == "table":
        primary = "EntityKey" if node.role != "Aggregate" else "GroupKey"
        lines.extend((f"Primary key: {primary}", "Schema:", *_schema_lines(node.role)))
    if node.kind == "test":
        primary = "EntityKey" if node.role == "JoinedMeasuresMatch" else "GroupKey"
        lines.append(f"Primary key: {primary}")
    return lines


def _python_header(node: RepresentativeDeclaration) -> str:
    return '"""\n' + "\n".join(_metadata_lines(node)) + '\n"""\n'


def _sql_header(node: RepresentativeDeclaration) -> str:
    return "/*\n" + "\n".join(_metadata_lines(node)) + "\n*/\n"


def _root_rows(spec: RepresentativeLakehouseSpec, motif: int, stream: int) -> str:
    rng = random.Random(spec.seed * 1_000_003 + motif * 101 + stream * 17)
    rows = []
    for offset in range(4):
        entity = motif * 100 + offset + 1
        group = motif * 10 + (offset % 2) + 1
        amount = 100 + rng.randrange(1, 80) + stream * 5
        active = "false" if offset == 3 else "true"
        parent = "null" if offset == 0 else str(motif * 100 + 1)
        rows.append(
            "select "
            f"{motif} as MotifKey, {entity}L as EntityKey, {group} as GroupKey, "
            f"cast({amount}.00 as decimal(18, 2)) as Amount, "
            f"{active} as ActiveFlag, cast({parent} as bigint) as ParentKey"
        )
    return "\nunion all\n".join(rows)


def _root_table_source(
    node: RepresentativeDeclaration, spec: RepresentativeLakehouseSpec, stream: int
) -> str:
    class_name = _class_name(node.object_id)
    query = _root_rows(spec, node.motif, stream).replace('"', '\\"')
    return (
        _python_header(node)
        + "from weaver import Table\n\n\n"
        + f"class {class_name}(Table):\n"
        + "    def read(self):\n"
        + f'        return self.spark.sql("""{query}""")\n'
    )


def _joined_table_source(node: RepresentativeDeclaration) -> str:
    entity = _class_name(_object(node.motif, "SourceEntity"))
    adjustment = _class_name(_object(node.motif, "SourceAdjustment"))
    class_name = _class_name(node.object_id)
    return (
        _python_header(node)
        + f"from Tables.{entity} import {entity}\n"
        + f"from Tables.{adjustment} import {adjustment}\n\n"
        + "from weaver import Table\n\n\n"
        + f"class {class_name}(Table):\n"
        + "    def read(self):\n"
        + f'        entities = {entity}(self).dataframe().alias("e")\n'
        + f'        adjustments = {adjustment}(self).dataframe().alias("a")\n'
        + '        return entities.join(adjustments, on="EntityKey").selectExpr(\n'
        + '            "e.MotifKey",\n'
        + '            "EntityKey",\n'
        + '            "e.GroupKey",\n'
        + '            "cast(e.Amount + a.Amount as decimal(18, 2)) as EffectiveAmount",\n'
        + '            "e.ActiveFlag",\n'
        + '            "e.ParentKey",\n'
        + "        )\n"
    )


def _qualified(reference: str) -> tuple[str, str]:
    return tuple(_object_id(reference).split(".", 1))  # type: ignore[return-value]


def _aggregate_test_source(node: RepresentativeDeclaration) -> str:
    joined_schema, joined_object = _qualified(node.references[0])
    aggregate_schema, aggregate_object = _qualified(node.references[1])
    class_name = _class_name(node.object_id)
    methods = (
        "    def expected(self):\n"
        f'        joined = self.lakehouse.qualify("{joined_schema}", "{joined_object}")\n'
        "        local = self.spark.sql(\n"
        '            f"select MotifKey, GroupKey, "\n'
        '            f"cast(sum(EffectiveAmount) as decimal(18, 2)) as TotalAmount, "\n'
        '            f"cast(count(*) as bigint) as EntityCount "\n'
        '            f"from {joined} group by MotifKey, GroupKey"\n'
        "        )\n"
    )
    if len(node.references) == 3:
        bridge_schema, bridge_object = _qualified(node.references[2])
        methods += (
            f'        upstream = self.lakehouse.qualify("{bridge_schema}", "{bridge_object}")\n'
            "        return local.unionByName(\n"
            "            self.spark.table(upstream).select(\n"
            '                "MotifKey", "GroupKey", "TotalAmount", "EntityCount"\n'
            "            )\n"
            "        )\n\n"
        )
    else:
        methods += "        return local\n\n"
    methods += (
        "    def actual(self):\n"
        f'        aggregate = self.lakehouse.qualify("{aggregate_schema}", "{aggregate_object}")\n'
        "        return self.spark.table(aggregate).select(\n"
        '            "MotifKey", "GroupKey", "TotalAmount", "EntityCount"\n'
        "        )\n"
    )
    return (
        _python_header(node)
        + "from weaver import Test\n\n\n"
        + f"class {class_name}(Test):\n"
        + methods
    )


def _joined_test_source(node: RepresentativeDeclaration) -> str:
    entity, adjustment, joined = (
        _class_name(_object_id(reference)) for reference in node.references
    )
    class_name = _class_name(node.object_id)
    return (
        _python_header(node)
        + f"from Tables.{entity} import {entity}\n"
        + f"from Tables.{adjustment} import {adjustment}\n"
        + f"from Tables.{joined} import {joined}\n\n"
        + "from weaver import Test\n\n\n"
        + f"class {class_name}(Test):\n"
        + "    def expected(self):\n"
        + f'        left = {entity}(self).dataframe().alias("e")\n'
        + f'        right = {adjustment}(self).dataframe().alias("a")\n'
        + '        return left.join(right, on="EntityKey").selectExpr(\n'
        + '            "e.MotifKey as MotifKey",\n'
        + '            "EntityKey",\n'
        + '            "e.GroupKey as GroupKey",\n'
        + '            "cast(e.Amount + a.Amount as decimal(18, 2)) as EffectiveAmount",\n'
        + '            "e.ActiveFlag as ActiveFlag",\n'
        + '            "e.ParentKey as ParentKey",\n'
        + "        )\n\n"
        + "    def actual(self):\n"
        + f"        return {joined}(self).dataframe().select(\n"
        + '            "MotifKey", "EntityKey", "GroupKey", "EffectiveAmount",\n'
        + '            "ActiveFlag", "ParentKey",\n'
        + "        )\n"
    )


def _assumption_source(node: RepresentativeDeclaration) -> str:
    adjustment = _class_name(_object_id(node.references[0]))
    entity = _class_name(_object_id(node.references[1]))
    class_name = _class_name(node.object_id)
    return (
        _python_header(node)
        + f"from Tables.{adjustment} import {adjustment}\n"
        + f"from Tables.{entity} import {entity}\n\n"
        + "from weaver import Assumption\n\n\n"
        + f"class {class_name}(Assumption):\n"
        + "    def read(self):\n"
        + f'        parents = {entity}(self).dataframe().select("EntityKey")\n'
        + f'        children = {adjustment}(self).dataframe().where("ParentKey is not null").select("ParentKey")\n'
        + '        return children.join(parents, children.ParentKey == parents.EntityKey, "left_anti")\n'
    )


def _local(motif: int, role: str) -> str:
    return _object(motif, role)


def _sql_body(node: RepresentativeDeclaration) -> str:
    motif = node.motif

    def name(role: str) -> str:
        return _local(motif, role)

    if node.role == "Aggregate":
        local = f"""select MotifKey, GroupKey,
    cast(sum(EffectiveAmount) as decimal(18, 2)) as TotalAmount,
    cast(count(*) as bigint) as EntityCount
from {name("Joined")}
group by MotifKey, GroupKey"""
        if len(node.references) == 2:
            bridge = _object_id(node.references[-1])
            return f"""{local}
union all
select MotifKey, GroupKey, TotalAmount, EntityCount
from {bridge}"""
        return local
    if node.role == "ActiveEntities":
        return f"select * from {name('Joined')} where ActiveFlag = true"
    if node.role == "EntityProjection":
        return f"""select MotifKey, EntityKey, GroupKey,
    cast(EffectiveAmount * cast(1.10 as decimal(4, 2)) as decimal(18, 2)) as AdjustedAmount,
    EffectiveAmount
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
    cast(count(*) as bigint) as EntityCount
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
        return f"""select MotifKey, GroupKey, TotalAmount, EntityCount,
    cast(TotalAmount / nullif(EntityCount, 0) as decimal(18, 2)) as AverageAmount
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
    cast(max(u.EntityCount + j.EntityCount) as bigint) as EntityCount
from {name("SummaryUnion")} as u
join {name("SummaryJoin")} as j on j.GroupKey = u.GroupKey
left join {name("RankedEntities")} as r on r.GroupKey = u.GroupKey
group by u.MotifKey, u.GroupKey, u.TotalAmount, j.TotalAmount"""
    if node.role == "PublishedBridge":
        return f"""select MotifKey, GroupKey, TotalAmount, EntityCount
from {name("TerminalSummary")}"""
    if node.role == "TerminalSummaryMatches":
        return f"""select u.GroupKey,
    cast(u.TotalAmount + j.TotalAmount + coalesce(sum(r.EnrichedAmount), 0)
        as decimal(18, 2)) as TotalAmount,
    cast(max(u.EntityCount + j.EntityCount) as bigint) as EntityCount
from {name("SummaryUnion")} as u
join {name("SummaryJoin")} as j on j.GroupKey = u.GroupKey
left join {name("RankedEntities")} as r on r.GroupKey = u.GroupKey
group by u.GroupKey, u.TotalAmount, j.TotalAmount;

select GroupKey, TotalAmount, EntityCount
from {name("PublishedBridge")}"""
    if node.role == "NonNegative":
        return f"select GroupKey, TotalAmount from {name('TerminalSummary')} where TotalAmount < 0"
    raise ValueError(f"unknown SQL role: {node.role}")


def _source(node: RepresentativeDeclaration, spec: RepresentativeLakehouseSpec) -> str:
    if node.role == "SourceEntity":
        return _root_table_source(node, spec, 0)
    if node.role == "SourceAdjustment":
        return _root_table_source(node, spec, 1)
    if node.role == "Joined":
        return _joined_table_source(node)
    if node.role == "AggregateReconciles":
        return _aggregate_test_source(node)
    if node.role == "JoinedMeasuresMatch":
        return _joined_test_source(node)
    if node.role == "NoOrphans":
        return _assumption_source(node)
    return _sql_header(node) + _sql_body(node) + ";\n"


def write_representative_lakehouse_estate(
    root: Path, plan: RepresentativeLakehousePlan
) -> None:
    """Write a generated estate without replacing an existing source tree."""

    root = Path(root)
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"estate directory is not empty: {root}")
    for item in sorted({node.item for node in plan.declarations}):
        schema = root / item / "schemas" / f"{SCHEMA}.yml"
        schema.parent.mkdir(parents=True, exist_ok=True)
        schema.write_text(_schema_source(), encoding="utf-8")
    for node in plan.declarations:
        path = root / node.relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_source(node, plan.spec), encoding="utf-8")

    for shortcut in plan.shortcuts:
        variable = _class_name(_object_id(shortcut.destination))
        source = (
            "from weaver import Shortcut\n\n"
            f"{variable} = Shortcut(\n"
            f'    shortcut_type="{shortcut.kind}",\n'
            f'    target_type="{shortcut.target_type}",\n'
            f'    target="{shortcut.source}",\n'
            ")\n"
        )
        path = root / shortcut.destination_item / "shortcuts.py"
        path.write_text(source, encoding="utf-8")


def parse_representative_lakehouse_estate(root: Path):
    """Parse generated files through Weaver's repository boundary."""

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    return parse_item_repository(Location(str(root)))


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
        ready = {
            node
            for node in pending
            if not incoming[node] or incoming[node] <= depths.keys()
        }
        if not ready:
            raise AssertionError("representative Lakehouse graph contains a cycle")
        for node in ready:
            depths[node] = max(
                (depths[parent] + 1 for parent in incoming[node]), default=0
            )
        pending -= ready

    components = 0
    unseen = set(nodes)
    while unseen:
        components += 1
        stack = [next(iter(unseen))]
        while stack:
            node = stack.pop()
            if node not in unseen:
                continue
            unseen.remove(node)
            stack.extend(undirected[node] & unseen)

    roots = nodes - {downstream for _upstream, downstream in edges}
    leaves = nodes - {upstream for upstream, _downstream in edges}
    return {
        "nodes": len(nodes),
        "edges": len(edges),
        "roots": len(roots),
        "leaves": len(leaves),
        "maximum_depth": max(depths.values(), default=0),
        "maximum_fan_in": max((len(incoming[node]) for node in nodes), default=0),
        "maximum_fan_out": max((len(outgoing[node]) for node in nodes), default=0),
        "connected_components": components,
    }


def _repository_evidence(repository, plan: RepresentativeLakehousePlan) -> dict:
    expected_identities = set(plan.identities)
    expected_nodes = set(plan.graph_nodes)
    actual_documents = {
        str(identity): source
        for identity, source in repository.source_documents.items()
        if str(identity) in expected_identities
    }
    actual_identities = set(actual_documents)
    actual_kinds = {
        identity: str(source.kind).casefold()
        for identity, source in actual_documents.items()
    }
    expected_kinds = {node.identity: node.kind for node in plan.declarations}

    actual_graph_edges = {
        (edge.upstream, edge.downstream)
        for edge in repository.dependency_graph.edges
        if edge.upstream in expected_nodes or edge.downstream in expected_nodes
    }
    expected_shortcut_edges = set(plan.shortcut_edges)
    actual_shortcut_edges = actual_graph_edges & expected_shortcut_edges
    actual_ordinary_edges = actual_graph_edges - expected_shortcut_edges
    actual_metrics = _graph_metrics(expected_nodes, actual_graph_edges)

    expected_shortcuts = {
        (shortcut.source, shortcut.destination) for shortcut in plan.shortcuts
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
    item_counts = Counter(node.item for node in plan.declarations)
    actual_item_counts = Counter(
        node.item for node in plan.declarations if node.identity in actual_documents
    )
    validation_counts = Counter(
        source.kind.casefold()
        for source in actual_documents.values()
        if source.is_validation
    )
    graph_consumers = {
        shortcut.destination: tuple(
            sorted(
                downstream
                for upstream, downstream in actual_graph_edges
                if upstream == shortcut.destination
            )
        )
        for shortcut in plan.shortcuts
    }

    matches = {
        "declaration_identities": actual_identities == expected_identities,
        "declaration_kinds": actual_kinds == expected_kinds,
        "ordinary_edges": actual_ordinary_edges == set(plan.ordinary_edges),
        "shortcut_edges": actual_shortcut_edges == expected_shortcut_edges,
        "graph_metrics": actual_metrics == plan.statistics,
        "item_engine_distribution": actual_item_counts == item_counts,
        "validation_counts": validation_counts
        == Counter(
            {
                "test": plan.kind_counts["test"],
                "assumption": plan.kind_counts["assumption"],
            }
        ),
        "shortcut_census": actual_shortcuts == expected_shortcuts
        and len(authored_shortcuts) == len(plan.shortcuts)
        and all(shortcut.shortcut_type == "table" for shortcut in authored_shortcuts)
        and all(
            graph_consumers[shortcut.destination] == shortcut.consumers
            for shortcut in plan.shortcuts
        ),
    }
    return {"matches": matches, "metrics": actual_metrics}


def qualify_representative_lakehouse_estate(
    root: Path, spec: RepresentativeLakehouseSpec
) -> dict:
    """Generate source and compare the parsed repository with the source plan."""

    plan = make_representative_lakehouse_plan(spec)
    write_representative_lakehouse_estate(root, plan)
    repository = parse_representative_lakehouse_estate(root)
    observed = _repository_evidence(repository, plan)
    failed = [name for name, matches in observed["matches"].items() if not matches]
    if failed:
        raise AssertionError(
            "parsed representative estate differs from its source plan: "
            + ", ".join(failed)
        )

    counts = plan.kind_counts
    languages = Counter(node.language for node in plan.declarations)
    return {
        "profile": PROFILE,
        "generator": {"seed": spec.seed, "motifs": spec.motifs},
        "declarations": {
            "total": len(plan.declarations),
            "by_kind": counts,
            "by_language": dict(sorted(languages.items())),
        },
        "items": {
            "engine": {"lakehouse": len(plan.declarations)},
            "count": len({node.item for node in plan.declarations}),
            "declarations_by_item": dict(
                sorted(Counter(node.item for node in plan.declarations).items())
            ),
        },
        "validations": {
            "total": counts["test"] + counts["assumption"],
            "test": counts["test"],
            "assumption": counts["assumption"],
            "runtime_qualified": False,
            "runtime_qualification_gate": "Fabric Build-Load-Test smoke",
        },
        "shortcuts": {
            "count": len(plan.shortcuts),
            "consumed": sum(shortcut.consumed for shortcut in plan.shortcuts),
            "census": [
                {
                    "source": shortcut.source,
                    "destination": shortcut.destination,
                    "consumers": list(shortcut.consumers),
                    "kind": shortcut.kind,
                    "target_type": shortcut.target_type,
                }
                for shortcut in plan.shortcuts
            ],
        },
        "graph": observed["metrics"],
        "oracle_matches_repository": observed["matches"],
    }


# Project-side benchmark harnesses retain these stable names.
make_representative_oracle = make_representative_lakehouse_plan
write_representative_estate = write_representative_lakehouse_estate
parse_representative_estate = parse_representative_lakehouse_estate
qualify_representative_estate = qualify_representative_lakehouse_estate
