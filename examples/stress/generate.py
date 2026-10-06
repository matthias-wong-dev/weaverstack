#!/usr/bin/env python3
"""Generate the stress estate: a source project, and the estate it feeds.

The estate is what is measured. The source project only produces its data:
every source computes its rows from a day number, so each load of the source
project is one simulated day of change.

    python examples/stress/generate.py stress --workspace "Stress" --environment weaver

writes ``source/`` and ``estate/`` project folders, a workspace configuration
for each, and ``plan.json``, which records what was generated. The same
arguments always generate the same files.
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

#: Logical items. Each workspace configuration binds them to physical items.
SOURCE_LAKEHOUSE = "Lakehouse/Source"
SOURCE_WAREHOUSE = "Warehouse/SourceWarehouse"
LAKE = "Lakehouse/Lake"
CORE = "Warehouse/Core"
MART = "Warehouse/Mart"

DOMAINS = (
    "Sales",
    "Finance",
    "Inventory",
    "Logistics",
    "Customer",
    "Product",
    "Marketing",
    "Support",
    "Billing",
    "Payroll",
    "Procurement",
    "Assets",
)
NOUNS = (
    "Order",
    "Invoice",
    "Account",
    "Shipment",
    "Payment",
    "Event",
    "Contract",
    "Ticket",
    "Employee",
    "Budget",
    "Claim",
    "Ledger",
    "Supplier",
    "Visit",
    "Quote",
    "Return",
)

#: Every table and view carries this business shape: name, Delta type, T-SQL
#: type. Sources compute it from Id and Epoch; everything downstream copies it.
COLUMNS = (
    ("Id", "bigint", "bigint"),
    ("Epoch", "integer", "int"),
    ("IsDeleted", "boolean", "bit"),
    ("Category", "integer", "int"),
    ("Code", "string", "varchar(16)"),
    ("Amount", "decimal(18,2)", "decimal(18,2)"),
    ("Quantity", "integer", "int"),
    ("EventDate", "date", "date"),
    ("Status", "string", "varchar(8)"),
    ("Ref", "bigint", "bigint"),
)
NAMES = tuple(name for name, _delta, _tsql in COLUMNS)

#: A dimension holds this many rows at every scale, so every Category joins.
DIMENSION_ROWS = 1000

LARGE = 10_000_000

#: Nodes a load runs at once. Loads are latency-bound rather than
#: compute-bound: on an F64, 60 Lakehouse loads ran 3.5 times faster at 24
#: Spark lanes than at 4.
SPARK_CONCURRENCY = 24
WAREHOUSE_CONCURRENCY = 12
#: Actions a build runs at once, by capability.
BUILD_SPARK_CONCURRENCY = 16
BUILD_WAREHOUSE_CONCURRENCY = 8
BUILD_ONELAKE_CONCURRENCY = 16

#: The largest table that compares its whole source on every load.
UPSERT_ROWS = 100_000


@dataclass(frozen=True)
class Tier:
    """Sources of one size, and how they change on the days they change."""

    name: str
    #: Share of the tabular sources.
    share: float
    rows: int
    #: Rows inserted each day the source changes, at scale 1.
    inserts: int
    #: The source changes every ``period`` days.
    period: int
    #: Each change updates one row in ``updates`` and marks one in ``deletes``
    #: deleted. Zero means never.
    updates: int
    deletes: int
    #: Share of the tier that only ever appends.
    append: float = 0.0

    @property
    def feed(self) -> bool:
        """Delivered as a window of changes rather than as the whole truth."""

        return self.rows >= LARGE


#: Below 10M rows each decade holds a similar share of tables, as real fleets
#: do. Above it the shape is capped, so the estate measures Weaver rather than
#: capacity.
TIERS = (
    Tier("500M", 1 / 960, 500_000_000, 1_000_000, 1, 0, 0, append=1.0),
    Tier("100M", 4 / 960, 100_000_000, 200_000, 1, 2_000, 20_000, append=0.5),
    Tier("50M", 12 / 960, 50_000_000, 50_000, 1, 1_000, 10_000, append=0.34),
    Tier("10M", 30 / 960, 10_000_000, 10_000, 2, 500, 5_000),
    Tier("1M", 100 / 960, 1_000_000, 2_000, 4, 200, 1_000),
    Tier("100k", 250 / 960, 100_000, 200, 7, 200, 1_000),
    Tier("10k", 300 / 960, 10_000, 20, 10, 200, 1_000),
    Tier("1k", 263 / 960, DIMENSION_ROWS, 2, 20, 200, 1_000),
)

#: Share of all sources that are folders of delivered files.
FOLDER_SHARE = 0.04

# Estate behaviours, in Weaver's terms.
APPEND = "append"  # no primary key, Incremental: true
INCREMENTAL = "incremental"  # primary key, Incremental: true, deletions as a flag
INCREMENTAL_DELETE = "incremental-delete"  # the same, deletions claimed
UPSERT = "upsert"  # primary key, Incremental: false, deletions by absence
STATIC = "static"  # Static: true
INCREMENTAL_BEHAVIOURS = (APPEND, INCREMENTAL, INCREMENTAL_DELETE)
VIEW = "view"
FOLDER = "folder"


@dataclass
class Source:
    index: int
    item: str
    schema: str
    name: str
    tier: Tier | None
    rows: int
    inserts: int
    period: int
    phase: int
    updates: int
    deletes: int
    feed: bool

    @property
    def id(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def folder(self) -> bool:
        return self.tier is None


@dataclass
class Node:
    """One estate object."""

    item: str
    schema: str
    name: str
    #: folder, python, sparksql, sparkview, tsql or tsqlview.
    language: str
    behaviour: str
    rows: int
    layer: str
    parent: "Node | None" = None
    source: Source | None = None
    dimension: "Node | None" = None
    #: Holds only the rows not marked deleted.
    live: bool = False
    children: list = field(default_factory=list)

    @property
    def id(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def table(self) -> bool:
        return self.behaviour not in (VIEW, FOLDER)

    @property
    def removes(self) -> bool:
        """A deletion leaves no row behind, so a child cannot read incrementally."""

        return self.behaviour == INCREMENTAL_DELETE or self.live


@dataclass(frozen=True)
class Options:
    objects: int = 3000
    scale: float = 1.0
    seed: int = 20261005


# --- the plan ------------------------------------------------------------------


def plan_sources(options: Options) -> list[Source]:
    """About a third as many sources as the estate has objects."""

    rng = random.Random(options.seed)
    total = max(len(TIERS) + 1, options.objects // 3)
    folders = max(1, round(total * FOLDER_SHARE))
    tabular = total - folders

    counts = [max(1, round(tier.share * tabular)) for tier in TIERS[:-1]]
    counts.append(max(1, tabular - sum(counts)))

    sources: list[Source] = []
    for tier, count in zip(TIERS, counts):
        for position in range(count):
            index = len(sources)
            appends = position < round(count * tier.append)
            sources.append(
                Source(
                    index=index,
                    item=(
                        SOURCE_WAREHOUSE
                        if tier is not TIERS[0] and index % 5 in (1, 3)
                        else SOURCE_LAKEHOUSE
                    ),
                    schema=DOMAINS[index % len(DOMAINS)],
                    name=f"{NOUNS[(index // len(DOMAINS)) % len(NOUNS)]}{index:04d}",
                    tier=tier,
                    rows=(
                        DIMENSION_ROWS
                        if tier.rows == DIMENSION_ROWS
                        else max(10, round(tier.rows * options.scale))
                    ),
                    inserts=max(1, round(tier.inserts * options.scale)),
                    period=tier.period,
                    phase=rng.randrange(tier.period),
                    updates=0 if appends else tier.updates,
                    deletes=0 if appends else tier.deletes,
                    feed=tier.feed,
                )
            )
    for _ in range(folders):
        index = len(sources)
        period = rng.choice((1, 2, 3))
        sources.append(
            Source(
                index=index,
                item=SOURCE_LAKEHOUSE,
                schema="Drop",
                name=f"Delivery{index:04d}",
                tier=None,
                rows=0,
                inserts=max(
                    1, round(rng.choice((1_000, 5_000, 20_000)) * options.scale)
                ),
                period=period,
                phase=rng.randrange(period),
                updates=0,
                deletes=0,
                feed=True,
            )
        )
    return sources


#: The estate's layers after landing, in dependency order: the item, the
#: language, the share of the estate, and the layers it reads.
LAYERS = (
    ("curated", LAKE, "sparksql", 0.1000, ("landing",)),
    ("curated-views", LAKE, "sparkview", 0.0833, ("landing", "curated")),
    ("refined", LAKE, "sparksql", 0.0533, ("curated", "curated-views")),
    ("refined-views", LAKE, "sparkview", 0.0500, ("refined", "curated-views")),
    ("core", CORE, "tsql", 0.1000, ("landing", "curated")),
    ("core-views", CORE, "tsqlview", 0.0333, ("core",)),
    ("conformed", CORE, "tsql", 0.0500, ("core", "core-views")),
    ("conformed-views", CORE, "tsqlview", 0.0167, ("conformed",)),
    ("mart", MART, "tsql", 0.0567, ("core", "core-views", "conformed")),
    ("mart-views", MART, "tsqlview", 0.0567, ("mart",)),
    ("serving", MART, "tsql", 0.0267, ("mart", "mart-views")),
    ("serving-views", MART, "tsqlview", 0.0267, ("serving", "mart-views")),
)

PREFIX = {
    "landing": "Land",
    "curated": "Cur",
    "curated-views": "Cur",
    "refined": "Ref",
    "refined-views": "Ref",
    "core": "Core",
    "core-views": "Core",
    "conformed": "Conf",
    "conformed-views": "Conf",
    "mart": "Mart",
    "mart-views": "Mart",
    "serving": "Serve",
    "serving-views": "Serve",
}


def _weighted(rng: random.Random, choices) -> str:
    point = rng.random() * sum(weight for _choice, weight in choices)
    for choice, weight in choices:
        point -= weight
        if point <= 0:
            return choice
    return choices[-1][0]


def _landing_behaviour(source: Source, rng: random.Random, largest: int) -> str:
    """Landing keeps a deletion as a flag, so every layer above can read it."""

    if source.folder:
        return FOLDER
    if source.rows == largest:
        # Only the largest fact appends without a key.
        return APPEND
    if source.rows <= DIMENSION_ROWS and rng.random() < 0.2:
        return STATIC
    return INCREMENTAL


def _appends(node: Node) -> bool:
    """Every row upstream of ``node`` is only ever inserted."""

    while node.behaviour == VIEW:
        node = node.parent
    return node.behaviour == APPEND


def _child_behaviour(parent: Node, rng: random.Random) -> str | None:
    """How a table reads ``parent``, or None when no behaviour suits it."""

    choices = []
    if _appends(parent):
        choices.append((INCREMENTAL, 1.0))
    elif not parent.removes:
        choices += [(INCREMENTAL, 0.65), (INCREMENTAL_DELETE, 0.25)]
    if parent.rows <= UPSERT_ROWS:
        choices.append((UPSERT, 0.06 if choices else 1.0))
    if parent.rows <= DIMENSION_ROWS:
        choices.append((STATIC, 0.04))
    return _weighted(rng, choices) if choices else None


def _domain(node: Node) -> str:
    while node.parent is not None:
        node = node.parent
    return node.source.schema


def plan_estate(sources: list[Source], options: Options) -> list[Node]:
    rng = random.Random(options.seed + 1)
    layers: dict[str, list[Node]] = {}

    largest = max(source.rows for source in sources)
    landing = []
    for source in sources:
        behaviour = _landing_behaviour(source, rng, largest)
        landing.append(
            Node(
                item=LAKE,
                schema=f"Land{source.schema}",
                name=source.name,
                language="folder" if source.folder else "python",
                behaviour=behaviour,
                rows=source.rows,
                layer="landing",
                source=source,
                live=behaviour == UPSERT,
            )
        )
    layers["landing"] = landing
    nodes = list(landing)

    # Each delivered folder is parsed by a Python table, because it reads files.
    parsed = []
    for folder in (node for node in landing if node.behaviour == FOLDER):
        node = Node(
            item=LAKE,
            schema=f"Cur{folder.source.schema}",
            name=f"{folder.name}Rows",
            language="python",
            behaviour=INCREMENTAL,
            rows=folder.source.inserts * 30,
            layer="curated",
            parent=folder,
        )
        folder.children.append(node)
        parsed.append(node)

    quotas = _quotas(options.objects - len(landing))
    sequence = 0
    for name, item, language, _share, parents in LAYERS:
        quota = quotas[name]
        made: list[Node] = []
        if name == "curated":
            made += parsed
        pool = [
            node
            for layer in parents
            for node in layers.get(layer, ())
            if node.behaviour != FOLDER
            # A Warehouse reads a Lakehouse through its SQL endpoint, which
            # lists tables only.
            and (item == LAKE or node.item != LAKE or node.table)
        ]
        dimensions = [
            node
            for node in nodes
            if node.item == item and node.table and node.rows == DIMENSION_ROWS
        ]
        if name == "core":
            # The largest fact reaches the Warehouse: the estate's heavy path.
            for parent in (n for n in landing if n.rows == largest):
                made.append(
                    _node(parent, name, item, language, APPEND, f"{parent.name}Fact")
                )
        attempts = 0
        while len(made) < quota and attempts < quota * 50:
            attempts += 1
            parent = rng.choice(pool)
            if language in ("sparkview", "tsqlview"):
                behaviour = VIEW
                if sum(1 for c in parent.children if c.layer == name) >= 2:
                    continue
            else:
                # A table that deletes is read by views, so its deletions never
                # have to reach a table that reads it.
                if parent.removes and rng.random() < 0.95:
                    continue
                behaviour = _child_behaviour(parent, rng)
                if behaviour is None:
                    continue
                # A large table is copied once per layer, never fanned out.
                if parent.rows > LARGE and any(
                    c.layer == name for c in parent.children
                ):
                    continue
            sequence += 1
            noun = NOUNS[sequence % len(NOUNS)]
            # A build describes a Spark SQL query before its table exists, so a
            # table that reads its own high-water mark is Python, whose read()
            # only a load runs.
            written = (
                "python"
                if language == "sparksql" and behaviour in INCREMENTAL_BEHAVIOURS
                else language
            )
            node = _node(
                parent, name, item, written, behaviour, f"{noun}{sequence:04d}"
            )
            if behaviour == VIEW:
                node.live = parent.removes or rng.random() < 0.5
                candidates = [d for d in dimensions if d is not parent]
                if candidates and rng.random() < 0.3:
                    node.dimension = rng.choice(candidates)
            else:
                node.live = behaviour == UPSERT or (
                    behaviour == STATIC and parent.removes
                )
            made.append(node)
        layers[name] = made
        nodes += made
    return nodes


def _quotas(total: int) -> dict[str, int]:
    """Each layer's share of ``total``, adding up to it exactly."""

    shares = sum(share for _name, _item, _language, share, _parents in LAYERS)
    quotas = {
        name: max(1, round(total * share / shares))
        for name, _item, _language, share, _parents in LAYERS
    }
    last = LAYERS[-1][0]
    quotas[last] = max(1, quotas[last] + total - sum(quotas.values()))
    return quotas


def _node(parent, layer, item, language, behaviour, name) -> Node:
    node = Node(
        item=item,
        schema=f"{PREFIX[layer]}{_domain(parent)}",
        name=name,
        language=language,
        behaviour=behaviour,
        rows=parent.rows,
        layer=layer,
        parent=parent,
    )
    parent.children.append(node)
    return node


# --- rendering: shared ------------------------------------------------------------


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _high_water(table: str) -> str:
    """Declare ``@held``: the newest Epoch ``table`` holds, or -1.

    A build runs the query to shape the table before the table exists. Fabric
    resolves a name inside ``if`` only when the block runs, so the guard lets the
    first build pass.
    """

    return (
        "declare @held int = -1;\n"
        f"if object_id(N'{table}') is not null\n"
        "begin\n"
        f"    set @held = (select coalesce(max([Epoch]), -1) from {table});\n"
        "end;\n\n"
    )


def _schema_document(root: Path, item: str, schema: str, description: str) -> None:
    _write(
        root / item / "schemas" / f"{schema}.yml",
        f"Schema ID: {schema}\n\nDescription: {description}\n",
    )


def _delta_schema() -> str:
    return "\n".join(f"  {name}: {delta}" for name, delta, _tsql in COLUMNS)


def _tsql_schema() -> str:
    return "\n".join(f"  {name}: {tsql}" for name, _delta, tsql in COLUMNS)


def _keys(behaviour: str) -> str:
    """The header lines that say how a table loads."""

    lines = []
    if behaviour in (INCREMENTAL, INCREMENTAL_DELETE, UPSERT):
        lines.append("Primary key: Id")
    if behaviour in (APPEND, INCREMENTAL, INCREMENTAL_DELETE):
        lines.append("Incremental: true")
    if behaviour == STATIC:
        lines.append("Static: true")
    return "\n\n".join(lines) + "\n\n" if lines else ""


def _description(node: Node) -> str:
    what = {
        APPEND: "appends what is new",
        INCREMENTAL: "merges what changed, keeping deletions as a flag",
        INCREMENTAL_DELETE: "merges what changed and deletes what was deleted",
        UPSERT: "compares the whole source and deletes what is gone",
        STATIC: "loads once",
        VIEW: "presents",
        FOLDER: "copies the files that are new",
    }[node.behaviour]
    upstream = node.parent.id if node.parent else node.source.id
    return f"A {node.rows:,}-row {node.layer} object that {what} from {upstream}."


# --- rendering: the source project ----------------------------------------------

SOURCE_LIBRARY = '''"""Deterministic source rows, from a source's parameters and the day.

A source holds ids ``[0, rows + epoch * inserts)``. Each epoch it inserts
``inserts`` rows, updates the rows whose id is congruent to the epoch modulo
``updates``, and marks deleted the rows congruent to it modulo ``deletes``. A
row's Epoch is when it last changed, so a reader takes only what is newer than
what it holds.
"""

from pyspark.sql import functions as F

#: Ids are generated in partitions of about this many rows.
PARTITION_ROWS = 4_000_000


def today(clock) -> int:
    value = clock.dataframe().agg(F.max("Day")).first()[0]
    return 0 if value is None else int(value)


def epoch(day: int, period: int, phase: int) -> int:
    return (day + phase) // period


def high_water(table) -> int:
    value = table.dataframe().agg(F.max("Epoch")).first()[0]
    return -1 if value is None else int(value)


def _ids(spark, start: int, stop: int, step: int = 1):
    count = max(0, -(-(stop - start) // step))
    return spark.range(start, max(start, stop), step, max(1, count // PARTITION_ROWS))


def _state(ids, epoch: int, rows: int, inserts: int, updates: int, deletes: int):
    frame = ids.withColumn(
        "birth", F.expr(f"case when id < {rows} then 0 else 1 + (id - {rows}) div {inserts} end")
    )
    if updates:
        frame = frame.withColumn(
            "r", F.expr(f"{epoch} - pmod({epoch} - id, {updates})")
        ).withColumn("u", F.expr("case when r >= 1 and r > birth then r else birth end"))
    else:
        frame = frame.withColumn("u", F.col("birth"))
    if deletes:
        frame = frame.withColumn(
            "m", F.expr(f"birth + 1 + pmod(id - birth - 1, {deletes})")
        ).withColumn("gone", F.expr(f"m <= {epoch}"))
    else:
        frame = frame.withColumn("m", F.lit(0)).withColumn("gone", F.lit(False))
    return frame.selectExpr(
        "id as Id",
        "cast(case when gone then m else u end as int) as Epoch",
        "gone as IsDeleted",
    ).selectExpr(
        "Id",
        "Epoch",
        "IsDeleted",
        "cast(Id % 1000 as int) as Category",
        "concat('C', cast(Id % 100000 as string)) as Code",
        "cast(((Id * 37 + Epoch * 101) % 1000000) / 100.0 as decimal(18,2)) as Amount",
        "cast((Id + Epoch) % 500 as int) as Quantity",
        "date_add(date'2020-01-01', cast(Id % 2000 as int)) as EventDate",
        "case (Id + Epoch) % 4 when 0 then 'open' when 1 then 'closed' "
        "when 2 then 'pending' else 'void' end as Status",
        "Id * 3 + Epoch as Ref",
    )


def snapshot(table, clock, *, rows, inserts, period, phase, updates, deletes):
    """Every row the source holds today."""

    current = epoch(today(clock), period, phase)
    ids = _ids(table.spark, 0, rows + current * inserts)
    return _state(ids, current, rows, inserts, updates, deletes)


def feed(table, clock, *, rows, inserts, period, phase, updates, deletes):
    """The rows that changed since this source last loaded."""

    spark = table.spark
    current = epoch(today(clock), period, phase)
    high = high_water(table)
    if high >= current:
        return table.empty_dataframe(), None
    if high < 0:
        ids = _ids(spark, 0, rows + current * inserts)
    else:
        ids = _ids(spark, rows + high * inserts, rows + current * inserts)
        windows = [
            _ids(spark, day % every, rows + (day - 1) * inserts, every)
            for day in range(high + 1, current + 1)
            for every in (updates, deletes)
            if every
        ]
        for window in windows:
            ids = ids.unionByName(window)
        if windows:
            ids = ids.distinct()
    changed = _state(ids, current, rows, inserts, updates, deletes)
    return changed.where(f"Epoch > {high}"), None
'''

CLOCK_FOLDER = '''"""
Folder ID: Clock.Days

Description: One file per load of the source project. The count is the day.

Lineage: Generated, a file for each load.

File key: "day-*.json"

Incremental: true
"""

import json

from weaver import Folder


class Clock__Days(Folder):
    def read(self):
        day = len(list(self.path().glob("day-*.json")))
        with self.staging_folder() as staging:
            (staging.path / f"day-{day:05d}.json").write_text(
                json.dumps({"Day": day}) + "\\n", encoding="utf-8"
            )
        return staging, []
'''

CLOCK_TABLE = '''"""
Table ID: Clock.Day

Description: The days the source project has lived through.

Lineage: $Files/Clock.Days

Primary key: Day

Schema:
  Day: integer
"""

from Files.Clock__Days import Clock__Days

from weaver import Table


class Clock__Day(Table):
    def read(self):
        days = sorted(
            int(path.stem.split("-")[1])
            for path in Clock__Days(self).path().glob("day-*.json")
        )
        return self.spark.createDataFrame([(day,) for day in days], "Day int")
'''


def _source_keys(source: Source) -> str:
    return "Primary key: Id\n\nIncremental: true" if source.feed else "Primary key: Id"


def _python_source(source: Source) -> str:
    kind = "feed" if source.feed else "snapshot"
    return f'''"""
Table ID: {source.id}

Description: A generated {source.tier.name} source, delivered as a {kind}.

Lineage: Generated by lib/stress.py from Clock.Day.

{_source_keys(source)}

Schema:
{_delta_schema()}
"""

from Tables.Clock__Day import Clock__Day
from lib.stress import {kind}

from weaver import Table


class {source.schema}__{source.name}(Table):
    def read(self):
        return {kind}(
            self,
            Clock__Day(self),
            rows={source.rows},
            inserts={source.inserts},
            period={source.period},
            phase={source.phase},
            updates={source.updates},
            deletes={source.deletes},
        )
'''


def _drop_folder(source: Source) -> str:
    return f'''"""
Folder ID: {source.id}

Description: A file of {source.inserts:,} rows every {source.period} day(s).

Lineage: Generated, a file for each epoch the source has reached.

File key: "*.csv"

Incremental: true
"""

from Files.Clock__Days import Clock__Days

from weaver import Folder

ROWS = {source.inserts}
PERIOD = {source.period}
PHASE = {source.phase}
HEADER = "{",".join(NAMES)}"
STATUS = ("open", "closed", "pending", "void")


def _row(epoch: int, offset: int) -> str:
    id = epoch * ROWS + offset
    amount = ((id * 37 + epoch * 101) % 1000000) / 100
    return (
        f"{{id}},{{epoch}},false,{{id % 1000}},C{{id % 100000}},{{amount:.2f}},"
        f"{{(id + epoch) % 500}},2020-01-01,{{STATUS[(id + epoch) % 4]}},{{id * 3 + epoch}}"
    )


class {source.schema}__{source.name}(Folder):
    def read(self):
        day = max(0, len(list(Clock__Days(self).path().glob("day-*.json"))) - 1)
        current = (day + PHASE) // PERIOD
        held = {{path.name for path in self.path().glob("*.csv")}}
        with self.staging_folder() as staging:
            for epoch in range(current + 1):
                name = f"part-{{epoch:05d}}.csv"
                if name not in held:
                    lines = [HEADER, *(_row(epoch, offset) for offset in range(ROWS))]
                    (staging.path / name).write_text(
                        "\\n".join(lines) + "\\n", encoding="utf-8"
                    )
        return staging, []
'''


def _tsql_source(source: Source) -> str:
    rows, inserts = source.rows, source.inserts
    updates, deletes = source.updates, source.deletes
    total = f"cast({rows} as bigint) + today.e * cast({inserts} as bigint) - 1"
    clock = (
        f"    select (coalesce(max([Day]), 0) + {source.phase}) / {source.period} as e\n"
        "    from [Clock].[Day]"
    )
    if source.feed:
        windows = [
            "    select g.value as Id\n"
            "    from today cross join held\n"
            "    cross apply generate_series(\n"
            "        case when held.h < 0 then cast(0 as bigint)\n"
            f"             else cast({rows} as bigint) + held.h * cast({inserts} as bigint) end,\n"
            f"        {total},\n"
            "        cast(1 as bigint)) as g\n"
            "    where held.h < today.e"
        ]
        for every in (updates, deletes):
            if every:
                windows.append(
                    "    select g.value as Id\n"
                    "    from today cross join held\n"
                    "    cross apply generate_series(held.h + 1, today.e, 1) as d\n"
                    "    cross apply generate_series(\n"
                    f"        cast(d.value % {every} as bigint),\n"
                    f"        cast({rows} as bigint)"
                    f" + cast(d.value - 1 as bigint) * cast({inserts} as bigint) - 1,\n"
                    f"        cast({every} as bigint)) as g\n"
                    "    where held.h >= 0"
                )
        ids = "\n    union\n".join(windows)
        ctes = (
            f"today as (\n{clock}\n),\n"
            "held as (\n"
            "    select @held as h\n"
            f"),\nids as (\n{ids}\n)"
        )
        window = "\nwhere x.Epoch > @held"
        preamble = _high_water(f"[{source.schema}].[{source.name}]")
    else:
        ctes = (
            f"today as (\n{clock}\n),\n"
            "ids as (\n"
            "    select g.value as Id\n"
            "    from today\n"
            f"    cross apply generate_series(cast(0 as bigint), {total}, cast(1 as bigint)) as g\n"
            ")"
        )
        window = ""
        preamble = ""
    update = (
        f"today.e - (((today.e - i.Id) % {updates}) + {updates}) % {updates}"
        if updates
        else "cast(0 as bigint)"
    )
    delete = (
        f"b.birth + 1 + (((i.Id - b.birth - 1) % {deletes}) + {deletes}) % {deletes}"
        if deletes
        else "cast(9223372036854775807 as bigint)"
    )
    kind = "feed" if source.feed else "snapshot"
    return f"""/*
Table ID: {source.id}

Description: A generated {source.tier.name} source, delivered as a {kind}.

Lineage: Generated from the day in Clock.Day.

{_source_keys(source)}

Dependencies:
  - Clock.Day

Schema:
{_tsql_schema()}
*/
{preamble}with {ctes}
select x.Id
     , x.Epoch
     , x.IsDeleted
     , cast(x.Id % 1000 as int) as Category
     , cast(concat('C', x.Id % 100000) as varchar(16)) as Code
     , cast(((x.Id * 37 + x.Epoch * 101) % 1000000) / 100.0 as decimal(18,2)) as Amount
     , cast((x.Id + x.Epoch) % 500 as int) as Quantity
     , dateadd(day, cast(x.Id % 2000 as int), cast('2020-01-01' as date)) as EventDate
     , cast(case (x.Id + x.Epoch) % 4 when 0 then 'open' when 1 then 'closed'
            when 2 then 'pending' else 'void' end as varchar(8)) as Status
     , x.Id * 3 + x.Epoch as Ref
from (
    select i.Id
         , cast(case when m.m <= today.e then m.m
                     when u.r >= 1 and u.r > b.birth then u.r
                     else b.birth end as int) as Epoch
         , cast(case when m.m <= today.e then 1 else 0 end as bit) as IsDeleted
    from ids as i
    cross join today
    cross apply (select case when i.Id < {rows} then cast(0 as bigint)
                        else 1 + (i.Id - {rows}) / {inserts} end as birth) as b
    cross apply (select {update} as r) as u
    cross apply (select {delete} as m) as m
) as x{window};
"""


def render_source(root: Path, sources: list[Source]) -> None:
    lakehouse = root / SOURCE_LAKEHOUSE
    warehouse = root / SOURCE_WAREHOUSE
    _write(lakehouse / "lib" / "stress.py", SOURCE_LIBRARY)
    _write(lakehouse / "Files" / "Clock__Days.py", CLOCK_FOLDER)
    _write(lakehouse / "Tables" / "Clock__Day.py", CLOCK_TABLE)
    _schema_document(root, SOURCE_LAKEHOUSE, "Clock", "The source project's day.")
    _schema_document(root, SOURCE_WAREHOUSE, "Clock", "The day, from the Lakehouse.")
    _write(
        warehouse / "shortcuts.yml",
        f"logical:\n  {SOURCE_WAREHOUSE}/Clock.Day: {SOURCE_LAKEHOUSE}/Tables/Clock.Day\n",
    )
    schemas = set()
    for source in sources:
        schemas.add((source.item, source.schema))
        module = f"{source.schema}__{source.name}.py"
        if source.folder:
            _write(lakehouse / "Files" / module, _drop_folder(source))
        elif source.item == SOURCE_LAKEHOUSE:
            _write(lakehouse / "Tables" / module, _python_source(source))
        else:
            _write(warehouse / f"{source.id}.sql", _tsql_source(source))
    for item, schema in sorted(schemas):
        _schema_document(root, item, schema, f"Generated {schema} sources.")


# --- rendering: the estate -------------------------------------------------------

ESTATE_LIBRARY = '''"""What the estate's Python objects share."""

from pyspark.sql import functions as F

COLUMNS = {columns}


def high_water(table) -> int:
    """The newest Epoch this table holds, or -1 when it holds nothing."""

    value = table.dataframe().agg(F.max("Epoch")).first()[0]
    return -1 if value is None else int(value)
'''


#: The UTC change stamp each load writes, as each engine names it.
DELTA_STAMP = "row_update_datetime"
WAREHOUSE_STAMP = "Row update datetime"


def _python_read(behaviour: str, read: str, *, stamp: str | None) -> str:
    """A read() body. A table parent is read past this table's bookmark.

    Each load of a parent table stamps what it changed in UTC, so a child reads
    only what changed since its own last clean load began, and Delta skips every
    file older than that. A view carries no stamp, so a reader of one uses the
    newest Epoch it holds.
    """

    if stamp is not None:
        newer = f'rows.where(rows["{stamp}"] > self.bookmark()).select(*COLUMNS)'
        rows = read
    else:
        newer = "rows.where(rows.Epoch > high_water(self))"
        rows = f"{read}.select(*COLUMNS)"
    return {
        APPEND: f"        rows = {rows}\n        return {newer}, None",
        INCREMENTAL: f"        rows = {rows}\n        return {newer}, None",
        INCREMENTAL_DELETE: (
            f"        rows = {rows}\n"
            f"        changed = {newer}\n"
            "        return (\n"
            "            changed.where(~changed.IsDeleted),\n"
            '            changed.where(changed.IsDeleted).select("Id"),\n'
            "        )"
        ),
        UPSERT: (
            f"        rows = {read}.select(*COLUMNS)\n"
            "        return rows.where(~rows.IsDeleted)"
        ),
    }.get(behaviour, f"        return {read}.select(*COLUMNS)")


def _python_landing(node: Node, shortcut: str) -> str:
    source = node.source
    read = f"{shortcut}(self).{source.name}.dataframe()"
    stamp = WAREHOUSE_STAMP if source.item == SOURCE_WAREHOUSE else DELTA_STAMP
    body = _python_read(node.behaviour, read, stamp=stamp)
    imported = "COLUMNS, high_water" if "high_water" in body else "COLUMNS"
    return f'''"""
Table ID: {node.id}

Description: {_description(node)}

Lineage: {source.id}, through the {shortcut} schema shortcut.

{_keys(node.behaviour)}Schema:
{_delta_schema()}
"""

from lib.estate import {imported}
from shortcuts import {shortcut}

from weaver import Table


class {node.schema}__{node.name}(Table):
    def read(self):
{body}
'''


def _python_table(node: Node) -> str:
    parent = node.parent
    read = (
        f'self.spark.table(self.lakehouse.qualify("{parent.schema}", "{parent.name}"))'
    )
    body = _python_read(
        node.behaviour, read, stamp=DELTA_STAMP if parent.table else None
    )
    imported = "COLUMNS, high_water" if "high_water" in body else "COLUMNS"
    return f'''"""
Table ID: {node.id}

Description: {_description(node)}

Lineage: Copied from {parent.id}.

{_keys(node.behaviour)}Dependencies:
  - {parent.id}

Schema:
{_delta_schema()}
"""

from lib.estate import {imported}

from weaver import Table


class {node.schema}__{node.name}(Table):
    def read(self):
{body}
'''


def _landing_folder(node: Node, shortcut: str) -> str:
    return f'''"""
Folder ID: {node.id}

Description: {_description(node)}

Lineage: {node.source.id}, through a folder shortcut.

File key: "*.csv"

Incremental: true
"""

import shutil

from shortcuts import {shortcut}

from weaver import Folder


class {node.schema}__{node.name}(Folder):
    def read(self):
        held = {{path.name for path in self.path().glob("*.csv")}}
        with self.staging_folder() as staging:
            for path in sorted({shortcut}(self).path().glob("*.csv")):
                if path.name not in held:
                    shutil.copyfile(path, staging.path / path.name)
        return staging, []
'''


def _parsed_folder(node: Node) -> str:
    folder = node.parent
    module = f"{folder.schema}__{folder.name}"
    shape = ", ".join(
        f"{name} {'int' if delta == 'integer' else delta}"
        for name, delta, _t in COLUMNS
    )
    return f'''"""
Table ID: {node.id}

Description: The rows of each file {folder.id} receives, merged as they arrive.

Lineage: $Files/{folder.id}

{_keys(node.behaviour)}Schema:
{_delta_schema()}
"""

from Files.{module} import {module}
from lib.estate import COLUMNS

from weaver import Table

SHAPE = "{shape}"


class {node.schema}__{node.name}(Table):
    def read(self):
        folder = {module}(self)
        arrived = folder.files_since(self.bookmark())
        if not arrived:
            return self.empty_dataframe(), None
        root = folder.spark_path()
        paths = [f"{{root}}/{{path.name}}" for path in sorted(arrived)]
        rows = self.spark.read.option("header", True).schema(SHAPE).csv(paths)
        return rows.select(*COLUMNS), None
'''


def _quoted(name: str, tsql: bool) -> str:
    return f"[{name}]" if tsql else name


def _columns(tsql: bool, alias: str = "p") -> str:
    return "\n     , ".join(f"{alias}.{_quoted(name, tsql)}" for name in NAMES)


def _sql_table(node: Node, parent: str, *, tsql: bool) -> str:
    behaviour = node.behaviour
    # A Spark SQL query cannot guard a read of its own table.
    assert tsql or behaviour not in INCREMENTAL_BEHAVIOURS, node.id
    deleted = "p.[IsDeleted] = 1" if tsql else "p.IsDeleted"
    live = "p.[IsDeleted] = 0" if tsql else "not p.IsDeleted"
    newer = "p.[Epoch] > @held"
    query = f"select {_columns(tsql)}\nfrom {parent} as p"
    preamble = _high_water(_tsql_name(node)) if tsql else ""
    if behaviour in (APPEND, INCREMENTAL):
        body = f"{preamble}{query}\nwhere {newer};\n"
    elif behaviour == INCREMENTAL_DELETE:
        body = (
            f"{preamble}{query}\nwhere {newer}\n  and {live};\n\n"
            f"select p.[Id]\nfrom {parent} as p\nwhere {newer}\n  and {deleted};\n"
        )
    elif behaviour == UPSERT and not node.parent.removes:
        body = f"{query}\nwhere {live};\n"
    else:
        body = f"{query};\n"
    return f"""/*
Table ID: {node.id}

Description: {_description(node)}

Lineage: Copied from {node.parent.id}.

{_keys(behaviour)}Dependencies:
  - {node.parent.id}

Schema:
{_tsql_schema() if tsql else _delta_schema()}
*/
{body}"""


def _sql_view(node: Node, parent: str, dimension: str | None, *, tsql: bool) -> str:
    columns = _columns(tsql)
    dependencies = [node.parent.id]
    joined = ""
    if dimension is not None:
        dependencies.append(node.dimension.id)
        columns += (
            f"\n     , d.{_quoted('Code', tsql)} as {_quoted('CategoryCode', tsql)}"
        )
        on = f"d.{_quoted('Id', tsql)} = p.{_quoted('Category', tsql)}"
        joined = f"\nleft join {dimension} as d on {on}"
    where = ""
    if node.live and not node.parent.removes:
        where = "\nwhere p.[IsDeleted] = 0" if tsql else "\nwhere not p.IsDeleted"
    listed = "\n".join(f"  - {dependency}" for dependency in dependencies)
    return f"""/*
View ID: {node.id}

Description: {_description(node)}

Lineage: {node.parent.id}

Dependencies:
{listed}
*/
select {columns}
from {parent} as p{joined}{where};
"""


def _tsql_name(node: Node) -> str:
    return f"[{node.schema}].[{node.name}]"


def _schema_shortcut(target: str, workspace: str, kind: str = "schema") -> str:
    return (
        "Shortcut(\n"
        f'    shortcut_type="{kind}",\n'
        '    target_type="physical",\n'
        f'    target="{target}",\n'
        f'    workspace="{workspace}",\n'
        ")"
    )


def render_estate(root: Path, nodes: list[Node], names: dict) -> None:
    lake = root / LAKE
    _write(lake / "lib" / "estate.py", ESTATE_LIBRARY.format(columns=repr(NAMES)))

    schemas: dict[str, set[str]] = {LAKE: set(), CORE: set(), MART: set()}
    lake_shortcuts: dict[str, str] = {}
    view_shortcuts: dict[str, dict[str, str]] = {CORE: {}, MART: {}}
    sources = {
        SOURCE_LAKEHOUSE: f"Lakehouse/{names['source_lakehouse']}",
        SOURCE_WAREHOUSE: f"Warehouse/{names['source_warehouse']}",
    }

    for node in nodes:
        schemas[node.item].add(node.schema)
        module = f"{node.schema}__{node.name}.py"
        if node.layer == "landing":
            source = node.source
            if source.folder:
                shortcut = f"SrcDrop__{source.name}"
                lake_shortcuts[shortcut] = _schema_shortcut(
                    f"{sources[source.item]}/Files/{source.schema}/{source.name}",
                    names["source_workspace"],
                    kind="folder",
                )
                _write(lake / "Files" / module, _landing_folder(node, shortcut))
                continue
            prefix = "Src" if source.item == SOURCE_LAKEHOUSE else "Whs"
            shortcut = f"{prefix}{source.schema}"
            lake_shortcuts[shortcut] = _schema_shortcut(
                f"{sources[source.item]}/{source.schema}", names["source_workspace"]
            )
            _write(lake / "Tables" / module, _python_landing(node, shortcut))
            continue
        if node.item == LAKE:
            if node.language == "python":
                parsed = node.parent.behaviour == FOLDER
                text = _parsed_folder(node) if parsed else _python_table(node)
                path = lake / "Tables" / module
            elif node.behaviour == VIEW:
                dimension = node.dimension.id if node.dimension else None
                text = _sql_view(node, node.parent.id, dimension, tsql=False)
                path = lake / "Tables" / f"{node.id}.sql"
            else:
                text = _sql_table(node, node.parent.id, tsql=False)
                path = lake / "Tables" / f"{node.id}.sql"
            _write(path, text)
            continue
        # A Warehouse reads another item through a view of the same name.
        for upstream in filter(None, (node.parent, node.dimension)):
            if upstream.item != node.item:
                area = "Tables/" if upstream.item == LAKE else ""
                view_shortcuts[node.item][f"{node.item}/{upstream.id}"] = (
                    f"{upstream.item}/{area}{upstream.id}"
                )
                schemas[node.item].add(upstream.schema)
        if node.behaviour == VIEW:
            dimension = _tsql_name(node.dimension) if node.dimension else None
            text = _sql_view(node, _tsql_name(node.parent), dimension, tsql=True)
        else:
            text = _sql_table(node, _tsql_name(node.parent), tsql=True)
        _write(root / node.item / f"{node.id}.sql", text)

    declared = "\n\n".join(
        f"{name} = {declaration}"
        for name, declaration in sorted(lake_shortcuts.items())
    )
    _write(lake / "shortcuts.py", f"from weaver import Shortcut\n\n{declared}\n")
    for item, entries in view_shortcuts.items():
        if entries:
            lines = "".join(f"  {d}: {t}\n" for d, t in sorted(entries.items()))
            _write(root / item / "shortcuts.yml", f"logical:\n{lines}")
    for item, item_schemas in schemas.items():
        for schema in sorted(item_schemas):
            _schema_document(root, item, schema, f"Generated {schema} objects.")
    render_tests(root, nodes)


def render_tests(root: Path, nodes: list[Node]) -> None:
    """One Test per item and behaviour, each on a small table.

    Each compares a table with what its parent says it should hold, so
    ``weaver test`` shows every behaviour converged after a load.
    """

    chosen: dict[tuple[str, str], Node] = {}
    for node in nodes:
        if (
            node.table
            and node.parent is not None
            and node.behaviour != STATIC
            and node.rows <= 100_000
            and node.parent.behaviour != FOLDER
        ):
            chosen.setdefault((node.item, node.behaviour), node)
    for (item, behaviour), node in sorted(chosen.items()):
        tsql = node.language == "tsql"
        parent = _tsql_name(node.parent) if tsql else node.parent.id
        me = _tsql_name(node) if tsql else node.id
        columns = ", ".join(
            _quoted(name, tsql) for name in ("Id", "Epoch", "IsDeleted")
        )
        expected = f"select {columns} from {parent}"
        if node.live or behaviour in (UPSERT, INCREMENTAL_DELETE):
            expected += " where [IsDeleted] = 0" if tsql else " where not IsDeleted"
        name = f"{node.schema}.{node.name}Converged"
        dependencies = (
            "" if tsql else f"Dependencies:\n  - {node.parent.id}\n  - {node.id}\n\n"
        )
        _write(
            root / item / "tests" / f"{name}.sql",
            f"""/*
Test ID: {name}

Description: {node.id} holds what its {behaviour} load should leave.

Primary key: Id

{dependencies}*/

-- Expected: the parent, as this behaviour presents it.
{expected};

-- Actual: what the loads left.
select {columns} from {me};
""",
        )


# --- configuration and plan ---------------------------------------------------------


def render_configuration(out: Path, names: dict) -> None:
    def config(workspace, catalogue, targets) -> str:
        bound = "".join(f"  {logical}: {physical}\n" for logical, physical in targets)
        return (
            f"workspace: {workspace}\n"
            f"environment: {names['environment']}\n"
            f"catalogue: Warehouse/{catalogue}\n\n"
            f"targets:\n{bound}\n"
            "execution:\n"
            "  build:\n"
            f"    spark_concurrency: {BUILD_SPARK_CONCURRENCY}\n"
            f"    warehouse_concurrency: {BUILD_WAREHOUSE_CONCURRENCY}\n"
            f"    onelake_concurrency: {BUILD_ONELAKE_CONCURRENCY}\n"
            "  run:\n"
            f"    spark_concurrency: {SPARK_CONCURRENCY}\n"
            f"    warehouse_concurrency: {WAREHOUSE_CONCURRENCY}\n"
        )

    _write(
        out / "source.yml",
        config(
            names["source_workspace"],
            names["source_catalogue"],
            [
                (SOURCE_LAKEHOUSE, names["source_lakehouse"]),
                (SOURCE_WAREHOUSE, names["source_warehouse"]),
            ],
        ),
    )
    _write(
        out / "estate.yml",
        config(
            names["workspace"],
            names["catalogue"],
            [(LAKE, names["lakehouse"]), (CORE, names["core"]), (MART, names["mart"])],
        ),
    )


def summary(sources: list[Source], nodes: list[Node], options: Options) -> dict:
    def count(values) -> dict:
        return dict(sorted(Counter(values).items()))

    tables = [node for node in nodes if node.table]
    return {
        "options": {
            "objects": options.objects,
            "scale": options.scale,
            "seed": options.seed,
        },
        "source": {
            # The clock folder and table beside the generated sources.
            "objects": len(sources) + 2,
            "by_item": count(source.item for source in sources),
            "by_tier": count(
                source.tier.name if source.tier else "folder" for source in sources
            ),
        },
        "estate": {
            "objects": len(nodes),
            "by_item": count(node.item for node in nodes),
            "by_language": count(node.language for node in nodes),
            "by_behaviour": count(node.behaviour for node in nodes),
            "by_layer": count(node.layer for node in nodes),
            "tables": len(tables),
            "tables_over_10M_rows": sum(1 for node in tables if node.rows > LARGE),
            "rows_in_tables": sum(node.rows for node in tables),
        },
    }


def generate(out: Path, names: dict, options: Options) -> dict:
    sources = plan_sources(options)
    nodes = plan_estate(sources, options)
    for project in ("source", "estate"):
        shutil.rmtree(out / project, ignore_errors=True)
    render_source(out / "source", sources)
    render_estate(out / "estate", nodes, names)
    render_configuration(out, names)
    plan = summary(sources, nodes, options)
    _write(out / "plan.json", json.dumps(plan, indent=2) + "\n")
    return plan


def arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path, help="Folder to write the projects into.")
    parser.add_argument("--workspace", required=True, help="The estate's workspace.")
    parser.add_argument(
        "--source-workspace", help="The source project's workspace. Default: the same."
    )
    parser.add_argument("--environment", required=True, help="The Fabric Environment.")
    parser.add_argument("--objects", type=int, default=3000, help="Estate objects.")
    parser.add_argument("--scale", type=float, default=1.0, help="Row count factor.")
    parser.add_argument("--seed", type=int, default=20261005)
    parser.add_argument("--lakehouse", default="StressLake")
    parser.add_argument("--core", default="StressCore")
    parser.add_argument("--mart", default="StressMart")
    parser.add_argument("--catalogue", default="StressCatalogue")
    parser.add_argument("--source-lakehouse", default="StressSource")
    parser.add_argument("--source-warehouse", default="StressSourceWarehouse")
    parser.add_argument("--source-catalogue", default="StressSourceCatalogue")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    options = arguments(argv)
    names = {
        "workspace": options.workspace,
        "source_workspace": options.source_workspace or options.workspace,
        "environment": options.environment,
        "lakehouse": options.lakehouse,
        "core": options.core,
        "mart": options.mart,
        "catalogue": options.catalogue,
        "source_lakehouse": options.source_lakehouse,
        "source_warehouse": options.source_warehouse,
        "source_catalogue": options.source_catalogue,
    }
    plan = generate(
        options.out,
        names,
        Options(objects=options.objects, scale=options.scale, seed=options.seed),
    )
    print(json.dumps(plan, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
