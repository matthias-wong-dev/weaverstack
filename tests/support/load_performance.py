"""Time ``weaver load`` of keyed incremental Tables in real Fabric.

Each engine has one item holding Tables of the same 20-column business shape.
Their rows are generated inside the authored load from a control table,
``dbo.WeaverLoadBench``, which the harness rewrites before each load. A control
row is a segment: ``RowTotal`` ids from ``FirstId`` every ``Stride``, at a
``Revision`` that changes the non-key columns. A ``delete`` segment is an
explicit delete claim. ``dbo`` is outside every item's managed schemas, so a
Build never prunes the control table.

The same segments and revision always produce the same rows, so a target's
expected state is known exactly and checked after each load.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

LAKEHOUSE = "lakehouse"
WAREHOUSE = "warehouse"

ITEM = "LoadBench"
SCHEMA = "Bench"
CONTROL = "WeaverLoadBench"

UPSERT = "upsert"
DELETE = "delete"


@dataclass(frozen=True)
class Column:
    name: str
    #: The Lakehouse schema declaration's type.
    declared: str
    #: Spark SQL over ``id`` and ``v``.
    spark: str
    #: T-SQL over ``i.id`` and ``i.v``.
    tsql: str


#: The business shape: a bigint key and 19 columns of varied types, every one a
#: function of the key, and the measures and status also of the revision.
COLUMNS = (
    Column("OrderId", "long", "id", "cast(i.id as bigint)"),
    Column(
        "CustomerId",
        "integer",
        "cast(id % 100000 as int)",
        "cast(i.id % 100000 as int)",
    ),
    Column(
        "ProductCode",
        "string",
        "concat('P', lpad(cast(id % 5000 as string), 5, '0'))",
        "cast(concat('P', right('00000' + cast(i.id % 5000 as varchar(5)), 5))"
        " as varchar(6))",
    ),
    Column(
        "Region",
        "string",
        "element_at(array('North', 'South', 'East', 'West', 'Central'),"
        " cast(id % 5 as int) + 1)",
        "cast(choose(i.id % 5 + 1, 'North', 'South', 'East', 'West', 'Central')"
        " as varchar(8))",
    ),
    Column(
        "Channel",
        "string",
        "element_at(array('Web', 'Store', 'Phone', 'Partner'), cast(id % 4 as int) + 1)",
        "cast(choose(i.id % 4 + 1, 'Web', 'Store', 'Phone', 'Partner') as varchar(8))",
    ),
    Column(
        "Quantity",
        "integer",
        "cast(id % 17 + 1 + v as int)",
        "cast(i.id % 17 + 1 + i.v as int)",
    ),
    Column(
        "UnitPrice",
        "decimal(18, 2)",
        "cast((id % 1000 + 1) * 1.25 as decimal(18, 2))",
        "cast((i.id % 1000 + 1) * 1.25 as decimal(18, 2))",
    ),
    Column(
        "Discount",
        "decimal(9, 4)",
        "cast((id % 20) / 100.0 as decimal(9, 4))",
        "cast((i.id % 20) / 100.0 as decimal(9, 4))",
    ),
    Column(
        "Amount",
        "decimal(18, 2)",
        "cast((id % 17 + 1 + v) * (id % 1000 + 1) * 1.25 as decimal(18, 2))",
        "cast((i.id % 17 + 1 + i.v) * (i.id % 1000 + 1) * 1.25 as decimal(18, 2))",
    ),
    Column(
        "TaxRate",
        "double",
        "cast((id % 3) * 0.05 as double)",
        "cast((i.id % 3) * 0.05 as float)",
    ),
    Column(
        "OrderDate",
        "date",
        "date_add(date'2020-01-01', cast(id % 2000 as int))",
        "dateadd(day, cast(i.id % 2000 as int), cast('2020-01-01' as date))",
    ),
    Column(
        "ShippedAt",
        "timestamp",
        "timestamp_seconds(1577836800 + id % 63072000 + v * 3600)",
        "dateadd(second, cast(i.id % 63072000 as int) + i.v * 3600,"
        " cast('2020-01-01' as datetime2(6)))",
    ),
    Column(
        "IsPriority",
        "boolean",
        "id % 7 = 0",
        "cast(case when i.id % 7 = 0 then 1 else 0 end as bit)",
    ),
    Column(
        "Status",
        "string",
        "case when v = 0 then 'Open' else 'Shipped' end",
        "cast(case when i.v = 0 then 'Open' else 'Shipped' end as varchar(8))",
    ),
    Column(
        "Notes",
        "string",
        "concat('Order ', cast(id as string), ' revision ', cast(v as string))",
        "cast(concat('Order ', i.id, ' revision ', i.v) as varchar(64))",
    ),
    Column(
        "WeightKg",
        "double",
        "cast(id % 500 / 10.0 as double)",
        "cast(i.id % 500 / 10.0 as float)",
    ),
    Column(
        "LineCount",
        "integer",
        "cast(id % 9 + 1 as int)",
        "cast(i.id % 9 + 1 as int)",
    ),
    Column("Currency", "string", "'AUD'", "cast('AUD' as varchar(3))"),
    Column(
        "SalesRepId",
        "integer",
        "cast(id % 250 as int)",
        "cast(i.id % 250 as int)",
    ),
    Column("Revision", "integer", "cast(v as int)", "cast(i.v as int)"),
)


#: The Tables each engine's item holds. ``Small`` serves the tiny, empty and
#: unchanged loads, ``Large`` the large initial load, and ``Huge`` the small
#: change against a very large target, which is kept between runs.
TABLES = ("Small", "Large", "Huge")


@dataclass(frozen=True)
class Segment:
    first_id: int
    rows: int
    stride: int = 1
    revision: int = 0
    role: str = UPSERT

    def ids(self) -> range:
        return range(
            self.first_id, self.first_id + self.rows * self.stride, self.stride
        )


# --- the estate ---------------------------------------------------------------


def write_load_estate(root: Path, tables=TABLES) -> Path:
    """Write both engines' items under ``root/source`` and return it."""

    source = Path(root) / "source"
    for kind in ("Lakehouse", "Warehouse"):
        schema = source / kind / ITEM / "schemas" / f"{SCHEMA}.yml"
        schema.parent.mkdir(parents=True, exist_ok=True)
        schema.write_text(
            f"Schema ID: {SCHEMA}\nDescription: Load benchmark tables.\n",
            encoding="utf-8",
        )
    for name in tables:
        path = source / "Lakehouse" / ITEM / "Tables" / f"{SCHEMA}__{name}.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_python_table(name), encoding="utf-8")
        (source / "Warehouse" / ITEM / f"{SCHEMA}.{name}.sql").write_text(
            _tsql_table(name), encoding="utf-8"
        )
    write_flow_estate(source)
    return source


#: Independent branches for estate throughput: A feeds B, C feeds D, and E
#: stands alone. A, C and E are generated; B and D copy their upstream.
FLOW_ITEM = "LoadFlow"
FLOW_SCHEMA = "Flow"
FLOW = {"A": None, "B": "A", "C": None, "D": "C", "E": None}


def write_flow_estate(source: Path) -> None:
    """Add the flow item to both engines beside the benchmark item."""

    for kind in ("Lakehouse", "Warehouse"):
        schema = source / kind / FLOW_ITEM / "schemas" / f"{FLOW_SCHEMA}.yml"
        schema.parent.mkdir(parents=True, exist_ok=True)
        schema.write_text(
            f"Schema ID: {FLOW_SCHEMA}\nDescription: Load benchmark branches.\n",
            encoding="utf-8",
        )
    for name, upstream in FLOW.items():
        lakehouse = source / "Lakehouse" / FLOW_ITEM / "Tables"
        lakehouse.mkdir(parents=True, exist_ok=True)
        warehouse = source / "Warehouse" / FLOW_ITEM
        control = f"{FLOW_SCHEMA}.{name}"
        if upstream is None:
            python = _python_table(name, FLOW_SCHEMA, control)
            tsql = _tsql_table(name, FLOW_SCHEMA, control)
        else:
            python, tsql = _copied_tables(name, upstream)
        (lakehouse / f"{FLOW_SCHEMA}__{name}.py").write_text(python, encoding="utf-8")
        (warehouse / f"{FLOW_SCHEMA}.{name}.sql").write_text(tsql, encoding="utf-8")


def _copied_tables(name: str, upstream: str) -> tuple[str, str]:
    """A Table that copies its upstream, so it depends on it in each engine."""

    header = _header(name, declared=True, schema=FLOW_SCHEMA, generated=False)
    source = f"{FLOW_SCHEMA}__{upstream}"
    python = (
        f'"""\n{header}\n"""\n\n'
        f"from Tables.{source} import {source}\n\n"
        "from weaver import Table\n\n\n"
        f"class {FLOW_SCHEMA}__{name}(Table):\n"
        "    def read(self):\n"
        f"        return {source}(self).dataframe()\n"
    )
    columns = ", ".join(c.name for c in COLUMNS)
    tsql = (
        f"/*\n{_header(name, declared=False, schema=FLOW_SCHEMA, generated=False)}\n*/\n"
        f"select {columns}\nfrom [{FLOW_SCHEMA}].[{upstream}];\n"
    )
    return python, tsql


def _header(
    name: str, *, declared: bool, schema: str = SCHEMA, generated: bool = True
) -> str:
    lines = [
        f"Table ID: {schema}.{name}",
        "",
        "Description: Generated orders for the Load benchmark.",
        "",
        "Lineage: Generated from the benchmark's control segments.",
        "",
    ]
    if generated:
        # The control table is outside every item, so nothing is discovered.
        lines += ["Dependencies: []", ""]
    lines += [
        "Primary key: OrderId",
        "",
        "Incremental: true",
    ]
    if declared:
        lines += ["", "Schema:"] + [f"  {c.name}: {c.declared}" for c in COLUMNS]
    return "\n".join(lines)


def _python_table(name: str, schema: str = SCHEMA, control: str | None = None) -> str:
    control = control or name
    shape = ",\n".join(f'            "{c.spark} AS {c.name}"' for c in COLUMNS)
    return f'''"""
{_header(name, declared=True, schema=schema)}
"""

from weaver import Table

SHAPE = (
{shape},
)

#: Ids are generated a chunk per row and exploded, so a large segment spreads
#: across partitions without a shuffle.
CHUNK = 1_000_000


def _ids(spark, first, rows, stride):
    chunks = -(-rows // CHUNK)
    return spark.range(0, chunks, 1, chunks).selectExpr(
        f"explode(sequence(id * {{CHUNK}}, least((id + 1) * {{CHUNK}}, {{rows}}) - 1)) AS n"
    ).selectExpr(f"{{first}} + n * {{stride}} AS id")


class {schema}__{name}(Table):
    def read(self):
        control = self.spark.table(self.lakehouse.qualify("dbo", "{CONTROL}"))
        segments = control.where("TableName = '{control}'").collect()
        rows = self.spark.createDataFrame([], "id long, v int")
        claims = self.spark.createDataFrame([], "OrderId long")
        for s in segments:
            if not s.RowTotal:
                continue
            ids = _ids(self.spark, s.FirstId, s.RowTotal, s.Stride)
            if s.Role == "{DELETE}":
                claims = claims.unionAll(ids.selectExpr("id AS OrderId"))
            else:
                rows = rows.unionAll(ids.selectExpr("id", f"{{int(s.Revision)}} AS v"))
        return rows.selectExpr(*SHAPE), claims
'''


def _tsql_table(name: str, schema: str = SCHEMA, control: str | None = None) -> str:
    control = control or name
    shape = "\n  , ".join(f"{c.tsql} as {c.name}" for c in COLUMNS)

    def segments(role: str) -> str:
        return (
            "select s.FirstId + g.value * s.Stride as id, s.Revision as v\n"
            f"    from [dbo].[{CONTROL}] as s\n"
            "    cross apply generate_series(cast(0 as bigint), s.RowTotal - 1) as g\n"
            f"    where s.TableName = '{control}' and s.Role = '{role}'"
        )

    return (
        f"/*\n{_header(name, declared=False, schema=schema)}\n*/\n"
        f"select\n    {shape}\nfrom (\n    {segments(UPSERT)}\n) as i;\n\n"
        f"select cast(i.id as bigint) as OrderId\nfrom (\n    {segments(DELETE)}\n) as i;\n"
    )


# --- the harness ----------------------------------------------------------------


def _names() -> dict[str, str]:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "fabric"))
    import performance_estate as estate

    return {
        "catalogue": estate.name("perf_load_weaver"),
        LAKEHOUSE: estate.name("perf_load_lakehouse"),
        WAREHOUSE: estate.name("perf_load_warehouse"),
    }


def _kind(engine: str) -> str:
    return "Lakehouse" if engine == LAKEHOUSE else "Warehouse"


def _update_column(engine: str) -> str:
    from weaver.declaration.metadata import AUDIT_UPDATE, PYTHON, SQL, audit_column_name

    return audit_column_name(AUDIT_UPDATE, PYTHON if engine == LAKEHOUSE else SQL)


@dataclass
class LoadTiming:
    """One timed ``weaver.load`` and the evidence that explains it."""

    scenario: str
    engine: str
    table: str
    seconds: float
    succeeded: bool
    counts: dict = field(default_factory=dict)
    steps: dict = field(default_factory=dict)
    crossings: dict = field(default_factory=dict)
    spark: dict = field(default_factory=dict)
    error: str | None = None

    def describe(self) -> str:
        lines = [
            f"{self.scenario} {self.engine} {self.table}: {self.seconds:.1f}s "
            + ("ok" if self.succeeded else "FAILED")
        ]
        if self.error:
            lines.append(f"  error: {self.error}")
        for title, values in (
            ("counts", self.counts),
            ("steps", self.steps),
            ("crossings", self.crossings),
            ("spark", self.spark),
        ):
            if values:
                lines.append(
                    f"  {title}: " + ", ".join(f"{k}={v}" for k, v in values.items())
                )
        return "\n".join(lines)

    def to_mapping(self) -> dict:
        return {
            "scenario": self.scenario,
            "engine": self.engine,
            "table": self.table,
            "seconds": round(self.seconds, 2),
            "succeeded": self.succeeded,
            "counts": self.counts,
            "steps": self.steps,
            "crossings": self.crossings,
            "spark": self.spark,
            "error": self.error,
        }


COUNTS = ("rows_read", "rows_inserted", "rows_updated", "rows_deleted", "rows_rejected")


class LoadBench:
    """Drive the benchmark items through one Session.

    ``livy`` is the Livy session the Lakehouse work runs in, used directly for
    the control table and for verification, which are outside every timing.
    """

    def __init__(self, *, session, livy, workspace_name: str, environment=None):
        self.session = session
        self.livy = livy
        self.workspace_name = workspace_name
        self.environment = environment
        self.names = _names()
        self.catalogue = f"Warehouse/{self.names['catalogue']}"
        self.timings: list[LoadTiming] = []

    # --- setup ---------------------------------------------------------------

    def prepare(self, source: Path) -> None:
        """Stand the catalogue up, create the control tables and build."""

        import weaver
        from support.catalogue import build_catalogue_item
        from weaver.fabric import OneLakeDfsClient
        from weaver.targets import ItemRef
        from weaver.workspaces import Workspace

        result = build_catalogue_item(
            catalogue=ItemRef(self.names["catalogue"]),
            workspace=Workspace(
                workspace=self.workspace_name, catalogue=self.catalogue
            ),
            store=OneLakeDfsClient(),
            session=self.session,
        )
        if not result.succeeded:
            raise AssertionError(f"the benchmark catalogue was not built: {result}")
        self._sql().execute_script(
            f"if object_id(N'dbo.{CONTROL}') is null\n"
            f"create table dbo.{CONTROL} (TableName varchar(32), Role varchar(8),"
            " FirstId bigint, RowTotal bigint, Stride bigint, Revision int);"
        )
        self.spark(
            f"spark.sql('''CREATE TABLE IF NOT EXISTS {self._control()} "
            "(TableName STRING, Role STRING, FirstId BIGINT, RowTotal BIGINT,"
            " Stride BIGINT, Revision INT) USING delta''')"
        )
        built = weaver.build(
            str(source),
            items=[
                f"{_kind(engine)}/{item}={_kind(engine)}/{self.names[engine]}"
                for engine in (LAKEHOUSE, WAREHOUSE)
                for item in (ITEM, FLOW_ITEM)
            ],
            session=self.session,
            workspace=self.workspace_name,
            catalogue=self.catalogue,
            environment=self.environment,
        )
        if not built.succeeded:
            raise AssertionError(f"the benchmark estate did not build: {built}")

    # --- the control table ---------------------------------------------------

    def control(self, engine: str, table: str, *segments: Segment) -> None:
        values = ", ".join(
            f"('{table}', '{s.role}', {s.first_id}, {s.rows}, {s.stride}, {s.revision})"
            for s in segments
        )
        if engine == WAREHOUSE:
            script = f"delete from dbo.{CONTROL} where TableName = '{table}';"
            if values:
                script += f"\ninsert into dbo.{CONTROL} values {values};"
            self._sql().execute_script(script)
            return
        body = f"spark.sql(\"DELETE FROM {self._control()} WHERE TableName = '{table}'\")\n"
        if values:
            body += f'spark.sql("INSERT INTO {self._control()} VALUES {values}")\n'
        self.spark(body)

    # --- loading -------------------------------------------------------------

    def load(self, scenario: str, engine: str, table: str, **policy) -> LoadTiming:
        """Time one ``weaver.load`` of one Table and keep its evidence."""

        selector = (
            f"Tables/{SCHEMA}.{table}" if engine == LAKEHOUSE else f"{SCHEMA}.{table}"
        )
        timing, report = self._timed(
            scenario,
            (engine,),
            table,
            [f"{_kind(engine)}/{ITEM}"],
            names=[selector],
            **policy,
        )
        if report is not None:
            result = next(
                (node.result for node in report.nodes if node.result is not None), None
            )
            if result is not None and hasattr(result, "rows_read"):
                timing.counts = {name: getattr(result, name) for name in COUNTS}
        return timing

    def load_flow(self, scenario: str, engines, **policy) -> tuple:
        """Time one ``weaver.load`` of every flow item of ``engines``.

        Returns the timing and each node's result and span, in seconds from
        the first node's start, by logical identity.
        """

        timing, report = self._timed(
            scenario,
            tuple(engines),
            FLOW_ITEM,
            [f"{_kind(engine)}/{FLOW_ITEM}" for engine in engines],
            **policy,
        )
        nodes = {}
        if report is not None:
            from datetime import datetime

            ran = [node for node in report.nodes if node.started_at]
            origin = min(
                (datetime.fromisoformat(node.started_at) for node in ran), default=None
            )
            for node in ran:
                began = datetime.fromisoformat(node.started_at) - origin
                ended = datetime.fromisoformat(node.finished_at) - origin
                nodes[node.logical_id] = (
                    node.result,
                    round(began.total_seconds(), 1),
                    round(ended.total_seconds(), 1),
                )
            timing.counts = {
                identity: getattr(result, "rows_inserted", None)
                for identity, (result, _b, _e) in nodes.items()
            }
            timing.steps.update(
                {
                    f"  {identity}": f"{began}s to {ended}s"
                    for identity, (_r, began, ended) in nodes.items()
                }
            )
        return timing, nodes

    def _timed(self, scenario, engines, label, items, **arguments):
        import weaver

        lakehouse = LAKEHOUSE in engines
        before_jobs = self._spark_counters() if lakehouse else None
        events = len(self.session.telemetry.events())
        frames = len(self.session.timings)
        started = time.perf_counter()
        error = None
        report = None
        try:
            report = weaver.load(
                items,
                session=self.session,
                workspace=self.workspace_name,
                catalogue=self.catalogue,
                environment=self.environment,
                **arguments,
            )
        except Exception as exc:  # noqa: BLE001 - a failed load is evidence too
            error = f"{type(exc).__name__}: {exc}"[:800]
            report = getattr(exc, "report", None)
        seconds = time.perf_counter() - started
        timing = LoadTiming(
            scenario=scenario,
            engine="+".join(engines),
            table=label,
            seconds=seconds,
            succeeded=error is None and report is not None and report.succeeded,
            error=error,
        )
        timing.steps = _steps(self.session.timings[frames:])
        timing.crossings = _crossings(self.session.telemetry.events()[events:])
        if before_jobs is not None:
            after = self._spark_counters()
            timing.spark = {
                "jobs": after["jobs"] - before_jobs["jobs"],
                "stages": after["stages"] - before_jobs["stages"],
            }
        self.timings.append(timing)
        return timing, report

    # --- observation ---------------------------------------------------------

    def observe(
        self, engine: str, table: str, *, checks: dict[str, str], schema: str = SCHEMA
    ) -> dict:
        """Answer each named scalar query against one target at one moment.

        ``checks`` are SQL over ``{target}``, ``{generated(...)}`` is not used:
        a caller builds the generator text with :meth:`generated`.
        """

        target = self.target(engine, table, schema)
        if engine == WAREHOUSE:
            sql = self._sql()
            return {
                name: sql.query(query.format(target=target))[0]["n"]
                for name, query in checks.items()
            }
        body = (
            "emit({"
            + ", ".join(
                f"{name!r}: spark.sql({query.format(target=target)!r}).collect()[0]['n']"
                for name, query in checks.items()
            )
            + "})\n"
        )
        return self.spark(body)

    def target(self, engine: str, table: str, schema: str = SCHEMA) -> str:
        if engine == WAREHOUSE:
            return f"[{schema}].[{table}]"
        return f"`{self.workspace_name}`.`{self.names[LAKEHOUSE]}`.`{schema}`.`{table}`"

    def generated(self, engine: str, segments) -> str:
        """The rows ``segments`` generate, as a relation in ``engine``'s SQL."""

        if engine == WAREHOUSE:
            values = " union all ".join(
                f"select cast({s.first_id} as bigint) + g.value * {s.stride} as id,"
                f" {s.revision} as v from generate_series(cast(0 as bigint),"
                f" cast({s.rows - 1} as bigint)) as g"
                for s in segments
            )
            shape = ", ".join(f"{c.tsql} as {c.name}" for c in COLUMNS)
            return f"(select {shape} from ({values}) as i)"
        values = " UNION ALL ".join(
            f"SELECT id, {s.revision} AS v FROM range({s.first_id},"
            f" {s.first_id + s.rows * s.stride}, {s.stride})"
            for s in segments
        )
        shape = ", ".join(f"{c.spark} AS {c.name}" for c in COLUMNS)
        return f"(SELECT {shape} FROM ({values}))"

    def update_column(self, engine: str) -> str:
        name = _update_column(engine)
        return f"[{name}]" if engine == WAREHOUSE else f"`{name}`"

    def business(self, engine: str) -> str:
        quote = (lambda n: f"[{n}]") if engine == WAREHOUSE else (lambda n: f"`{n}`")
        return ", ".join(quote(c.name) for c in COLUMNS)

    # --- plumbing ------------------------------------------------------------

    def spark(self, body: str):
        return self.livy.run(body).payload

    def _sql(self):
        from weaver.targets import ItemRef, WarehouseTarget
        from weaver.workspaces import Workspace

        return self.session.sql_executor(
            WarehouseTarget(ItemRef(self.names[WAREHOUSE])),
            workspace=Workspace(
                workspace=self.workspace_name, catalogue=self.catalogue
            ),
        )

    def _control(self) -> str:
        return f"`{self.workspace_name}`.`{self.names[LAKEHOUSE]}`.`dbo`.`{CONTROL}`"

    def _spark_counters(self) -> dict:
        # The scheduler's next job and stage ids, so a difference counts what
        # the load submitted whatever job group Livy gave its statements.
        return self.spark(
            "_dag = spark.sparkContext._jsc.sc().dagScheduler()\n"
            "_value = lambda counter: getattr(counter, 'get', lambda: counter)()\n"
            "emit({'jobs': _value(_dag.nextJobId()),"
            " 'stages': _value(_dag.nextStageId())})\n"
        )


def _steps(frames) -> dict:
    """Seconds by Step, and by Sub-step where a Step has them."""

    found: dict[str, float] = {}
    for frame in frames:
        if frame.elapsed is None or frame.kind == "task":
            continue
        key = frame.name if frame.kind == "step" else f"  {frame.name}"
        found[key] = round(found.get(key, 0.0) + frame.elapsed, 2)
    return found


def _crossings(events) -> dict:
    """Calls and seconds by resource and operation."""

    by_operation: dict[str, list[float]] = defaultdict(list)
    for event in events:
        by_operation[f"{event.resource}.{event.operation}"].append(event.seconds)
    return {
        name: f"{len(seconds)} calls {sum(seconds):.1f}s"
        for name, seconds in sorted(by_operation.items(), key=lambda kv: -sum(kv[1]))
    }


# --- scenarios ------------------------------------------------------------------

#: Rows in the large initial load and in the very large target, overridable
#: so a smaller capacity can run the same scenarios.
LARGE_ROWS_ENV = "WEAVER_LOAD_BENCH_LARGE_ROWS"
HUGE_ROWS_ENV = "WEAVER_LOAD_BENCH_HUGE_ROWS"
LARGE_ROWS = 50_000_000
HUGE_ROWS = 500_000_000

#: The change applied to the very large target: updates spread across it,
#: inserts beyond it, and explicit deletes of rows the updates do not touch.
CHANGE_UPDATES = 25_000
CHANGE_INSERTS = 25_000
CHANGE_DELETES = 1_000


def scale(name: str, default: int) -> int:
    import os

    return int(os.environ.get(name, default))


@dataclass
class ScenarioRun:
    engine: str
    timings: dict[str, LoadTiming] = field(default_factory=dict)
    #: What each scenario's verification found wrong, empty when exact.
    findings: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    def describe(self) -> str:
        return "\n".join(t.describe() for t in self.timings.values())


def _expect(run: ScenarioRun, scenario: str, timing: LoadTiming, **counts) -> None:
    if not timing.succeeded:
        run.findings[scenario].append(f"load failed: {timing.error}")
        return
    wanted = {name: counts.get(name.removeprefix("rows_"), 0) for name in COUNTS}
    if timing.counts != wanted:
        run.findings[scenario].append(f"counts {timing.counts}, expected {wanted}")


def _check(run: ScenarioRun, scenario: str, seen: dict, **expected) -> None:
    for name, value in expected.items():
        if seen.get(name) != value:
            run.findings[scenario].append(
                f"{name} = {seen.get(name)}, expected {value}"
            )


def _state(bench: LoadBench, engine: str, table: str) -> dict:
    target = "{target}"
    return bench.observe(
        engine,
        table,
        checks={
            "rows": f"SELECT count(*) AS n FROM {target}",
            # As text, which both engines render with microseconds.
            "touched": f"SELECT {_text(engine, f'max({bench.update_column(engine)})')}"
            f" AS n FROM {target}",
        },
    )


def _exact(bench: LoadBench, engine: str, table: str, segments, *, absent=()) -> dict:
    """Count generated rows the target lacks, and absent ids it still holds."""

    generated = bench.generated(engine, segments)
    columns = bench.business(engine)
    checks = {
        "keys": "SELECT count(DISTINCT OrderId) AS n FROM {target}",
        "missing": (
            f"SELECT count(*) AS n FROM (SELECT {columns} FROM {generated} AS g"
            f" EXCEPT SELECT {columns} FROM {{target}}"
            f" WHERE OrderId IN (SELECT OrderId FROM {generated} AS k)) AS m"
        ),
    }
    if absent:
        checks["present"] = (
            "SELECT count(*) AS n FROM {target} WHERE OrderId IN"
            f" (SELECT OrderId FROM {bench.generated(engine, absent)} AS d)"
        )
    return bench.observe(engine, table, checks=checks)


def run_small(bench: LoadBench, engine: str, run: ScenarioRun) -> None:
    """A tiny initial load, an empty incremental load, and an unchanged one."""

    table = "Small"
    bench.control(engine, table)
    bench.load("reset", engine, table, reload=True)

    tiny = Segment(0, 5)
    bench.control(engine, table, tiny)
    timing = run.timings["A"] = bench.load("A", engine, table)
    _expect(run, "A", timing, read=5, inserted=5)
    _check(run, "A", _exact(bench, engine, table, [tiny]), keys=5, missing=0)

    before = _state(bench, engine, table)
    bench.control(engine, table)
    timing = run.timings["B"] = bench.load("B", engine, table)
    _expect(run, "B", timing)
    _check(run, "B", _state(bench, engine, table), **before)

    proposed = Segment(0, 5_000)
    bench.control(engine, table, proposed)
    _expect(run, "C", bench.load("C setup", engine, table), read=5_000, inserted=4_995)
    before = _state(bench, engine, table)
    timing = run.timings["C"] = bench.load("C", engine, table)
    _expect(run, "C", timing, read=5_000)
    _check(run, "C", _state(bench, engine, table), **before)
    _check(run, "C", _exact(bench, engine, table, [proposed]), keys=5_000, missing=0)


def run_large(bench: LoadBench, engine: str, run: ScenarioRun) -> None:
    """A large initial load into an emptied Table."""

    table = "Large"
    rows = scale(LARGE_ROWS_ENV, LARGE_ROWS)
    bench.control(engine, table)
    bench.load("reset", engine, table, reload=True)
    everything = Segment(0, rows)
    bench.control(engine, table, everything)
    timing = run.timings["D"] = bench.load("D", engine, table)
    _expect(run, "D", timing, read=rows, inserted=rows)
    seen = _exact(bench, engine, table, [everything])
    seen.update(_state(bench, engine, table))
    _check(run, "D", seen, rows=rows, keys=rows, missing=0)


def change(rows: int) -> tuple[Segment, Segment, Segment]:
    """The updates, inserts and deletes applied to a target of ``rows``."""

    updates = Segment(0, CHANGE_UPDATES, rows // CHANGE_UPDATES, revision=1)
    inserts = Segment(rows, CHANGE_INSERTS)
    # Offset from every update, so no delete names an updated row.
    deletes = Segment(7, CHANGE_DELETES, rows // CHANGE_DELETES, role=DELETE)
    return updates, inserts, deletes


def run_huge(bench: LoadBench, engine: str, run: ScenarioRun) -> None:
    """A small change to a very large target, then the change that undoes it.

    The target is kept between runs at ``rows`` rows of revision 0. Seeding it
    is a load like any other, and it runs only when the target is not in that
    state.
    """

    table = "Huge"
    rows = scale(HUGE_ROWS_ENV, HUGE_ROWS)
    base = Segment(0, rows)
    updates, inserts, deletes = change(rows)
    seen = bench.observe(
        engine,
        table,
        checks={
            "rows": "SELECT count(*) AS n FROM {target}",
            "revised": "SELECT count(*) AS n FROM {target} WHERE Revision <> 0",
            "beyond": f"SELECT count(*) AS n FROM {{target}} WHERE OrderId >= {rows}",
        },
    )
    if seen != {"rows": rows, "revised": 0, "beyond": 0}:
        bench.control(engine, table)
        bench.load("reset", engine, table, reload=True)
        bench.control(engine, table, base)
        timing = run.timings["E seed"] = bench.load("E seed", engine, table)
        _expect(run, "E seed", timing, read=rows, inserted=rows)
        if run.findings.get("E seed"):
            run.findings["E"].append("the target could not be seeded")
            return

    before = _state(bench, engine, table)
    bench.control(engine, table, updates, inserts, deletes)
    timing = run.timings["E"] = bench.load("E", engine, table)
    _expect(
        run,
        "E",
        timing,
        read=CHANGE_UPDATES + CHANGE_INSERTS,
        inserted=CHANGE_INSERTS,
        updated=CHANGE_UPDATES,
        deleted=CHANGE_DELETES,
    )
    seen = _exact(bench, engine, table, [updates, inserts], absent=[deletes])
    seen.update(
        bench.observe(
            engine,
            table,
            checks={
                "rows": "SELECT count(*) AS n FROM {target}",
                "revised": "SELECT count(*) AS n FROM {target} WHERE Revision <> 0",
                "touched": (
                    "SELECT count(*) AS n FROM {target} WHERE "
                    f"{bench.update_column(engine)} > {_instant(engine, before['touched'])}"
                ),
            },
        )
    )
    _check(
        run,
        "E",
        seen,
        rows=rows + CHANGE_INSERTS - CHANGE_DELETES,
        revised=CHANGE_UPDATES,
        touched=CHANGE_UPDATES + CHANGE_INSERTS,
        missing=0,
        present=0,
    )

    # The inverse: revert the updates, restore the deleted rows and delete the
    # inserted ones, which returns the target to its kept state.
    restored = Segment(deletes.first_id, deletes.rows, deletes.stride)
    bench.control(
        engine,
        table,
        Segment(updates.first_id, updates.rows, updates.stride),
        restored,
        Segment(inserts.first_id, inserts.rows, role=DELETE),
    )
    timing = run.timings["E inverse"] = bench.load("E inverse", engine, table)
    _expect(
        run,
        "E inverse",
        timing,
        read=CHANGE_UPDATES + CHANGE_DELETES,
        inserted=CHANGE_DELETES,
        updated=CHANGE_UPDATES,
        deleted=CHANGE_INSERTS,
    )
    seen = _exact(
        bench,
        engine,
        table,
        [Segment(updates.first_id, updates.rows, updates.stride), restored],
        absent=[inserts],
    )
    seen.update(
        bench.observe(
            engine,
            table,
            checks={
                "rows": "SELECT count(*) AS n FROM {target}",
                "revised": "SELECT count(*) AS n FROM {target} WHERE Revision <> 0",
            },
        )
    )
    _check(run, "E inverse", seen, rows=rows, revised=0, missing=0, present=0)


def _text(engine: str, expression: str) -> str:
    if engine == WAREHOUSE:
        return f"convert(varchar(27), {expression}, 121)"
    return f"date_format({expression}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')"


def _instant(engine: str, text: str) -> str:
    if engine == WAREHOUSE:
        return f"cast('{text}' as datetime2(6))"
    return f"timestamp'{text}'"


def record(run: ScenarioRun, context: dict) -> None:
    """Append each timing as one JSON line to ``WEAVER_PERFORMANCE_RESULTS``."""

    import json
    import os
    from datetime import datetime, timezone

    path = os.environ.get("WEAVER_PERFORMANCE_RESULTS")
    if not path:
        return
    at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(path, "a", encoding="utf-8") as results:
        for scenario, timing in run.timings.items():
            line = {"at": at, **context, **timing.to_mapping()}
            line["findings"] = run.findings.get(scenario, [])
            results.write(json.dumps(line, default=str) + "\n")


#: Rows each generated flow Table loads, and so each copy too.
FLOW_ROWS_ENV = "WEAVER_LOAD_BENCH_FLOW_ROWS"
FLOW_ROWS = 2_000_000


def flow_identity(engine: str, name: str) -> str:
    area = "Tables/" if engine == LAKEHOUSE else ""
    return f"{_kind(engine)}/{FLOW_ITEM}/{area}{FLOW_SCHEMA}.{name}"


def run_flow(bench: LoadBench, engines, run: ScenarioRun, scenario: str) -> None:
    """Reload every flow Table of ``engines`` in one ``weaver.load``.

    Each Table loads ``FLOW_ROWS`` rows; a copy starts only once its upstream
    finished, and holds exactly its upstream's rows.
    """

    rows = scale(FLOW_ROWS_ENV, FLOW_ROWS)
    for engine in engines:
        for name, upstream in FLOW.items():
            if upstream is None:
                bench.control(engine, f"{FLOW_SCHEMA}.{name}", Segment(0, rows))
    timing, nodes = bench.load_flow(scenario, engines, reload=True)
    run.timings[scenario] = timing
    if not timing.succeeded:
        run.findings[scenario].append(f"load failed: {timing.error}")
    for engine in engines:
        for name, upstream in FLOW.items():
            identity = flow_identity(engine, name)
            result, began, _ended = nodes.get(identity, (None, None, None))
            inserted = getattr(result, "rows_inserted", None)
            if inserted != rows:
                run.findings[scenario].append(f"{identity} inserted {inserted}")
            if upstream is not None and identity in nodes:
                finished = nodes.get(flow_identity(engine, upstream), (None, 0, 0))[2]
                if began < finished:
                    run.findings[scenario].append(
                        f"{identity} started at {began}s, before its upstream "
                        f"finished at {finished}s"
                    )
            checks = {"rows": "SELECT count(*) AS n FROM {target}"}
            if upstream is not None:
                columns = bench.business(engine)
                source = bench.target(engine, upstream, FLOW_SCHEMA)
                checks["differ"] = (
                    f"SELECT count(*) AS n FROM (SELECT {columns} FROM {source}"
                    f" EXCEPT SELECT {columns} FROM {{target}}) AS d"
                )
            seen = bench.observe(engine, name, checks=checks, schema=FLOW_SCHEMA)
            for check, value in seen.items():
                wanted = rows if check == "rows" else 0
                if value != wanted:
                    run.findings[scenario].append(
                        f"{identity} {check} = {value}, expected {wanted}"
                    )
