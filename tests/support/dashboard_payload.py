"""The Catalogue Dashboard's page payloads, built in Python from catalogue rows.

This restates the model's Data measures so that renderer tests, the Health
parity test and the scale benchmark can run without Fabric. The Fabric
verification compares the real measures' output with the catalogue, so a drift
between the two shows up there.

``rows`` maps a public catalogue table name to rows keyed by Python column
names, as ``Catalogue.rows`` holds them.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone

FIELD, ROW, TABLE, LESS = "", "", "", ""

#: The data each page measure concatenates, in order.
PAGES = {
    "overview": ("catalogue", "graph", "status", "workflow", "build"),
    "health": ("catalogue", "graph", "status", "workflow", "build"),
    "orchestration": ("catalogue", "graph", "status", "workflow"),
    "gantt": ("catalogue", "workflow", "log"),
    "performance": ("catalogue", "workflow", "statistic"),
    "entities": ("catalogue", "graph", "metadata"),
    "semantic": ("catalogue", "graph", "status", "semantic"),
}

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
LOG_BUDGET = 20000


def _aware(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def ms(value) -> str:
    value = _aware(value)
    return "" if value is None else str(round((value - EPOCH).total_seconds() * 1000))


def text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "True" if value else "False"
    return str(value)


def flag(value) -> str:
    return "1" if value else ""


def item(row, prefix="item") -> str:
    return f"{row[f'{prefix}_type']}/{row[f'{prefix}_name']}"


def table(name: str, columns: Sequence[str], rows) -> str:
    head = TABLE + name + "".join(FIELD + column for column in columns)
    return head + "".join(ROW + FIELD.join(fields) for fields in rows)


def _rows(rows, name):
    """A table's rows as the Warehouse stores them: vocabulary columns hold
    their public words, as the catalogue writer renders them."""

    from weaver.catalogue.tables import TABLES_BY_NAME

    table = TABLES_BY_NAME.get(name)
    vocabularies = {
        column.name: column.vocabulary
        for column in (table.columns if table is not None else ())
        if column.vocabulary
    }
    return [
        {
            key: vocabularies[key].get(str(value), value)
            if key in vocabularies and value is not None
            else value
            for key, value in row.items()
        }
        for row in rows.get(name, ())
    ]


def catalogue(rows, at) -> str:
    return table("meta", ("at", "format"), [(ms(at), "1")]) + table(
        "items",
        ("item", "workspace", "target", "version"),
        [
            (
                item(r),
                text(r.get("workspace_name")),
                text(r.get("target_name")),
                text(r.get("weaver_version")),
            )
            for r in _rows(rows, "Installation")
        ],
    )


def graph(rows, at) -> str:
    nodes = [r for r in _rows(rows, "LineageNode") if not r["is_internal"]]
    keys = {r["node_key"] for r in nodes}
    visible = {r["item_label"] for r in nodes}
    internal = sorted(
        {r["item_label"] for r in _rows(rows, "LineageNode") if r["is_internal"]}
        - visible
    )
    return (
        table(
            "nodes",
            (
                "key",
                "item",
                "schema",
                "object",
                "kind",
                "label",
                "flags",
                "description",
            ),
            [
                (
                    r["node_key"],
                    r["item_label"],
                    text(r["schema_name"]),
                    text(r["object_name"]),
                    r["node_kind"],
                    r["label"],
                    str(
                        int(bool(r["can_load"]))
                        + 2 * int(bool(r["is_static"]))
                        + 4 * int(bool(r["is_mirrored"]))
                        + 8 * int(bool(r["is_installed"]))
                    ),
                    text(r.get("description"))[:400],
                )
                for r in nodes
            ],
        )
        + table(
            "edges",
            ("down", "up", "kind", "through", "reference"),
            [
                (
                    r["downstream_node_key"],
                    r["upstream_node_key"],
                    r["edge_kind"][:1],
                    text(r.get("through_node_key")),
                    r["upstream_node_id"] if r["edge_kind"] == "External" else "",
                )
                for r in _rows(rows, "LineageEdge")
                if r["downstream_node_key"] in keys
            ],
        )
        + table("internal", ("item",), [(label,) for label in internal])
    )


def status(rows, at) -> str:
    def ident(r):
        return (item(r), text(r["schema_name"]), text(r["object_name"]))

    return (
        table(
            "loads",
            ("item", "schema", "object", "result", "completed", "duration"),
            [
                (
                    *ident(r),
                    text(r.get("result")),
                    ms(r.get("completed_datetime")),
                    text(r.get("duration_milliseconds")),
                )
                for r in _rows(rows, "LoadStatus")
            ],
        )
        + table(
            "tests",
            ("item", "schema", "object", "result", "completed", "failures"),
            [
                (
                    *ident(r),
                    text(r.get("result")),
                    ms(r.get("completed_datetime")),
                    text(r.get("failure_count")),
                )
                for r in _rows(rows, "TestStatus")
            ],
        )
        + table(
            "built",
            ("item", "schema", "object", "built"),
            [
                (*ident(r), ms(r.get("build_datetime")))
                for r in _rows(rows, "Registry")
                if r.get("object_role") == "Data"
            ],
        )
    )


def build(rows, at) -> str:
    def identity(r):
        return (r["item_type"], r["item_name"], r["schema_name"], r["object_name"])

    declared = {
        identity(r)
        for name in ("TableDictionary", "FolderDictionary")
        for r in _rows(rows, name)
    }
    certified = {identity(r) for r in _rows(rows, "Registry")}
    return table(
        "uncertified",
        ("item", "schema", "object"),
        [(f"{t}/{n}", s, o) for t, n, s, o in sorted(declared - certified)],
    )


def _workflows(rows):
    found = {}
    for r in _rows(rows, "Log"):
        w = found.setdefault(
            r["workflow_id"],
            {"start": None, "end": None, "records": 0, "failed": 0, "tasks": set()},
        )
        started, completed = (
            _aware(r.get("started_datetime")),
            _aware(r.get("completed_datetime")),
        )
        if started is not None and (w["start"] is None or started < w["start"]):
            w["start"] = started
        if completed is not None and (w["end"] is None or completed > w["end"]):
            w["end"] = completed
        w["records"] += 1
        w["failed"] += r.get("result") in ("Failed", "Error", "Blocked")
        w["tasks"].add(r.get("task_type"))
    return found


def _latest(found, count):
    floor = datetime.min.replace(tzinfo=timezone.utc)
    return sorted(found, key=lambda w: (-(found[w]["end"] or floor).timestamp(), w))[
        :count
    ]


def workflow(rows, at) -> str:
    found = _workflows(rows)
    return table("workflowcount", ("count",), [(str(len(found)),)]) + table(
        "workflows",
        ("workflow", "started", "completed", "records", "failed", "tasks"),
        [
            (
                w,
                ms(found[w]["start"]),
                ms(found[w]["end"]),
                str(found[w]["records"]),
                str(found[w]["failed"]),
                ",".join(sorted(t for t in found[w]["tasks"] if t)),
            )
            for w in _latest(found, 40)
        ],
    )


def log(rows, at) -> str:
    found = _workflows(rows)
    taken, budget, records, index = [], LOG_BUDGET, {}, {}
    for r in _rows(rows, "Log"):
        records.setdefault(r["workflow_id"], []).append(r)
    for w in _latest(found, 12):
        take = max(0, min(found[w]["records"], budget))
        budget -= take
        taken.append((w, found[w]["records"], take))
        index[w] = str(len(taken))
    chosen = []
    for w, _, take in taken:
        ordered = sorted(
            records[w],
            key=lambda r: (_aware(r.get("started_datetime")) or EPOCH, r["log_sk"]),
        )
        chosen.extend(ordered[:take])

    def message(r):
        explains = not r.get("object_name") or r.get("result") != "Succeeded"
        return text(r.get("message"))[:300] if explains else ""

    return table(
        "logflows",
        ("index", "workflow", "records", "included"),
        [(index[w], w, str(n), str(t)) for w, n, t in taken],
    ) + table(
        "log",
        (
            "id",
            "flow",
            "task",
            "targettype",
            "target",
            "schema",
            "object",
            "result",
            "started",
            "completed",
            "duration",
            "message",
        ),
        [
            (
                text(r["log_sk"]),
                index[r["workflow_id"]],
                text(r.get("task_type")),
                text(r.get("target_type")),
                text(r.get("target_name")),
                text(r.get("schema_name")),
                text(r.get("object_name")),
                text(r.get("result")),
                ms(r.get("started_datetime")),
                ms(r.get("completed_datetime")),
                text(r.get("duration_milliseconds")),
                message(r),
            )
            for r in chosen
        ],
    )


def statistic(rows, at) -> str:
    at = _aware(at)
    stats = _rows(rows, "LoadStatistic")

    def window(days):
        grouped = {}
        for r in stats:
            completed = _aware(r.get("completed_datetime"))
            if completed is None or completed < at - timedelta(days=days):
                continue
            grouped.setdefault(
                (item(r), r["schema_name"], r["object_name"]), []
            ).append(r)
        out = []
        for (label, schema, name), group in grouped.items():

            def total(column, of=group):
                values = [g[column] for g in of if g.get(column) is not None]
                return str(sum(values)) if values else ""

            timed = [
                g
                for g in group
                if g.get("rows_read") is not None
                and (g.get("duration_milliseconds") or 0) > 0
            ]
            durations = [
                g["duration_milliseconds"]
                for g in group
                if g.get("duration_milliseconds") is not None
            ]
            out.append(
                (
                    label,
                    text(schema),
                    text(name),
                    str(len(group)),
                    total("duration_milliseconds"),
                    str(max(durations)) if durations else "",
                    total("rows_read"),
                    total("rows_inserted"),
                    total("rows_updated"),
                    total("rows_deleted"),
                    total("rows_rejected"),
                    str(sum(1 for g in group if g.get("is_reload"))),
                    str(sum(1 for g in group if g.get("is_static_skip"))),
                    total("rows_read", timed),
                    total("duration_milliseconds", timed),
                    ms(max(_aware(g["completed_datetime"]) for g in group)),
                )
            )
        return out

    header = (
        "item",
        "schema",
        "object",
        "loads",
        "duration",
        "longest",
        "read",
        "inserted",
        "updated",
        "deleted",
        "rejected",
        "reloads",
        "skips",
        "timedread",
        "timedduration",
        "last",
    )
    today = at.replace(hour=0, minute=0, second=0, microsecond=0)
    trend = []
    for back in range(30):
        day = today - timedelta(days=back)
        grouped = {}
        for r in stats:
            completed = _aware(r.get("completed_datetime"))
            if completed is not None and day <= completed < day + timedelta(days=1):
                grouped.setdefault(item(r), []).append(r)
        for label, group in grouped.items():

            def total(column, of=group):
                values = [g[column] for g in of if g.get(column) is not None]
                return str(sum(values)) if values else ""

            changed = [
                sum(
                    g.get(c) or 0
                    for c in ("rows_inserted", "rows_updated", "rows_deleted")
                )
                for g in group
            ]
            trend.append(
                (
                    ms(day),
                    label,
                    str(len(group)),
                    total("duration_milliseconds"),
                    total("rows_read"),
                    str(sum(changed)),
                    total("rows_rejected"),
                )
            )
    week = [
        r
        for r in stats
        if _aware(r.get("completed_datetime"))
        and _aware(r["completed_datetime"]) >= at - timedelta(days=7)
    ]

    def single(r):
        return (
            text(r["load_statistic_sk"]),
            item(r),
            text(r["schema_name"]),
            text(r["object_name"]),
            text(r.get("workflow_id")),
            ms(r.get("started_datetime")),
            ms(r.get("completed_datetime")),
            text(r.get("duration_milliseconds")),
            text(r.get("rows_read")),
            text(r.get("rows_inserted")),
            text(r.get("rows_updated")),
            text(r.get("rows_deleted")),
            text(r.get("rows_rejected")),
            flag(r.get("is_reload")),
            flag(r.get("is_static_skip")),
        )

    columns = (
        "id",
        "item",
        "schema",
        "object",
        "workflow",
        "started",
        "completed",
        "duration",
        "read",
        "inserted",
        "updated",
        "deleted",
        "rejected",
        "reload",
        "skip",
    )
    slowest = sorted(
        week,
        key=lambda r: (-(r.get("duration_milliseconds") or 0), r["load_statistic_sk"]),
    )[:50]
    largest = sorted(
        (
            r
            for r in week
            if any(
                r.get(c) is not None
                for c in ("rows_inserted", "rows_updated", "rows_deleted")
            )
        ),
        key=lambda r: (
            -sum(
                r.get(c) or 0 for c in ("rows_inserted", "rows_updated", "rows_deleted")
            ),
            r["load_statistic_sk"],
        ),
    )[:50]
    return (
        table("perfday", header, window(1))
        + table("perfmonth", header, window(30))
        + table(
            "perftrend",
            ("day", "item", "loads", "duration", "read", "changed", "rejected"),
            trend,
        )
        + table("slowest", columns, [single(r) for r in slowest])
        + table("largest", columns, [single(r) for r in largest])
    )


def metadata(rows, at) -> str:
    def ident(r):
        return (item(r), text(r["schema_name"]), text(r["object_name"]))

    return (
        table(
            "schemas",
            ("item", "schema", "description"),
            [
                (item(r), text(r["schema_name"]), text(r.get("description"))[:1000])
                for r in _rows(rows, "SchemaDictionary")
            ],
        )
        + table(
            "tables",
            (
                "item",
                "schema",
                "object",
                "type",
                "description",
                "lineage",
                "primarykey",
                "notnull",
                "identity",
                "incremental",
                "static",
                "prohibitrebuild",
            ),
            [
                (
                    *ident(r),
                    text(r.get("object_type")),
                    text(r.get("description"))[:1000],
                    text(r.get("lineage"))[:1000],
                    text(r.get("primary_key")),
                    text(r.get("not_null_columns")),
                    text(r.get("identity_column")),
                    flag(r.get("is_incremental")),
                    flag(r.get("is_static")),
                    flag(r.get("prohibit_rebuild")),
                )
                for r in _rows(rows, "TableDictionary")
            ],
        )
        + table(
            "folders",
            (
                "item",
                "schema",
                "object",
                "description",
                "lineage",
                "filekey",
                "incremental",
                "static",
            ),
            [
                (
                    *ident(r),
                    text(r.get("description"))[:1000],
                    text(r.get("lineage"))[:1000],
                    text(r.get("file_key")),
                    flag(r.get("is_incremental")),
                    flag(r.get("is_static")),
                )
                for r in _rows(rows, "FolderDictionary")
            ],
        )
        + table(
            "columns",
            ("item", "schema", "object", "column", "description", "identity"),
            [
                (
                    *ident(r),
                    text(r["column_name"]),
                    text(r.get("description"))[:400],
                    flag(r.get("is_identity")),
                )
                for r in _rows(rows, "ColumnDictionary")
            ],
        )
        + table(
            "keys",
            ("item", "schema", "object", "type", "columns"),
            [
                (*ident(r), text(r.get("key_type")), text(r.get("column_set")))
                for r in _rows(rows, "KeyDictionary")
                if r.get("key_type") != "Primary key"
            ],
        )
        + table(
            "foreignkeys",
            (
                "item",
                "schema",
                "object",
                "columns",
                "primaryitem",
                "primaryschema",
                "primaryobject",
                "primarycolumns",
            ),
            [
                (
                    item(r),
                    text(r["foreign_schema_name"]),
                    text(r["foreign_object_name"]),
                    text(r["foreign_column_set"]),
                    item(r, "primary_item"),
                    text(r["primary_schema_name"]),
                    text(r["primary_object_name"]),
                    text(r["primary_column_set"]),
                )
                for r in _rows(rows, "ForeignKeyDictionary")
            ],
        )
        + table(
            "validations",
            ("item", "schema", "object", "type", "description", "primarykey"),
            [
                (
                    *ident(r),
                    text(r.get("test_type")),
                    text(r.get("description"))[:1000],
                    text(r.get("primary_key")),
                )
                for r in _rows(rows, "TestDictionary")
            ],
        )
    )


def semantic(rows, at) -> str:
    def active(value):
        return "" if value is None else ("1" if value else "0")

    return (
        table(
            "models",
            ("item", "description"),
            [
                (item(r), text(r.get("description"))[:1000])
                for r in _rows(rows, "SemanticModel")
            ],
        )
        + table(
            "smtables",
            ("item", "table", "ordinal", "description", "mode", "access"),
            [
                (
                    item(r),
                    r["table_name"],
                    text(r.get("table_ordinal")),
                    text(r.get("description"))[:1000],
                    text(r.get("source_mode")),
                    text(r.get("source_access")),
                )
                for r in _rows(rows, "SemanticModelTable")
            ],
        )
        + table(
            "smcolumns",
            (
                "item",
                "table",
                "column",
                "description",
                "datatype",
                "columntype",
                "sourcecolumn",
                "expression",
            ),
            [
                (
                    item(r),
                    r["table_name"],
                    r["column_name"],
                    text(r.get("description"))[:400],
                    text(r.get("data_type")),
                    text(r.get("column_type")),
                    text(r.get("source_column")),
                    text(r.get("expression"))[:400],
                )
                for r in _rows(rows, "SemanticModelColumn")
            ],
        )
        + table(
            "smmeasures",
            ("item", "table", "measure", "description", "expression", "format"),
            [
                (
                    item(r),
                    r["table_name"],
                    r["measure_name"],
                    text(r.get("description"))[:400],
                    text(r.get("expression"))[:1000],
                    text(r.get("format_string"))[:200],
                )
                for r in _rows(rows, "SemanticModelMeasure")
            ],
        )
        + table(
            "smrelationships",
            (
                "item",
                "name",
                "fromtable",
                "fromcolumn",
                "totable",
                "tocolumn",
                "fromcardinality",
                "tocardinality",
                "crossfilter",
                "active",
            ),
            [
                (
                    item(r),
                    r["relationship_name"],
                    text(r.get("from_table")),
                    text(r.get("from_column")),
                    text(r.get("to_table")),
                    text(r.get("to_column")),
                    text(r.get("from_cardinality")),
                    text(r.get("to_cardinality")),
                    text(r.get("cross_filtering_behavior")),
                    active(r.get("is_active")),
                )
                for r in _rows(rows, "SemanticModelRelationship")
            ],
        )
        + table(
            "smsources",
            (
                "item",
                "table",
                "reference",
                "sourceitem",
                "sourceschema",
                "sourceobject",
            ),
            [
                (
                    item(r),
                    text(r["referencing_object_name"]),
                    text(r["dependency_reference"]),
                    f"{r['referenced_item_type']}/{r['referenced_item_name']}"
                    if r.get("referenced_item_type")
                    else "",
                    text(r.get("referenced_schema_name")),
                    text(r.get("referenced_object_name")),
                )
                for r in _rows(rows, "Dependency")
                if r["item_type"] == "SemanticModel"
            ],
        )
    )


BLOCKS = {
    "catalogue": catalogue,
    "graph": graph,
    "status": status,
    "workflow": workflow,
    "log": log,
    "statistic": statistic,
    "metadata": metadata,
    "semantic": semantic,
    "build": build,
}


def page_payload(
    rows: Mapping[str, Sequence[Mapping]], page: str, *, at: datetime
) -> str:
    """What the page measure places in its data element, before ``<`` is replaced."""

    body = TABLE + "page" + FIELD + "name" + ROW + page
    return body + "".join(BLOCKS[block](rows, at) for block in PAGES[page])


def page_html(rows, page: str, *, at: datetime, renderer: str) -> str:
    """The page measure's whole output: data element, then the renderer."""

    data = page_payload(rows, page, at=at).replace("<", LESS)
    return (
        '<div id="wd-app" class="wd"></div><script type="text/plain" id="wd-data">'
        + data
        + "</script>"
        + renderer
    )
