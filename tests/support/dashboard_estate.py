"""A deterministic estate of catalogue rows for the Catalogue Dashboard.

The rows have the shape the catalogue holds, keyed by public table name with
Python column names: graph nodes and edges across Lakehouses, Warehouses,
semantic models and Reports, with shortcuts, Views, Tests and Assumptions;
current status; Log and load-statistic history; declared metadata; and semantic
model readback. It exists to exercise the renderer at a chosen scale, so its
topology is plausible rather than parsed from a project.
"""

from __future__ import annotations

import hashlib
import json
import random
from datetime import datetime, timedelta, timezone

from weaver.catalogue.graph_rows import node_key

CATALOGUE_NAME = "Weaver"
WORKSPACE = "Analytics"
MODELS = ("Sales", "Operations", "Finance", "Inventory", "People", "Service")


class _Estate:
    def __init__(self, objects: int, seed: int, at: datetime):
        self.random = random.Random(seed)
        self.at = at
        self.objects = objects
        self.rows: dict[str, list[dict]] = {}
        self.nodes: dict[str, dict] = {}
        self.loadable: list[dict] = []

    def add(self, table: str, row: dict) -> dict:
        self.rows.setdefault(table, []).append(row)
        return row

    # --- items and nodes ------------------------------------------------------

    def install(self, item_type: str, item_name: str, target: str | None = None):
        self.add(
            "Installation",
            {
                "item_type": item_type,
                "item_name": item_name,
                "workspace_name": WORKSPACE,
                "workspace_id": _guid(WORKSPACE),
                "target_name": target or item_name,
                "item_id": _guid(item_type + item_name),
                "weaver_version": "0.9.0",
                "signature": "s",
            },
        )

    def node(
        self,
        item_type,
        item_name,
        schema,
        name,
        kind,
        *,
        label=None,
        description=None,
        can_load=True,
        static=False,
        installed=True,
        internal=False,
        files=False,
    ):
        if kind in ("Semantic model", "Report"):
            node_id = f"{item_type}/{item_name}"
        elif files:
            node_id = f"{item_type}/{item_name}/{schema}/{name}"
        else:
            node_id = f"{item_type}/{item_name}/{schema}.{name}"
        row = self.add(
            "GraphNode",
            {
                "item_type": item_type,
                "item_name": item_name,
                "schema_name": schema,
                "object_name": name,
                "node_id": node_id,
                "node_key": node_key(node_id),
                "node_kind": kind,
                "label": label
                or (
                    name
                    if kind in ("Semantic model", "Report")
                    else f"{schema.split('/')[-1]}.{name}"
                ),
                "item_label": f"{item_type}/{item_name}",
                "description": description,
                "search_text": "",
                "is_internal": internal,
                "can_load": can_load
                and kind not in ("Shortcut", "Test", "Assumption", "View", "Report"),
                "is_static": static,
                "is_mirrored": False,
                "is_installed": installed,
                "signature": "s",
            },
        )
        self.nodes[node_id] = row
        if not internal and kind not in ("Test", "Assumption"):
            self.add(
                "Registry",
                {
                    "item_type": item_type,
                    "item_name": item_name,
                    "schema_name": schema,
                    "object_name": name,
                    "object_type": kind.lower(),
                    "object_role": "data",
                    "signature": "s",
                    "build_datetime": self.at
                    - timedelta(days=3, minutes=self.random.randint(0, 600)),
                },
            )
        if row["can_load"]:
            self.loadable.append(row)
        return row

    def edge(self, down, up, kind="Dependency", through=None):
        self.add(
            "GraphEdge",
            {
                "item_type": down["item_type"],
                "item_name": down["item_name"],
                "downstream_node_id": down["node_id"],
                "upstream_node_id": up["node_id"],
                "edge_kind": kind,
                "through_node_id": through["node_id"] if through else None,
                "downstream_node_key": down["node_key"],
                "upstream_node_key": up["node_key"],
                "through_node_key": through["node_key"] if through else None,
                "signature": "s",
            },
        )

    def external(self, down, reference):
        self.add(
            "GraphEdge",
            {
                "item_type": down["item_type"],
                "item_name": down["item_name"],
                "downstream_node_id": down["node_id"],
                "upstream_node_id": reference,
                "edge_kind": "External",
                "through_node_id": None,
                "downstream_node_key": down["node_key"],
                "upstream_node_key": node_key(reference),
                "through_node_key": None,
                "signature": "s",
            },
        )

    def describe(self, node, kind):
        identity = {
            k: node[k] for k in ("item_type", "item_name", "schema_name", "object_name")
        }
        if kind == "Folder":
            self.add(
                "FolderDictionary",
                {
                    **identity,
                    "description": node["description"],
                    "lineage": "Delivered by the source system each night.",
                    "file_key": "*.csv",
                    "is_incremental": True,
                    "is_static": False,
                    "signature": "s",
                },
            )
            return
        columns = [
            f"{node['object_name']} ID",
            "Name",
            "Code",
            "Status",
            "Amount",
            "Quantity",
            "Effective date",
            "Updated",
        ]
        self.add(
            "TableDictionary",
            {
                **identity,
                "object_type": kind,
                "description": node["description"],
                "lineage": "Derived from its upstream objects in the same domain.",
                "primary_key": columns[0],
                "not_null_columns": "Name",
                "identity_column": None,
                "is_incremental": self.random.random() < 0.3,
                "is_static": False,
                "prohibit_rebuild": False,
                "signature": "s",
            },
        )
        for index, column in enumerate(columns):
            if index % 2 == 0:
                self.add(
                    "ColumnDictionary",
                    {
                        **identity,
                        "column_name": column,
                        "description": f"The {column.lower()} of each row.",
                        "is_identity": False,
                        "signature": "s",
                    },
                )
        self.add(
            "KeyDictionary",
            {
                **identity,
                "key_type": "Primary key",
                "column_set": columns[0],
                "signature": "s",
            },
        )

    # --- runtime --------------------------------------------------------------

    def statuses(self):
        r = self.random
        for row in self.loadable:
            roll = r.random()
            result = (
                "Succeeded"
                if roll < 0.86
                else "Failed"
                if roll < 0.9
                else "Pending"
                if roll < 0.95
                else "Rejected"
                if roll < 0.98
                else "Blocked"
            )
            completed = (
                self.at - timedelta(minutes=r.randint(1, 900))
                if roll < 0.97
                else self.at - timedelta(hours=40)
            )
            duration = r.randint(400, 180000)
            pending = result == "Pending"
            self.add(
                "LoadStatus",
                {
                    **{
                        k: row[k]
                        for k in (
                            "item_type",
                            "item_name",
                            "schema_name",
                            "object_name",
                        )
                    },
                    "result": result,
                    "workflow_id": _guid("latest"),
                    "started_datetime": None
                    if pending
                    else completed - timedelta(milliseconds=duration),
                    "completed_datetime": None if pending else completed,
                    "duration_milliseconds": None if pending else duration,
                },
            )
        for row in self.rows["GraphNode"]:
            if row["node_kind"] not in ("Test", "Assumption") or row["is_internal"]:
                continue
            roll = r.random()
            if roll < 0.06:
                continue
            result = "Succeeded" if roll < 0.88 else "Failed"
            completed = self.at - timedelta(minutes=r.randint(1, 300))
            self.add(
                "TestStatus",
                {
                    **{
                        k: row[k]
                        for k in (
                            "item_type",
                            "item_name",
                            "schema_name",
                            "object_name",
                        )
                    },
                    "test_type": row["node_kind"],
                    "result": result,
                    "workflow_id": _guid("tests"),
                    "started_datetime": completed - timedelta(seconds=3),
                    "completed_datetime": completed,
                    "duration_milliseconds": 3000,
                    "failure_count": r.randint(1, 40) if result == "Failed" else 0,
                },
            )

    def history(self, workflows: int, days: int):
        r, sk, stat = self.random, 0, 0
        targets = {
            f"{row['item_type']}/{row['item_name']}": row["target_name"]
            for row in self.rows["Installation"]
        }
        for w in range(workflows):
            flow = _guid(f"workflow{w}")
            start = self.at - timedelta(hours=w * 6 + 1)
            members = (
                self.loadable
                if w == 0
                else r.sample(self.loadable, max(1, len(self.loadable) // (2 + w % 3)))
            )
            clock = start
            sk += 1
            self.add(
                "Log",
                {
                    "log_sk": sk,
                    "workflow_id": flow,
                    "task_type": "load",
                    "target_type": None,
                    "target_name": None,
                    "schema_name": None,
                    "object_name": None,
                    "result": "Succeeded",
                    "started_datetime": start,
                    "completed_datetime": start + timedelta(seconds=2),
                    "duration_milliseconds": 2000,
                    "message": "load: planned " + str(len(members)) + " objects",
                    "details": None,
                },
            )
            for index, row in enumerate(members):
                begin = clock + timedelta(milliseconds=r.randint(0, 3000))
                ms = r.randint(300, 40000)
                clock = begin if index % 4 else clock + timedelta(milliseconds=ms // 3)
                sk += 1
                roll = r.random()
                result = (
                    "Succeeded"
                    if roll < 0.95
                    else "Failed"
                    if roll < 0.98
                    else "Rejected"
                )
                self.add(
                    "Log",
                    {
                        "log_sk": sk,
                        "workflow_id": flow,
                        "task_type": "load",
                        "target_type": row["item_type"],
                        "target_name": targets[row["item_label"]],
                        "schema_name": row["schema_name"],
                        "object_name": row["object_name"],
                        "result": result,
                        "started_datetime": begin,
                        "completed_datetime": begin + timedelta(milliseconds=ms),
                        "duration_milliseconds": ms,
                        "message": "Loaded"
                        if result == "Succeeded"
                        else "The load raised an error <check> the log",
                        "details": json.dumps({"node_id": row["node_id"]}),
                    },
                )
        for day in range(days):
            for row in self.loadable:
                if r.random() < 0.25:
                    continue
                stat += 1
                completed = self.at - timedelta(days=day, minutes=r.randint(0, 1200))
                ms = r.randint(300, 120000)
                known = r.random() < 0.92
                read = r.randint(100, 2_000_000) if known else None
                self.add(
                    "LoadStatistic",
                    {
                        "load_statistic_sk": stat,
                        "item_type": row["item_type"],
                        "item_name": row["item_name"],
                        "schema_name": row["schema_name"],
                        "object_name": row["object_name"],
                        "workflow_id": _guid(f"day{day}"),
                        "started_datetime": completed - timedelta(milliseconds=ms),
                        "completed_datetime": completed,
                        "duration_milliseconds": ms,
                        "rows_read": read,
                        "rows_inserted": r.randint(0, read // 10) if known else None,
                        "rows_updated": r.randint(0, 500) if known else None,
                        "rows_deleted": r.randint(0, 50) if known else None,
                        "rows_rejected": r.choice([0, 0, 0, 0, 3]) if known else None,
                        "is_reload": r.random() < 0.02,
                        "is_static_skip": row["is_static"],
                    },
                )


def _guid(text: str) -> str:
    digest = hashlib.sha256(text.encode()).hexdigest()
    return (
        f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"
    )


def make_estate(
    objects: int = 300,
    *,
    seed: int = 20261016,
    at: datetime | None = None,
    workflows: int = 15,
    days: int = 30,
) -> dict[str, list[dict]]:
    """Catalogue rows for an estate of about ``objects`` graph nodes."""

    at = at or datetime(2026, 10, 16, 2, 0, tzinfo=timezone.utc)
    e = _Estate(objects, seed, at)
    r = e.random
    domains = max(2, objects // 150)
    per = max(10, objects // domains)

    e.install("Warehouse", "_weaver", CATALOGUE_NAME)
    e.install("SemanticModel", "Catalogue Dashboard")
    e.install("Report", "Catalogue Dashboard")
    for item_type, item_name in (
        ("Lakehouse", "Landing"),
        ("Lakehouse", "Refined"),
        ("Warehouse", "Curated"),
        ("Warehouse", "Reporting"),
    ):
        e.install(
            item_type,
            item_name,
            f"{item_name}_{'LH' if item_type == 'Lakehouse' else 'WH'}",
        )
    # The catalogue's own surface, which the Dashboard leaves out.
    e.node("Warehouse", "_weaver", "_", "Log", "Table", internal=True)
    e.node("Lakehouse", "Landing", "Tables/_", "Log", "Shortcut", internal=True)

    curated_by_domain = []
    for d in range(domains):
        name = f"Domain{d:02d}"
        landing, refined, curated, reporting = [], [], [], []
        for i in range(max(2, per // 10)):
            folder = e.node(
                "Lakehouse",
                "Landing",
                f"Files/{name}",
                f"Extract{i:03d}",
                "Folder",
                files=True,
                description=f"Nightly extract {i} for {name}.",
                static=i % 9 == 0,
            )
            e.describe(folder, "Folder")
            table = e.node(
                "Lakehouse",
                "Landing",
                f"Tables/{name}",
                f"Raw{i:03d}",
                "Table",
                description=f"Raw rows of extract {i}.",
            )
            e.describe(table, "Table")
            e.edge(table, folder)
            landing.append(table)
        for i in range(max(2, per * 3 // 10)):
            table = e.node(
                "Lakehouse",
                "Refined",
                f"Tables/{name}",
                f"Clean{i:03d}",
                "Table" if i % 7 else "View",
                description=f"Cleaned {name} entity {i}." if i % 4 else None,
            )
            e.describe(table, table["node_kind"])
            for up in r.sample(
                landing + refined[-6:],
                min(len(landing + refined[-6:]), r.randint(1, 3)),
            ):
                e.edge(table, up)
            if i % 23 == 5:
                e.external(table, f"Ledger.dbo.Account{i}")
            refined.append(table)
        shortcuts = []
        for i in range(max(2, per // 10)):
            source = refined[(i * 3) % len(refined)]
            shortcut = e.node(
                "Warehouse",
                "Curated",
                name,
                source["object_name"],
                "Shortcut",
                can_load=False,
            )
            e.edge(shortcut, source, "Shortcut")
            shortcuts.append((shortcut, source))
        for i in range(max(3, per * 3 // 10)):
            kind = "View" if i % 5 == 0 else "Table"
            table = e.node(
                "Warehouse",
                "Curated",
                name,
                ("Dim" if i % 3 == 0 else "Fact") + f"{i:03d}",
                kind,
                description=f"Curated {name} {'dimension' if i % 3 == 0 else 'fact'} {i}.",
            )
            e.describe(table, kind)
            for shortcut, source in r.sample(
                shortcuts, min(len(shortcuts), r.randint(0, 2))
            ):
                e.edge(table, source, "Dependency", through=shortcut)
            for up in r.sample(
                curated[-8:],
                min(len(curated[-8:]), r.randint(0 if shortcuts else 1, 2)),
            ):
                e.edge(table, up)
            curated.append(table)
            if i % 4 == 0:
                test = e.node(
                    "Warehouse",
                    "Curated",
                    name,
                    table["object_name"] + "Reconciles",
                    "Test" if i % 8 == 0 else "Assumption",
                    description="Every row reconciles with its source.",
                    installed=i % 40 != 4,
                )
                e.edge(test, table, "Validation")
                e.add(
                    "TestDictionary",
                    {
                        **{
                            k: test[k]
                            for k in (
                                "item_type",
                                "item_name",
                                "schema_name",
                                "object_name",
                            )
                        },
                        "test_type": test["node_kind"],
                        "description": test["description"],
                        "primary_key": None,
                        "signature": "s",
                    },
                )
        dims = [t for t in curated if t["object_name"].startswith("Dim")]
        for fact in (t for t in curated if t["object_name"].startswith("Fact")):
            for dim in r.sample(dims, min(len(dims), r.randint(1, 3))):
                e.add(
                    "ForeignKeyDictionary",
                    {
                        "item_type": "Warehouse",
                        "item_name": "Curated",
                        "foreign_schema_name": name,
                        "foreign_object_name": fact["object_name"],
                        "foreign_column_set": f"{dim['object_name']} ID",
                        "primary_item_type": "Warehouse",
                        "primary_item_name": "Curated",
                        "primary_schema_name": name,
                        "primary_object_name": dim["object_name"],
                        "primary_column_set": f"{dim['object_name']} ID",
                        "signature": "s",
                    },
                )
        for i in range(max(1, per // 10)):
            view = e.node(
                "Warehouse",
                "Reporting",
                name,
                f"Report{i:03d}",
                "View",
                description=f"Report-ready {name} view {i}.",
            )
            e.describe(view, "View")
            for up in r.sample(curated, min(len(curated), 2)):
                e.edge(view, up)
            reporting.append(view)
        curated_by_domain.append((name, curated))

    for index, model_name in enumerate(
        MODELS[: max(1, min(len(MODELS), objects // 200 + 1))]
    ):
        e.install("SemanticModel", model_name)
        e.install("Report", model_name)
        model = e.node(
            "SemanticModel",
            model_name,
            "",
            "",
            "Semantic model",
            label=model_name,
            description=f"The {model_name} model.",
        )
        report = e.node(
            "Report", model_name, "", "", "Report", label=model_name, can_load=False
        )
        e.edge(report, model)
        e.add(
            "SemanticModel",
            {
                "item_type": "SemanticModel",
                "item_name": model_name,
                "description": model["description"],
                "signature": "s",
            },
        )
        _, curated = curated_by_domain[index % len(curated_by_domain)]
        sources = [t for t in curated if t["node_kind"] == "Table"][:14]
        names = []
        for ordinal, source in enumerate(sources, start=1):
            table_name = (
                "Dim " if source["object_name"].startswith("Dim") else ""
            ) + source["object_name"].replace("Dim", "").replace("Fact", "Fact ")
            names.append((table_name, source))
            e.edge(model, source)
            e.add(
                "SemanticModelTable",
                {
                    "item_type": "SemanticModel",
                    "item_name": model_name,
                    "table_name": table_name,
                    "table_ordinal": ordinal,
                    "description": source["description"],
                    "source_mode": "directLake",
                    "source_access": "sql",
                    "signature": "s",
                },
            )
            e.add(
                "Dependency",
                {
                    "item_type": "SemanticModel",
                    "item_name": model_name,
                    "referencing_schema_name": "",
                    "referencing_object_name": table_name,
                    "dependency_reference": f"Warehouse/Curated/{source['schema_name']}.{source['object_name']}",
                    "referenced_item_type": "Warehouse",
                    "referenced_item_name": "Curated",
                    "referenced_schema_name": source["schema_name"],
                    "referenced_object_name": source["object_name"],
                    "signature": "s",
                },
            )
            for column in ("Key", "Name", "Amount"):
                e.add(
                    "SemanticModelColumn",
                    {
                        "item_type": "SemanticModel",
                        "item_name": model_name,
                        "table_name": table_name,
                        "column_name": f"{table_name} {column}",
                        "description": f"The {column.lower()}.",
                        "data_type": "int64"
                        if column == "Key"
                        else "string"
                        if column == "Name"
                        else "decimal",
                        "column_type": "data",
                        "source_column": column,
                        "expression": None,
                        "signature": "s",
                    },
                )
        e.add(
            "SemanticModelTable",
            {
                "item_type": "SemanticModel",
                "item_name": model_name,
                "table_name": "Measures",
                "table_ordinal": len(names) + 1,
                "description": None,
                "source_mode": None,
                "source_access": None,
                "signature": "s",
            },
        )
        e.add(
            "SemanticModelMeasure",
            {
                "item_type": "SemanticModel",
                "item_name": model_name,
                "table_name": "Measures",
                "measure_name": "Total amount",
                "description": "Sum of amounts.",
                "expression": "SUM('Fact 001'[Fact 001 Amount])",
                "format_string": "#,##0",
                "signature": "s",
            },
        )
        facts = [n for n in names if not n[0].startswith("Dim")]
        dims = [n for n in names if n[0].startswith("Dim")]
        for f_index, (fact, _) in enumerate(facts):
            for d_index, (dim, _) in enumerate(dims[:4]):
                if (f_index + d_index) % 3 == 2:
                    continue
                e.add(
                    "SemanticModelRelationship",
                    {
                        "item_type": "SemanticModel",
                        "item_name": model_name,
                        "relationship_name": f"{fact}-{dim}",
                        "from_table": fact,
                        "from_column": f"{dim} Key",
                        "to_table": dim,
                        "to_column": f"{dim} Key",
                        "from_cardinality": None,
                        "to_cardinality": None,
                        "cross_filtering_behavior": "oneDirection",
                        "is_active": d_index != 3,
                        "signature": "s",
                    },
                )
        if len(dims) > 1:
            e.add(
                "SemanticModelRelationship",
                {
                    "item_type": "SemanticModel",
                    "item_name": model_name,
                    "relationship_name": "dim-bridge",
                    "from_table": dims[0][0],
                    "from_column": "Bridge",
                    "to_table": dims[1][0],
                    "to_column": "Bridge",
                    "from_cardinality": "many",
                    "to_cardinality": "many",
                    "cross_filtering_behavior": "bothDirections",
                    "is_active": True,
                    "signature": "s",
                },
            )
        validation = e.node(
            "SemanticModel",
            model_name,
            "Checks",
            "TotalsMatch",
            "Test",
            description="Model totals match the Warehouse.",
        )
        e.edge(validation, model, "Validation")
    e.statuses()
    e.history(workflows, days)
    return e.rows
