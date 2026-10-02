"""A Mirror compiles to one plan whose binding switch comes last."""

from __future__ import annotations

from support.bundles import runs_before
from support.weaver_test import weaver_test

from weaver.catalogue.borrow import Borrowed
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.fabric.resources import Item
from weaver.locations import Location
from weaver.mirror_plan import mirror_mutation_plan
from weaver.operations.mirror import MirrorItem, MirrorPlan, ResolvedMirror
from weaver.spark import FabricSparkTarget
from weaver.store import FilesystemStore
from weaver.workspaces import CatalogueRef, Workspace

WORKSPACE = "Analytics"


def _resolved(item, kind, *, relations=(), programmables=()):
    workspace = Workspace(
        workspace=WORKSPACE,
        catalogue="Warehouse/Weaver_Dev",
        mirror="Warehouse/Weaver",
    )
    logical = WeaverItemId.parse(f"{kind}/{item}")
    each = MirrorItem(
        item=logical,
        source_target=item,
        destination=f"{item}_Dev",
        relations=tuple(relations),
        programmables=tuple(programmables),
    )
    return ResolvedMirror(
        plan=MirrorPlan(
            workspace=workspace,
            source=CatalogueRef(workspace=WORKSPACE, name="Weaver"),
            destination=CatalogueRef(workspace=WORKSPACE, name="Weaver_Dev"),
            items=(f"{kind}/{item}={kind}/{item}_Dev",),
        ),
        items=(each,),
        bindings={logical: f"{item}_Dev"},
        installations={
            logical: {
                "item_type": kind,
                "item_name": item,
                "target_name": item,
                "signature": "signed",
            }
        },
    )


class _SourceSql:
    def __init__(self, rows):
        self.rows = rows

    def query(self, statement):
        return self.rows


class _Session:
    def __init__(self, *, rows=(), resolver=None, store=None):
        self.rows = list(rows)
        self._resolver = resolver
        self._store = store

    def sql_executor(self, target, *, workspace=None):
        return _SourceSql(self.rows)

    def resolver(self, workspace=None):
        return self._resolver

    def store(self, workspace=None):
        return self._store


def _actions(plan):
    return {action.id: action for _s, _b, action in plan.actions()}


@weaver_test()
def test_a_warehouse_mirror_binds_last_after_its_reconstruction_and_the_fork():
    relation = Borrowed(
        WeaverDocumentId.parse("Warehouse/Model/Rpt.Sales"), "table", "view"
    )
    resolved = _resolved("Model", "Warehouse", relations=(relation,))
    session = _Session(
        rows=[
            {
                "schema_name": "Rpt",
                "object_name": "Refresh",
                "definition": "CREATE PROCEDURE [Rpt].[Refresh] AS SELECT 1",
            }
        ]
    )

    plan, payloads, summary = mirror_mutation_plan(resolved, session=session)

    actions = _actions(plan)
    bind = "mirror-bind-warehouse-Model_Dev"
    assert not any(bind in a.depends_on for a in actions.values())
    for action in actions:
        if action != bind and not action.startswith("wipe-warehouse-Weaver_Dev"):
            assert runs_before(plan, action, bind) or action in {"complete-build"}, (
                action
            )
    assert runs_before(plan, "wipe-warehouse-Weaver_Dev", "complete-build")
    assert runs_before(plan, "complete-build", "mirror-fork-catalogue")
    assert runs_before(plan, "complete-build", "mirror-surface-warehouse-Model_Dev-000")
    # The item's own reconstruction does not wait for the catalogue.
    assert not runs_before(
        plan, "wipe-warehouse-Weaver_Dev", "mirror-relations-warehouse-Model_Dev-000"
    )
    assert plan.execution.spark_home_target_id is None
    relations = payloads[actions["mirror-relations-warehouse-Model_Dev-000"].payload]
    assert (
        b"exec sp_executesql N'create or alter view [Rpt].[Sales] as select * from "
        b"[Model].[Rpt].[Sales];';"
    ) in relations
    code = payloads[actions["mirror-code-warehouse-Model_Dev-000"].payload]
    assert b"create or alter PROCEDURE [Rpt].[Refresh] AS SELECT 1" in code
    assert summary["Warehouse/Model"]["programmables"] == 1


@weaver_test()
def test_a_warehouse_mirror_orders_same_item_reads_and_creates_each_schema_once():
    """Reconstruction branches run on concurrent Warehouse lanes."""

    from dataclasses import replace

    from weaver.declaration.model import LOGICAL_TARGET, VIEW_SHORTCUT
    from weaver.installed import InstalledShortcut

    relation = Borrowed(
        WeaverDocumentId.parse("Warehouse/Model/Rpt.Sales"), "table", "view"
    )
    resolved = _resolved("Model", "Warehouse", relations=(relation,))
    alias = InstalledShortcut(
        destination=WeaverDocumentId.parse("Warehouse/Model/Mart.Sales"),
        source=WeaverDocumentId.parse("Warehouse/Model/Rpt.Sales"),
        shortcut_type=VIEW_SHORTCUT,
        target_type=LOGICAL_TARGET,
        target_item=WeaverItemId.parse("Warehouse/Model"),
        target_schema="Rpt",
        target_object="Sales",
    )
    resolved = replace(
        resolved, items=(replace(resolved.items[0], shortcuts=(alias,)),)
    )
    session = _Session(
        rows=[
            {
                "schema_name": "Code",
                "object_name": "Refresh",
                "definition": "CREATE PROCEDURE [Code].[Refresh] AS SELECT * "
                "FROM [Mart].[Sales]",
            }
        ]
    )

    plan, payloads, _summary = mirror_mutation_plan(resolved, session=session)

    actions = _actions(plan)
    schemas = "mirror-schemas-warehouse-Model_Dev-000"
    relations = "mirror-relations-warehouse-Model_Dev-000"
    surface = "mirror-surface-warehouse-Model_Dev-000"
    pointers = "mirror-pointers-warehouse-Model_Dev-000"
    code = "mirror-code-warehouse-Model_Dev-000"
    for branch in (relations, surface, pointers, code):
        assert runs_before(plan, schemas, branch), branch
        assert b"create schema" not in payloads[actions[branch].payload], branch
    created = payloads[actions[schemas].payload]
    for schema in (b"[_]", b"[Rpt]", b"[Mart]", b"[Code]"):
        assert schema in created
    # The pointer reads a relation rebuilt in this same destination.
    assert runs_before(plan, relations, pointers)
    assert not runs_before(plan, surface, pointers)
    for branch in (relations, surface, pointers):
        assert runs_before(plan, branch, code), branch


class _Resolver:
    def __init__(self, root):
        self.root = root

    def external_item(self, name, *, item_type, workspace=None):
        return Item(id=f"{name}-id", name=name, type=item_type, workspace_id="ws-id")

    def external_root(self, item):
        return Location(str(self.root / item.name))

    def spark_destination(self, item):
        return FabricSparkTarget(workspace=WORKSPACE, lakehouse=item.name)


@weaver_test()
def test_a_lakehouse_mirror_resolves_case_exact_sources_and_awaits_its_shortcuts(
    tmp_path,
):
    (tmp_path / "Input" / "Tables" / "Sales" / "customer").mkdir(parents=True)
    load = tmp_path / "Input" / "Files" / "_" / "Load" / "Tables"
    load.mkdir(parents=True)
    (load / "Sales__Customer.py").write_text("runtime", encoding="utf-8")
    pointer = Borrowed(
        WeaverDocumentId.parse("Lakehouse/Input/Tables/Sales.Customer"),
        "table",
        "table",
    )
    view = Borrowed(
        WeaverDocumentId.parse("Lakehouse/Input/Tables/Sales.Active"), "view", "view"
    )
    resolved = _resolved("Input", "Lakehouse", relations=(pointer, view))
    session = _Session(resolver=_Resolver(tmp_path), store=FilesystemStore())

    plan, payloads, summary = mirror_mutation_plan(resolved, session=session)

    actions = _actions(plan)
    import json

    pointers = json.loads(
        payloads[actions["mirror-pointers-lakehouse-Input_Dev"].payload]
    )
    assert pointers["shortcuts"][0]["source_path"] == "Tables/Sales/customer"
    assert runs_before(
        plan,
        "mirror-pointers-lakehouse-Input_Dev",
        "mirror-await-tables-pointers-lakehouse-Input_Dev",
    )
    # The ``_`` surface reads the destination catalogue, so it follows its build.
    assert runs_before(plan, "complete-build", "mirror-surface-lakehouse-Input_Dev")
    assert not runs_before(
        plan, "complete-build", "mirror-pointers-lakehouse-Input_Dev"
    )
    copy = json.loads(payloads[actions["mirror-load-tree-lakehouse-Input_Dev"].payload])
    assert copy["files"] == ["Tables/Sales__Customer.py"]
    assert runs_before(
        plan,
        "clear-files-lakehouse-Input_Dev",
        copy_id := "mirror-load-tree-lakehouse-Input_Dev",
    )
    assert runs_before(plan, copy_id, "mirror-bind-lakehouse-Input_Dev")
    assert runs_before(
        plan,
        "mirror-await-tables-pointers-lakehouse-Input_Dev",
        "mirror-bind-lakehouse-Input_Dev",
    )
    assert plan.execution.spark_home_target_id == "lakehouse-Input_Dev"
    assert summary["Lakehouse/Input"]["files"] == 1
    assert summary["Lakehouse/Input"]["views"] == 1


@weaver_test()
def test_a_shortcut_into_another_mirrored_item_reads_what_this_plan_builds(tmp_path):
    """The producer's destination is empty until this plan fills it."""

    import json
    from dataclasses import replace

    from weaver.catalogue.borrow import Borrowed
    from weaver.installed import InstalledShortcut

    (tmp_path / "Input" / "Tables" / "Sales" / "Customer").mkdir(parents=True)
    producer = _resolved(
        "Input",
        "Lakehouse",
        relations=(
            Borrowed(
                WeaverDocumentId.parse("Lakehouse/Input/Tables/Sales.Customer"),
                "table",
                "table",
            ),
        ),
    )
    consumer_item = WeaverItemId.parse("Lakehouse/Model")
    shortcut = InstalledShortcut(
        destination=WeaverDocumentId.parse("Lakehouse/Model/Tables/Sales.Upstream"),
        source=WeaverDocumentId.parse("Lakehouse/Input/Tables/Sales.Customer"),
        shortcut_type="table",
        target_type="logical",
        target_item=WeaverItemId.parse("Lakehouse/Input"),
        target_schema="Sales",
        target_object="Customer",
    )
    consumer = MirrorItem(
        item=consumer_item,
        source_target="Model",
        destination="Model_Dev",
        shortcuts=(shortcut,),
    )
    resolved = replace(
        producer,
        items=(*producer.items, consumer),
        bindings={**producer.bindings, consumer_item: "Model_Dev"},
        installations={**producer.installations, consumer_item: {}},
    )
    session = _Session(resolver=_Resolver(tmp_path), store=FilesystemStore())

    plan, payloads, _summary = mirror_mutation_plan(resolved, session=session)

    actions = _actions(plan)
    recreated = json.loads(
        payloads[actions["mirror-recreated-lakehouse-Model_Dev"].payload]
    )
    assert recreated["shortcuts"][0]["source_path"] == "Tables/Sales/Customer"
    assert recreated["shortcuts"][0]["source_item_name"] == "Input_Dev"
    assert runs_before(
        plan,
        "mirror-await-tables-pointers-lakehouse-Input_Dev",
        "mirror-recreated-lakehouse-Model_Dev",
    )


@weaver_test()
def test_a_shortcut_into_its_own_destination_waits_for_what_it_reads(tmp_path):
    import json
    from dataclasses import replace

    from weaver.installed import InstalledShortcut

    (tmp_path / "Input" / "Tables" / "Sales" / "Customer").mkdir(parents=True)
    resolved = _resolved(
        "Input",
        "Lakehouse",
        relations=(
            Borrowed(
                WeaverDocumentId.parse("Lakehouse/Input/Tables/Sales.Customer"),
                "table",
                "table",
            ),
        ),
    )
    alias = InstalledShortcut(
        destination=WeaverDocumentId.parse("Lakehouse/Input/Tables/Sales.Alias"),
        source=WeaverDocumentId.parse("Lakehouse/Input/Tables/Sales.Customer"),
        shortcut_type="table",
        target_type="logical",
        target_item=WeaverItemId.parse("Lakehouse/Input"),
        target_schema="Sales",
        target_object="Customer",
    )
    resolved = replace(
        resolved, items=(replace(resolved.items[0], shortcuts=(alias,)),)
    )
    session = _Session(resolver=_Resolver(tmp_path), store=FilesystemStore())

    plan, payloads, _summary = mirror_mutation_plan(resolved, session=session)

    actions = _actions(plan)
    recreated = "mirror-recreated-lakehouse-Input_Dev"
    assert (
        json.loads(payloads[actions[recreated].payload])["shortcuts"][0][
            "source_item_name"
        ]
        == "Input_Dev"
    )
    assert runs_before(
        plan, "mirror-await-tables-pointers-lakehouse-Input_Dev", recreated
    )


@weaver_test()
def test_a_wide_lakehouse_mirror_spreads_its_views_and_waits_across_actions(tmp_path):
    from weaver.build_bundle.shortcuts import READINESS_CHUNK
    from weaver.mirror_plan import VIEWS_PER_ACTION

    tables = tmp_path / "Input" / "Tables" / "Sales"
    relations = []
    for n in range(2 * READINESS_CHUNK + 10):
        (tables / f"T{n:03d}").mkdir(parents=True)
        relations.append(
            Borrowed(
                WeaverDocumentId.parse(f"Lakehouse/Input/Tables/Sales.T{n:03d}"),
                "table",
                "table",
            )
        )
    for n in range(3 * VIEWS_PER_ACTION):
        relations.append(
            Borrowed(
                WeaverDocumentId.parse(f"Lakehouse/Input/Tables/Sales.V{n:03d}"),
                "view",
                "view",
            )
        )
    resolved = _resolved("Input", "Lakehouse", relations=relations)
    session = _Session(resolver=_Resolver(tmp_path), store=FilesystemStore())

    plan, _payloads, _summary = mirror_mutation_plan(resolved, session=session)

    actions = _actions(plan)
    views = [a for a in actions if a.startswith("mirror-views-")]
    waits = [a for a in actions if a.startswith("mirror-await-tables-pointers-")]
    assert len(views) == 3 and len(waits) == 3
    for each in (*views, *waits):
        assert runs_before(plan, each, "mirror-bind-lakehouse-Input_Dev")
    assert not any(runs_before(plan, a, b) for a in views for b in views if a != b)
