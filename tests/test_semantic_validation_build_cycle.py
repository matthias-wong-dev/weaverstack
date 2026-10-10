"""A semantic model's validations publish to the catalogue without a definition."""

import json

from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import ITEM
from test_semantic_source_build_cycle import (
    answer_catalogue,
    capture_publication,
    read_bindings,
    source_catalogue,
    source_project,
    source_session,
)
from test_semantic_validation_declaration import ASSUMPTION, TEST

import weaver

SELECTOR = f"{ITEM}=SemanticModel/Reporting_Dev"


def with_validations(tmp_path):
    root = source_project(tmp_path)
    source = root / str(ITEM) / f"{ITEM.item_name}.tmdl"
    folder = root / "PowerBI/Commerce"
    folder.mkdir(parents=True)
    source.rename(folder / source.name)
    source.parent.rmdir()
    for directory, name, text in (
        ("tests", "Sales.RevenueReconciles", TEST),
        ("assumptions", "Sales.RevenueIsPositive", ASSUMPTION),
    ):
        owned = folder / directory / ITEM.item_name
        owned.mkdir(parents=True)
        (owned / f"{name}.dax").write_text(text, encoding="utf-8")
    return root


@weaver_test()
def test_validations_publish_their_dictionary_definition_and_dependencies(
    tmp_path, monkeypatch
):
    root = with_validations(tmp_path)
    with source_session() as session:
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(root, items=SELECTOR, session=session)
        assert result.succeeded, result.errors
        rows = published()[ITEM]
    dictionary = {r["object_name"]: r for r in rows["TestDictionary"]}
    assert {
        name: (row["schema_name"], row["test_type"], row["primary_key"])
        for name, row in dictionary.items()
    } == {
        "RevenueReconciles": ("Sales", "test", "Month"),
        "RevenueIsPositive": ("Sales", "assumption", None),
    }
    definitions = {
        r["object_name"]: json.loads(r["definition"]) for r in rows["SemanticModelTest"]
    }
    assert definitions["RevenueIsPositive"] == {
        "version": 1,
        "dax": 'EVALUATE FILTER(ROW("Revenue", [Revenue]), [Revenue] < 0)',
    }
    test = definitions["RevenueReconciles"]
    assert test["expectedSource"] == "Warehouse/Serving"
    assert test["expectedSql"].startswith("SELECT Month")
    assert test["dax"].startswith("EVALUATE")
    assert {
        (
            r["referencing_schema_name"],
            r["referencing_object_name"],
            r["dependency_reference"],
        )
        for r in rows["Dependency"]
        if r["referencing_schema_name"]
    } == {("Sales", "RevenueReconciles", "Warehouse/Serving/Cake.Sales")}
    # A semantic validation is a definition, not an installed object.
    assert {
        r["object_type"] for r in rows["Registry"] if r["object_role"] == "data"
    } == {"semantic_model"}
    assert {r["object_role"] for r in rows["Registry"]} == {"data", "source"}
    assert not {"test", "assumption"} & {r["object_role"] for r in rows["Registry"]}


@weaver_test()
def test_the_browser_rows_are_the_models_installed_graph(tmp_path, monkeypatch):
    """The model, its validations and their reads, as the catalogue graph has them.

    The graph is read once the build's rows and Fabric's deployed tables are both
    installed; the browser rows were projected before deployment.
    """

    from weaver.catalogue.state import Catalogue

    root = with_validations(tmp_path)
    with source_session() as session:
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(root, items=SELECTOR, session=session)
        assert result.succeeded, result.errors
        rows = published()
    dag = Catalogue({**source_catalogue().rows, **rows}).dag()
    validations = {node.node_id for node in dag.nodes if node.is_validation}

    nodes = {row["node_id"] for row in rows[ITEM]["BrowserNode"]}
    edges = {
        (row["upstream_node_id"], row["downstream_node_id"], row["edge_kind"])
        for row in rows[ITEM]["BrowserEdge"]
    }

    assert nodes == {node.node_id for node in dag.nodes if node.item == ITEM}
    assert edges == {
        (
            str(edge.upstream),
            str(edge.downstream),
            "validation" if str(edge.downstream) in validations else "dependency",
        )
        for edge in dag.edges
        if edge.downstream.item == ITEM
    }
    assert (str(ITEM), f"{ITEM}/Sales.RevenueIsPositive", "validation") in edges
    assert ("Warehouse/Serving/Cake.Sales", str(ITEM), "dependency") in edges
    kinds = {row["label"]: row["node_kind"] for row in rows[ITEM]["BrowserNode"]}
    assert kinds == {
        ITEM.item_name: "semantic_model",
        "Sales.RevenueReconciles": "test",
        "Sales.RevenueIsPositive": "assumption",
    }


def answer_installed(session, rows):
    from weaver.catalogue.state import Catalogue
    from weaver.catalogue.tables import CATALOGUE_TABLES

    answer_catalogue(
        session, Catalogue({**source_catalogue().rows, **rows}), read_bindings()
    )
    for statement in tuple(session.tsql):
        if "from sys.objects" in statement:
            session.answer_tsql(
                statement,
                [
                    {"schema_name": "_", "object_name": t.name, "object_type": "U"}
                    for t in CATALOGUE_TABLES
                ],
            )
        elif "from sys.schemas" in statement:
            session.answer_tsql(statement, [{"name": "_"}])


def pending(session, since):
    return {
        name
        for statement in session.tsql[since:]
        if "[_].[TestStatus]" in statement and "N'Pending'" in statement
        for name in ("RevenueReconciles", "RevenueIsPositive")
        if f"N'{name}'" in statement
    }


@weaver_test()
def test_a_validation_edit_resets_only_that_validation(tmp_path, monkeypatch):
    root = with_validations(tmp_path)
    with source_session() as session:
        answer_catalogue(session, source_catalogue(), read_bindings())
        with monkeypatch.context() as patcher:
            published = capture_publication(patcher, session)
            first = weaver.build(root, items=SELECTOR, session=session)
            assert first.succeeded, first.errors
            rows = published()
        client = session.semantic_model("Reporting_Dev")

        answer_installed(session, rows)
        client.calls.clear()
        since = len(session.tsql)
        unchanged = weaver.build(root, items=SELECTOR, session=session)
        assert unchanged.succeeded, unchanged.errors
        assert not unchanged.selection.impact.changed
        assert [kind for kind, _ in client.calls] == [
            "update_definition",
            "invalid_measures",
            "get_definition",
        ]
        assert not pending(session, since)

        path = (
            root
            / f"PowerBI/Commerce/tests/{ITEM.item_name}/Sales.RevenueReconciles.dax"
        )
        path.write_text(TEST.replace("[Revenue])", "[Revenue] * 1)"), encoding="utf-8")
        client.calls.clear()
        since = len(session.tsql)
        with monkeypatch.context() as patcher:
            published = capture_publication(patcher, session)
            edited = weaver.build(root, items=SELECTOR, session=session)
            assert edited.succeeded, edited.errors
            republished = published()[ITEM]
        assert not edited.selection.impact.changed
        assert (
            republished["SemanticModel"][0]["signature"]
            == rows[ITEM]["SemanticModel"][0]["signature"]
        )
        assert [kind for kind, _ in client.calls] == [
            "update_definition",
            "invalid_measures",
            "get_definition",
        ]
        assert pending(session, since) == {"RevenueReconciles"}
        assert {r["object_name"] for r in republished["SemanticModelTest"]} == {
            "RevenueReconciles"
        }
        assert any(
            "[_].[LoadStatus]" in s and not s.lstrip().upper().startswith("SELECT")
            for s in session.tsql[since:]
        )


def installed(tmp_path, monkeypatch):
    """The catalogue a first build of the model and its validations leaves."""

    from weaver.catalogue.state import Catalogue

    root = with_validations(tmp_path)
    with source_session() as session:
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(root, items=SELECTOR, session=session)
        assert result.succeeded, result.errors
        rows = published()
    return Catalogue({**source_catalogue().rows, **rows})


@weaver_test()
def test_installed_validations_depend_on_their_model_and_expected_reads(
    tmp_path, monkeypatch
):
    from weaver.declaration.model import WeaverDocumentId

    dag = installed(tmp_path, monkeypatch).dag()
    validations = {str(n.identity): n for n in dag.validations()}
    assert set(validations) == {
        "SemanticModel/Reporting/Sales.RevenueReconciles",
        "SemanticModel/Reporting/Sales.RevenueIsPositive",
    }
    test = validations["SemanticModel/Reporting/Sales.RevenueReconciles"]
    assert test.is_installed and test.artefact is None
    assert json.loads(test.definition)["expectedSource"] == "Warehouse/Serving"
    assert test.bound_item.name == "Reporting_Dev"
    model = WeaverDocumentId.model_root(ITEM)
    assert {n.identity for n in dag.ancestors(test.identity)} >= {
        model,
        WeaverDocumentId.parse("Warehouse/Serving/Cake.Sales"),
    }
    assumption = validations["SemanticModel/Reporting/Sales.RevenueIsPositive"]
    assert model in {n.identity for n in dag.ancestors(assumption.identity)}
    assert not dag.unresolved


@weaver_test()
def test_health_reports_a_missing_definition_and_pending_validations(
    tmp_path, monkeypatch
):
    from datetime import datetime, timezone

    from weaver.catalogue.state import Catalogue
    from weaver.health import assess
    from weaver.operations.health import HEALTH_TABLES

    catalogue = installed(tmp_path, monkeypatch)
    read = {table.name for table in HEALTH_TABLES}
    rows = {
        item: {name: rows for name, rows in tables.items() if name in read}
        for item, tables in catalogue.rows.items()
    }
    rows[ITEM]["SemanticModelTest"] = tuple(
        r
        for r in rows[ITEM]["SemanticModelTest"]
        if r["object_name"] != "RevenueIsPositive"
    )
    now = datetime(2026, 10, 8, tzinfo=timezone.utc)
    report = assess(Catalogue(rows), as_of=now, generated_at=now, items=[ITEM])
    findings = {(f.area, f.code, f.object_id) for f in report.findings}
    assert (
        "build",
        "missing_validation_artefact",
        "SemanticModel/Reporting/Sales.RevenueIsPositive",
    ) in findings
    assert (
        "tests",
        "test_pending",
        "SemanticModel/Reporting/Sales.RevenueReconciles",
    ) in findings
    assert (
        "build",
        "missing_validation_artefact",
        "SemanticModel/Reporting/Sales.RevenueReconciles",
    ) not in findings


@weaver_test()
def test_health_reports_a_model_behind_its_source(tmp_path, monkeypatch):
    """A source View rebuilt after the model refreshed leaves the model behind."""

    from datetime import datetime, timezone

    from weaver.catalogue.state import Catalogue
    from weaver.declaration.model import WeaverItemId
    from weaver.health import assess
    from weaver.operations.health import HEALTH_TABLES

    catalogue = installed(tmp_path, monkeypatch)
    read = {table.name for table in HEALTH_TABLES}
    rows = {
        item: {name: rows for name, rows in tables.items() if name in read}
        for item, tables in catalogue.rows.items()
    }

    def status(item, schema, name, completed):
        at = datetime(2026, 10, 7, completed, tzinfo=timezone.utc)
        return {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "schema_name": schema,
            "object_name": name,
            "result": "succeeded",
            "started_datetime": at,
            "completed_datetime": at,
        }

    source = WeaverItemId.parse("Warehouse/Serving")
    rows[ITEM]["LoadStatus"] = (status(ITEM, "", "", 9),)
    rows[source]["LoadStatus"] = (status(source, "Cake", "Summary", 10),)
    now = datetime(2026, 10, 7, 8, tzinfo=timezone.utc)
    report = assess(Catalogue(rows), as_of=now, generated_at=now, items=[ITEM])
    (behind,) = (f for f in report.findings if f.object_id == "SemanticModel/Reporting")
    assert (behind.area, behind.code) == ("load", "load_stale_ancestor")
    assert "Warehouse/Serving/Cake.Summary" in behind.message


@weaver_test()
def test_health_reports_validations_stale_after_their_model_is_rebuilt(
    tmp_path, monkeypatch
):
    """A rebuilt model is Pending, yet its validations' last pass is out of date."""

    from datetime import datetime, timezone

    from weaver.catalogue.state import Catalogue
    from weaver.health import assess
    from weaver.operations.health import HEALTH_TABLES

    catalogue = installed(tmp_path, monkeypatch)
    read = {table.name for table in HEALTH_TABLES}
    rows = {
        item: {name: rows for name, rows in tables.items() if name in read}
        for item, tables in catalogue.rows.items()
    }
    passed = datetime(2026, 10, 7, 9, tzinfo=timezone.utc)
    scope = {"item_type": ITEM.item_type, "item_name": ITEM.item_name}
    rows[ITEM]["TestStatus"] = tuple(
        {
            **scope,
            "schema_name": row["schema_name"],
            "object_name": row["object_name"],
            "test_type": row["test_type"],
            "result": "succeeded",
            "started_datetime": passed,
            "completed_datetime": passed,
        }
        for row in rows[ITEM]["TestDictionary"]
    )
    rows[ITEM]["LoadStatus"] = (
        {**scope, "schema_name": "", "object_name": "", "result": "pending"},
    )
    rows[ITEM]["Registry"] = tuple(
        {**row, "build_datetime": "2026-10-07 10:00:00.000000"}
        if row["schema_name"] == "" and row["object_name"] == ""
        else row
        for row in rows[ITEM]["Registry"]
    )
    now = datetime(2026, 10, 7, 8, tzinfo=timezone.utc)
    report = assess(Catalogue(rows), as_of=now, generated_at=now, items=[ITEM])
    stale = {
        f.object_id: f.message
        for f in report.tests.findings
        if f.code == "test_stale_dependency"
    }
    assert stale == {
        f"SemanticModel/Reporting/Sales.{name}": (
            "SemanticModel/Reporting has been built since this validation passed"
        )
        for name in ("RevenueIsPositive", "RevenueReconciles")
    }
