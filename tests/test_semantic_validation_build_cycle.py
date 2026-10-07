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
    folder = root / str(ITEM)
    for directory, name, text in (
        ("tests", "Sales.RevenueReconciles", TEST),
        ("assumptions", "Sales.RevenueIsPositive", ASSUMPTION),
    ):
        (folder / directory).mkdir()
        (folder / directory / f"{name}.dax").write_text(text, encoding="utf-8")
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
    assert all(r["object_type"] == "semantic_model" for r in rows["Registry"])


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
        assert not client.calls and not pending(session, since)

        path = root / str(ITEM) / "tests/Sales.RevenueReconciles.dax"
        path.write_text(TEST.replace("[Revenue])", "[Revenue] * 1)"), encoding="utf-8")
        since = len(session.tsql)
        with monkeypatch.context() as patcher:
            published = capture_publication(patcher, session)
            edited = weaver.build(root, items=SELECTOR, session=session)
            assert edited.succeeded, edited.errors
            republished = published()[ITEM]
        assert not any(kind == "update_definition" for kind, _ in client.calls)
        assert pending(session, since) == {"RevenueReconciles"}
        assert {r["object_name"] for r in republished["SemanticModelTest"]} == {
            "RevenueReconciles"
        }
        assert not any(
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
