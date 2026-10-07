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
