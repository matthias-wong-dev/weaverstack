"""SemanticModel validations are ordinary Test and Assumption documents in DAX."""

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverDocumentId
from weaver.declaration.repository import parse_item_repository
from weaver.errors import WeaverError
from weaver.locations import Location

TEST = """/*
Test ID: Sales.RevenueReconciles

Description: Revenue agrees with the serving Warehouse.

Primary key: Month

Expected source: Warehouse/Serving

Expected SQL: |
  SELECT Month, SUM(Revenue) AS Revenue
  FROM Cake.Sales
  GROUP BY Month
*/

EVALUATE
SUMMARIZECOLUMNS('Sales'[Month], "Revenue", [Revenue])
"""

ASSUMPTION = """/*
Assumption ID: Sales.RevenueIsPositive

Description: Revenue is never negative.
*/

EVALUATE FILTER(ROW("Revenue", [Revenue]), [Revenue] < 0)
"""

SERVING = """/*
Table ID: Cake.Sales

Description: Sales facts.

Lineage: Constant

Schema:
  Month: int
  Revenue: decimal(18, 2)
*/
SELECT 1 AS Month, CAST(1 AS decimal(18, 2)) AS Revenue
"""


def project(tmp_path, files, *, serving=False):
    root = tmp_path / "project"
    model = root / "PowerBI/Commerce"
    model.mkdir(parents=True)
    (model / "Sales.tmdl").write_text("table Sales\n\tmeasure Revenue = 1\n")
    for relative, text in files.items():
        directory, name = relative.split("/", 1)
        path = model / directory / "Sales" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    if serving:
        warehouse = root / "Warehouse/Serving"
        warehouse.mkdir(parents=True)
        (warehouse / "Cake.yml").write_text(
            "Schema ID: Cake\nDescription: Cake records.\n"
        )
        (warehouse / "Cake.Sales.sql").write_text(SERVING, encoding="utf-8")
    return root


def parse(root):
    return parse_item_repository(Location(root.as_posix()))


@weaver_test()
def test_tests_and_assumptions_are_validation_documents(tmp_path):
    repository = parse(
        project(
            tmp_path,
            {
                "tests/Sales.RevenueReconciles.dax": TEST,
                "assumptions/Sales.RevenueIsPositive.dax": ASSUMPTION,
            },
        )
    )
    test = repository.source_documents[
        WeaverDocumentId.parse("SemanticModel/Sales/Sales.RevenueReconciles")
    ]
    assumption = repository.source_documents[
        WeaverDocumentId.parse("SemanticModel/Sales/Sales.RevenueIsPositive")
    ]
    assert (test.kind, test.language) == ("Test", "dax")
    assert test.document.primary_key == ("Month",)
    assert test.document.expected_source == "Warehouse/Serving"
    assert test.document.expected_sql.startswith("SELECT Month")
    assert test.dax_body.startswith("EVALUATE")
    assert (assumption.kind, assumption.document.expected_source) == (
        "Assumption",
        None,
    )
    (model,) = [i for i in repository.items if i.identity.item_type == "SemanticModel"]
    assert {str(v) for v in model.validations} == {
        "SemanticModel/Sales/Sales.RevenueReconciles",
        "SemanticModel/Sales/Sales.RevenueIsPositive",
    }


@weaver_test()
def test_a_validation_edit_leaves_the_model_definition_unchanged(tmp_path):
    root = project(tmp_path, {"tests/Sales.RevenueReconciles.dax": TEST})
    before = parse(root)
    path = root / "PowerBI/Commerce/tests/Sales/Sales.RevenueReconciles.dax"
    path.write_text(TEST.replace("[Revenue])", "[Revenue] * 1)"), encoding="utf-8")
    after = parse(root)
    item = next(iter(before.semantic_models))
    assert after.semantic_models[item].parts == before.semantic_models[item].parts
    assert (
        after.semantic_models[item].signature == before.semantic_models[item].signature
    )
    assert not any("tests/" in p for p in after.semantic_models[item].parts)
    assert after.signature != before.signature


@weaver_test()
def test_expected_sql_reads_resolve_in_the_expected_source(tmp_path):
    repository = parse(
        project(tmp_path, {"tests/Sales.RevenueReconciles.dax": TEST}, serving=True)
    )
    (edge,) = [
        e
        for e in repository.dependency_edges
        if e.consumer.item.item_type == "SemanticModel"
    ]
    assert edge.reference == "Warehouse/Serving/Cake.Sales"
    assert edge.producer == WeaverDocumentId.parse("Warehouse/Serving/Cake.Sales")


@pytest.mark.parametrize(
    "relative, text, message",
    [
        (
            "assumptions/Sales.RevenueReconciles.dax",
            TEST,
            "assumptions/ declares a Assumption, and this file declares a Test",
        ),
        (
            "tests/Sales.RevenueIsPositive.dax",
            ASSUMPTION,
            "tests/ declares a Test, and this file declares a Assumption",
        ),
        ("tests/Sales.Other.dax", TEST, "must agree"),
        ("tests/RevenueReconciles.dax", TEST, "must name Schema and Object"),
        ("tests/Sales.RevenueReconciles.sql", TEST, "is a .dax file"),
        ("tests/nested/Sales.RevenueReconciles.dax", TEST, "no further subdirectories"),
        (
            "assumptions/Sales.RevenueIsPositive.dax",
            ASSUMPTION.replace(
                "*/", "Expected source: Warehouse/Serving\nExpected SQL: SELECT 1\n*/"
            ),
            "An Assumption has no expected side",
        ),
        (
            "assumptions/Sales.RevenueIsPositive.dax",
            ASSUMPTION.replace("*/", "Primary key: Month\n*/"),
            "must not declare Primary key",
        ),
        (
            "tests/Sales.RevenueReconciles.dax",
            TEST.replace("Expected source: Warehouse/Serving\n", ""),
            "must declare Expected source",
        ),
        (
            "tests/Sales.RevenueReconciles.dax",
            TEST.replace("Warehouse/Serving", "SemanticModel/Other"),
            "must be a Warehouse or Lakehouse",
        ),
        (
            "tests/Sales.RevenueReconciles.dax",
            TEST.replace("  GROUP BY Month\n", "  GROUP BY Month;\n  SELECT 2\n"),
            "Expected SQL requires exactly 1 result set",
        ),
        (
            "tests/Sales.RevenueReconciles.dax",
            TEST.replace("EVALUATE\n", "SELECT\n"),
            "beginning with EVALUATE or DEFINE",
        ),
    ],
)
@weaver_test()
def test_invalid_semantic_validations_are_refused(tmp_path, relative, text, message):
    root = project(tmp_path, {relative: text})
    with pytest.raises(WeaverError, match=message):
        parse(root)


@weaver_test()
def test_warehouse_validations_refuse_expected_side_keys(tmp_path):
    root = project(tmp_path, {}, serving=True)
    tests = root / "Warehouse/Serving/tests"
    tests.mkdir()
    (tests / "Cake.SalesExist.sql").write_text(
        "/*\nTest ID: Cake.SalesExist\nDescription: Sales exist.\n"
        "Expected source: Warehouse/Serving\n*/\nSELECT 1 AS N\nSELECT 1 AS N\n",
        encoding="utf-8",
    )
    with pytest.raises(WeaverError, match="belong to a SemanticModel DAX Test"):
        parse(root)


@weaver_test()
def test_an_expected_object_missing_from_a_project_source_is_refused(tmp_path):
    root = project(
        tmp_path,
        {"tests/Sales.RevenueReconciles.dax": TEST.replace("Cake.Sales", "Cake.Gone")},
        serving=True,
    )
    with pytest.raises(WeaverError, match="does not match an object"):
        parse(root)


@weaver_test()
def test_named_variants_own_validations_without_inheriting_base_tests(tmp_path):
    root = project(tmp_path, {"tests/Sales.RevenueReconciles.dax": TEST})
    scope = root / "PowerBI/Commerce"
    (scope / "Executive.tmdl").write_text(
        "model Model\n\tannotation Weaver.BaseSemanticModels = Sales\n"
    )
    before = parse(root)
    items = {item.identity.item_name: item for item in before.items}
    assert not items["Executive"].validations
    tests = scope / "tests/Executive"
    tests.mkdir()
    (tests / "Sales.RevenueReconciles.dax").write_text(TEST)
    after = parse(root)
    items = {item.identity.item_name: item for item in after.items}
    assert {str(v) for v in items["Executive"].validations} == {
        "SemanticModel/Executive/Sales.RevenueReconciles"
    }
    assert {str(v) for v in items["Sales"].validations} == {
        "SemanticModel/Sales/Sales.RevenueReconciles"
    }
    assert after.semantic_models == before.semantic_models
    assert after.signature != before.signature


@pytest.mark.parametrize("owner", ["Gone", "sales"])
@weaver_test()
def test_validation_owner_must_be_an_exact_local_definition(tmp_path, owner):
    root = project(tmp_path, {"tests/Sales.RevenueReconciles.dax": TEST})
    tests = root / "PowerBI/Commerce/tests"
    (tests / "Sales").rename(tests / owner)
    with pytest.raises(WeaverError, match=f"SemanticModel/{owner} is not defined"):
        parse(root)


@weaver_test()
def test_validation_path_names_the_model_in_a_composition_scope(tmp_path):
    root = project(tmp_path, {"tests/Sales.RevenueReconciles.dax": TEST})
    tests = root / "PowerBI/Commerce/tests"
    (tests / "Sales/Sales.RevenueReconciles.dax").rename(
        tests / "Sales.RevenueReconciles.dax"
    )
    with pytest.raises(WeaverError, match="<Model>/<Schema>.<Object>.dax"):
        parse(root)


@weaver_test()
def test_fabric_journey_fixture_hides_signature_in_the_final_compilation(
    tmp_path, monkeypatch
):
    from pathlib import Path

    monkeypatch.syspath_prepend(str(Path(__file__).parent / "fabric"))
    from test_semantic_acceptance_journey import _project

    from weaver.semantic_models.binding import bind_semantic_sources
    from weaver.semantic_models.objects import TmdlDefinition

    root = tmp_path / "project"
    _project(root)
    repository = parse(root)
    (item,) = repository.semantic_models
    reference = "Warehouse/_weaver/_.TableDictionary"
    source = {
        "reference": reference,
        "server": "catalogue.datawarehouse.fabric.microsoft.com",
        "database": "catalogue",
        "schema": "_",
        "object": "TableDictionary",
        "source_columns": [
            {"column_name": name, "data_type": "varchar"}
            for name in ("Schema name", "Item type", "Item name", "Signature")
        ],
    }
    bound = bind_semantic_sources(repository, {reference: source}, {item})
    model = TmdlDefinition(bound.semantic_models[item].parts).model
    assert model.tables["Objects"].columns["Signature"].isHidden is True
    assert repository.semantic_models[item].source_references == {
        "Objects": reference,
        "Reference": reference,
    }
    assert {str(v) for v in repository[str(item)].validations} == {
        f"{item}/Acceptance.ObjectsReconcile",
        f"{item}/Acceptance.ObjectsAreSigned",
    }
