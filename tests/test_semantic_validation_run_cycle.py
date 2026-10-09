"""weaver test runs semantic validations from their installed definitions."""

import json
from decimal import Decimal

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import ITEM, DefinitionClient
from test_semantic_model_load_cycle import answer_installed
from test_semantic_source_build_cycle import source_session
from test_semantic_validation_build_cycle import installed

import weaver


class QueryClient(DefinitionClient):
    def __init__(self, answers):
        super().__init__()
        self.answers = answers

    def query_dax(self, query):
        self.calls.append(("query_dax", query))
        answer = self.answers[query]
        if isinstance(answer, Exception):
            raise answer
        return answer


def definitions(catalogue):
    return {
        row["object_name"]: json.loads(row["definition"])
        for row in catalogue.rows[ITEM]["SemanticModelTest"]
    }


def run(tmp_path, monkeypatch, *, test_rows, assumption_rows, expected_rows, name=None):
    catalogue = installed(tmp_path, monkeypatch)
    found = definitions(catalogue)
    with source_session() as session:
        client = QueryClient(
            {
                found["RevenueReconciles"]["dax"]: test_rows,
                found["RevenueIsPositive"]["dax"]: assumption_rows,
            }
        )
        (model,) = catalogue.rows[ITEM]["Installation"]
        session.answer_semantic_model(model["workspace_id"], model["item_id"], client)
        session.answer_tsql(found["RevenueReconciles"]["expectedSql"], expected_rows)
        answer_installed(session, catalogue.rows)
        report = weaver.test(str(ITEM), name=name, session=session)
        return report, session, client


def statuses(session):
    """The last result each validation's TestStatus row was written with."""

    import re

    row = re.compile(
        r"N'(RevenueReconciles|RevenueIsPositive)'.*?N'(Succeeded|Failed|Error)'"
    )
    written = {}
    for statement in session.tsql:
        if statement.startswith("MERGE INTO [_].[TestStatus]"):
            # One written row per line, whether or not writes were batched.
            for line in statement.splitlines():
                if match := row.search(line):
                    written[match[1]] = match[2]
    return written


MATCHING = [
    {"Sales[Month]": 1, "[Revenue]": 10.0},
    {"Sales[Month]": 2, "[Revenue]": 20},
]
EXPECTED = [
    {"Month": 1, "Revenue": Decimal("10.00")},
    {"Month": 2, "Revenue": Decimal("20.00")},
]


@weaver_test()
def test_passing_validations_record_succeeded(tmp_path, monkeypatch):
    report, session, client = run(
        tmp_path,
        monkeypatch,
        test_rows=MATCHING,
        assumption_rows=[],
        expected_rows=EXPECTED,
    )
    assert statuses(session) == {
        "RevenueReconciles": "Succeeded",
        "RevenueIsPositive": "Succeeded",
    }
    assert [kind for kind, _ in client.calls].count("query_dax") == 2
    assert not session.spark_sql and not session.python


@weaver_test()
def test_differences_and_violations_record_failed(tmp_path, monkeypatch):
    report, session, _ = run(
        tmp_path,
        monkeypatch,
        test_rows=[MATCHING[0], {"Sales[Month]": 2, "[Revenue]": 21}],
        assumption_rows=[{"[Revenue]": -1}, {"[Revenue]": -2}],
        expected_rows=EXPECTED,
    )
    assert statuses(session) == {
        "RevenueReconciles": "Failed",
        "RevenueIsPositive": "Failed",
    }
    counts = {
        node["logical_id"].rsplit(".", 1)[1]: node.get(
            "failure_count", node.get("violation_count")
        )
        for node in report.to_mapping()["nodes"]
    }
    assert counts == {"RevenueReconciles": 2, "RevenueIsPositive": 2}


@pytest.mark.parametrize(
    "test_rows, expected_rows",
    [
        (RuntimeError("DAX failed: syntax"), EXPECTED),
        ([{"Sales[Month]": 1}], EXPECTED),
    ],
)
@weaver_test()
def test_a_validation_that_cannot_run_records_error(
    tmp_path, monkeypatch, test_rows, expected_rows
):
    _, session, _ = run(
        tmp_path,
        monkeypatch,
        test_rows=test_rows,
        assumption_rows=[],
        expected_rows=expected_rows,
    )
    assert statuses(session) == {
        "RevenueReconciles": "Error",
        "RevenueIsPositive": "Succeeded",
    }


@pytest.mark.parametrize("reserved", ["_weaver_side", "_weaver_sk"])
@pytest.mark.parametrize("populated", ["expected", "actual"])
@weaver_test()
def test_reserved_columns_on_one_populated_side_record_error(
    tmp_path, monkeypatch, reserved, populated
):
    expected = [{"Month": 1, "Revenue": 10, reserved: "user"}]
    actual = [{"Sales[Month]": 1, "[Revenue]": 11, f"[{reserved}]": "user"}]
    report, session, _ = run(
        tmp_path,
        monkeypatch,
        test_rows=actual if populated == "actual" else [],
        assumption_rows=[],
        expected_rows=expected if populated == "expected" else [],
        name="Sales.RevenueReconciles",
    )
    assert statuses(session) == {"RevenueReconciles": "Error"}
    (node,) = report.nodes
    assert not node.succeeded
    assert "reserved for diagnostics" in node.result.error_message


@weaver_test()
def test_a_named_test_returns_paired_diagnostics(tmp_path, monkeypatch):
    report, _, _ = run(
        tmp_path,
        monkeypatch,
        test_rows=[MATCHING[0], {"Sales[Month]": 2, "[Revenue]": 21}],
        assumption_rows=[],
        expected_rows=EXPECTED,
        name="Sales.RevenueReconciles",
    )
    (node,) = report.nodes
    assert [
        (row["_weaver_side"], row["_weaver_sk"], row["Month"], row["Revenue"])
        for row in node.diagnostics
    ] == [("expected", 1, 2, Decimal("20.00")), ("actual", 1, 2, 21)]
    assert "diagnostics" not in json.dumps(report.to_mapping(), default=str)


@weaver_test()
def test_a_lakehouse_expected_source_runs_addressed_spark_sql():
    from weaver.declaration.model import WeaverDocumentId
    from weaver.fabric.resources import Item
    from weaver.semantic_validation import run_semantic_validation
    from weaver.targets import PhysicalTargetRef
    from weaver.test_plan import InstalledValidation

    definition = {
        "version": 1,
        "dax": "EVALUATE Sales",
        "expectedSource": "Lakehouse/Curated",
        "expectedSql": "CREATE TEMP VIEW Recent AS SELECT * FROM Cake.Sales;\n"
        "SELECT Month, Revenue FROM Recent;",
    }
    validation = InstalledValidation(
        logical=WeaverDocumentId.parse("SemanticModel/Reporting/Sales.Lake"),
        kind="Test",
        target=PhysicalTargetRef(kind="semanticmodel", name="Reporting_Dev"),
        artefact=None,
        definition=json.dumps(definition),
        bound_item=Item(
            id="m", name="Reporting_Dev", type="SemanticModel", workspace_id="w"
        ),
        expected_target=PhysicalTargetRef(kind="lakehouse", name="Serving_Dev"),
    )
    with source_session() as session:
        session.answer_semantic_model(
            "w", "m", QueryClient({"EVALUATE Sales": MATCHING})
        )
        session.answer_spark_sql("SELECT Month, Revenue FROM Recent", EXPECTED)
        result, _ = run_semantic_validation(
            validation, session=session, workspace=session.workspace, collect=False
        )
        assert result.succeeded
        assert session.spark_sql == (
            "CREATE TEMP VIEW Recent AS SELECT * FROM `Demo`.`Serving_Dev`.`Cake`.`Sales`",
            "SELECT Month, Revenue FROM Recent",
        )
        assert not session.tsql
