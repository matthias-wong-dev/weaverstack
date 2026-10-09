"""weaver test runs semantic validations from their installed definitions."""

import json
from decimal import Decimal

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import ITEM, DefinitionClient
from test_semantic_model_load_cycle import answer_installed
from test_semantic_source_build_cycle import source_session
from test_semantic_validation_build_cycle import installed, with_validations

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


def run(
    tmp_path,
    monkeypatch,
    *,
    test_rows,
    assumption_rows,
    expected_rows,
    name=None,
    primary_key=None,
    items=str(ITEM),
    source=None,
):
    catalogue = installed(tmp_path, monkeypatch)
    if primary_key is not None:
        from weaver.catalogue.state import Catalogue

        catalogue = Catalogue(
            {
                **catalogue.rows,
                ITEM: {
                    **catalogue.rows[ITEM],
                    "TestDictionary": tuple(
                        {**row, "primary_key": ", ".join(primary_key) or None}
                        if row["object_name"] == "RevenueReconciles"
                        else row
                        for row in catalogue.rows[ITEM]["TestDictionary"]
                    ),
                },
            }
        )
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
        report = weaver.test(items, names=name, source=source, session=session)
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


@weaver_test()
def test_a_power_bi_selector_runs_its_models_installed_validations(
    tmp_path, monkeypatch
):
    report, session, _ = run(
        tmp_path,
        monkeypatch,
        test_rows=MATCHING,
        assumption_rows=[],
        expected_rows=EXPECTED,
        items="PowerBI/Commerce",
        source=tmp_path / "project",
    )
    assert statuses(session) == {
        "RevenueReconciles": "Succeeded",
        "RevenueIsPositive": "Succeeded",
    }
    assert report.workflow_id


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


@pytest.mark.parametrize("primary_key", [(), ("Month",)])
@pytest.mark.parametrize("empty_side", ["expected", "actual", "both"])
@weaver_test()
def test_empty_results_without_schema_record_error(
    tmp_path, monkeypatch, primary_key, empty_side
):
    report, session, _ = run(
        tmp_path,
        monkeypatch,
        test_rows=[] if empty_side in {"actual", "both"} else MATCHING,
        assumption_rows=[],
        expected_rows=[] if empty_side in {"expected", "both"} else EXPECTED,
        name="Sales.RevenueReconciles",
        primary_key=primary_key,
    )
    assert statuses(session) == {"RevenueReconciles": "Error"}
    (node,) = report.nodes
    assert not node.succeeded
    assert "no column metadata" in node.result.error_message
    assert "RevenueReconciles" in node.result.error_message
    assert not node.diagnostics
    assert not session.spark_sql and not session.python


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


# --- file mode: from the project folder, with no catalogue --------------------


class ModelQueries(DefinitionClient):
    """Answers the Test's DAX and the Assumption's DAX by what each asks."""

    def __init__(self, test_rows, assumption_rows):
        super().__init__()
        self.test_rows = test_rows
        self.assumption_rows = assumption_rows

    def query_dax(self, query):
        self.calls.append(("query_dax", query))
        return self.test_rows if "SUMMARIZECOLUMNS" in query else self.assumption_rows


def expected_sql(root):
    from weaver.declaration.repository import parse_item_repository
    from weaver.locations import Location

    repository = parse_item_repository(Location(str(root)))
    (sql,) = [
        source.document.expected_sql
        for source in repository.source_documents.values()
        if source.is_validation and source.document.expected_sql
    ]
    return sql


def file_mode(tmp_path, *, test_rows=MATCHING, assumption_rows=(), **asked):
    """Run ``weaver test`` with no catalogue, Serving mapped to Serving_Dev."""

    from test_semantic_source_build_cycle import SOURCE

    from weaver.workspaces import TargetDeclaration, Workspace

    root = with_validations(tmp_path)
    workspace = Workspace(
        workspace="Demo",
        targets={
            ITEM: TargetDeclaration(physical="Reporting_Dev"),
            SOURCE: TargetDeclaration(physical="Serving_Dev"),
        },
    )
    with source_session(workspace=workspace) as session:
        model = session.resolve_item("Reporting_Dev", item_type="SemanticModel")
        client = ModelQueries(test_rows, list(assumption_rows))
        session.answer_semantic_model(model.workspace_id, model.id, client)
        session.answer_tsql(expected_sql(root), EXPECTED)
        report = weaver.test(source=root, session=session, **asked)
        return report, session, client


@weaver_test()
def test_file_mode_runs_project_validations_without_a_catalogue(tmp_path):
    report, session, client = file_mode(tmp_path)

    assert {node.logical_id: node.status for node in report.nodes} == {
        "SemanticModel/Reporting/Sales.RevenueIsPositive": "passed",
        "SemanticModel/Reporting/Sales.RevenueReconciles": "passed",
    }
    assert {node.physical_target for node in report.nodes} == {
        "SemanticModel/Reporting_Dev"
    }
    assert [kind for kind, _ in client.calls].count("query_dax") == 2
    # Nothing is recorded, and the Expected SQL ran in Serving's mapped target.
    assert report.workflow_id is None
    (expected,) = [call for call in session.calls if call.kind == "tsql"]
    assert expected.detail["target"].warehouse.name == "Serving_Dev"
    assert not session.spark_sql and not session.python


@weaver_test()
def test_file_mode_reports_a_difference_with_its_diagnostic_rows(tmp_path):
    report, _, _ = file_mode(
        tmp_path,
        test_rows=[MATCHING[0], {"Sales[Month]": 2, "[Revenue]": 21}],
        names=r"Sales\.RevenueRecon.*",
    )

    (node,) = report.nodes
    assert node.status == "failed"
    assert node.result.missing_count == 1
    assert [row["_weaver_side"] for row in node.diagnostics] == ["expected", "actual"]


@weaver_test()
def test_file_mode_runs_only_the_files_given(tmp_path):
    root = tmp_path / "project"
    report, _, client = file_mode(
        tmp_path,
        files=[str(root / "PowerBI/Commerce/assumptions/Reporting/*.dax")],
    )

    assert [node.logical_id for node in report.nodes] == [
        "SemanticModel/Reporting/Sales.RevenueIsPositive"
    ]
    assert [kind for kind, _ in client.calls] == ["query_dax"]


@weaver_test()
def test_file_mode_refuses_a_name_that_matches_no_validation(tmp_path):
    from weaver.errors import CommandError

    with pytest.raises(CommandError, match="matches 'Sales.Missing'"):
        file_mode(tmp_path, names=["Sales.RevenueIsPositive", "Sales.Missing"])


# --- catalogue mode: certified models only ------------------------------------


@weaver_test()
def test_catalogue_mode_refuses_a_model_that_is_not_certified(tmp_path, monkeypatch):
    """Load's rule: an uncertified deployment is neither loaded nor tested."""

    from weaver.catalogue.state import Catalogue
    from weaver.errors import ValidationError

    catalogue = installed(tmp_path, monkeypatch)
    rows = dict(catalogue.rows[ITEM])
    # A Build whose publication did not complete leaves no certified definition.
    rows["SemanticModel"] = ()
    catalogue = Catalogue({**catalogue.rows, ITEM: rows})
    with source_session() as session:
        answer_installed(session, catalogue.rows)
        with pytest.raises(ValidationError) as refused:
            weaver.test(str(ITEM), session=session)

    assert str(refused.value) == (
        "SemanticModel/Reporting is not certified for Test. Build "
        "SemanticModel/Reporting before testing it."
    )
