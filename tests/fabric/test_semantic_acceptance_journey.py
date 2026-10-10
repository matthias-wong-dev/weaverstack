"""A semantic model's whole lifecycle on a fixed Fabric item, through public calls.

Build, a Load without the project folder, Test and Health; repeated
selected-model deployment; a model change, which makes passed Tests stale once
it loads; a Test edit with stable model signature; a wipe keeping the data
source; and a rebuild back to green. The Tests run real DAX against the model
and real T-SQL against the catalogue.
"""

import json

from support.semantic_projects import ITEM, SCOPE, SOURCE_TEXT
from support.weaver_test import weaver_test
from test_semantic_model_public_cycle import (
    semantic_build_context as semantic_build_context,
)

import weaver
from weaver.catalogue.reader import read_table
from weaver.catalogue.tables import (
    BOOKMARK,
    CURRENT_STATE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    PROJECTED_TABLES,
    REGISTRY,
    SEMANTIC_MODEL,
    SEMANTIC_MODEL_TEST,
    TEST_DICTIONARY,
    TEST_STATUS,
)
from weaver.catalogue.tsql import literal
from weaver.semantic_models import TmdlDefinition
from weaver.semantic_models.binding import m_string
from weaver.semantic_models.definition import decode_model

ANNOTATION = '''from weaver.semantic_models import Annotation


class Acceptance__HideSignatures(Annotation):
    """Hide every column named Signature."""

    scopes = {"model"}

    def apply(self, target):
        for table in target.tables:
            for column in table.columns:
                if column.name == "Signature":
                    column.isHidden = True
'''

DAX_COUNT = '"Objects", COUNTROWS(Objects)'
TEST = f"""/*
Test ID: Acceptance.ObjectsReconcile

Description: The model counts the catalogue's own objects as the catalogue does.

Primary key: Schema name

Expected source: Warehouse/_weaver

Expected SQL: |
  SELECT [Schema name], COUNT(*) AS [Objects]
  FROM _.TableDictionary
  WHERE [Item type] = 'Warehouse' AND [Item name] = '_weaver'
  GROUP BY [Schema name]
*/

EVALUATE
SUMMARIZECOLUMNS(
    Objects[Schema name],
    TREATAS({{"Warehouse"}}, Objects[Item type]),
    TREATAS({{"_weaver"}}, Objects[Item name]),
    {DAX_COUNT}
)
"""
ASSUMPTION = """/*
Assumption ID: Acceptance.ObjectsAreSigned

Description: Every catalogued object carries a signature.
*/

EVALUATE
FILTER(Objects, ISBLANK(Objects[Signature]))
"""


def _catalogue_source(session):
    return session.semantic_source(
        session.workspace.catalogue_item,
        item_type="Warehouse",
        schema="_",
        name="TableDictionary",
        include_columns=False,
    )


def _project(root, *, source_metadata, native_source=None):
    source = TmdlDefinition({"definition/model.tmdl": SOURCE_TEXT.encode()})
    # Snapshot below is calculated; keep this journey separate from Direct Lake.
    source.model.tables["Metric"].remove()
    partitions = []
    for name, columns in (
        (
            "Objects",
            ("Item type", "Item name", "Schema name", "Object type", "Signature"),
        ),
        ("Reference", ("Item type", "Item name")),
    ):
        table = source.model.tables[name]
        for column_name in columns:
            if column_name not in table.columns:
                column = table.columns.add(column_name)
                column.dataType = "string"
                column.sourceColumn = column_name
        partitions.append(
            (
                name,
                f"    partition {name} = m\n"
                "        mode: import\n"
                "        source =\n"
                "            let\n"
                f"                Source = Sql.Database({m_string(source_metadata['server'])}, "
                f"{m_string(source_metadata['database'])}),\n"
                f"                Navigation = Source{{[Schema={m_string(source_metadata['schema'])}, "
                f"Item={m_string(source_metadata['object'])}]}}[Data]\n"
                "            in\n                Navigation\n",
            )
        )
    if native_source is not None:
        partitions = [
            (
                name,
                f"    partition {name} = m\n        mode: import\n        source =\n"
                + "\n".join(
                    "            " + line for line in native_source.splitlines()
                )
                + "\n",
            )
            for name, _ in partitions
        ]
    text = source.parts["definition/model.tmdl"].decode()
    for name, partition in partitions:
        text = text.replace(f"table {name}\n", f"table {name}\n{partition}", 1)
    folder = root / "PowerBI/Acceptance"
    (folder / "tests" / ITEM.item_name).mkdir(parents=True)
    (folder / "assumptions" / ITEM.item_name).mkdir(parents=True)
    (folder / f"{ITEM.item_name}.tmdl").write_text(
        text.replace(
            "    annotation Weaver.AutoHideForeignKeys = true\n",
            "    annotation Weaver.AutoHideForeignKeys = true\n"
            "    annotation Acceptance.HideSignatures = true\n",
            1,
        ),
        encoding="utf-8",
    )
    annotations = root / "PowerBI/annotations"
    annotations.mkdir(parents=True)
    (annotations / "Acceptance__HideSignatures.py").write_text(
        ANNOTATION, encoding="utf-8"
    )
    test = folder / f"tests/{ITEM.item_name}/Acceptance.ObjectsReconcile.dax"
    test.write_text(TEST, encoding="utf-8")
    (
        folder / f"assumptions/{ITEM.item_name}/Acceptance.ObjectsAreSigned.dax"
    ).write_text(ASSUMPTION, encoding="utf-8")
    return folder, test


def _results(connection, table):
    return {
        row["object_name"]: str(row["result"]).casefold()
        for row in read_table(connection, table, scope=SCOPE)
    }


def _statuses(connection):
    return _results(connection, TEST_STATUS)


def _load_result(connection):
    (status,) = read_table(connection, LOAD_STATUS, scope=SCOPE)
    return str(status["result"]).casefold()


def _project_bytes(root):
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def _health(session):
    report = weaver.health(str(ITEM), session=session)
    return report, {section.area: section.status for section in report.sections}


def _findings(report):
    return [(f.area, f.code, f.object_id, f.message) for f in report.findings]


@weaver_test(remote=True, resources={"rest", "tds"})
def test_semantic_model_build_load_test_health_lifecycle(
    semantic_build_context, tmp_path
):
    context = semantic_build_context
    connection = context.connection
    root = tmp_path / "project"
    source_metadata = _catalogue_source(context.session)
    context.source.require_metadata(source_metadata)
    folder, test_path = _project(
        root,
        source_metadata=source_metadata,
        native_source=context.source.expression,
    )
    selection = f"{ITEM}=SemanticModel/{context.target}"
    original_sources = _project_bytes(root)
    evidence = {}

    def build():
        result = weaver.build(root, items=selection, session=context.session)
        assert result.succeeded, result.errors
        return result

    def semantic_actions(result):
        return [
            outcome.action_id
            for outcome in result.installation_report.action_results()
            if outcome.action_id.startswith("semantic_model-")
        ]

    def run_validations():
        report = weaver.test(str(ITEM), session=context.session)
        assert report.succeeded, report.to_mapping()
        assert {node.primitive_kind for node in report.nodes} == {"semantic_validation"}
        return report

    # Build: the model deploys and its validations publish as definitions.
    built = build()
    assert semantic_actions(built)
    dictionary = {
        row["object_name"]: row
        for row in read_table(connection, TEST_DICTIONARY, scope=SCOPE)
    }
    assert {name: row["test_type"] for name, row in dictionary.items()} == {
        "ObjectsReconcile": "test",
        "ObjectsAreSigned": "assumption",
    }
    assert dictionary["ObjectsReconcile"]["primary_key"] == "Schema name"
    definitions = {
        row["object_name"]: json.loads(row["definition"])
        for row in read_table(connection, SEMANTIC_MODEL_TEST, scope=SCOPE)
    }
    assert definitions["ObjectsReconcile"]["expectedSource"] == "Warehouse/_weaver"
    assert {
        row["dependency_reference"]
        for row in read_table(connection, DEPENDENCY, scope=SCOPE)
        if row["referencing_object_name"] == "ObjectsReconcile"
    } == {"Warehouse/_weaver/_.TableDictionary"}
    registry = read_table(connection, REGISTRY, scope=SCOPE)
    assert {row["object_type"] for row in registry if row["object_role"] == "data"} == {
        "semantic_model"
    }
    (binding,) = read_table(connection, INSTALLATION, scope=SCOPE)
    assert (binding["workspace_id"], binding["item_id"]) == (
        context.model.workspace_id,
        context.model.model_id,
    )
    (definition,) = read_table(connection, SEMANTIC_MODEL, scope=SCOPE)
    (registered,) = (
        row
        for row in registry
        if (
            row["schema_name"],
            row["object_name"],
            row["object_type"],
            row["object_role"],
        )
        == ("", "", "semantic_model", "data")
    )
    assert registered["signature"] == definition["signature"]
    assert "definition" not in definition
    deployed = decode_model(context.model.get_definition())["model"]
    objects = next(t for t in deployed["tables"] if t["name"] == "Objects")
    assert next(c for c in objects["columns"] if c["name"] == "Signature")["isHidden"]
    context.source.verify("first-build")
    assert _load_result(connection) == "pending"

    # Load, Test, Health: the first certification of the whole lifecycle. The
    # Load needs only the catalogue, not the project folder.
    away = root.with_name("source-not-present")
    root.rename(away)
    try:
        loaded = context.load(str(ITEM), session=context.session)
    finally:
        away.rename(root)
    assert loaded.succeeded, loaded.to_mapping()
    (node,) = loaded.nodes
    assert node.result.status == "Completed" and node.result.request_id
    assert node.result.start_time and node.result.end_time
    assert not hasattr(node.result, "rows_inserted")
    (status,) = read_table(connection, LOAD_STATUS, scope=SCOPE)
    assert str(status["result"]).casefold() == "succeeded"
    assert status["workflow_id"] == loaded.workflow_id
    assert status["started_datetime"] and status["completed_datetime"]
    logs = read_table(
        connection, LOG, predicate=f"[Workflow ID] = {literal(loaded.workflow_id)}"
    )
    assert any(node.result.request_id in (row["details"] or "") for row in logs)
    assert not read_table(connection, BOOKMARK, scope=SCOPE)
    assert not read_table(connection, LOAD_STATISTIC, scope=SCOPE)
    assert _project_bytes(root) == original_sources
    tested = run_validations()
    assert _statuses(connection) == {
        "ObjectsReconcile": "succeeded",
        "ObjectsAreSigned": "succeeded",
    }
    named = weaver.test(
        str(ITEM), names="Acceptance.ObjectsReconcile", session=context.session
    )
    (node,) = named.nodes
    assert named.succeeded and list(node.diagnostics) == []
    report, sections = _health(context.session)
    assert report.is_healthy, _findings(report)
    evidence["first"] = {"test": tested.to_mapping(), "health": sections}

    # A selected model deploys again with a stable effective signature.
    unchanged = build()
    assert semantic_actions(unchanged)
    assert not unchanged.selection.impact.changed
    assert read_table(connection, SEMANTIC_MODEL, scope=SCOPE) == (definition,)
    assert _load_result(connection) == "pending"
    assert set(_statuses(connection).values()) == {"succeeded"}
    assert _project_bytes(root) == original_sources
    # Healthy again, so the change below is what makes the model unhealthy.
    assert context.load(str(ITEM), session=context.session).succeeded
    run_validations()
    assert _health(context.session)[0].is_healthy

    # A model change deploys, and a Test that passed before the reload is stale.
    extension = folder / f"{ITEM.item_name}.tmdl"
    extension.write_text(
        extension.read_text().replace(
            "    measure One = 1\n", "    measure One = 1\n\n    measure Two = 2\n"
        ),
        encoding="utf-8",
    )
    changed = build()
    assert semantic_actions(changed)
    assert _load_result(connection) == "pending"
    assert not _health(context.session)[0].is_healthy
    assert context.load(str(ITEM), session=context.session).succeeded
    stale, sections = _health(context.session)
    assert not stale.is_healthy
    assert {
        (finding.code, finding.object_id)
        for finding in stale.findings
        if finding.area == "tests"
    } == {
        ("test_stale_dependency", f"{ITEM}/Acceptance.ObjectsReconcile"),
        ("test_stale_dependency", f"{ITEM}/Acceptance.ObjectsAreSigned"),
    }
    run_validations()
    assert _health(context.session)[0].is_healthy

    # A validation edit leaves the effective model signature unchanged.
    test_path.write_text(TEST.replace(DAX_COUNT, '"Objects", [Rows]'), encoding="utf-8")
    before = context.model.get_definition()
    edited = build()
    assert semantic_actions(edited)
    assert not edited.selection.impact.changed
    assert decode_model(context.model.get_definition()) == decode_model(before)
    assert _load_result(connection) == "pending"
    assert _statuses(connection) == {
        "ObjectsReconcile": "pending",
        "ObjectsAreSigned": "succeeded",
    }
    assert context.load(str(ITEM), session=context.session).succeeded
    run_validations()
    assert _health(context.session)[0].is_healthy

    # Wipe keeping the data source, then rebuild to green.
    wiped = context.wipe(
        f"SemanticModel/{context.target}",
        preserve_data_source=True,
        session=context.session,
    )
    assert wiped.emptied == (f"SemanticModel/{context.target}",)
    for table in (*PROJECTED_TABLES, *CURRENT_STATE_TABLES):
        assert not read_table(connection, table, scope=SCOPE), table.name
    context.source.verify("wipe-rebuild")
    build()
    assert context.load(str(ITEM), session=context.session).succeeded
    context.source.verify("wipe-rebuild")
    run_validations()
    final, sections = _health(context.session)
    assert final.is_healthy, _findings(final)
    assert not {"livy", "onelake"} & {
        event.resource for event in context.session.telemetry.events()
    }
    evidence["final"] = {"health": sections}
    print(json.dumps(evidence, default=str))
