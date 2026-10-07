"""A semantic model's whole lifecycle on a fixed Fabric item, through public calls.

Build, Load, Test and Health; repeated selected-model deployment; a model change,
which makes passed Tests stale once it loads; a Test edit with stable model
signature; a wipe keeping the data source; and a rebuild back to green. The
Tests run real DAX against the model and real T-SQL against the catalogue.
"""

import json

from support.weaver_test import weaver_test
from test_semantic_annotation_public_cycle import SOURCE_TEXT
from test_semantic_model_public_cycle import ITEM, SCOPE
from test_semantic_model_public_cycle import (
    semantic_build_context as semantic_build_context,
)

import weaver
from weaver.catalogue.reader import read_table
from weaver.catalogue.tables import (
    CURRENT_STATE_TABLES,
    DEPENDENCY,
    LOAD_STATUS,
    PROJECTED_TABLES,
    REGISTRY,
    SEMANTIC_MODEL_TEST,
    TEST_DICTIONARY,
    TEST_STATUS,
)
from weaver.semantic_models.definition import decode_model
from weaver.semantic_models.wipe import connection_signature

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


def _project(root):
    folder = root / "PowerBI/Acceptance"
    (folder / "tests" / ITEM.item_name).mkdir(parents=True)
    (folder / "assumptions" / ITEM.item_name).mkdir(parents=True)
    (folder / f"{ITEM.item_name}.tmdl").write_text(
        SOURCE_TEXT.replace(
            "    annotation Weaver.Source = Warehouse/_weaver/_.TableDictionary\n",
            "    annotation Weaver.Source = Warehouse/_weaver/_.TableDictionary\n"
            "\n    column Signature\n"
            "        dataType: string\n"
            "        sourceColumn: Signature\n",
        ).replace(
            "    annotation Weaver.AutoHideForeignKeys = true\n",
            "    annotation Weaver.AutoHideForeignKeys = true\n"
            "    annotation Acceptance.HideSignatures = true\n",
            1,
        ),
        encoding="utf-8",
    )
    annotations = root / "SemanticModel/annotations"
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
    folder, test_path = _project(root)
    selection = f"{ITEM}=SemanticModel/{context.target}"
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
    assert {
        row["object_type"]
        for row in read_table(connection, REGISTRY, scope=SCOPE)
        if row["object_role"] == "data"
    } == {"semantic_model"}
    deployed = decode_model(context.model.get_definition())["model"]
    objects = next(t for t in deployed["tables"] if t["name"] == "Objects")
    assert next(c for c in objects["columns"] if c["name"] == "Signature")["isHidden"]
    connections = connection_signature(context.model.get_connections())
    assert _load_result(connection) == "pending"

    # Load, Test, Health: the first certification of the whole lifecycle.
    assert weaver.load(str(ITEM), session=context.session).succeeded
    assert _load_result(connection) == "succeeded"
    tested = run_validations()
    assert _statuses(connection) == {
        "ObjectsReconcile": "succeeded",
        "ObjectsAreSigned": "succeeded",
    }
    named = weaver.test(
        str(ITEM), name="Acceptance.ObjectsReconcile", session=context.session
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
    assert _load_result(connection) == "pending"
    assert set(_statuses(connection).values()) == {"succeeded"}
    assert weaver.load(str(ITEM), session=context.session).succeeded
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
    assert weaver.load(str(ITEM), session=context.session).succeeded
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
    assert weaver.load(str(ITEM), session=context.session).succeeded
    run_validations()
    assert _health(context.session)[0].is_healthy

    # Wipe keeping the data source, then rebuild to green.
    wiped = weaver.wipe(
        f"SemanticModel/{context.target}",
        preserve_data_source=True,
        session=context.session,
    )
    assert wiped.emptied == (f"SemanticModel/{context.target}",)
    for table in (*PROJECTED_TABLES, *CURRENT_STATE_TABLES):
        assert not read_table(connection, table, scope=SCOPE), table.name
    assert connection_signature(context.model.get_connections()) == connections
    build()
    assert weaver.load(str(ITEM), session=context.session).succeeded
    assert connection_signature(context.model.get_connections()) == connections
    run_validations()
    final, sections = _health(context.session)
    assert final.is_healthy, _findings(final)
    repeated = build()
    assert semantic_actions(repeated)
    assert not repeated.selection.impact.changed
    assert _load_result(connection) == "pending"
    assert weaver.load(str(ITEM), session=context.session).succeeded
    run_validations()
    assert _health(context.session)[0].is_healthy
    assert not {"livy", "onelake"} & {
        event.resource for event in context.session.telemetry.events()
    }
    evidence["final"] = {"health": sections}
    print(json.dumps(evidence, default=str))
