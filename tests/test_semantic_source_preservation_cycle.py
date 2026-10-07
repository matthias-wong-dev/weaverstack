"""Logical sources enrich native tables without owning their data definitions."""

import copy

import pytest
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM, source_project
from test_semantic_source_build_cycle import (
    SubmittedDefinition,
    answer_catalogue,
    capture_publication,
    read_bindings,
    source_catalogue,
    source_session,
    submitted_parts,
)

import weaver
from weaver.semantic_models import TmdlDefinition


@pytest.mark.parametrize("mapped", [False, True])
@weaver_test()
def test_public_source_build_preserves_authored_data_and_enriches_native_mappings(
    tmp_path, monkeypatch, mapped
):
    root = source_project(tmp_path)
    path = root / str(ITEM) / "Reporting.tmdl"
    partition = '\tpartition Native = m\n\t\tmode: dual\n\t\tsource = Table.FirstN(#table({"Id"}, {{1}}), 1)\n'
    path.write_text(
        path.read_text()
        + "\tcolumn DisplayId\n\t\tdataType: string\n\t\tsourceColumn: Id\n"
        + "\t/// Authored label\n\tcolumn Label\n\t\tdataType: string\n\t\tsourceColumn: Label\n"
        + "\tcolumn Retired\n\t\tdataType: int64\n\t\tsourceColumn: NoLongerUpstream\n"
        + partition
    )
    if not mapped:
        path.write_text(
            path.read_text()
            .replace("DisplayId", "Id")
            .replace("\t\tsourceColumn: Id\n", "")
        )
    before = path.read_bytes()
    observed = {
        "compatibilityLevel": 1606,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "tables": [
                {
                    "name": "Sales",
                    "description": "Sales description",
                    "annotations": [
                        {
                            "name": "Weaver.Source",
                            "value": "Warehouse/Serving/Cake.Sales",
                        }
                    ],
                    "columns": [
                        {
                            "name": "DisplayId",
                            "dataType": "string",
                            "sourceColumn": "Id",
                            "description": "Sales key",
                        },
                        {
                            "name": "Label",
                            "dataType": "string",
                            "sourceColumn": "Label",
                            "description": "Authored label",
                        },
                        {
                            "name": "Retired",
                            "dataType": "int64",
                            "sourceColumn": "NoLongerUpstream",
                        },
                    ],
                    "partitions": [
                        {
                            "name": "Native",
                            "mode": "dual",
                            "source": {
                                "type": "m",
                                "expression": 'Table.FirstN(#table({"Id"}, {{1}}), 1)',
                            },
                        }
                    ],
                }
            ],
        },
    }
    if not mapped:
        column = observed["model"]["tables"][0]["columns"][0]
        column["name"] = "Id"
        del column["sourceColumn"]
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        parts = submitted_parts(session)
        assert partition.encode() in parts["definition/tables/Sales.tmdl"]
        table = TmdlDefinition(parts).model.tables["Sales"]
        assert [
            (c.name, c.dataType, c.sourceColumn, c.description) for c in table.columns
        ] == [
            (
                "DisplayId" if mapped else "Id",
                "string",
                "Id" if mapped else None,
                "Sales key",
            ),
            ("Label", "string", "Label", "Authored label"),
            ("Retired", "int64", "NoLongerUpstream", None),
        ]
        assert table.description == "Sales description"
        rows = published()[ITEM]
        assert {
            (r["referencing_object_name"], r["dependency_reference"])
            for r in rows["Dependency"]
        } == {("Sales", "Warehouse/Serving/Cake.Sales")}
        assert path.read_bytes() == before
        assert not any(
            "Cake" in sql and "INFORMATION_SCHEMA.COLUMNS" in sql
            for sql in session.tsql
        )
        assert not any(
            "connectionString" in path
            for path in session.resolver(session.workspace).client.requested
        )


@pytest.mark.parametrize("catalogued", [False, True])
@weaver_test()
def test_authored_source_build_preserves_native_definition_without_catalogue_metadata(
    tmp_path, catalogued
):
    from weaver.catalogue.state import Catalogue
    from weaver.workspaces import Workspace

    root = source_project(tmp_path)
    path = root / str(ITEM) / "Reporting.tmdl"
    partition = '\tpartition Native = calculated\n\t\tsource = ROW("Value", 1)\n'
    path.write_text("/// Authored table\n" + path.read_text() + partition)
    observed = {
        "compatibilityLevel": 1606,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "tables": [
                {
                    "name": "Sales",
                    "description": "Authored table",
                    "annotations": [
                        {
                            "name": "Weaver.Source",
                            "value": "Warehouse/Serving/Cake.Sales",
                        }
                    ],
                    "partitions": [
                        {
                            "name": "Native",
                            "source": {
                                "type": "calculated",
                                "expression": 'ROW("Value", 1)',
                            },
                        }
                    ],
                    "columns": [
                        {
                            "name": "Value",
                            "type": "calculatedTableColumn",
                            "dataType": "int64",
                            "sourceColumn": "[Value]",
                        }
                    ],
                }
            ],
        },
    }
    workspace = Workspace(
        workspace="Demo", catalogue="Warehouse/Catalogue" if catalogued else None
    )
    with source_session(workspace=workspace) as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        if catalogued:
            answer_catalogue(session, Catalogue({}), read_bindings())
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        parts = submitted_parts(session)
        assert partition.encode() in parts["definition/tables/Sales.tmdl"]
        assert b"column" not in parts["definition/tables/Sales.tmdl"]
        assert (
            TmdlDefinition(parts).model.tables["Sales"].description == "Authored table"
        )
        assert b"Warehouse/Serving/Cake.Sales" in parts["definition/tables/Sales.tmdl"]


@weaver_test()
def test_public_build_executes_annotations_once_before_source_generation(
    tmp_path, monkeypatch
):
    import builtins

    from support.semantic_models import source_model

    from weaver.semantic_models.builtin_annotations import (
        Weaver__AutoHideColumns,
        Weaver__Source,
    )

    root = source_project(tmp_path)
    path = root / str(ITEM) / "Reporting.tmdl"
    path.write_text(
        "model Model\n\tannotation ACME.Columns = true\n\tannotation Weaver.AutoHideColumns = Id\n\n"
        + path.read_text()
    )
    custom = root / "SemanticModel/annotations/ACME__Columns.py"
    custom.parent.mkdir(parents=True)
    custom.write_text(
        'from weaver.semantic_models import Annotation\nimport builtins\nclass ACME__Columns(Annotation):\n    scopes = {"model"}\n    def apply(self, target):\n        builtins._stage_a_annotations.append(("custom", tuple(c.name for c in target.tables["Sales"].columns)))\n'
    )
    calls = []
    monkeypatch.setattr(builtins, "_stage_a_annotations", calls, raising=False)
    for cls in (Weaver__Source, Weaver__AutoHideColumns):
        original = cls.apply

        def tracked(self, target, original=original, name=cls.__name__):
            calls.append((name, ()))
            return original(self, target)

        monkeypatch.setattr(cls, "apply", tracked)
    observed = source_model(relations={"Sales": "Sales"})
    observed["model"]["annotations"] = [
        {"name": "ACME.Columns", "value": "true"},
        {"name": "Weaver.AutoHideColumns", "value": "Id"},
    ]
    observed["model"]["tables"][0]["annotations"] = [
        {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
    ]
    observed["model"]["tables"][0]["columns"][0]["isHidden"] = True
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
    assert calls == [
        ("custom", ()),
        ("Weaver__AutoHideColumns", ()),
        ("Weaver__Source", ()),
    ]


@pytest.mark.parametrize("change", ["column", "description"])
@weaver_test()
def test_public_selected_build_reads_upstream_changes_with_immutable_semantic_sources(
    tmp_path, monkeypatch, change
):
    from support.semantic_models import source_model

    from weaver.catalogue.state import Catalogue

    root = source_project(tmp_path)
    original = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    signatures, definitions = [], []
    for updated in (False, True):
        source = source_catalogue()
        observed = source_model(relations={"Sales": "Sales"})
        table = observed["model"]["tables"][0]
        table["annotations"] = [
            {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
        ]
        rows = copy.deepcopy(dict(source.rows))
        if updated and change == "description":
            table["description"] = "Changed upstream description"
            rows[next(iter(rows))]["TableDictionary"][0]["description"] = table[
                "description"
            ]
        with source_session() as session:
            if updated and change == "column":
                session.source_columns = [
                    *session.source_columns,
                    {"column_name": "C", "data_type": "int"},
                ]
                table["columns"].append(
                    {"name": "C", "dataType": "int64", "sourceColumn": "C"}
                )
            session.answer_semantic_model(
                "Demo", "Reporting_Dev", SubmittedDefinition(observed)
            )
            answer_catalogue(session, Catalogue(rows), read_bindings())
            published = capture_publication(monkeypatch, session)
            result = weaver.build(
                root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
            )
            assert result.succeeded, result.errors
            definitions.append(submitted_parts(session))
            certified = published()[ITEM]
            signature = certified["SemanticModel"][0]["signature"]
            signatures.append(signature)
            assert all(
                r["signature"] == signature
                for r in certified["Registry"]
                if r["object_role"] == "data"
            )
    assert signatures[0] != signatures[1]
    assert definitions[0] != definitions[1]
    assert {p: p.read_bytes() for p in original} == original
    if change == "column":
        assert b"column 'C'" in definitions[1]["definition/tables/Sales.tmdl"]
    else:
        assert (
            b"Changed upstream description"
            in definitions[1]["definition/tables/Sales.tmdl"]
        )
