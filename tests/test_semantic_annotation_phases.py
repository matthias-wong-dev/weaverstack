"""Schema and post-schema annotation dispatch."""

import pytest
from support.weaver_test import weaver_test
from test_semantic_custom_annotation_declaration import parse, project

from weaver.errors import ConfigError
from weaver.semantic_models import Annotation


@weaver_test()
def test_parsing_does_not_execute_annotation_handlers(tmp_path):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models import TmdlDefinition

    root = project(
        tmp_path,
        "model Model\n\tannotation Acme.Stage = true\n",
        {
            "Acme__Stage": """
        from weaver.semantic_models import Annotation

        class Acme__Stage(Annotation):
            scopes = {"model"}
            phase = "schema"

            def apply(self, target):
                target.description = "executed"
        """
        },
    )
    contribution = parse(root).semantic_models[ITEM]
    assert TmdlDefinition(contribution.parts).model.description is None


@weaver_test()
def test_schema_then_post_schema_execute_once_in_phase_order(tmp_path, monkeypatch):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.annotation import apply_annotations

    root = project(
        tmp_path,
        "model Model\n\tannotation Acme.Post = true\n\tannotation Acme.Schema = true\n",
        {
            "Acme__Post": """
            from weaver.semantic_models import Annotation

            class Acme__Post(Annotation):
                scopes = {"model"}

                def apply(self, target):
                    target.description = (target.description or "") + " post"
            """,
            "Acme__Schema": """
            from weaver.semantic_models import Annotation

            class Acme__Schema(Annotation):
                scopes = {"model"}
                phase = "schema"

                def apply(self, target):
                    target.description = "schema"
            """,
        },
    )
    contribution = parse(root).semantic_models[ITEM]
    calls = []
    for name, cls in contribution.annotations.classes.items():
        if not name.startswith("Acme."):
            continue
        original = cls.apply

        def spy(self, target, original=original, name=name):
            calls.append(name)
            original(self, target)

        monkeypatch.setattr(cls, "apply", spy)
    compiled = apply_annotations(contribution)
    assert calls == ["Acme.Schema", "Acme.Post"]
    assert TmdlDefinition(compiled.parts).model.description == "schema post"


@weaver_test()
def test_annotation_defaults_to_post_schema():
    assert Annotation.phase == "post_schema"


@weaver_test()
def test_builtin_annotation_phases():
    from weaver.semantic_models.annotation import builtin_registry

    phases = {name: cls.phase for name, cls in builtin_registry().classes.items()}
    assert phases == {
        "Weaver.Source": "schema",
        "Weaver.MeasureTable": "schema",
        "Weaver.Switch": "post_schema",
        "Weaver.AutoHideColumns": "post_schema",
        "Weaver.AutoHideForeignKeys": "post_schema",
        "Weaver.Exclude": "post_schema",
    }


@weaver_test()
def test_post_schema_custom_annotation_sees_generated_source_columns(tmp_path):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.binding import bind_semantic_sources

    root = project(
        tmp_path,
        "model Model\n\tannotation Acme.Hide = true\n\n"
        "table Sales\n\tannotation Weaver.Source = Warehouse/Serving/Cake.Sales\n",
        {
            "Acme__Hide": """
        from weaver.semantic_models import Annotation

        class Acme__Hide(Annotation):
            scopes = {"model"}

            def apply(self, target):
                for table in target.tables:
                    for column in table.columns:
                        column.isHidden = True
        """
        },
    )
    repository = parse(root)
    reference = "Warehouse/Serving/Cake.Sales"
    compiled = bind_semantic_sources(
        repository,
        {
            reference: {
                "reference": reference,
                "server": "source.example",
                "database": "Serving",
                "schema": "Cake",
                "object": "Sales",
                "object_type": "table",
                "source_columns": [{"column_name": "Id", "data_type": "bigint"}],
            }
        },
        {ITEM},
    ).semantic_models[ITEM]
    column = TmdlDefinition(compiled.parts).model.tables["Sales"].columns["Id"]
    assert column.isHidden is True
    expected = next(t for t in compiled.requested["tables"] if t["name"] == "Sales")
    assert expected["columns"][0]["isHidden"] is True
    assert (
        compiled.provenance["/model/tables/Sales/columns/Id/isHidden"]["reason"]
        == "Acme.Hide"
    )


@weaver_test()
def test_schema_introduced_source_is_materialised_before_post_schema(tmp_path):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.binding import bind_semantic_sources

    root = project(
        tmp_path,
        "model Model\n\tannotation Acme.Create = true\n\tannotation Acme.Check = true\n",
        {
            "Acme__Create": """
            from weaver.semantic_models import Annotation

            class Acme__Create(Annotation):
                scopes = {"model"}
                phase = "schema"

                def apply(self, target):
                    table = target.tables.add("Sales")
                    table.annotations.add("Weaver.Source", "Warehouse/Serving/Cake.Sales")
            """,
            "Acme__Check": """
            from weaver.semantic_models import Annotation

            class Acme__Check(Annotation):
                scopes = {"model"}

                def apply(self, target):
                    target.tables["Sales"].columns["Id"].isHidden = True
            """,
        },
    )
    repository = parse(root)
    reference = "Warehouse/Serving/Cake.Sales"
    compiled = bind_semantic_sources(
        repository,
        {
            reference: {
                "reference": reference,
                "server": "source.example",
                "database": "Serving",
                "schema": "Cake",
                "object": "Sales",
                "object_type": "table",
                "source_columns": [{"column_name": "Id", "data_type": "bigint"}],
            }
        },
        {ITEM},
    ).semantic_models[ITEM]
    assert (
        TmdlDefinition(compiled.parts).model.tables["Sales"].columns["Id"].isHidden
        is True
    )
    assert compiled.source_references == {"Sales": reference}


@weaver_test()
def test_post_schema_new_source_dependency_is_refused(tmp_path):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models.binding import bind_semantic_sources

    root = project(
        tmp_path,
        "model Model\n\tannotation Acme.Late = true\n",
        {
            "Acme__Late": """
        from weaver.semantic_models import Annotation

        class Acme__Late(Annotation):
            scopes = {"model"}

            def apply(self, target):
                table = target.tables.add("Late")
                table.annotations.add("Weaver.Source", "Warehouse/Serving/Cake.Sales")
        """,
        },
    )
    with pytest.raises(ConfigError, match="Acme.Late.*post_schema.*source.*schema"):
        bind_semantic_sources(parse(root), {}, {ITEM})


@weaver_test()
def test_authored_source_partition_and_column_types_are_preserved(tmp_path):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.binding import bind_semantic_sources

    root = project(
        tmp_path,
        "table Sales\n\tannotation Weaver.Source = Warehouse/Serving/Cake.Sales\n"
        "\tcolumn Id\n\t\tdataType: string\n\t\tsourceColumn: Original\n"
        '\tpartition Sales = m\n\t\tmode: import\n\t\tsource = #table({"Original"}, {{"1"}})\n',
    )
    reference = "Warehouse/Serving/Cake.Sales"
    compiled = bind_semantic_sources(
        parse(root),
        {
            reference: {
                "reference": reference,
                "server": "source.example",
                "database": "Serving",
                "schema": "Cake",
                "object": "Sales",
                "object_type": "table",
                "source_columns": [{"column_name": "Original", "data_type": "bigint"}],
                "column_notes": {"Original": "Key"},
            }
        },
        {ITEM},
    ).semantic_models[ITEM]
    table = TmdlDefinition(compiled.parts).model.tables["Sales"]
    assert table.partitions["Sales"].source == '#table({"Original"}, {{"1"}})'
    assert table.columns["Id"].dataType == "string"
    assert table.columns["Id"].sourceColumn == "Original"
    assert table.columns["Id"].description == "Key"


@weaver_test()
def test_shared_journal_unset_removes_prior_expectation(tmp_path):
    from weaver.semantic_models.annotation import apply_annotations

    root = project(
        tmp_path,
        "table Sales\n\tannotation Acme.Set = true\n\tannotation Acme.Unset = true\n",
        {
            "Acme__Set": """
        from weaver.semantic_models import Annotation
        class Acme__Set(Annotation):
            scopes = {"table"}
            phase = "schema"
            def apply(self, target):
                target.description = "Schema description"
        """,
            "Acme__Unset": """
        from weaver.semantic_models import Annotation
        class Acme__Unset(Annotation):
            scopes = {"table"}
            def apply(self, target):
                target.description = None
        """,
        },
    )
    from test_semantic_annotation_declaration import ITEM

    compiled = apply_annotations(parse(root).semantic_models[ITEM])
    assert "description" not in compiled.requested["tables"][0]
    assert "/model/tables/Sales/description" not in compiled.provenance


@weaver_test()
def test_authored_source_lineage_needs_no_physical_metadata_resolution(tmp_path):
    from types import SimpleNamespace

    from test_semantic_annotation_declaration import ITEM

    from weaver.build_bundle.semantic_sources import read_semantic_sources
    from weaver.catalogue import Catalogue
    from weaver.semantic_models.binding import begin_semantic_sources

    reference = "Warehouse/Serving/Cake.Sales"
    root = project(
        tmp_path,
        "table Sales\n\tannotation Weaver.Source = "
        + reference
        + '\n\tpartition Sales = calculated\n\t\tmode: import\n\t\tsource = ROW("Id", 1)\n',
    )
    repository = begin_semantic_sources(parse(root), {ITEM})
    observed = read_semantic_sources(
        repository,
        SimpleNamespace(by_item={ITEM: None}),
        Catalogue({}),
        session=SimpleNamespace(),
        workspace=SimpleNamespace(),
        inventories={},
    )
    assert observed[reference]["reference"] == reference


@weaver_test()
def test_measure_table_columns_are_visible_to_default_custom_annotation(tmp_path):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.annotation import apply_annotations
    from weaver.semantic_models.builtin_annotations import MEASURE_TABLE_COLUMNS

    root = project(
        tmp_path,
        "model Model\n\tannotation Acme.Hide = true\n\n"
        "table Measures\n\tannotation Weaver.MeasureTable = true\n",
        {
            "Acme__Hide": """
        from weaver.semantic_models import Annotation
        class Acme__Hide(Annotation):
            scopes = {"model"}
            def apply(self, target):
                for column in target.tables["Measures"].columns:
                    column.isHidden = True
        """,
        },
    )
    compiled = apply_annotations(parse(root).semantic_models[ITEM])
    columns = list(TmdlDefinition(compiled.parts).model.tables["Measures"].columns)
    assert [c.name for c in columns] == [n for n, _ in MEASURE_TABLE_COLUMNS]
    assert all(c.isHidden for c in columns)
    assert all(c.sourceColumn == "[" + c.name + "]" for c in columns)
    assert all(
        c.get("type") != "data" for c in compiled.requested["tables"][0]["columns"]
    )


@pytest.mark.parametrize(
    "annotation,value", [("AutoHideColumns", "Id"), ("AutoHideForeignKeys", "true")]
)
@weaver_test()
def test_builtin_hiding_operates_on_completed_source_columns(
    tmp_path, annotation, value
):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.binding import bind_semantic_sources

    root = project(
        tmp_path,
        f"model Model\n\tannotation Weaver.{annotation} = {value}\n\n"
        "table Sales\n\tannotation Weaver.Source = Warehouse/Serving/Cake.Sales\n\n"
        "table Dimension\n\tcolumn Id\n\t\tdataType: int64\n\n"
        "relationship Sales_Dimension\n\tfromColumn: Sales.Id\n\ttoColumn: Dimension.Id\n",
    )
    reference = "Warehouse/Serving/Cake.Sales"
    compiled = bind_semantic_sources(
        parse(root),
        {
            reference: {
                "reference": reference,
                "server": "source.example",
                "database": "Serving",
                "schema": "Cake",
                "object": "Sales",
                "object_type": "table",
                "source_columns": [{"column_name": "Id", "data_type": "bigint"}],
            }
        },
        {ITEM},
    ).semantic_models[ITEM]
    assert TmdlDefinition(compiled.parts).model.tables["Sales"].columns["Id"].isHidden


@weaver_test()
def test_exclusion_after_generation_removes_lineage_and_owned_expectations(tmp_path):
    from test_semantic_annotation_declaration import ITEM

    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.binding import bind_semantic_sources

    root = project(
        tmp_path,
        "table Sales\n\tannotation Weaver.Source = Warehouse/Serving/Cake.Sales\n"
        "\tannotation Weaver.Exclude = true\n",
    )
    reference = "Warehouse/Serving/Cake.Sales"
    compiled = bind_semantic_sources(
        parse(root),
        {
            reference: {
                "reference": reference,
                "server": "source.example",
                "database": "Serving",
                "schema": "Cake",
                "object": "Sales",
                "object_type": "table",
                "source_columns": [{"column_name": "Id", "data_type": "bigint"}],
            }
        },
        {ITEM},
    ).semantic_models[ITEM]
    assert "Sales" not in TmdlDefinition(compiled.parts).model.tables
    assert not compiled.source_references and not compiled.source_bindings
    assert not any(p.startswith("/model/tables/Sales") for p in compiled.owned)
    assert not any(p.startswith("/model/tables/Sales") for p in compiled.provenance)
    assert not compiled.requested.get("tables")
    assert (("table", "Sales"),) in compiled.absent


@pytest.mark.parametrize("phase", ["before", "SCHEMA", "", None, 1])
@weaver_test()
def test_invalid_annotation_phase_fails_registration(tmp_path, phase):
    root = project(
        tmp_path,
        "table Sales\n",
        {
            "Acme__Phase": f"""
            from weaver.semantic_models import Annotation

            class Acme__Phase(Annotation):
                scopes = {{"table"}}
                phase = {phase!r}

                def apply(self, target):
                    pass
            """,
        },
    )
    with pytest.raises(ConfigError, match="Acme__Phase.*phase.*schema.*post_schema"):
        parse(root)
