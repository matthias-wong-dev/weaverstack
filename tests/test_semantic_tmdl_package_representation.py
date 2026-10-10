"""PBIP definition bytes remain the desired semantic Build representation."""

import shutil
from pathlib import Path

import pytest
from support.semantic_models import policy_path
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location

ITEM = WeaverItemId.parse("SemanticModel/Reporting")
FIXTURE = Path(__file__).parent / "fixtures/semantic_model/Probe"


def pbip_project(tmp_path):
    folder = tmp_path / str(ITEM)
    shutil.copytree(FIXTURE, folder)
    return folder, folder / "Probe.SemanticModel"


@weaver_test()
def test_unknown_tmdl_passes_through_as_original_definition_bytes(tmp_path):
    folder, model = pbip_project(tmp_path)
    opaque = model / "definition/cultures/en-US.tmdl"
    opaque.parent.mkdir()
    opaque.write_bytes(
        b"culture en-US\r\n\tlinguisticMetadata = ```\r\n"
        b'\t\t{"Version":"1.0.0","FutureProperty":{"opaque":true}}\r\n'
        b"\t\t```\r\n"
    )
    table = model / "definition/tables/Sales.tmdl"
    table.write_bytes(table.read_bytes() + b"\n\tunknownFutureProperty: retained\n")
    before = {
        p.relative_to(model).as_posix(): p.read_bytes()
        for p in model.rglob("*")
        if p.is_file()
    }
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    contribution = repository.semantic_models[ITEM]
    assert dict(contribution.parts) == before
    assert not hasattr(contribution, "model")
    assert contribution.requested == {}
    assert before == {
        p.relative_to(model).as_posix(): p.read_bytes()
        for p in model.rglob("*")
        if p.is_file()
    }
    repeated = parse_item_repository(Location(tmp_path.as_posix())).semantic_models[
        ITEM
    ]
    assert repeated.signature == contribution.signature
    opaque.write_bytes(opaque.read_bytes().replace(b"true", b"false"))
    changed = parse_item_repository(Location(tmp_path.as_posix())).semantic_models[ITEM]
    assert changed.signature != contribution.signature


@weaver_test()
def test_model_extension_changes_only_requested_property_and_local_wins(tmp_path):
    folder, model = pbip_project(tmp_path)
    path = model / "definition/model.tmdl"
    original = path.read_bytes() + b"\n\tunknownFutureProperty: preserve-me\n"
    path.write_bytes(original)
    untouched = {
        p.relative_to(model).as_posix(): p.read_bytes()
        for p in model.rglob("*.tmdl")
        if p != path
    }
    (policy_path(folder.parent.parent)).write_text(
        "model Model\n\tculture: en-AU\n", encoding="utf-8"
    )
    (folder / f"{folder.name}.tmdl").write_text(
        "model Model\n\tculture: en-GB\n", encoding="utf-8"
    )
    c = parse_item_repository(Location(tmp_path.as_posix())).semantic_models[ITEM]
    assert c.parts["definition/model.tmdl"] == original.replace(
        b"culture: en-US", b"culture: en-GB"
    )
    assert {p: c.parts[p] for p in untouched} == untouched
    assert c.requested == {"culture": "en-GB"}
    assert (
        c.provenance["/model/culture"]["source"]
        == str(ITEM) + f"/{ITEM.item_name}.tmdl"
    )
    (policy_path(folder.parent.parent)).write_text(
        "model Model\n\tculture: fr-FR\n", encoding="utf-8"
    )
    assert (
        parse_item_repository(Location(tmp_path.as_posix()))
        .semantic_models[ITEM]
        .signature
        == c.signature
    )
    assert path.read_bytes() == original


@weaver_test()
def test_column_patch_preserves_unknown_neighbours_and_expression_text(tmp_path):
    folder, model = pbip_project(tmp_path)
    path = model / "definition/tables/Sales.tmdl"
    original = (
        "table Sales\r\n"
        "\tcolumn 'Product''s ID'\r\n"
        "\t\tdataType: int64\r\n"
        "\t\tunknownColumnProperty: untouched\r\n\r\n"
        "\tmeasure Notes = ```\r\n"
        "column 'Product''s ID'\r\n"
        "  isHidden: false \t\r\n"
        "\t\t```\r\n"
        "\t\tunknownMeasureProperty: untouched\r\n"
    ).encode()
    path.write_bytes(original)
    (folder / f"{folder.name}.tmdl").write_text(
        "ref table Sales\n\tcolumn 'Product''s ID'\n\t\tisHidden: true\n",
        encoding="utf-8",
    )
    c = parse_item_repository(Location(tmp_path.as_posix())).semantic_models[ITEM]
    assert c.parts["definition/tables/Sales.tmdl"] == original.replace(
        b"\t\tunknownColumnProperty: untouched\r\n",
        b"\t\tunknownColumnProperty: untouched\r\n\t\tisHidden: true\r\n",
        1,
    )
    assert path.read_bytes() == original
    assert c.requested["tables"] == [
        {"name": "Sales", "columns": [{"name": "Product's ID", "isHidden": True}]}
    ]


@pytest.mark.parametrize("pbip", [False, True])
@weaver_test()
def test_native_calculated_table_uses_the_same_tmdl_package(tmp_path, pbip):
    if pbip:
        folder, model = pbip_project(tmp_path)
        before = {
            p.relative_to(model).as_posix(): p.read_bytes()
            for p in model.rglob("*")
            if p.is_file()
        }
    else:
        folder = tmp_path / str(ITEM)
        folder.mkdir(parents=True)
        before = {}
    (folder / f"{folder.name}.tmdl").write_text(
        "/// Model measures\ntable '_Measure'\n\tpartition '_Measure' = calculated\n\t\tsource = INFO.VIEW.MEASURES()\n",
        encoding="utf-8",
    )
    c = parse_item_repository(Location(tmp_path.as_posix())).semantic_models[ITEM]
    assert {p: c.parts[p] for p in before} == before
    assert "definition.pbism" in c.parts and "definition/model.tmdl" in c.parts
    generated = c.parts["definition/tables/_Measure.tmdl"].decode()
    assert "/// Model measures" in generated
    assert "table '_Measure'" in generated
    assert "partition '_Measure' = calculated" in generated
    assert "INFO.VIEW.MEASURES()" in generated
    assert "model.bim" not in c.parts
    assert not hasattr(c, "model")
    (table,) = c.requested["tables"]
    assert table["partitions"][0]["source"]["expression"] == "INFO.VIEW.MEASURES()"
    assert (
        c.provenance["/model/tables/_Measure/partitions/_Measure/source/expression"][
            "reason"
        ]
        == "extension"
    )


@weaver_test()
def test_public_build_sends_opaque_tmdl_and_projects_only_observed_tmsl(tmp_path):
    import base64

    from test_semantic_model_build_cycle import session_for

    import weaver
    from weaver.semantic_models.definition import encode_definition

    folder, model = pbip_project(tmp_path)
    opaque = model / "definition/perspectives/Reporting.tmdl"
    opaque.parent.mkdir()
    opaque.write_bytes(b"perspective Reporting\n\tperspectiveTable Sales\n")
    expected = {
        p.relative_to(model).as_posix(): p.read_bytes()
        for p in model.rglob("*")
        if p.is_file()
    }
    session = session_for()
    client = session.semantic_model("Reporting_Dev")
    client.definition = encode_definition(
        {
            "model": {
                "culture": "en-US",
                "tables": [
                    {
                        "name": "Sales",
                        "description": "Observed description",
                        "columns": [{"name": "ProductId", "dataType": "int64"}],
                    }
                ],
                "perspectives": [{"name": "Reporting"}],
            }
        }
    )
    result = weaver.build(
        tmp_path, items=str(ITEM) + "=SemanticModel/Reporting_Dev", session=session
    )
    assert result.succeeded, result.errors
    (submitted,) = [
        value for kind, value in client.calls if kind == "update_definition"
    ]
    assert submitted["allow_purge_data"] is True
    assert submitted["definition"]["format"] == "TMDL"
    assert {
        p["path"]: base64.b64decode(p["payload"])
        for p in submitted["definition"]["parts"]
    } == expected
    assert not session.spark_sql and not session.python
    writes = session.tsql
    observed_write = next(
        i
        for i, s in enumerate(writes)
        if "MERGE" in s and "[_].[SemanticModelTable]" in s
    )
    certify = next(
        i
        for i, s in enumerate(writes)
        if "MERGE" in s and "[_].[Registry]" in s and "Reporting" in s
    )
    assert "Observed description" in writes[observed_write]
    assert observed_write < certify


@weaver_test()
def test_shared_source_binding_preserves_partition_bytes_and_mode(tmp_path):
    from weaver.semantic_models.expressions import bind_expression_sources

    folder, model = pbip_project(tmp_path)
    sales_path = model / "definition/tables/Sales.tmdl"
    original = (
        "table Sales\n\tunknownTableProperty: untouched\n"
        "\tcolumn ProductId\n\t\tdataType: int64\n\t\tsourceColumn: Id\n"
        "\t\tunknownColumnProperty: untouched\n"
        "\tpartition Sales = m\n\t\tmode: directQuery\n"
        '\t\tsource = #"Warehouse/Serving"{[Schema="Cake", Item="Sales"]}[Data]\n'
        "\tmeasure Revenue = SUM(Sales[Amount])\n\t\tunknownMeasureProperty: untouched\n"
    ).encode()
    sales_path.write_bytes(original)
    (model / "definition/expressions.tmdl").write_text(
        'expression \'Warehouse/Serving\' = Sql.Database("old-server", "old-database")\n'
    )
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    before = dict(repository.semantic_models[ITEM].parts)
    observation = {
        "server": "new-server",
        "database": "new-database",
        "connector": "sql",
        "relations": [
            {
                "schema": "Cake",
                "object": "Sales",
                "reference": "Warehouse/Serving/Cake.Sales",
                "object_type": "table",
            }
        ],
    }
    bound = bind_expression_sources(
        repository, {str(ITEM): {"Warehouse/Serving": observation}}
    ).semantic_models[ITEM]
    assert bound.parts["definition/tables/Sales.tmdl"] == original
    assert {
        p: v for p, v in bound.parts.items() if p != "definition/expressions.tmdl"
    } == {p: v for p, v in before.items() if p != "definition/expressions.tmdl"}
    assert (
        b'Sql.Database("new-server", "new-database")'
        in bound.parts["definition/expressions.tmdl"]
    )
    assert sales_path.read_bytes() == original
    assert bound.source_bindings["Sales"]["mode"] == "directQuery"
    assert bound.source_bindings["Sales"]["reference"] == "Warehouse/Serving/Cake.Sales"
    assert bound.signature != repository.semantic_models[ITEM].signature
