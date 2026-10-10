"""Lineage recognises common Desktop M shapes and leaves the rest untraced."""

import pytest
from support.weaver_test import weaver_test

from weaver.semantic_models.lineage import source_relation

NAVIGATION = 'Source{[Schema="Ref",Item="Country"]}[Data]'
TRACED = ("Warehouse/Serving", "Ref", "Country")


def partition(expression):
    return {"type": "m", "expression": expression}


@pytest.mark.parametrize(
    "expression",
    [
        '#"Warehouse/Serving"{[Schema="Ref",Item="Country"]}[Data]',
        'let\n    Source = #"Warehouse/Serving",\n'
        f"    Data = {NAVIGATION}\nin\n    Data",
        # Comments and irregular whitespace.
        'let\r\n  // shared source\r\n  Source  =  #"Warehouse/Serving" , /* step */\r\n'
        '\tData = Source { [ Schema = "Ref" , Item = "Country" ] } [Data]\r\nin Data // end',
        # Desktop's quoted step names.
        'let\n    Source = #"Warehouse/Serving",\n'
        f'    #"Navigation 1" = {NAVIGATION}\nin\n    #"Navigation 1"',
        # Renamed steps, field order and an alias.
        'let\n    Serving = #"Warehouse/Serving",\n    Source = Serving,\n'
        '    Country = Source{[Item="Country",Schema="Ref"]}[Data]\nin\n    Country',
        # Table shaping after navigation.
        'let\n    Source = #"Warehouse/Serving",\n'
        f"    Data = {NAVIGATION},\n"
        '    #"Removed Other Columns" = Table.SelectColumns(Data,{"Code", "Name"}),\n'
        '    #"Changed Type" = Table.TransformColumnTypes(#"Removed Other Columns",'
        '{{"Code", type text}, {"Id", Int64.Type}}),\n'
        '    Kept = Table.SelectRows(#"Changed Type", each [Country code] <> null)\n'
        "in\n    Kept",
    ],
    ids=["direct", "let", "comments", "navigation-step", "renamed", "select-columns"],
)
@weaver_test()
def test_desktop_variations_of_one_navigation_are_traced(expression):
    assert source_relation(partition(expression)) == TRACED
    assert source_relation(partition(expression.split("\n"))) == TRACED


@pytest.mark.parametrize(
    "expression",
    [
        # A second source joined in.
        'let\n    Source = #"Warehouse/Serving",\n'
        f"    Data = {NAVIGATION},\n"
        '    Other = Source{[Schema="Ref",Item="Region"]}[Data],\n'
        '    Merged = Table.NestedJoin(Data, {"Id"}, Other, {"Id"}, "R", JoinKind.LeftOuter)\n'
        "in\n    Merged",
        # Another query named in a transform.
        'let\n    Source = #"Warehouse/Serving",\n'
        f"    Data = {NAVIGATION},\n"
        '    Kept = Table.SelectRows(Data, each List.Contains(#"Allowed", [Id]))\n'
        "in\n    Kept",
        # A native query.
        'Value.NativeQuery(#"Warehouse/Serving", "SELECT * FROM Ref.Country")',
        # A direct connection rather than a shared expression.
        'let\n    Source = Sql.Database("serving.example", "Serving", '
        "[CommandTimeout=#duration(0, 0, 20, 0)]),\n"
        f"    Data = {NAVIGATION}\nin\n    Data",
        # A navigation record with more fields.
        'let\n    Source = #"Warehouse/Serving",\n'
        '    Data = Source{[Schema="Ref",Item="Country",Kind="Table"]}[Data]\n'
        "in\n    Data",
        'let in , = {[ "unterminated',
        "/* unterminated",
        "",
    ],
    ids=[
        "join",
        "other-query",
        "native-query",
        "inline-database",
        "extra-field",
        "malformed",
        "comment",
        "empty",
    ],
)
@weaver_test()
def test_ambiguous_or_unfamiliar_m_stays_untraced(expression):
    assert source_relation(partition(expression)) is None


@weaver_test()
def test_recognition_never_raises_on_arbitrary_text():
    import random

    from weaver.semantic_models.m_source import relation

    pieces = list('letin =,{}[]()"#/*\n.') + ["let ", " in ", "Source", '#"', "//"]
    chance = random.Random(7)
    for _ in range(5000):
        relation("".join(chance.choice(pieces) for _ in range(chance.randint(0, 30))))


@pytest.mark.parametrize(
    "authored, expected",
    [
        (
            'Sql.Database("old.example", "Old", [CommandTimeout=#duration(0, 0, 20, 0)])',
            'Sql.Database("new.example", "New", [CommandTimeout=#duration(0, 0, 20, 0)])',
        ),
        ('Sql.Database("old.example", "Old")', 'Sql.Database("new.example", "New")'),
        (
            'let Source = Sql.Database("old.example", "Old") in Source',
            'Sql.Database("new.example", "New")',
        ),
    ],
    ids=["options", "plain", "let"],
)
@weaver_test()
def test_a_mapped_sql_expression_keeps_its_options_record(authored, expected):
    from weaver.semantic_models.expressions import _sql_database

    assert _sql_database(authored, "new.example", "New") == expected


def lineage_contribution():
    from types import SimpleNamespace

    reference = "Warehouse/Serving/Ref.Country"
    return SimpleNamespace(
        expression_sources={
            "Warehouse/Serving": {
                "connector": "sql",
                "relations": [
                    {
                        "schema": "Ref",
                        "object": "Country",
                        "reference": reference,
                        "object_type": "table",
                    }
                ],
            }
        },
        source_bindings={
            "Country": {
                "expression": "Warehouse/Serving",
                "schema": "Ref",
                "object": "Country",
                "reference": reference,
                "mode": "import",
                "access": "sql",
            }
        },
        source_references={},
        signature="signature",
        table_names=("Country",),
        artifact_signatures={},
    )


def deployed(expression):
    return {
        "model": {
            "tables": [
                {
                    "name": "Country",
                    "partitions": [
                        {
                            "name": "Country",
                            "mode": "import",
                            "source": partition(expression),
                        }
                    ],
                }
            ]
        }
    }


@weaver_test()
def test_fabric_rewriting_traced_m_is_a_difference_and_lineage_stands():
    from weaver.catalogue.semantic import project_semantic_model
    from weaver.declaration.model import WeaverItemId
    from weaver.semantic_models.lineage import verify_lineage

    contribution = lineage_contribution()
    traced = deployed(
        'let\n    Source = #"Warehouse/Serving",\n'
        f"    Data = {NAVIGATION}\nin\n    Data"
    )
    assert verify_lineage(contribution, traced) == ()
    rewritten = deployed(
        'Value.Buffer(#"Warehouse/Serving"{[Schema="Ref",Item="Country"]}[Data])'
    )
    assert verify_lineage(contribution, rewritten) == (
        "/model/tables/Country/partitions",
    )
    rows = project_semantic_model(
        WeaverItemId.parse("SemanticModel/Reporting"), contribution, deployed=rewritten
    )
    (dependency,) = rows["Dependency"]
    assert dependency["referencing_object_name"] == "Country"
    assert dependency["dependency_reference"] == "Warehouse/Serving/Ref.Country"
    (table,) = rows["SemanticModelTable"]
    assert (table["source_mode"], table["source_access"]) == ("import", "sql")


@pytest.mark.parametrize(
    "expression, reads",
    [
        ("#table(type table [Column1 = text], {})", False),
        ('let\n    Source = #table({"Id"}, {{1}, {2}})\nin\n    Source', False),
        ('Table.FromRows({{1, "Cake"}}, {"Id", "Name"})', False),
        ('Sql.Database("server", "Serving")', True),
        ('Web.Contents("https://example.com/sales.csv")', True),
        ('Value.NativeQuery(#"Warehouse/Serving", "SELECT 1")', True),
        ("let\n    Source = Serving\nin\n    Source", True),
        ('Unknown.Function("x")', True),
    ],
)
@weaver_test()
def test_only_m_that_can_reach_outside_the_model_reads_data(expression, reads):
    from weaver.semantic_models.m_source import reads_data

    assert reads_data(expression, shared=("Serving", "Warehouse/Serving")) is reads
