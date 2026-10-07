import pytest
from support.weaver_test import weaver_test
from test_powerbi_project_declaration import native, parse, write

from weaver.declaration.model import WeaverItemId
from weaver.errors import ConfigError


def model(name):
    return WeaverItemId("SemanticModel", name)


@weaver_test()
def test_project_discovers_named_native_pair_and_separate_standalone(tmp_path):
    native(tmp_path, model="Normal", report="Normal")
    write(tmp_path, "PowerBI/Sales/Normal.tmdl", "model Model\n\tculture: en-GB\n")
    write(tmp_path, "PowerBI/Sales/Public.tmdl", "model Model\n\tculture: en-AU\n")
    repository = parse(tmp_path)
    assert set(repository.semantic_models) == {model("Normal"), model("Public")}
    assert repository.semantic_models[model("Normal")].requested["culture"] == "en-GB"
    assert repository.semantic_models[model("Public")].requested["culture"] == "en-AU"
    assert set(repository.powerbi_projects["Sales"].items) == {
        model("Normal"),
        model("Public"),
        WeaverItemId("Report", "Normal"),
    }


@weaver_test()
def test_recursive_local_definition_composes_before_one_policy_and_target(tmp_path):
    from weaver.semantic_models.objects import TmdlDefinition

    write(tmp_path, "PowerBI/policy.tmdl", "model Model\n\tculture: en-AU\n")
    write(
        tmp_path,
        "PowerBI/Sales/Base.tmdl",
        "model Model\n\tculture: en-US\n\ntable Zebra\n\tmeasure Amount = 1\n\ntable Alpha\n",
    )
    write(
        tmp_path,
        "PowerBI/Sales/Finance.tmdl",
        "model Model\n\tannotation Weaver.BaseSemanticModels = Base\n\tculture: en-GB\n\ntable Zebra\n\tmeasure Amount = 2\n",
    )
    write(
        tmp_path,
        "PowerBI/Sales/Executive.tmdl",
        "model Model\n\tannotation Weaver.BaseSemanticModels = Finance\n\ntable Middle\n",
    )
    repository = parse(tmp_path)
    effective = repository.semantic_models[model("Executive")]
    assert effective.table_names == ("Zebra", "Alpha", "Middle")
    definition = TmdlDefinition(effective.parts).model
    assert definition.culture == "en-AU"
    assert definition.tables["Zebra"].measures["Amount"].expression == "2"
    write(
        tmp_path,
        "PowerBI/Sales/Executive.tmdl",
        "model Model\n\tannotation Weaver.BaseSemanticModels = Finance\n\tculture: en-NZ\n\ntable Middle\n",
    )
    assert (
        TmdlDefinition(
            parse(tmp_path).semantic_models[model("Executive")].parts
        ).model.culture
        == "en-NZ"
    )


@pytest.mark.parametrize(
    "declarations,diagnostic",
    [
        ({"Executive": ["Missing"]}, "SemanticModel/Executive.*Missing.*PowerBI/Sales"),
        ({"Executive": ["Executive"]}, "self-reference.*Executive"),
        (
            {"Executive": ["Finance"], "Finance": ["Base"], "Base": ["Executive"]},
            "composition cycle.*Executive.*Finance.*Base.*Executive|composition cycle.*Base.*Executive.*Finance.*Base",
        ),
        ({"Executive": ["Base", "Base"], "Base": []}, "repeated.*Base.*Executive"),
        (
            {
                "Executive": ["Finance", "Public"],
                "Finance": ["Base"],
                "Public": ["Base"],
                "Base": [],
            },
            "repeated.*Base.*Executive",
        ),
    ],
)
@weaver_test()
def test_invalid_local_composition_has_actionable_scope_and_path(
    tmp_path, declarations, diagnostic
):
    for name, bases in declarations.items():
        annotation = (
            "\tannotation Weaver.BaseSemanticModels = ```\n"
            + "".join(f"\t\t{base}\n" for base in bases)
            + "\t\t```\n"
            if bases
            else ""
        )
        write(tmp_path, f"PowerBI/Sales/{name}.tmdl", "model Model\n" + annotation)
    write(tmp_path, "PowerBI/Finance/Missing.tmdl", "model Model\n")
    with pytest.raises(ConfigError, match=diagnostic):
        parse(tmp_path)


@weaver_test()
def test_two_native_bases_keep_distinct_artifact_signatures_and_order(tmp_path):
    from weaver.catalogue.semantic import project_semantic_model
    from weaver.semantic_models.binding import bind_semantic_sources
    from weaver.semantic_models.objects import TmdlDefinition

    for name, value in (("Normal", 1), ("Shared Finance, EU", 2)):
        native(tmp_path, model=name, report=name)
        write(
            tmp_path,
            f"PowerBI/Sales/{name}.SemanticModel/definition/tables/Common.tmdl",
            f"table Common\n\tmeasure Amount = {value}\n",
        )
        write(
            tmp_path,
            f"PowerBI/Sales/{name}.tmdl",
            f"table Common\n\tmeasure Amount\n\t\tformatString: {name}\n",
        )
    write(tmp_path, "PowerBI/policy.tmdl", "model Model\n\tculture: en-AU\n")
    target = "PowerBI/Sales/Executive.tmdl"

    def effective(bases):
        write(
            tmp_path,
            target,
            "model Model\n\tannotation Weaver.BaseSemanticModels = ```\n"
            + "".join(f"\t\t{name}\n" for name in bases)
            + "\t\t```\n\ntable Middle\n",
        )
        repository = bind_semantic_sources(
            parse(tmp_path), {}, {model("Executive"): None}
        )
        return repository.semantic_models[model("Executive")]

    first = effective(["Normal", "Shared Finance, EU"])
    second = effective(["Shared Finance, EU", "Normal"])
    assert (
        TmdlDefinition(first.parts).model.tables["Common"].measures["Amount"].expression
        == "2"
    )
    assert (
        TmdlDefinition(second.parts)
        .model.tables["Common"]
        .measures["Amount"]
        .expression
        == "1"
    )
    assert first.signature != second.signature
    artifacts = first.artifact_signatures
    assert set(artifacts) == {
        ("Definition", "Normal.SemanticModel"),
        ("Definition", "Shared Finance, EU.SemanticModel"),
        ("Definition", "Normal.tmdl"),
        ("Definition", "Shared Finance, EU.tmdl"),
        ("Definition", "Executive.tmdl"),
        ("Policy", "policy.tmdl"),
    }
    assert (
        artifacts[("Definition", "Normal.SemanticModel")]
        != artifacts[("Definition", "Shared Finance, EU.SemanticModel")]
    )
    rows = project_semantic_model(
        model("Executive"),
        first,
        deployed={"model": {"tables": [{"name": n} for n in first.table_names]}},
    )
    assert {
        (r["schema_name"], r["object_name"])
        for r in rows["Registry"]
        if r["object_role"] == "source"
    } == set(artifacts)


@weaver_test()
def test_target_refs_resolve_after_raw_bases_are_composed(tmp_path):
    write(tmp_path, "PowerBI/Sales/Normal.tmdl", "table Zebra\n\tmeasure Amount = 1\n")
    write(
        tmp_path,
        "PowerBI/Sales/Executive.tmdl",
        "model Model\n\tannotation Weaver.BaseSemanticModels = Normal\n\nref table Zebra\n\tmeasure Amount\n\t\tformatString: #,##0\n",
    )
    effective = parse(tmp_path).semantic_models[model("Executive")]
    assert effective.table_names == ("Zebra",)
    assert b"formatString: #,##0" in effective.parts["definition/tables/Zebra.tmdl"]


@weaver_test()
def test_native_refs_merge_after_new_objects_with_final_composed_order(tmp_path):
    for name, names in (
        ("Normal", ["Zebra", "Alpha"]),
        ("Finance", ["Orange", "Blue"]),
    ):
        native(tmp_path, model=name, report=name)
        prefix = f"PowerBI/Sales/{name}.SemanticModel/"
        write(
            tmp_path,
            prefix + "definition.pbism",
            '{"version":"4.2","settings":{"marker":"' + name + '"}}',
        )
        write(
            tmp_path,
            prefix + "definition/model.tmdl",
            "model Model\n" + "".join(f"\tref table {n}\n" for n in names),
        )
        for table in names:
            write(
                tmp_path, prefix + f"definition/tables/{table}.tmdl", f"table {table}\n"
            )
    write(
        tmp_path,
        "PowerBI/Sales/Executive.tmdl",
        "model Model\n\tannotation Weaver.BaseSemanticModels = ```\n\t\tNormal\n\t\tFinance\n\t\t```\n\ntable Last\n",
    )
    contribution = parse(tmp_path).semantic_models[model("Executive")]
    assert contribution.table_names == ("Zebra", "Alpha", "Orange", "Blue", "Last")
    assert contribution.properties["settings"] == {"marker": "Finance"}


@weaver_test()
def test_duplicate_native_definition_names_show_both_actual_paths(tmp_path):
    from weaver.errors import DiscoveryError

    for path in (
        "PowerBI/Sales/Normal.SemanticModel",
        "PowerBI/Sales/Other/Normal.SemanticModel",
    ):
        write(tmp_path, path + "/definition.pbism", '{"version":"4.2"}')
        write(tmp_path, path + "/definition/model.tmdl", "model Model\n")
    with pytest.raises(DiscoveryError) as raised:
        parse(tmp_path)
    assert "PowerBI/Sales/Normal.SemanticModel" in str(raised.value)
    assert "PowerBI/Sales/Other/Normal.SemanticModel" in str(raised.value)


@weaver_test()
def test_named_native_discovery_does_not_require_single_pbip_entrypoint(tmp_path):
    native(tmp_path, model="Normal", report="Normal")
    native(tmp_path, model="Executive", report="Executive")
    write(
        tmp_path,
        "PowerBI/Sales/Normal.pbip",
        '{"artifacts":[{"report":{"path":"Reports/Normal.Report"}}]}',
    )
    write(
        tmp_path,
        "PowerBI/Sales/Executive.pbip",
        '{"artifacts":[{"report":{"path":"Reports/Executive.Report"}}]}',
    )
    repository = parse(tmp_path)
    assert set(repository.semantic_models) == {model("Normal"), model("Executive")}
