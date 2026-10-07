import json

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


def write(root, path, data):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data if isinstance(data, bytes) else data.encode())


def parse(root):
    return parse_item_repository(Location(root.as_posix()))


@weaver_test()
def test_native_extension_named_parts_are_preserved(tmp_path):
    path = native(tmp_path)
    write(tmp_path, path + "/StaticResources/extension.tmdl", b"opaque\x00\xff")
    model_path = "PowerBI/Sales/Revenue.SemanticModel/definition/extension.tmdl"
    write(tmp_path, model_path, b"expression Native = 1\r\n")
    repository = parse(tmp_path)
    assert (
        repository.reports[WeaverItemId("Report", "Executive")].parts[
            "StaticResources/extension.tmdl"
        ]
        == b"opaque\x00\xff"
    )
    assert (
        repository.semantic_models[WeaverItemId("SemanticModel", "Revenue")].parts[
            "definition/extension.tmdl"
        ]
        == b"expression Native = 1\r\n"
    )


@pytest.mark.parametrize("names", [("Serving", "serving"), ("serving", "Serving")])
@weaver_test()
def test_duplicate_typed_source_items_report_both_paths(tmp_path, names):
    from weaver.errors import DiscoveryError
    from weaver.store import Entry, FilesystemStore

    root = Location(tmp_path.as_posix())

    class CaseSensitiveListing(FilesystemStore):
        def list(self, location, *, recursive=False):
            if location == root:
                # A case-sensitive Store can expose both names on every host.
                return [Entry(root / "Warehouse" / name, True) for name in names]
            return super().list(location, recursive=recursive)

    with pytest.raises(DiscoveryError) as raised:
        parse_item_repository(root, store=CaseSensitiveListing())
    assert "Warehouse/Serving" in str(raised.value)
    assert "Warehouse/serving" in str(raised.value)


@pytest.mark.parametrize("replacement", [None, "model.bim"])
@weaver_test()
def test_native_model_requires_supported_tmdl_definition(tmp_path, replacement):
    from weaver.errors import ConfigError

    native(tmp_path)
    model = tmp_path / "PowerBI/Sales/Revenue.SemanticModel/definition/model.tmdl"
    model.unlink()
    if replacement:
        write(
            tmp_path,
            "PowerBI/Sales/Revenue.SemanticModel/definition/" + replacement,
            "{}",
        )
    with pytest.raises(ConfigError, match="Revenue.SemanticModel.*TMDL definition"):
        parse(tmp_path)


@weaver_test()
def test_standalone_report_tree_requires_powerbi_local_context(tmp_path):
    from weaver.errors import ConfigError

    write(tmp_path, "Report/Executive/definition.pbir", "{}")
    with pytest.raises(ConfigError, match="Report/Executive.*PowerBI"):
        parse(tmp_path)


@weaver_test()
def test_report_resources_are_not_semantic_authored_documents(tmp_path):
    path = native(tmp_path)
    write(tmp_path, path + "/StaticResources/script.py", "opaque resource\n")
    write(tmp_path, path + "/StaticResources/data.tmdl", "opaque resource\n")
    repository = parse(tmp_path)
    assert (
        repository.reports[WeaverItemId("Report", "Executive")].parts[
            "StaticResources/script.py"
        ]
        == b"opaque resource\n"
    )
    assert not any(
        "StaticResources" in p
        for c in repository.semantic_models.values()
        for p in c.sources
    )


@pytest.mark.parametrize("folder", ["Revenue.SemanticModel", "Executive.Report"])
@weaver_test()
def test_empty_native_artifact_directory_is_not_silently_ignored(tmp_path, folder):
    from weaver.errors import ConfigError

    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n")
    (tmp_path / "PowerBI/Sales" / folder).mkdir()
    with pytest.raises(ConfigError, match=folder):
        parse(tmp_path)


@pytest.mark.parametrize("properties", ["{}", "[]", "not json", '{"version":1}'])
@weaver_test()
def test_native_model_requires_versioned_properties(tmp_path, properties):
    from weaver.errors import ConfigError

    native(tmp_path)
    write(tmp_path, "PowerBI/Sales/Revenue.SemanticModel/definition.pbism", properties)
    with pytest.raises(ConfigError, match="definition.pbism"):
        parse(tmp_path)


@pytest.mark.parametrize(
    "artifact",
    [
        "../Outside.Report",
        "Reports/Missing.Report",
        "/Outside.Report",
        "Reports\\Executive.Report",
    ],
)
@weaver_test()
def test_pbip_artifact_must_name_discovered_local_report(tmp_path, artifact):
    from weaver.errors import ConfigError

    native(tmp_path)
    write(
        tmp_path,
        "PowerBI/Sales/Sales.pbip",
        json.dumps({"artifacts": [{"report": {"path": artifact}}]}),
    )
    with pytest.raises(ConfigError, match="Sales.pbip"):
        parse(tmp_path)


@pytest.mark.parametrize(
    "path, replacement",
    [
        ("PowerBI/extension.tmdl", "PowerBI/policy.tmdl"),
        ("SemanticModel/extension.tmdl", "PowerBI/policy.tmdl"),
        ("PowerBI/Sales/extension.tmdl", "Revenue.tmdl"),
        ("SemanticModel/Revenue/extension.tmdl", "Revenue.tmdl"),
    ],
)
@weaver_test()
def test_retired_extension_name_has_migration_guidance(tmp_path, path, replacement):
    from weaver.errors import ConfigError

    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n")
    write(tmp_path, path, "model Model\n")
    with pytest.raises(ConfigError) as raised:
        parse(tmp_path)
    assert path in str(raised.value)
    assert replacement in str(raised.value)


@pytest.mark.parametrize(
    "kind,first,second",
    [
        ("SemanticModel", "PowerBI/Sales/Revenue.tmdl", "PowerBI/Finance/Revenue.tmdl"),
        (
            "SemanticModel",
            "SemanticModel/Revenue/Revenue.tmdl",
            "PowerBI/Finance/Revenue.tmdl",
        ),
        (
            "Report",
            "PowerBI/Sales/Executive.Report/definition.pbir",
            "PowerBI/Finance/Executive.Report/definition.pbir",
        ),
        (
            "Report",
            "PowerBI/Sales/Executive.Report/definition.pbir",
            "PowerBI/Sales/Reports/Executive.Report/definition.pbir",
        ),
    ],
)
@weaver_test()
def test_duplicate_logical_items_report_both_source_paths(
    tmp_path, kind, first, second
):
    from weaver.errors import DiscoveryError

    write(tmp_path, first, "model Model\n")
    write(tmp_path, second, "model Model\n")
    if kind == "Report":
        write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n")
        write(tmp_path, "PowerBI/Finance/Finance.tmdl", "model Model\n")
    with pytest.raises(DiscoveryError) as raised:
        parse(tmp_path)
    assert first.rsplit("/definition.pbir", 1)[0].rsplit("/Revenue.tmdl", 1)[0] in str(
        raised.value
    )
    assert second.rsplit("/definition.pbir", 1)[0] in str(raised.value)
    assert kind in str(raised.value)


@weaver_test()
def test_report_only_project_retains_authored_connection_without_logical_inference(
    tmp_path,
):
    write(
        tmp_path,
        "PowerBI/Sales/Executive.Report/definition.pbir",
        json.dumps(
            {"datasetReference": {"byPath": {"path": "../Revenue.SemanticModel"}}}
        ),
    )
    repository = parse(tmp_path)
    item = WeaverItemId("Report", "Executive")
    assert repository.powerbi_projects["Sales"].items == (item,)
    assert repository.reports[item].model is None
    assert not repository.semantic_models
    assert json.loads(repository.reports[item].parts["definition.pbir"])[
        "datasetReference"
    ] == {"byPath": {"path": "../Revenue.SemanticModel"}}


@weaver_test()
def test_declared_model_without_native_base_supports_local_report(tmp_path):
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n")
    write(
        tmp_path,
        "PowerBI/Sales/Executive.Report/definition.pbir",
        json.dumps(
            {"datasetReference": {"byPath": {"path": "../Revenue.SemanticModel"}}}
        ),
    )
    repository = parse(tmp_path)
    assert repository.reports[
        WeaverItemId("Report", "Executive")
    ].model == WeaverItemId("SemanticModel", "Revenue")


@pytest.mark.parametrize(
    "reference",
    [
        {"byConnection": {"connectionString": "external"}},
        {"byPath": {"path": "../../Other/Revenue.SemanticModel"}},
        {"byPath": {"path": "../../Other.SemanticModel"}},
        {"byPath": {"path": "/Revenue.SemanticModel"}},
        {"byPath": {"path": "..\\Revenue.SemanticModel"}},
    ],
)
@weaver_test()
def test_report_reference_metadata_does_not_override_sole_model(tmp_path, reference):
    path = native(tmp_path)
    write(
        tmp_path, path + "/definition.pbir", json.dumps({"datasetReference": reference})
    )
    contribution = parse(tmp_path).reports[WeaverItemId("Report", "Executive")]
    assert contribution.model == WeaverItemId("SemanticModel", "Revenue")
    assert (
        json.loads(contribution.parts["definition.pbir"])["datasetReference"]
        == reference
    )


@pytest.mark.parametrize(
    "reference", [{"byPath": {"path": None}}, {}, {"byConnection": {}}]
)
@weaver_test()
def test_report_requires_native_reference_shape(tmp_path, reference):
    from weaver.errors import ConfigError

    path = native(tmp_path)
    write(
        tmp_path, path + "/definition.pbir", json.dumps({"datasetReference": reference})
    )
    with pytest.raises(ConfigError, match="definition.pbir"):
        parse(tmp_path)


@weaver_test()
def test_named_model_uses_policy_then_item_tmdl(tmp_path):
    write(tmp_path, "PowerBI/policy.tmdl", "model Model\n\tculture: en-AU\n")
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n\tculture: en-GB\n")
    repository = parse(tmp_path)
    item = WeaverItemId("SemanticModel", "Revenue")
    assert item in repository.semantic_models
    model = repository.semantic_models[item]
    assert model.requested["culture"] == "en-GB"
    assert model.provenance["/model/culture"]["source"] == "PowerBI/Sales/Revenue.tmdl"
    assert repository.powerbi_projects["Sales"].items == (item,)


@pytest.mark.parametrize("other", ["Other.tmdl", "Other.SemanticModel"])
@weaver_test()
def test_different_native_and_tmdl_names_are_separate_models(tmp_path, other):
    write(tmp_path, "PowerBI/Sales/Revenue.tmdl", "model Model\n")
    if other.endswith(".tmdl"):
        write(tmp_path, "PowerBI/Sales/" + other, "model Model\n")
    else:
        native(tmp_path, model="Other", report="Other")
    assert set(parse(tmp_path).semantic_models) == {
        WeaverItemId("SemanticModel", "Revenue"),
        WeaverItemId("SemanticModel", "Other"),
    }


def native(
    root, project="Sales", model="Revenue", report="Executive", nested="Reports/"
):
    prefix = f"PowerBI/{project}"
    write(
        root,
        f"{prefix}/{model}.SemanticModel/definition.pbism",
        b'{"version":"4.2","settings":{}}\r\n',
    )
    write(
        root,
        f"{prefix}/{model}.SemanticModel/definition/model.tmdl",
        b"model Model\r\n\tculture: en-US\r\n",
    )
    path = f"{prefix}/{nested}{report}.Report"
    reference = "../" * (nested.count("/") + 1) + f"{model}.SemanticModel"
    write(
        root,
        path + "/definition.pbir",
        json.dumps(
            {"version": "4.0", "datasetReference": {"byPath": {"path": reference}}}
        ),
    )
    write(root, path + "/StaticResources/theme.bin", b"\x00\xff\r\n")
    write(root, path + "/definition/pages/page.json", b'{"name":"Page"}\r\n')
    write(
        root,
        f"{prefix}/{project}.pbip",
        json.dumps({"artifacts": [{"report": {"path": f"{nested}{report}.Report"}}]}),
    )
    return path


@weaver_test()
def test_native_pbip_discovers_nested_report_and_preserves_bytes(tmp_path):
    path = native(tmp_path)
    before = {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    repository = parse(tmp_path)
    model = WeaverItemId("SemanticModel", "Revenue")
    report = WeaverItemId("Report", "Executive")
    assert repository.powerbi_projects["Sales"].model == model
    contribution = repository.reports[report]
    assert contribution.model == model
    assert contribution.path == path
    assert contribution.parts == {
        p[len(path) + 1 :]: b for p, b in before.items() if p.startswith(path + "/")
    }
    semantic = repository.semantic_models[model]
    assert (
        semantic.parts["definition/model.tmdl"]
        == before["PowerBI/Sales/Revenue.SemanticModel/definition/model.tmdl"]
    )
    assert before == {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
