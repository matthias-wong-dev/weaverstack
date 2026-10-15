"""Named examples retain independent source work as a project grows."""

from itertools import combinations

import pytest
from support.weaver_test import weaver_test
from test_initialise_resources_declaration import fabric as fabric

from weaver.errors import CommandError, WeaverError
from weaver.initialise import _generated_files, _write
from weaver.onboarding.project import ProjectRequest
from weaver.operations.check import check
from weaver_cli.main import build_parser


def snapshot(root):
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("example", ["Parcel", "Sales"])
@pytest.mark.parametrize(
    "order",
    [
        ("lakehouse", "warehouse"),
        ("warehouse", "lakehouse"),
        ("warehouse", "semantic_model"),
        ("semantic_model", "warehouse"),
        ("lakehouse", "warehouse", "semantic_model"),
    ],
)
@weaver_test()
def test_progressive_extension_preserves_edited_sources(
    tmp_path, fabric, example, order
):
    import yaml

    import weaver

    project = tmp_path / "project"
    first = order[0]
    weaver.initialise(
        project,
        workspace="Analytics",
        example=example,
        session=fabric.session,
        **{first: ITEMS[first]},
    )
    if first == "lakehouse":
        relative = "Lakehouse/Landing/Files/" + (
            "Parcel__Events.py" if example == "Parcel" else "Sales__Customers.py"
        )
        old, new = (
            ("Central", "West")
            if example == "Parcel"
            else ("Harbour Freight Pty Ltd", "Edited Customer")
        )
    elif first == "warehouse":
        relative = "Warehouse/Operations/" + (
            "Parcel.StatusEvent.sql" if example == "Parcel" else "Sales.Customer.sql"
        )
        old, new = (
            ("Central", "West")
            if example == "Parcel"
            else ("Harbour Freight Pty Ltd", "Edited Customer")
        )
    else:
        relative = "PowerBI/Model/Model.SemanticModel/definition/tables/" + (
            "Delivery.tmdl" if example == "Parcel" else "Customer.tmdl"
        )
        old, new = (
            ("SUM(Delivery[Delivered])", "COUNTROWS(Delivery)")
            if example == "Parcel"
            else ("COUNTROWS(Customer)", "COUNTROWS(Customer) + 1")
        )
    source = project / relative
    previous = source.read_text()
    assert old in previous
    source.write_text(previous.replace(old, new))
    cfg = project / "workspace-config.yml"
    data = yaml.safe_load(cfg.read_text())
    data["execution"] = {"run": {"spark_concurrency": 2}}
    cfg.write_text(yaml.safe_dump(data, sort_keys=False))
    original = snapshot(project)
    for item in order[1:]:
        weaver.initialise(
            project,
            workspace="Analytics",
            example=example,
            session=fabric.session,
            **{item: ITEMS[item]},
        )
        check(project)
    after = snapshot(project)
    for path, content in original.items():
        if path.startswith(("Lakehouse/", "Warehouse/", "PowerBI/", "Environment/")):
            assert after[path] == content, path
    assert yaml.safe_load(cfg.read_text())["execution"] == data["execution"]
    before_repeat = snapshot(project)
    created = list(fabric.created)
    weaver.initialise(
        project, workspace="Analytics", example=example, session=fabric.session
    )
    assert snapshot(project) == before_repeat and fabric.created == created


@weaver_test()
def test_new_file_collision_refuses_without_partial_extension(tmp_path, fabric):
    import weaver
    from weaver.initialise import InitialiseError

    weaver.initialise(
        tmp_path,
        workspace="Analytics",
        warehouse="Operations",
        example="Parcel",
        session=fabric.session,
    )
    conflict = tmp_path / "Warehouse/Operations/shortcuts.yml"
    conflict.write_text("logical: {}\n")
    before = snapshot(tmp_path)
    created = list(fabric.created)
    with pytest.raises(InitialiseError, match="shortcuts.yml"):
        weaver.initialise(
            tmp_path,
            workspace="Analytics",
            lakehouse="Landing",
            example="Parcel",
            session=fabric.session,
        )
    assert snapshot(tmp_path) == before and fabric.created == created


@pytest.mark.parametrize(
    "field,value",
    [
        ("workspace", "Other"),
        ("catalogue", "Other"),
        ("environment", "Other"),
        ("warehouse", "Other"),
    ],
)
@weaver_test()
def test_extension_refuses_retargeting_without_mutation(tmp_path, fabric, field, value):
    import weaver

    weaver.initialise(
        tmp_path,
        workspace="Analytics",
        warehouse="Operations",
        example="Parcel",
        session=fabric.session,
    )
    before = snapshot(tmp_path)
    created = list(fabric.created)
    kwargs = {
        "workspace": "Analytics",
        "example": "Parcel",
        "session": fabric.session,
        field: value,
    }
    with pytest.raises(WeaverError):
        weaver.initialise(tmp_path, **kwargs)
    assert snapshot(tmp_path) == before and fabric.created == created


@weaver_test()
def test_added_managed_model_cannot_claim_an_existing_service_item(tmp_path, fabric):
    import weaver
    from weaver.fabric.resources import Item
    from weaver.initialise import InitialiseError

    weaver.initialise(
        tmp_path,
        workspace="Analytics",
        semantic_model="Model",
        example="Parcel",
        session=fabric.session,
    )
    fabric.held.append(Item("existing", "ModelWarehouse", "SemanticModel", "ws"))
    before = snapshot(tmp_path)
    created = list(fabric.created)
    with pytest.raises(InitialiseError, match="ModelWarehouse"):
        weaver.initialise(
            tmp_path,
            workspace="Analytics",
            warehouse="Operations",
            example="Parcel",
            session=fabric.session,
        )
    assert snapshot(tmp_path) == before and fabric.created == created


@weaver_test()
def test_invalid_edited_source_prevents_extension_before_creation(tmp_path, fabric):
    import weaver
    from weaver.initialise import InitialiseError

    weaver.initialise(
        tmp_path,
        workspace="Analytics",
        lakehouse="Landing",
        example="Parcel",
        session=fabric.session,
    )
    (tmp_path / "Lakehouse/Landing/Tables/Parcel__Event.py").write_text(
        "not python !!!"
    )
    before = snapshot(tmp_path)
    created = list(fabric.created)
    with pytest.raises(InitialiseError, match="invalid"):
        weaver.initialise(
            tmp_path,
            workspace="Analytics",
            warehouse="Operations",
            example="Parcel",
            session=fabric.session,
        )
    assert snapshot(tmp_path) == before and fabric.created == created


@weaver_test()
def test_physical_bindings_and_source_only_cli_rerun_survive_extension(
    tmp_path, fabric
):
    import yaml

    import weaver
    from weaver.fabric.resources import Item
    from weaver_cli.initialise import collect

    weaver.initialise(
        tmp_path,
        workspace="Analytics",
        lakehouse="Landing",
        example="Parcel",
        session=fabric.session,
    )
    cfg = tmp_path / "workspace-config.yml"
    current = yaml.safe_load(cfg.read_bytes())
    current["targets"]["Lakehouse/Landing"] = "ExistingPhysical"
    cfg.write_text(yaml.safe_dump(current, sort_keys=False))
    fabric.held.append(Item("physical", "ExistingPhysical", "Lakehouse", "ws"))
    created = list(fabric.created)
    weaver.initialise(
        tmp_path,
        workspace="Analytics",
        warehouse="Operations",
        example="Parcel",
        session=fabric.session,
    )
    assert (
        yaml.safe_load(cfg.read_bytes())["targets"]["Lakehouse/Landing"]
        == "ExistingPhysical"
    )
    assert fabric.created == created + ["Warehouse/Operations"]
    args = build_parser().parse_args(
        [
            "initialise",
            "--workspace",
            "Analytics",
            "--project-folder",
            str(tmp_path),
            "--non-interactive",
        ]
    )
    assert collect(args, ask=False) is False


ITEMS = {"lakehouse": "Landing", "warehouse": "Operations", "semantic_model": "Model"}
SELECTIONS = [tuple(c) for n in range(1, 4) for c in combinations(ITEMS, n)]


def request(example, selection):
    return ProjectRequest(
        workspace="Example Development",
        catalogue="Catalogue",
        environment="Weaver",
        example=example,
        **{k: ITEMS[k] for k in selection},
    )


@pytest.mark.parametrize("example", [None, "Parcel", "Sales"])
@pytest.mark.parametrize("selection", SELECTIONS)
@weaver_test()
def test_selection_produces_only_selected_valid_items(tmp_path, example, selection):
    from weaver.declaration.repository import parse_item_repository
    from weaver.locations import Location

    files = _generated_files(request(example, selection))
    _write(tmp_path, files)
    check(tmp_path)
    repository = parse_item_repository(Location(str(tmp_path)))
    if "semantic_model" not in selection:
        assert not repository.semantic_models and not repository.reports
    if example:
        assert any(
            example in content
            for path, content in files.items()
            if path.startswith(("Lakehouse/", "Warehouse/", "PowerBI/"))
        )
        if "semantic_model" in selection:
            assert (
                len(repository.semantic_models) == len(selection)
                and len(repository.reports) == 1
            )
            tables = [
                v for p, v in files.items() if ".SemanticModel/definition/tables/" in p
            ]
            assert tables and any("#table" in v for v in tables)


@pytest.mark.parametrize("example", ["Parcel", "Sales"])
@weaver_test()
def test_every_subset_retains_its_source_bytes(example):
    generated = {s: _generated_files(request(example, s)) for s in SELECTIONS}
    for smaller, first in generated.items():
        for larger, second in generated.items():
            if set(smaller) <= set(larger):
                for path, content in first.items():
                    if path.startswith(
                        ("Lakehouse/", "Warehouse/", "PowerBI/")
                    ) and not path.endswith(".gitkeep"):
                        assert second.get(path) == content, (
                            example,
                            smaller,
                            larger,
                            path,
                        )


@pytest.mark.parametrize("example", ["Parcel", "Sales"])
@pytest.mark.parametrize("selection", SELECTIONS)
@weaver_test()
def test_generated_compilation_and_load_readiness(tmp_path, example, selection):
    from factories import installed_catalogue, item_bindings

    from weaver.catalogue.semantic import project_semantic_model
    from weaver.catalogue.state import Catalogue
    from weaver.declaration.model import WeaverItemId
    from weaver.declaration.repository import parse_item_repository
    from weaver.load_plan import ENDPOINT_REFRESH, LoadDag
    from weaver.locations import Location
    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.binding import bind_semantic_sources

    req = request(example, selection)
    _write(tmp_path, _generated_files(req))
    before = snapshot(tmp_path)
    raw = parse_item_repository(Location(str(tmp_path)))
    observed = {}
    columns = (
        [("Parcel ID", "varchar"), ("Status", "varchar"), ("Depot", "varchar")]
        if example == "Parcel"
        else [
            ("Customer id", "varchar"),
            ("Customer name", "varchar"),
            ("Region code", "varchar"),
        ]
    )
    # Explicit offline column inputs test compilation, not a Fabric readback.
    for contribution in raw.semantic_models.values():
        for reference in contribution.source_references.values():
            relation = reference.rsplit("/", 1)[1]
            schema, entity = relation.split(".")
            observed[reference] = {
                "reference": reference,
                "server": "offline.example.invalid",
                "database": "Offline",
                "schema": schema,
                "object": entity,
                "source_columns": [
                    {"column_name": n, "data_type": t} for n, t in columns
                ],
                "description": "",
                "column_notes": {},
            }
    compiled = bind_semantic_sources(raw, observed, raw.semantic_models)
    source_items = [
        str(i.identity)
        for i in raw.items
        if i.identity.item_type in ("Lakehouse", "Warehouse")
    ]
    catalogue = installed_catalogue(
        raw, item_bindings(*[(i, i.split("/")[1]) for i in source_items])
    )
    rows = dict(catalogue.rows)
    for identity, contribution in compiled.semantic_models.items():
        native = TmdlDefinition(contribution.parts).model
        if identity.item_name == "Model":
            assert not contribution.source_references
            assert any(
                "#table" in str(p.source) for t in native.tables for p in t.partitions
            )
        else:
            assert all(p.mode == "import" for t in native.tables for p in t.partitions)
        # Project actual compiled table shape through the production projector.
        # Installation identifiers below are offline planning inputs only.
        projected = project_semantic_model(
            identity,
            contribution,
            deployed={"model": {"tables": [{"name": t.name} for t in native.tables]}},
        )
        projected["Installation"] = (
            {
                "item_type": "SemanticModel",
                "item_name": identity.item_name,
                "target_name": identity.item_name,
                "workspace_id": "11111111-1111-1111-1111-111111111111",
                "item_id": "22222222-2222-2222-2222-222222222222",
            },
        )
        rows[identity] = projected
    chosen = [WeaverItemId.parse(i) for i in source_items] + list(
        compiled.semantic_models
    )
    plan = LoadDag.from_catalogue(Catalogue(rows), items=chosen)
    ordered = list(plan.order())
    if req.lakehouse and req.warehouse:
        source = "Lakehouse/Landing/Tables/" + (
            "Parcel.Event" if example == "Parcel" else "Sales.Customer"
        )
        consumer = "Warehouse/Operations/" + (
            "Parcel.LakehouseCurrentStatus"
            if example == "Parcel"
            else "Sales.LakehouseCustomerByRegion"
        )
        logical = [str(n.logical_id) for n in ordered]
        assert logical.index(source) < logical.index(consumer)
        barriers = [n for n in ordered if n.primitive_kind == ENDPOINT_REFRESH]
        assert barriers and any(
            n.node_id in plan.upstream(ordered[logical.index(consumer)].node_id)
            for n in barriers
        )
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "example,base_table,managed_table",
    [("Parcel", "Delivery", "CurrentStatus"), ("Sales", "Customer", "ManagedCustomer")],
)
@weaver_test()
def test_additional_models_compose_native_foundation_instead_of_replacing_it(
    tmp_path, example, base_table, managed_table
):
    from weaver.declaration.repository import parse_item_repository
    from weaver.locations import Location

    req = request(example, ("warehouse", "semantic_model"))
    generated = _generated_files(req)
    assert "Weaver.BaseSemanticModels" in generated["PowerBI/Model/ModelWarehouse.tmdl"]
    _write(tmp_path, generated)
    raw = parse_item_repository(Location(str(tmp_path)))
    from weaver.declaration.model import WeaverItemId

    contribution = raw.semantic_models[
        WeaverItemId.parse("SemanticModel/ModelWarehouse")
    ]
    assert (
        base_table in contribution.table_names
        and managed_table in contribution.table_names
    )
    assert contribution.source_references.keys() == {managed_table}


@weaver_test()
def test_initialise_ownership_is_written_after_source_files(tmp_path, monkeypatch):
    from pathlib import Path

    writes = []
    original = Path.write_bytes

    def record(path, content):
        writes.append(path.name)
        return original(path, content)

    monkeypatch.setattr(Path, "write_bytes", record)
    _write(tmp_path, {".weaver-initialise.json": "{}", "source.sql": "select 1"})
    assert writes == ["source.sql", ".weaver-initialise.json"]


@weaver_test()
def test_named_example_option_requires_a_known_name():
    parser = build_parser()
    for name in ("Parcel", "Sales"):
        assert parser.parse_args(["initialise", "--example", name]).example == name
    assert parser.parse_args(["initialise"]).example is None
    for args in (["--example"], ["--example", "Unknown"], ["--no-example"]):
        with pytest.raises(SystemExit):
            parser.parse_args(["initialise", *args])
    for name in (True, False, "Unknown"):
        with pytest.raises(CommandError, match="Parcel.*Sales"):
            request(name, ("warehouse",))


@pytest.mark.parametrize("example", ["Parcel", "Sales"])
@weaver_test()
def test_native_only_initialise_requires_no_fabric_data_resources(
    tmp_path, fabric, example
):
    import yaml

    import weaver

    root = tmp_path / "native"
    report = weaver.initialise(
        root,
        workspace="Analytics",
        semantic_model="Model",
        example=example,
        session=fabric.session,
    )
    assert report.succeeded and report.resources == ()
    assert fabric.created == []
    assert "weaver health" not in report.next_commands
    assert "weaver test" not in report.next_commands
    config = yaml.safe_load((root / "workspace-config.yml").read_text())
    assert not config.get("catalogue") and not config.get("environment")
    assert not (root / "Environment").exists()
    assert "Environment/None" not in (root / "README.md").read_text()
    original = {p: b for p, b in snapshot(root).items() if p.startswith("PowerBI/")}
    weaver.initialise(
        root,
        workspace="Analytics",
        warehouse="Operations",
        example=example,
        session=fabric.session,
    )
    config = yaml.safe_load((root / "workspace-config.yml").read_text())
    assert config["catalogue"] == "Warehouse/Catalogue"
    assert config["environment"] == "Weaver"
    assert all(snapshot(root).get(p) == b for p, b in original.items())


@weaver_test()
def test_native_interactive_setup_does_not_request_data_infrastructure(
    tmp_path, capsys
):
    from io import StringIO

    from weaver_cli.initialise import collect

    args = build_parser().parse_args(
        [
            "initialise",
            "--workspace",
            "Demo",
            "--project-folder",
            str(tmp_path),
            "--semantic-model",
            "Model",
            "--example",
            "Parcel",
            "--interactive",
        ]
    )
    collect(args, stdin=StringIO("1\n"))
    assert args.catalogue is None and args.environment is None
    text = capsys.readouterr().out
    assert "Catalogue name" not in text and "Environment name" not in text
    assert "Publish the Environment" not in text


@weaver_test()
def test_native_extension_preserves_edited_readme_and_explicit_support(
    tmp_path, fabric
):
    import yaml

    import weaver

    root = tmp_path / "native"
    weaver.initialise(
        root,
        workspace="Analytics",
        semantic_model="Model",
        example="Parcel",
        catalogue="History",
        session=fabric.session,
    )
    assert not (root / "Environment").exists()
    readme = root / "README.md"
    readme.write_text(readme.read_text() + "\nUser guidance.\n")
    before = readme.read_bytes()
    weaver.initialise(
        root,
        workspace="Analytics",
        warehouse="Operations",
        example="Parcel",
        session=fabric.session,
    )
    assert readme.read_bytes() == before
    assert (
        yaml.safe_load((root / "workspace-config.yml").read_text())["catalogue"]
        == "Warehouse/History"
    )
