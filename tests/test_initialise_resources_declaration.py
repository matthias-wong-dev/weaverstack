"""Project setup, Environment adoption and destination protection."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.fabric import resources
from weaver.fabric.environment_definition import (
    EXTERNAL_LIBRARIES,
    EnvironmentDefinition,
)
from weaver.initialise import InitialiseError
from weaver.sessions.testing import TestSession


@pytest.fixture
def fabric(monkeypatch):
    held, created, published = [], [], []
    definition = EnvironmentDefinition(
        {
            EXTERNAL_LIBRARIES: b"dependencies:\n  - pip:\n      - pandas==2.2.3\n",
            "Setting/Sparkcompute.yml": b"instance_pool_id: pool\n",
            "Libraries/CustomLibraries/helper.whl": b"\x00\xffbinary",
        }
    )
    physical = resources.WorkspaceItem("ws", "Analytics")

    def create(workspace, name, kind):
        item = resources.Item(name, name, kind, workspace.id)
        held.append(item)
        created.append(f"{kind}/{name}")
        return item

    monkeypatch.setattr(resources, "find_workspace", lambda *a, **k: physical)
    monkeypatch.setattr(resources, "list_items", lambda *a, **k: tuple(held))
    monkeypatch.setattr(
        resources, "create_lakehouse", lambda w, n, **k: create(w, n, "Lakehouse")
    )
    monkeypatch.setattr(
        resources, "create_warehouse", lambda w, n, **k: create(w, n, "Warehouse")
    )
    monkeypatch.setattr(
        "weaver.fabric.environment.create_with_definition",
        lambda w, n, d, **k: create(w, n, "Environment"),
    )
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition", lambda *a, **k: definition
    )
    monkeypatch.setattr(
        "weaver.fabric.publish_environment",
        lambda *a, **k: published.append(k) or SimpleNamespace(action="updated"),
    )
    session = TestSession(resolver=SimpleNamespace(client=object()))
    return SimpleNamespace(
        held=held,
        created=created,
        published=published,
        session=session,
        definition=definition,
    )


def setup(path, fabric, **kwargs):
    return weaver.initialise(
        path,
        workspace="Analytics",
        lakehouse="Landing",
        warehouse="Curated",
        session=fabric.session,
        **kwargs,
    )


@weaver_test()
def test_missing_items_are_created_once_and_reruns_converge(tmp_path, fabric):
    first = setup(tmp_path, fabric)
    before = {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    second = setup(tmp_path, fabric)
    assert fabric.created == [
        "Warehouse/Catalogue",
        "Lakehouse/Landing",
        "Warehouse/Curated",
        "Environment/Weaver",
    ]
    assert first.environment_publication == second.environment_publication == "deferred"
    assert not fabric.published
    assert all(item.status == "existing" for item in second.resources)
    assert before == {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }


@weaver_test()
def test_existing_environment_import_preserves_packages_settings_and_binary_libraries(
    tmp_path, fabric
):
    fabric.held.append(resources.Item("env", "Weaver", "Environment", "ws"))
    report = setup(tmp_path, fabric)
    root = tmp_path / "Environment/Weaver.Environment"
    text = (root / EXTERNAL_LIBRARIES).read_text()
    assert "pandas==2.2.3" in text and "weaverstack" in text
    assert (
        root / "Libraries/CustomLibraries/helper.whl"
    ).read_bytes() == b"\x00\xffbinary"
    assert (
        root / "Setting/Sparkcompute.yml"
    ).read_bytes() == b"instance_pool_id: pool\n"
    assert report.environment_publication == "deferred"
    assert not fabric.published


@weaver_test()
def test_requested_publication_uses_the_local_definition_without_dev(tmp_path, fabric):
    report = setup(tmp_path, fabric, publish_environment=True)
    assert report.environment_publication == "published"
    assert fabric.published[0]["path"] == tmp_path / "Environment/Weaver.Environment"
    assert not fabric.published[0].get("dev")


@weaver_test()
def test_interrupted_publication_can_be_retried(tmp_path, fabric, monkeypatch):
    def fail(*a, **k):
        raise InitialiseError("Publication stopped")

    monkeypatch.setattr("weaver.fabric.publish_environment", fail)
    with pytest.raises(InitialiseError, match="Publication stopped"):
        setup(tmp_path, fabric, publish_environment=True)
    report = setup(tmp_path, fabric)
    assert report.succeeded and len(fabric.created) == 4


@weaver_test()
def test_dry_run_writes_and_creates_nothing(tmp_path, fabric):
    report = setup(tmp_path, fabric, dry_run=True, example="Sales")
    assert report.dry_run and report.example.generated
    assert not fabric.created and not fabric.published and not list(tmp_path.iterdir())


@pytest.mark.parametrize("path", ["workflow.yml", "README.md"])
@weaver_test()
def test_edited_project_helpers_are_preserved(tmp_path, fabric, path):
    setup(tmp_path, fabric)
    existing = tmp_path / path
    content = existing.read_text() + "\n# User addition\n"
    existing.write_text(content)
    fabric.created.clear()
    setup(tmp_path, fabric)
    assert not fabric.created and existing.read_text() == content


@weaver_test()
def test_invalid_existing_configuration_is_refused_before_mutation(tmp_path, fabric):
    setup(tmp_path, fabric)
    (tmp_path / "workspace-config.yml").write_text("user content")
    fabric.created.clear()
    with pytest.raises(weaver.errors.WeaverError):
        setup(tmp_path, fabric)
    assert not fabric.created


@weaver_test()
def test_a_rerun_keeps_an_edited_configuration_and_needs_no_item_named(
    tmp_path, fabric
):
    setup(tmp_path, fabric)
    config = tmp_path / "workspace-config.yml"
    edited = (
        config.read_text()
        + "\n# Reviewed\nexecution:\n  run:\n    spark_concurrency: 8\n"
    )
    config.write_text(edited)
    report = weaver.initialise(tmp_path, workspace="Analytics", session=fabric.session)
    assert report.succeeded and config.read_text() == edited


@weaver_test()
def test_a_new_item_adds_its_target_to_an_existing_configuration(tmp_path, fabric):
    weaver.initialise(
        tmp_path, workspace="Analytics", lakehouse="Landing", session=fabric.session
    )
    weaver.initialise(
        tmp_path, workspace="Analytics", warehouse="Curated", session=fabric.session
    )
    text = (tmp_path / "workspace-config.yml").read_text()
    assert "Lakehouse/Landing: Landing" in text and "Warehouse/Curated: Curated" in text


def _bound_project(path):
    (path / "workspace-config.yml").write_text(
        "workspace: Analytics\n"
        "environment: Weaver\n"
        "catalogue: Warehouse/Catalogue\n"
        "targets:\n"
        "  Lakehouse/Landing: LandingDev\n"
        "  Warehouse/Curated: CuratedDev\n"
    )


@weaver_test()
def test_an_adopted_project_gets_the_items_its_targets_name(tmp_path, fabric):
    _bound_project(tmp_path)
    first = weaver.initialise(tmp_path, workspace="Analytics", session=fabric.session)
    assert fabric.created == [
        "Warehouse/Catalogue",
        "Lakehouse/LandingDev",
        "Warehouse/CuratedDev",
        "Environment/Weaver",
    ]
    assert [(r.role, r.name, r.status) for r in first.resources] == [
        ("Catalogue", "Catalogue", "created"),
        ("Environment", "Weaver", "created"),
        ("Lakehouse", "LandingDev", "created"),
        ("Warehouse", "CuratedDev", "created"),
    ]
    again = weaver.initialise(tmp_path, workspace="Analytics", session=fabric.session)
    assert all(r.status == "existing" for r in again.resources)
    assert len(fabric.created) == 4


@weaver_test()
def test_a_named_item_the_targets_bind_is_created_under_its_physical_name(
    tmp_path, fabric
):
    _bound_project(tmp_path)
    weaver.initialise(
        tmp_path, workspace="Analytics", lakehouse="Landing", session=fabric.session
    )
    assert "Lakehouse/LandingDev" in fabric.created
    assert "Lakehouse/Landing" not in fabric.created
    assert (
        "Lakehouse/Landing: LandingDev"
        in (tmp_path / "workspace-config.yml").read_text()
    )


@weaver_test()
def test_a_dry_run_plans_the_items_the_targets_name(tmp_path, fabric):
    _bound_project(tmp_path)
    report = weaver.initialise(
        tmp_path, workspace="Analytics", dry_run=True, session=fabric.session
    )
    assert ("Lakehouse", "LandingDev", "planned") in [
        (r.role, r.name, r.status) for r in report.resources
    ]
    assert not fabric.created


@weaver_test()
def test_new_files_beside_unrelated_ones_are_still_refused(tmp_path, fabric):
    (tmp_path / "README.md").write_text("someone else's project")
    with pytest.raises(InitialiseError, match="Choose another project folder"):
        setup(tmp_path, fabric)
    assert not fabric.created


@weaver_test()
def test_edited_environment_is_preserved_without_ownership_tracking(tmp_path, fabric):
    setup(tmp_path, fabric)
    libraries = tmp_path / "Environment/Weaver.Environment" / EXTERNAL_LIBRARIES
    content = libraries.read_text() + "      - numpy\n"
    libraries.write_text(content)
    fabric.created.clear()
    report = setup(tmp_path, fabric)
    assert report.succeeded and not fabric.created
    assert libraries.read_text() == content
    assert not (tmp_path / ".weaver-generated.json").exists()


@pytest.mark.parametrize(
    "conflict", ["pyproject.toml", "Lakehouse", "Warehouse/Curated/invalid.py"]
)
@weaver_test()
def test_destination_conflicts_are_refused_before_mutation(tmp_path, fabric, conflict):
    path = tmp_path / conflict
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("invalid")
    with pytest.raises((InitialiseError, OSError)):
        setup(tmp_path, fabric)
    assert not fabric.created and not fabric.published


@weaver_test()
def test_item_kind_collision_stops_before_creation(tmp_path, fabric):
    fabric.held.append(resources.Item("id", "Landing", "Warehouse", "ws"))
    with pytest.raises(InitialiseError, match="already exists"):
        setup(tmp_path, fabric)
    assert not fabric.created


@weaver_test()
def test_example_only_generates_source(tmp_path, fabric, monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("setup executed data work")

    for name in ("build", "load", "test"):
        monkeypatch.setattr(f"weaver.operations.{name}.{name}", refuse)
    report = setup(tmp_path, fabric, example="Sales")
    assert report.example.generated and report.succeeded
    assert (tmp_path / "Lakehouse/Landing/Tables/Sales__Customer.py").is_file()


@weaver_test()
def test_the_example_refuses_a_power_bi_name_the_workspace_has(tmp_path, fabric):
    """Build would deploy the example over a model or Report of the same name."""

    fabric.held.append(resources.Item("id", "Sales", "SemanticModel", "ws"))
    with pytest.raises(InitialiseError, match="--semantic-model"):
        setup(tmp_path, fabric, example="Sales", semantic_model="Sales")
    assert not fabric.created and not (tmp_path / "PowerBI").exists()

    setup(tmp_path, fabric, example="Sales", semantic_model="Orders")
    assert (tmp_path / "PowerBI/Orders/Orders.pbip").is_file()


@weaver_test()
def test_an_example_rerun_after_build_converges(tmp_path, fabric):
    setup(tmp_path, fabric, example="Sales")
    before = {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    fabric.held.extend(
        resources.Item(kind, "Sales", kind, "ws")
        for kind in ("SemanticModel", "Report")
    )

    setup(tmp_path, fabric, example="Sales")

    assert before == {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }


@weaver_test()
def test_invalid_lakehouse_name_stops_before_rest(tmp_path, monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("reached REST")

    monkeypatch.setattr(resources, "find_workspace", refuse)
    with pytest.raises(weaver.errors.CommandError, match="valid Fabric Lakehouse"):
        weaver.initialise(tmp_path, workspace="Analytics", lakehouse="1")


@weaver_test()
def test_legacy_publication_argument_is_a_deprecated_alias(tmp_path, fabric):
    with pytest.warns(DeprecationWarning, match="publish_environment"):
        report = setup(tmp_path, fabric, install_weaver=True)
    assert report.environment_publication == "published"


@weaver_test()
def test_fabric_validation_fallback_hides_api_json(tmp_path, fabric, monkeypatch):
    from weaver.fabric.client import FabricError

    def rejected(*args, **kwargs):
        raise FabricError(
            '{"errorCode":"InvalidDisplayName","requestId":"internal"}', status_code=400
        )

    monkeypatch.setattr(resources, "create_warehouse", rejected)
    with pytest.raises(InitialiseError, match="Check the item name") as failure:
        setup(tmp_path, fabric)
    assert "errorCode" not in str(failure.value)
    assert not fabric.created


@weaver_test()
def test_interrupted_file_write_continues_without_replacing_existing_files(
    tmp_path, fabric, monkeypatch
):
    import importlib

    module = importlib.import_module("weaver.initialise")
    write = module._write
    interrupted = []

    def stop_once(destination, files):
        if destination == tmp_path and not interrupted:
            partial = dict(list(files.items())[:2])
            write(destination, partial)
            interrupted.extend(partial)
            raise OSError("interrupted file write")
        write(destination, files)

    monkeypatch.setattr(module, "_write", stop_once)
    with pytest.raises(OSError, match="interrupted file write"):
        setup(tmp_path, fabric)
    timestamps = {name: (tmp_path / name).stat().st_mtime_ns for name in interrupted}
    report = setup(tmp_path, fabric)
    assert report.succeeded and len(fabric.created) == 4
    assert timestamps == {
        name: (tmp_path / name).stat().st_mtime_ns for name in interrupted
    }


@weaver_test()
def test_preflight_has_progress_before_any_item_creation(tmp_path, fabric, monkeypatch):
    import importlib

    module = importlib.import_module("weaver.initialise")
    environment = importlib.import_module("weaver.fabric.environment")
    fabric.held.append(resources.Item("env", "Weaver", "Environment", "ws"))
    seen = []

    def observe(owner, name, step):
        original = getattr(owner, name)

        def run(*args, **kwargs):
            assert [frame.name for frame in fabric.session.frames] == [
                "Setting up the Weaver project",
                step,
            ]
            assert not fabric.created
            seen.append(step)
            return original(*args, **kwargs)

        monkeypatch.setattr(owner, name, run)

    observe(module, "_read_the_workspace", "Checking the workspace")
    observe(environment, "read_definition", "Reading the Environment")
    observe(module, "_parse_generated", "Checking the project files")
    assert setup(tmp_path, fabric).succeeded
    assert seen == [
        "Checking the workspace",
        "Reading the Environment",
        "Checking the project files",
    ]
    assert fabric.created and not fabric.session.frames


@weaver_test()
def test_initialise_never_deploys_a_configured_catalogue_dashboard(
    tmp_path, fabric, monkeypatch
):
    def refuse(*args, **kwargs):
        raise AssertionError("initialise deployed the Catalogue Dashboard")

    monkeypatch.setattr(
        "weaver.operations.catalogue_dashboard.deploy_catalogue_dashboard", refuse
    )
    monkeypatch.setattr("weaver.operations.build.build", refuse)
    setup(tmp_path, fabric)
    config = tmp_path / "workspace-config.yml"
    config.write_text(config.read_text() + "\ncatalogue_dashboard: Estate Dashboard\n")

    report = setup(tmp_path, fabric)

    assert report.succeeded
    assert {outcome.role for outcome in report.resources} == {
        "Catalogue",
        "Environment",
        "Lakehouse",
        "Warehouse",
    }


@weaver_test()
def test_a_session_in_another_workspace_is_refused_before_creation(tmp_path, fabric):
    from weaver.workspaces import Workspace

    setup(tmp_path, fabric)
    fabric.created.clear()
    elsewhere = TestSession(
        workspace=Workspace(workspace="Elsewhere"),
        resolver=SimpleNamespace(client=object()),
    )

    with pytest.raises(InitialiseError, match="binds workspace to 'Analytics'"):
        weaver.initialise(tmp_path, lakehouse="Landing", session=elsewhere)
    assert not fabric.created


@weaver_test()
def test_a_new_item_kind_names_the_workflow_steps_it_needs(tmp_path, fabric):
    weaver.initialise(
        tmp_path, workspace="Analytics", lakehouse="Landing", session=fabric.session
    )
    # An edited workflow is the user's, so initialise names what it lacks.
    workflow = (tmp_path / "workflow.yml").read_text() + "\n# Reviewed\n"
    (tmp_path / "workflow.yml").write_text(workflow)

    report = weaver.initialise(
        tmp_path,
        workspace="Analytics",
        semantic_model="Sales",
        session=fabric.session,
    )

    (guidance,) = report.guidance
    assert "weaver build --item Lakehouse" in guidance
    assert "weaver build --item PowerBI" in guidance
    assert "SemanticModel" in guidance
    assert report.to_mapping()["guidance"] == [guidance]
    assert (tmp_path / "workflow.yml").read_text() == workflow


@weaver_test()
def test_a_rerun_with_nothing_new_has_no_guidance(tmp_path, fabric):
    setup(tmp_path, fabric)
    assert setup(tmp_path, fabric).guidance == ()
