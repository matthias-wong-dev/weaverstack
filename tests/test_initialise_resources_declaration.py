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
    report = setup(tmp_path, fabric, dry_run=True, example=True)
    assert report.dry_run and report.example.generated
    assert not fabric.created and not fabric.published and not list(tmp_path.iterdir())


@pytest.mark.parametrize("path", ["workspace-config.yml", "compose.yml", "README.md"])
@weaver_test()
def test_existing_project_files_are_not_overwritten(tmp_path, fabric, path):
    setup(tmp_path, fabric)
    (tmp_path / path).write_text("user content")
    fabric.created.clear()
    with pytest.raises(InitialiseError, match="Choose another project folder"):
        setup(tmp_path, fabric)
    assert not fabric.created and (tmp_path / path).read_text() == "user content"


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
    report = setup(tmp_path, fabric, example=True)
    assert report.example.generated and report.succeeded
    assert (tmp_path / "Lakehouse/Landing/Tables/Sales__Customer.py").is_file()


@weaver_test()
def test_invalid_lakehouse_name_stops_before_rest(tmp_path, monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("reached REST")

    monkeypatch.setattr(resources, "find_workspace", refuse)
    with pytest.raises(weaver.errors.CommandError, match="valid Fabric Lakehouse"):
        weaver.initialise(tmp_path, workspace="Analytics", lakehouse="1")


@weaver_test()
def test_symlink_conflict_is_refused_before_mutation(tmp_path, fabric):
    external = tmp_path / "outside"
    external.mkdir()
    project = tmp_path / "project"
    project.mkdir()
    (project / "Lakehouse").symlink_to(external, target_is_directory=True)
    with pytest.raises(InitialiseError, match="symbolic link"):
        setup(project, fabric)
    assert not fabric.created and not list(external.iterdir())


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
