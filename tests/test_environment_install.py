"""Package-preserving Fabric Environment publication without Fabric."""

from __future__ import annotations

import types
import zipfile
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric import environment as env_mod
from weaver.fabric.client import FabricClient, FabricError
from weaver.fabric.environment import (
    _version_from_wheel,
    delete_stale_wheels,
    find_existing_environment,
    is_weaver_wheel,
    read_published,
    read_staging,
    resolve_environment_owner,
    staged_wheels,
)
from weaver.fabric.environment_packages import (
    EnvironmentPackageConflict,
    ResolvedWheel,
    describe_wheel_closure,
    inspect_libraries,
    plan_requirements,
)
from weaver.fabric.resources import Item, ItemNotFoundError, WorkspaceItem


def _env() -> Item:
    return Item(id="env1", name="Runtime", type="Environment", workspace_id="ws1")


def _libraries(*entries) -> dict:
    return {"libraries": list(entries)}


def _external(name: str, version: str) -> dict:
    return {"name": name, "libraryType": "External", "version": version}


def _custom(filename: str) -> dict:
    return {"name": filename, "libraryType": "Custom"}


def _wheel(
    name: str,
    version: str,
    required: str = "",
    *,
    dependencies=(),
    top_level=True,
) -> ResolvedWheel:
    filename = f"{name.replace('-', '_')}-{version}-py3-none-any.whl"
    return ResolvedWheel(
        name,
        version,
        filename,
        Path(filename),
        required,
        tuple(dependencies),
        top_level,
    )


def _write_wheel(path: Path, name: str, version: str, requires=()) -> None:
    metadata = [
        "Metadata-Version: 2.1",
        f"Name: {name}",
        f"Version: {version}",
        *(f"Requires-Dist: {requirement}" for requirement in requires),
        "",
    ]
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            f"{name.replace('-', '_')}-{version}.dist-info/METADATA",
            "\n".join(metadata),
        )


@weaver_test()
def test_only_weaver_wheels_are_owned():
    assert is_weaver_wheel("weaverstack-0.1.0-py3-none-any.whl")
    assert not is_weaver_wheel("sqlparse-0.5.3-py3-none-any.whl")
    assert not is_weaver_wheel("weaverstack-0.1.0.tar.gz")


@weaver_test()
def test_version_is_read_from_a_wheel_filename():
    assert _version_from_wheel("weaverstack-0.1.0-py3-none-any.whl") == "0.1.0"


@weaver_test()
def test_wheel_closure_carries_transitive_constraints(tmp_path):
    _write_wheel(
        tmp_path / "alpha-1.0-py3-none-any.whl",
        "alpha",
        "1.0",
        requires=("beta>=2",),
    )
    _write_wheel(tmp_path / "beta-2.1-py3-none-any.whl", "beta", "2.1")
    closure = describe_wheel_closure(tmp_path, ("alpha>=1",))
    by_name = {wheel.name: wheel for wheel in closure}
    assert by_name["alpha"].required == ">=1"
    assert by_name["alpha"].dependencies == ("beta",)
    assert by_name["alpha"].top_level is True
    assert by_name["beta"].required == ">=2"
    assert by_name["beta"].top_level is False


@weaver_test()
def test_ga_custom_libraries_are_read_by_name():
    libraries = _libraries(
        _custom("weaverstack-0.1.0-py3-none-any.whl"),
        _external("pyyaml", "6.0.2"),
    )
    assert staged_wheels(libraries) == [
        "weaverstack-0.1.0-py3-none-any.whl"
    ]


class _ReadClient:
    def __init__(self, response=None, status_code=None):
        self.response = response
        self.status_code = status_code
        self.paths = []

    def get_json(self, path):
        self.paths.append(path)
        if self.status_code is not None:
            raise FabricError("library lookup failed", status_code=self.status_code)
        return self.response


@weaver_test()
def test_library_reads_request_the_ga_contract():
    client = _ReadClient(_libraries())
    assert read_published(_env(), client=client) == _libraries()
    assert read_staging(_env(), client=client) == _libraries()
    assert client.paths == [
        "workspaces/ws1/environments/env1/libraries?beta=false",
        "workspaces/ws1/environments/env1/staging/libraries?beta=false",
    ]


@weaver_test()
def test_unpublished_library_lists_are_empty():
    client = _ReadClient(status_code=404)
    assert read_published(_env(), client=client) == {}
    assert read_staging(_env(), client=client) == {}


@pytest.mark.parametrize("status_code", [401, 403, 429, 500])
@weaver_test()
def test_library_failures_other_than_not_found_are_preserved(status_code):
    with pytest.raises(FabricError, match="lookup failed"):
        read_published(_env(), client=_ReadClient(status_code=status_code))


@weaver_test()
def test_environment_reference_owner_resolution():
    owner, reference = resolve_environment_owner("Analytics", "Runtime")
    assert owner == "Analytics"
    assert (reference.workspace, reference.name) == (None, "Runtime")

    owner, reference = resolve_environment_owner(None, "Platform/Runtime")
    assert owner == "Platform"
    assert (reference.workspace, reference.name) == ("Platform", "Runtime")


@weaver_test()
def test_qualified_environment_conflicting_with_workspace_is_rejected():
    with pytest.raises(CommandError, match="conflicts"):
        resolve_environment_owner("Analytics", "Platform/Runtime")


@weaver_test()
def test_publish_resolves_an_existing_environment_without_creation(monkeypatch):
    workspace = WorkspaceItem("ws1", "Analytics")
    monkeypatch.setattr(env_mod, "find_workspace", lambda *a, **k: workspace)
    monkeypatch.setattr(
        env_mod,
        "find_item",
        lambda *a, **k: Item("env1", "Runtime", "Environment", "ws1"),
    )
    found_workspace, found_environment = find_existing_environment(
        "Analytics", "Runtime", client=object()
    )
    assert found_workspace is workspace
    assert found_environment.id == "env1"


@weaver_test()
def test_missing_environment_names_the_fabric_provisioning_action(monkeypatch):
    monkeypatch.setattr(
        env_mod,
        "find_workspace",
        lambda *a, **k: WorkspaceItem("ws1", "Analytics"),
    )
    monkeypatch.setattr(
        env_mod,
        "find_item",
        lambda *a, **k: (_ for _ in ()).throw(ItemNotFoundError("missing")),
    )
    with pytest.raises(CommandError, match="Create the Environment in Fabric first"):
        find_existing_environment("Analytics", "Runtime", client=object())


class _DeleteClient:
    def __init__(self):
        self.deleted: list[str] = []

    def request(self, method, path, *, expected=()):
        assert method == "DELETE"
        self.deleted.append(path)


@weaver_test()
def test_stale_cleanup_uses_ga_paths_and_deletes_only_weaver_wheels():
    client = _DeleteClient()
    removed = delete_stale_wheels(
        _env(),
        "weaverstack-0.2.0-py3-none-any.whl",
        [
            "weaverstack-0.1.0-py3-none-any.whl",
            "networkx-3.4.2-py3-none-any.whl",
            "company_lib-1.0-py3-none-any.whl",
        ],
        client=client,
    )
    assert removed == ["weaverstack-0.1.0-py3-none-any.whl"]
    assert client.deleted == [
        "workspaces/ws1/environments/env1/staging/libraries/"
        "weaverstack-0.1.0-py3-none-any.whl"
    ]
    assert all(is_weaver_wheel(path.rsplit("/", 1)[-1]) for path in client.deleted)


@weaver_test()
def test_wheel_upload_uses_the_ga_binary_endpoint(monkeypatch, tmp_path):
    wheel = tmp_path / "dependency-1.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel bytes")
    seen = {}

    def send(method, url, **kwargs):
        seen.update(method=method, url=url, **kwargs)
        return types.SimpleNamespace(status_code=201, text="")

    monkeypatch.setattr(env_mod, "send", send)
    client = types.SimpleNamespace(
        api_base_url="https://fabric.example/v1", token="token", timeout=60
    )
    env_mod.upload_wheel(_env(), wheel, client=client)

    assert seen["method"] == "POST"
    assert seen["url"].endswith(
        "/workspaces/ws1/environments/env1/staging/libraries/"
        "dependency-1.0-py3-none-any.whl"
    )
    assert seen["headers"]["Content-Type"] == "application/octet-stream"
    assert seen["data"] == b"wheel bytes"


@weaver_test()
def test_ga_library_inspection_normalizes_external_and_custom_packages():
    packages = inspect_libraries(
        _libraries(
            _external("Foo_Bar", "2.1"),
            _custom("other.package-3.0-py3-none-any.whl"),
            {"name": "helpers.jar", "libraryType": "Custom"},
        )
    )
    assert [(item.name, item.version, item.source) for item in packages] == [
        ("foo-bar", "2.1", "external"),
        ("other-package", "3.0", "custom-wheel"),
    ]


@weaver_test()
def test_compatible_published_packages_are_reused():
    plan = plan_requirements(
        [_wheel("pyyaml", "6.0.2", ">=6")],
        published=_libraries(_external("PyYAML", "6.0.2")),
        staging=_libraries(),
    )
    assert plan.upload == ()
    assert plan.reused == ("pyyaml==6.0.2",)
    assert plan.requirements[0].source == "external"
    assert plan.needs_publish is False


@weaver_test()
def test_unpinned_external_package_satisfies_an_unbounded_requirement():
    plan = plan_requirements(
        [_wheel("pyyaml", "6.0.3")],
        published=_libraries(_external("pyyaml", "")),
        staging=_libraries(),
    )
    assert plan.upload == ()
    assert plan.reused == ("pyyaml (unversioned)",)
    assert plan.requirements[0].resolved_version == "unspecified"


@weaver_test()
def test_unpinned_external_package_cannot_prove_a_version_bound():
    with pytest.raises(EnvironmentPackageConflict):
        plan_requirements(
            [_wheel("pyyaml", "6.0.3", ">=6")],
            published=_libraries(_external("pyyaml", "")),
            staging=_libraries(),
        )


@weaver_test()
def test_compatible_user_custom_wheel_is_reused():
    plan = plan_requirements(
        [_wheel("sqlparse", "0.5.3", ">=0.5")],
        published=_libraries(_custom("sqlparse-0.5.2-py3-none-any.whl")),
        staging=_libraries(),
    )
    assert plan.upload == ()
    assert plan.requirements[0].resolved_version == "0.5.2"
    assert plan.requirements[0].source == "custom-wheel"


@weaver_test()
def test_external_requirement_reuses_fabrics_resolved_dependency_closure():
    root = _wheel(
        "mssql-python", "1.13.0", dependencies=("cryptography",)
    )
    dependency = _wheel(
        "cryptography", "50.0.1", ">=2.5", top_level=False
    )
    plan = plan_requirements(
        [root, dependency],
        published=_libraries(_external("mssql-python", "")),
        staging=_libraries(_external("mssql-python", "")),
    )
    assert plan.upload == ()
    assert [requirement.name for requirement in plan.requirements] == [
        "mssql-python"
    ]


@weaver_test()
def test_missing_custom_requirement_stages_its_complete_closure():
    root = _wheel("alpha", "1.0", dependencies=("beta",))
    dependency = _wheel("beta", "2.0", ">=2", top_level=False)
    plan = plan_requirements(
        [root, dependency], published=_libraries(), staging=_libraries()
    )
    assert [wheel.name for wheel in plan.upload] == ["alpha", "beta"]


@weaver_test()
def test_existing_custom_requirement_stages_its_missing_dependencies():
    root = _wheel("alpha", "1.0", dependencies=("beta",))
    dependency = _wheel("beta", "2.0", ">=2", top_level=False)
    plan = plan_requirements(
        [root, dependency],
        published=_libraries(_custom(root.filename)),
        staging=_libraries(_custom(root.filename)),
    )
    assert [wheel.name for wheel in plan.upload] == ["beta"]


@weaver_test()
def test_missing_requirement_selects_an_additive_wheel():
    wheel = _wheel("sqlparse", "0.5.3", ">=0.5")
    plan = plan_requirements(
        [wheel], published=_libraries(), staging=_libraries()
    )
    assert plan.upload == (wheel,)
    assert plan.requirements[0].source == "weaver-injected"
    assert plan.needs_publish is True


@weaver_test()
def test_compatible_pending_wheel_finishes_without_duplicate_upload():
    plan = plan_requirements(
        [_wheel("sqlparse", "0.5.3", ">=0.5")],
        published=_libraries(),
        staging=_libraries(_custom("sqlparse-0.5.3-py3-none-any.whl")),
    )
    assert plan.upload == ()
    assert plan.needs_publish is True


@pytest.mark.parametrize(
    ("published", "staging"),
    [
        (_libraries(_external("sqlparse", "0.4.0")), _libraries()),
        (_libraries(), _libraries(_custom("sqlparse-0.4.0-py3-none-any.whl"))),
        (
            _libraries(_external("sqlparse", "0.5.3")),
            _libraries(_custom("sqlparse-0.4.0-py3-none-any.whl")),
        ),
    ],
)
@weaver_test()
def test_incompatible_packages_are_rejected(published, staging):
    with pytest.raises(EnvironmentPackageConflict):
        plan_requirements(
            [_wheel("sqlparse", "0.5.3", ">=0.5")],
            published=published,
            staging=staging,
        )


def _wire_publish(
    monkeypatch,
    *,
    published: dict,
    staging: dict,
    closure: tuple[ResolvedWheel, ...],
    wheel_name: str,
):
    events = {"uploads": [], "deletes": [], "publishes": 0}
    monkeypatch.setattr(
        env_mod,
        "find_existing_environment",
        lambda *a, **k: (WorkspaceItem("ws1", "Analytics"), _env()),
    )
    monkeypatch.setattr(env_mod, "read_published", lambda *a, **k: published)
    monkeypatch.setattr(env_mod, "read_staging", lambda *a, **k: staging)
    monkeypatch.setattr(
        env_mod, "resolve_wheel_closure", lambda *a, **k: closure
    )
    monkeypatch.setattr(
        env_mod, "build_wheel", lambda *a, **k: Path("dist") / wheel_name
    )
    monkeypatch.setattr(
        env_mod,
        "upload_wheel",
        lambda _env, wheel, **k: events["uploads"].append(wheel.name),
    )
    monkeypatch.setattr(
        env_mod,
        "delete_stale_wheels",
        lambda _env, _keep, staged, **k: events["deletes"].extend(
            name for name in staged if is_weaver_wheel(name) and name != _keep
        ),
    )

    def publish(*args, **kwargs):
        events["publishes"] += 1
        return "Success"

    monkeypatch.setattr(env_mod, "publish_and_wait", publish)
    return events


@weaver_test()
def test_satisfied_environment_is_an_idempotent_noop(monkeypatch):
    current = "weaverstack-0.2.0-py3-none-any.whl"
    closure = (_wheel("pyyaml", "6.0.2", ">=6"),)
    published = _libraries(_external("pyyaml", "6.0.2"), _custom(current))
    staging = _libraries(
        _external("pyyaml", "6.0.2"),
        _custom(current),
        _custom("company_lib-1.0-py3-none-any.whl"),
    )
    events = _wire_publish(
        monkeypatch,
        published=published,
        staging=staging,
        closure=closure,
        wheel_name=current,
    )
    result = env_mod.publish_environment("Analytics", "Runtime", client=object())
    assert result.publish_status == "AlreadyInstalled"
    assert result.published is False
    assert result.wheel_changed is False
    assert events == {"uploads": [], "deletes": [], "publishes": 0}


@weaver_test()
def test_missing_dependency_and_new_weaver_are_staged_then_published(monkeypatch):
    old = "weaverstack-0.1.0-py3-none-any.whl"
    current = "weaverstack-0.2.0-py3-none-any.whl"
    dependency = _wheel("sqlparse", "0.5.3", ">=0.5")
    events = _wire_publish(
        monkeypatch,
        published=_libraries(_custom(old), _custom("company_lib-1.0-py3-none-any.whl")),
        staging=_libraries(_custom(old), _custom("company_lib-1.0-py3-none-any.whl")),
        closure=(dependency,),
        wheel_name=current,
    )
    result = env_mod.publish_environment("Analytics", "Runtime", client=object())
    assert events["uploads"] == [dependency.filename, current]
    assert events["deletes"] == [old]
    assert events["publishes"] == 1
    assert result.staged_dependency_wheels == (dependency.filename,)
    assert result.published is True


@weaver_test()
def test_package_conflict_fails_before_build_or_mutation(monkeypatch):
    dependency = _wheel("sqlparse", "0.5.3", ">=0.5")
    events = _wire_publish(
        monkeypatch,
        published=_libraries(_external("sqlparse", "0.4.0")),
        staging=_libraries(),
        closure=(dependency,),
        wheel_name="weaverstack-0.2.0-py3-none-any.whl",
    )
    monkeypatch.setattr(
        env_mod,
        "build_wheel",
        lambda *a, **k: pytest.fail("built after a detectable conflict"),
    )
    with pytest.raises(CommandError, match="No Environment changes were staged"):
        env_mod.publish_environment("Analytics", "Runtime", client=object())
    assert events == {"uploads": [], "deletes": [], "publishes": 0}


@weaver_test()
def test_fabric_client_preserves_failure_status(monkeypatch):
    response = types.SimpleNamespace(
        status_code=429, text="slow down", content=b"", headers={}
    )
    attempts: list = []
    monkeypatch.setattr(
        "requests.request", lambda *args, **kwargs: attempts.append(1) or response
    )
    monkeypatch.setattr("weaver.fabric.client.time.sleep", lambda _seconds: None)
    with pytest.raises(FabricError) as info:
        FabricClient(token="token").get_json("workspaces")
    assert info.value.status_code == 429
    assert len(attempts) == 4


@weaver_test()
def test_a_long_publish_reports_the_last_state(monkeypatch):
    monkeypatch.setattr(env_mod, "publish_state", lambda *a, **k: "Running")
    monkeypatch.setattr(env_mod.time, "sleep", lambda _seconds: None)

    class _Client:
        def request(self, *args, **kwargs):
            return None

    with pytest.raises(FabricError, match="'Running'"):
        env_mod.publish_and_wait(
            _env(), client=_Client(), timeout=0.01, poll_interval=0
        )
