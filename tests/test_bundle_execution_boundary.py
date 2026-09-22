"""A bundle is the whole deployment intent, and install reads it.

The workspace, the catalogue, the Environment and the Spark attachment are
frozen when the bundle is generated. Whoever installs it supplies credentials,
transport and reusable resources, and nothing else. These claims are about the
routing that follows from that, so they watch where capability calls actually
go rather than only that the manifest has the fields.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
import yaml
from support.bundles import CATALOGUE_TARGET
from support.sessions import given_session
from support.weaver_test import weaver_test

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildPlan,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
    Installer,
    compute_bundle_id,
    load_bundle,
    write_bundle,
)
from weaver.build_bundle.bundle import SUPPORTED_FORMAT_VERSION, plan_to_yaml
from weaver.build_bundle.execution import (
    BundleEnvironment,
    BundleExecution,
    execution_spark_home,
    execution_workspace,
)
from weaver.build_bundle.workflow import persist_bundle_archive
from weaver.errors import BuildError, CommandError
from weaver.locations import Location
from weaver.operations.install import install
from weaver.sessions.base import WorkspaceScope
from weaver.store import FilesystemStore
from weaver.workspaces import Workspace

HOME = BoundTarget(
    id="lakehouse-Sales_LH",
    kind="lakehouse",
    item_id="Sales_LH",
    item_name="Sales_LH",
)
OTHER = BoundTarget(
    id="lakehouse-Archive_LH",
    kind="lakehouse",
    item_id="Archive_LH",
    item_name="Archive_LH",
)

#: A Session whose own configuration disagrees with every bundle here.
ELSEWHERE = Workspace(
    workspace="Reporting", catalogue="Warehouse/Other", environment="Nightly"
)


class Recorder:
    """Stands in for one executor and records the actions it was given."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[str] = []

    def execute(self, action, payload, context):
        self.calls.append(action.id)
        return {"ran": action.id}


def _session(workspace):
    """A recording Session holding the resources the caller owns.

    Its inventory holds every item any bundle here names, so what a test proves
    is where calls were routed rather than whether a name resolved.
    """

    return given_session(
        workspace=workspace,
        store=FilesystemStore(),
        lakehouses=("Sales_LH", "Archive_LH", "Nightly_LH"),
        warehouses=("Weaver",),
    )


def _action(name: str, executor: str = "spark_sql") -> InstallAction:
    extension = ".spark.sql" if executor == "spark_sql" else ".sql"
    return InstallAction(
        id=name,
        kind="materialise",
        resource_node_id=None,
        executor=executor,
        payload=f"payload/{name}/stmt{extension}",
        payload_sha256=None,
    )


def _written(tmp_path, *, execution, targets, actions, name="bundle"):
    """Write and reload a bundle carrying exactly ``execution``."""

    payloads = {}
    filled = []
    for index, action in enumerate(actions):
        data = f"select {index}\n".encode("utf-8")
        payloads[action.payload] = data
        filled.append(replace(action, payload_sha256=hashlib.sha256(data).hexdigest()))
    sequences = (
        BuildSequence(
            number=10,
            description="build dependency layer",
            batches=(
                BuildBatch(id="b1", target_id=targets[0].id, actions=tuple(filled)),
            ),
        ),
    )
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig",
        targets=tuple(targets),
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=execution,
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    location = Location(str(tmp_path / name))
    return write_bundle(location, plan=plan, payloads=payloads, store=FilesystemStore())


def _spark_bundle(tmp_path, *, environment=None, workspace_name="Sales", **kwargs):
    targets = (HOME, OTHER, CATALOGUE_TARGET)
    execution = BundleExecution(
        workspace_name=workspace_name,
        catalogue_target_id=CATALOGUE_TARGET.id,
        environment=environment,
        spark_home_target_id=HOME.id,
    )
    return _written(
        tmp_path,
        execution=execution,
        targets=targets,
        actions=(_action("a1"),),
        **kwargs,
    )


def _installed(bundle, session, *, executor="spark_sql"):
    recorder = Recorder(executor)
    report = Installer(session, executors={executor: recorder}).install(bundle)
    return report, recorder


# --- the bundle decides where it lands ----------------------------------------


@weaver_test()
def test_a_bundle_installs_where_it_was_built_not_where_it_is_run(tmp_path):
    """The Session's own workspace, catalogue and Environment are all different.

    Every capability call still names the bundle's workspace, so moving the
    bundle to another machine or directory cannot move the deployment.
    """

    bundle = _spark_bundle(tmp_path, environment=BundleEnvironment(name="Runtime"))
    session = _session(ELSEWHERE)

    report, _ = _installed(bundle, session)

    assert report.succeeded
    scope = session.scope(execution_workspace(bundle.plan.execution, bundle.plan))
    assert scope.workspace.workspace == "Sales"
    assert scope.workspace.catalogue == "Warehouse/Weaver"
    assert str(scope.workspace.environment) == "Runtime"
    assert scope.spark_home == "Sales_LH"


@weaver_test()
def test_an_earlier_spark_home_offer_does_not_capture_the_attachment(tmp_path):
    """Offers accumulate per scope and ``Archive_LH`` sorts first.

    An installation that took the first offered name would attach a Spark
    session to a Lakehouse this bundle never chose.
    """

    bundle = _spark_bundle(tmp_path)
    workspace = execution_workspace(bundle.plan.execution, bundle.plan)
    session = _session(workspace)
    session.offer_spark_home(["Archive_LH"], workspace=workspace)

    _installed(bundle, session)

    assert session.scope(workspace).spark_home == "Sales_LH"


@weaver_test()
def test_a_sessions_own_workspace_never_supplies_the_attachment(tmp_path):
    """A different scope key is not the bundle's scope, and cannot lend to it."""

    bundle = _spark_bundle(tmp_path)
    session = _session(ELSEWHERE)
    session.offer_spark_home(["Nightly_LH"], workspace=ELSEWHERE)

    _installed(bundle, session)

    workspace = execution_workspace(bundle.plan.execution, bundle.plan)
    assert session.scope(workspace).spark_home == "Sales_LH"
    assert session.scope(ELSEWHERE).spark_home == "Nightly_LH"


@weaver_test()
def test_the_public_operation_reuses_a_session_and_routes_by_the_manifest(tmp_path):
    """The whole path, through the real executors.

    The Session is borrowed, so it stays open, and the Spark SQL it was asked
    to run names the bundle's workspace rather than the Session's own.
    """

    bundle = _spark_bundle(tmp_path)
    session = _session(ELSEWHERE)

    report = install(bundle.location, session=session)

    assert report.succeeded
    assert not session.closed
    (recorded,) = [call for call in session.calls if call.kind == "spark_sql"]
    assert recorded.workspace == "Sales"


# --- an incompatible live resource fails before the first action ---------------


class _AttachedScope(WorkspaceScope):
    """A scope whose Spark session is already attached somewhere."""

    attached = "Archive_LH"

    def attached_spark_home(self) -> str | None:
        return self.attached


@weaver_test()
def test_a_spark_session_attached_elsewhere_is_refused_before_any_action(tmp_path):
    bundle = _spark_bundle(tmp_path)
    session = _session(ELSEWHERE)
    session._new_scope = lambda workspace: _AttachedScope(  # noqa: SLF001
        workspace, telemetry=session.telemetry, executor=session._executor
    )
    recorder = Recorder("spark_sql")

    with pytest.raises(CommandError, match="attached to Lakehouse 'Archive_LH'"):
        Installer(session, executors={"spark_sql": recorder}).install(bundle)

    assert recorder.calls == []


@weaver_test()
def test_a_scope_that_started_nothing_takes_the_next_bundles_attachment(tmp_path):
    """One session serves one piece of work after another.

    Until Spark is running there is nothing to disagree with: a mirror, or a
    second build into another Lakehouse, is ordinary and would be refused by a
    requirement that outlived the work that made it.
    """

    bundle = _spark_bundle(tmp_path)
    workspace = execution_workspace(bundle.plan.execution, bundle.plan)
    session = _session(workspace)
    session.require_spark_home("Archive_LH", workspace=workspace)

    session.require_spark_home("Sales_LH", workspace=workspace)

    assert session.scope(workspace).spark_home == "Sales_LH"
    assert _installed(bundle, session)[0].succeeded


# --- Warehouse-only work stays off Spark ---------------------------------------


@weaver_test()
def test_a_warehouse_only_bundle_freezes_no_attachment_and_asks_for_none(tmp_path):
    execution = BundleExecution(
        workspace_name="Sales",
        catalogue_target_id=CATALOGUE_TARGET.id,
        spark_home_target_id=None,
    )
    bundle = _written(
        tmp_path,
        execution=execution,
        targets=(CATALOGUE_TARGET,),
        actions=(_action("t1", executor="tsql"),),
    )
    workspace = execution_workspace(execution, bundle.plan)
    session = _session(workspace)

    report, recorder = _installed(bundle, session, executor="tsql")

    assert report.succeeded
    assert recorder.calls == ["t1"]
    assert execution_spark_home(execution, bundle.plan) is None
    assert session.scope(workspace).spark_home is None
    assert session.spark_sql == ()


@weaver_test()
def test_install_declares_no_ambient_requirement_to_warm():
    """Install's requirements live in its bundle, which is not read when a
    shell or a workflow warms shared resources."""

    from weaver_cli.main import build_parser, command_requirements

    parsed = build_parser().parse_args(["install", "handover"])

    assert command_requirements(parsed) == frozenset()


# --- the descriptor is part of what a bundle is --------------------------------


@weaver_test()
def test_changing_the_environment_changes_the_bundle_identity(tmp_path):
    plain = _spark_bundle(tmp_path, name="plain")
    named = _spark_bundle(
        tmp_path, environment=BundleEnvironment(name="Runtime"), name="named"
    )

    assert plain.bundle_id != named.bundle_id


@weaver_test()
def test_changing_the_workspace_changes_the_bundle_identity(tmp_path):
    here = _spark_bundle(tmp_path, name="here")
    there = _spark_bundle(tmp_path, workspace_name="Sales_Dev", name="there")

    assert here.bundle_id != there.bundle_id


@weaver_test()
@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("catalogue_target_id", "warehouse-Missing", "does not declare"),
        ("catalogue_target_id", HOME.id, "not a Warehouse"),
        ("spark_home_target_id", "lakehouse-Missing", "does not declare"),
        ("spark_home_target_id", CATALOGUE_TARGET.id, "not a Lakehouse"),
        ("spark_home_target_id", None, "no Lakehouse for the Spark session"),
        ("workspace_name", "", "does not name the workspace"),
    ],
)
def test_a_tampered_descriptor_is_refused_before_any_target_access(
    tmp_path, field, value, message
):
    bundle = _spark_bundle(tmp_path)
    tampered = replace(bundle.plan.execution, **{field: value})
    plan = replace(bundle.plan, execution=tampered)
    store = FilesystemStore()
    store.write(bundle.location.join("plan.yml"), plan_to_yaml(plan).encode("utf-8"))

    with pytest.raises(BuildError, match=message):
        load_bundle(bundle.location, store=store)


@weaver_test()
def test_a_descriptor_and_a_target_cannot_name_different_workspaces(tmp_path):
    bundle = _spark_bundle(tmp_path)
    targets = tuple(
        replace(target, workspace_name="Reporting") for target in bundle.plan.targets
    )
    plan = replace(bundle.plan, targets=targets)
    store = FilesystemStore()
    store.write(bundle.location.join("plan.yml"), plan_to_yaml(plan).encode("utf-8"))

    with pytest.raises(BuildError, match="but sends target"):
        load_bundle(bundle.location, store=store)


# --- the format change is explicit ---------------------------------------------


@weaver_test()
def test_a_bundle_without_an_execution_descriptor_asks_to_be_regenerated(tmp_path):
    """A format-3 bundle has no descriptor at all.

    The version is checked before deserialisation, so the operator is told to
    regenerate rather than shown a missing field.
    """

    bundle = _spark_bundle(tmp_path)
    mapping = bundle.plan.to_mapping()
    mapping["format_version"] = 3
    mapping.pop("execution")
    store = FilesystemStore()
    store.write(
        bundle.location.join("plan.yml"), yaml.safe_dump(mapping).encode("utf-8")
    )

    with pytest.raises(BuildError) as refused:
        load_bundle(bundle.location, store=store)

    assert "Bundle format version 3 is not supported" in str(refused.value)
    assert "Regenerate the bundle" in str(refused.value)


@weaver_test()
def test_the_descriptor_round_trips_and_serialises_deterministically(tmp_path):
    bundle = _spark_bundle(
        tmp_path, environment=BundleEnvironment(name="Runtime", workspace="Platform")
    )

    restored = load_bundle(bundle.location, store=FilesystemStore()).plan

    assert restored.execution == bundle.plan.execution
    assert plan_to_yaml(restored) == plan_to_yaml(bundle.plan)
    assert compute_bundle_id(restored) == bundle.bundle_id


# --- Environment references -----------------------------------------------------


@weaver_test()
def test_explicitly_no_environment_is_installed_with_no_environment(tmp_path):
    bundle = _spark_bundle(tmp_path)

    workspace = execution_workspace(bundle.plan.execution, bundle.plan)

    assert bundle.plan.execution.environment is None
    assert workspace.environment is None


@weaver_test()
def test_a_qualified_environment_keeps_its_owning_workspace(tmp_path):
    bundle = _spark_bundle(
        tmp_path, environment=BundleEnvironment(name="Runtime", workspace="Platform")
    )

    workspace = execution_workspace(bundle.plan.execution, bundle.plan)

    assert workspace.environment.workspace == "Platform"
    assert workspace.environment.name == "Runtime"
    assert str(workspace.environment) == "Platform/Runtime"


# --- every installation route reads the same descriptor -------------------------


@weaver_test()
def test_a_directory_and_its_archive_install_into_the_same_place(tmp_path):
    bundle = _spark_bundle(tmp_path, environment=BundleEnvironment(name="Runtime"))
    archive = persist_bundle_archive(
        bundle,
        Location(str(tmp_path / "handover.weaver.zip")),
        store=FilesystemStore(),
    )

    from weaver.build_bundle import materialise_bundle_archive

    with materialise_bundle_archive(archive, store=FilesystemStore()) as extracted:
        assert extracted.plan.execution == bundle.plan.execution
        assert extracted.bundle_id == bundle.bundle_id


@weaver_test()
def test_the_low_level_archive_helper_takes_no_workspace(tmp_path):
    """``install_bundle_archive`` had an ambient-workspace bypass.

    A helper that accepted one could install a bundle somewhere its manifest
    does not name.
    """

    import inspect

    from weaver.build_bundle.workflow import install_bundle_archive

    parameters = inspect.signature(install_bundle_archive).parameters

    assert "workspace" not in parameters


@weaver_test()
def test_the_installer_installs_a_loaded_bundle_and_not_a_location():
    """Reading one needs the store it lives on, and a bundle's own store is not
    the workspace store it installs into. The caller says which."""

    import inspect

    from weaver.build_bundle import BuildBundle

    annotation = inspect.signature(Installer.install).parameters["bundle"].annotation

    assert annotation is BuildBundle or annotation == "BuildBundle"


@weaver_test()
def test_the_installer_takes_no_workspace_of_its_own():
    import inspect

    assert "workspace" not in inspect.signature(Installer.__init__).parameters


@weaver_test()
def test_a_workspace_bound_by_hand_cannot_redirect_a_bundle(tmp_path):
    """``bind`` is for a caller assembling one action's context without a
    manifest. Installing one overwrites it, so it can redirect nothing."""

    bundle = _spark_bundle(tmp_path)
    session = _session(ELSEWHERE)
    installer = Installer(session, executors={"spark_sql": Recorder("spark_sql")})

    installer.bind(ELSEWHERE)
    assert installer.workspace is ELSEWHERE

    installer.install(bundle)

    assert installer.workspace.workspace == "Sales"


# --- one build makes one attachment decision -----------------------------------
#
# Fabric attaches a Livy session to one Lakehouse, and a build binds several. A
# build that offered them all would attach to whichever physical name sorted
# first, while the bundle freezes the target of the logical item that sorts
# first. Where the two orders disagree the build's own installation is refused
# by the Spark session the build itself started, so the choice is made once,
# before anything can acquire Spark, and read from the same order both times.

#: Logical order and physical name order disagree, which is the whole point.
TWO_LAKEHOUSES = (("Lakehouse/Alpha", "Zulu_LH"), ("Lakehouse/Beta", "Alpha_LH"))


class _BuildHalted(Exception):
    """Raised at the platform seam, once the build has settled its attachment."""


def _two_lakehouse_repository(tmp_path):
    """One repository authoring both items, each with a table to build."""

    from factories import _write, lakehouse_table, schema_document

    from weaver.declaration.repository import parse_item_repository

    root = tmp_path / "estate"
    for logical, _physical in TWO_LAKEHOUSES:
        _write(root, f"{logical}/schemas/DWG.yml", schema_document("DWG"))
        _write(
            root,
            f"{logical}/Tables/DWG__Customer.py",
            lakehouse_table("DWG.Customer"),
        )
    return root, parse_item_repository(Location(str(root)))


def _two_lakehouse_bindings():
    from factories import item_bindings

    from weaver.build_bundle import effective_item_bindings
    from weaver.targets import ItemRef

    return effective_item_bindings(
        item_bindings(*TWO_LAKEHOUSES, workspace_name="Sales"),
        control_item=ItemRef("Weaver"),
        workspace_name="Sales",
    )


def _two_lakehouse_session(workspace, *, root=None):
    from support.workspaces import given_resolver

    return given_session(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=given_resolver(
            workspace=workspace,
            lakehouses=("Zulu_LH", "Alpha_LH"),
            warehouses=("Weaver",),
            root=root,
        ),
    )


def _attached_to(session, workspace, lakehouse: str):
    """A Session whose Spark resource in this scope is already live."""

    session.scope(workspace).attached_spark_home = lambda: lakehouse
    return session


def _build_two(root, session):
    import weaver

    return weaver.build(
        str(root),
        items=[
            f"{logical}=Lakehouse/{physical}" for logical, physical in TWO_LAKEHOUSES
        ],
        session=session,
    )


@pytest.fixture
def halted(monkeypatch):
    """Stop each build at the platform seam, after it settles its attachment.

    Preflight is a separate claim with its own tests, and reaching it here would
    need a tenant.
    """

    import weaver.operations.build

    def halt(workspace, **kwargs):
        raise _BuildHalted("build")

    monkeypatch.setattr(weaver.operations.build, "_preflight", lambda *a, **k: None)
    monkeypatch.setattr(weaver.operations.build, "_run_build", halt)


@weaver_test()
def test_a_build_attaches_where_its_first_item_binds_not_where_the_name_sorts(
    tmp_path, halted
):
    """``Alpha_LH`` is the name that sorts first and is not the attachment."""

    root, _repository = _two_lakehouse_repository(tmp_path)
    workspace = Workspace(workspace="Sales", catalogue="Warehouse/Weaver")
    session = _two_lakehouse_session(workspace)

    with pytest.raises(_BuildHalted):
        _build_two(root, session)

    assert session.scope(workspace).spark_home == "Zulu_LH"


@weaver_test()
def test_the_bundle_freezes_the_attachment_the_build_already_required(tmp_path):
    """Generate and install in one Session, as a build does.

    The installer refuses a bundle whose attachment disagrees with the live
    one, so a build that chose differently from its own planner could not
    install what it just generated.
    """

    from factories import target_inventory

    from weaver.build_bundle import build_repository_bundle
    from weaver.build_bundle.execution import ExecutionIdentity
    from weaver.build_bundle.targets import WarehouseBinding
    from weaver.build_bundle.workflow import BuildState
    from weaver.catalogue.state import Catalogue
    from weaver.operations.build import _spark_home
    from weaver.targets import ItemRef

    root, repository = _two_lakehouse_repository(tmp_path)
    bindings = _two_lakehouse_bindings()
    workspace = Workspace(workspace="Sales", catalogue="Warehouse/Weaver")
    session = _two_lakehouse_session(workspace, root=tmp_path / "onelake")
    store = FilesystemStore()
    resolver = session.resolver(workspace)
    for item in ("Zulu_LH", "Alpha_LH"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))

    # What the build settles before anything can acquire Spark.
    required = _spark_home(bindings)
    session.require_spark_home(required, workspace=workspace)

    bundle = build_repository_bundle(
        repository,
        bindings=bindings,
        state=BuildState(
            catalogue=Catalogue(rows={}),
            target_inventories={
                binding.item: target_inventory(
                    target_id=binding.to_bound_target().id,
                    kind=binding.to_bound_target().kind,
                    target_name=binding.to_bound_target().name,
                )
                for binding in bindings.entries
            },
        ),
        source_store=FilesystemStore(),
        catalogue_binding=WarehouseBinding(
            warehouse=ItemRef("Weaver"), workspace_name="Sales"
        ),
        execution=ExecutionIdentity(workspace_name="Sales"),
        output=Location(str(tmp_path / "bundle")),
    )

    assert required == "Zulu_LH"
    assert execution_spark_home(bundle.plan.execution, bundle.plan) == required
    report = Installer(
        session,
        executors={
            name: Recorder(name)
            for name in (
                "spark_sql",
                "spark_sql_batch",
                "spark_table",
                "folder",
                "shortcut",
                "sql_endpoint_refresh",
                "tsql",
                "tsql_batch",
                "load_file",
                "runtime_state",
            )
        },
    ).install(bundle)
    assert report.succeeded


@weaver_test()
def test_a_reused_session_already_attached_where_the_build_needs_it_is_kept(
    tmp_path, halted
):
    """Reuse is the point of passing a Session; only a mismatch is a problem."""

    root, _repository = _two_lakehouse_repository(tmp_path)
    workspace = Workspace(workspace="Sales", catalogue="Warehouse/Weaver")
    session = _attached_to(_two_lakehouse_session(workspace), workspace, "Zulu_LH")

    with pytest.raises(_BuildHalted):
        _build_two(root, session)

    assert session.scope(workspace).spark_home == "Zulu_LH"


@weaver_test()
def test_a_build_into_a_session_attached_elsewhere_is_refused_before_it_plans(
    tmp_path, halted
):
    """The refusal arrives at the build, not at the install it would reach."""

    root, _repository = _two_lakehouse_repository(tmp_path)
    workspace = Workspace(workspace="Sales", catalogue="Warehouse/Weaver")
    session = _attached_to(_two_lakehouse_session(workspace), workspace, "Archive_LH")

    with pytest.raises(CommandError, match="attached to Lakehouse 'Archive_LH'"):
        _build_two(root, session)
