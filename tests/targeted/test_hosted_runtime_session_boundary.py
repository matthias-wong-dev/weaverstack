"""Console Build runtime admission completes before mutation transport."""

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace
from test_mutation_archive_install import build_plan_fixture

from weaver.errors import BuildError
from weaver.fabric.livy import StatementResult
from weaver.sessions import ConsoleSession


@weaver_test()
@pytest.mark.parametrize("compatible", [False, True])
def test_console_admission_probes_the_published_runtime_without_installing_code(
    compatible,
):
    from weaver.sessions.hosted_runtime import identity

    observed = identity() if compatible else None
    calls = []

    class Livy:
        lakehouse_name = "Sales"

        def run(self, source, **options):
            calls.append((source, options))
            assert "weaver.sessions.hosted_runtime" in source
            return StatementResult("", {"runtime": observed})

        def ensure_weaver(self):
            pytest.fail("Build admission must not inject or install Weaver")

    workspace = given_workspace(environment="weaver")
    plan, _ = build_plan_fixture()
    with ConsoleSession(workspace=workspace, livy=Livy(), progress=False) as session:
        if compatible:
            assert (
                session.require_build_runtime(plan=plan, workspace=workspace)
                == observed
            )
        else:
            with pytest.raises(BuildError, match="[Pp]ublish"):
                session.require_build_runtime(plan=plan, workspace=workspace)
    assert len(calls) == 1
    assert calls[0][1].get("retry_submission", True) is True


@weaver_test()
def test_remote_mutation_refuses_runtime_before_creating_any_transport_directory():
    from types import SimpleNamespace

    from weaver.locations import Location

    calls = []

    class Store:
        def make_directory(self, location):
            pytest.fail("Mutation transport was created before runtime admission")

    class Session(ConsoleSession):
        def require_build_runtime(self, *, plan=None, workspace=None):
            calls.append(plan)
            raise BuildError("Publish the target Environment")

        def scope(self, workspace=None):
            return SimpleNamespace(
                transport_store=Store(),
                resolver=SimpleNamespace(
                    files_root=lambda item: Location(
                        "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files"
                    )
                ),
            )

    plan, payloads = build_plan_fixture()
    with Session(workspace=given_workspace(), progress=False) as session:
        with pytest.raises(BuildError, match="Publish"):
            session.execute_mutation_in_fabric(plan, payloads)
    assert calls == [plan]


@weaver_test()
def test_native_spark_plan_admits_runtime_before_any_physical_driver(tmp_path):
    from dataclasses import replace

    from weaver.locations import Location
    from weaver.mutation.bundle import compute_bundle_id
    from weaver.sessions.testing import TestSession
    from weaver.store import FilesystemStore

    class Resolver:
        def folder_object(self, target, schema, name):
            return Location(str(tmp_path / "Files" / schema / name))

        def files_root(self, item):
            return Location(str(tmp_path / "Files"))

        def folder_root(self, target):
            return Location(str(tmp_path / "Files"))

    calls = []

    class Session(TestSession):
        def require_build_runtime(self, *, plan=None, workspace=None):
            calls.append(plan)
            raise BuildError("Publish the target Environment")

    plan, payloads = build_plan_fixture()
    plan = replace(
        plan,
        bundle_id="",
        execution=replace(plan.execution, spark_home_target_id="sales"),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    with Session(
        workspace=given_workspace(), store=FilesystemStore(), resolver=Resolver()
    ) as session:
        with pytest.raises(BuildError, match="Publish"):
            session.execute_mutation(plan, payloads)
    assert calls == [plan]


@weaver_test()
def test_mixed_powerbi_and_lakehouse_request_is_refused_before_provisioning_or_admission(
    tmp_path,
):
    import shutil
    from pathlib import Path

    from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE
    from support.workspaces import CreatingInventoryClient
    from test_report_build_cycle import prepared_project

    import weaver

    root, session, *_ = prepared_project(tmp_path)
    root = Path(root)
    shutil.copytree(LAKEHOUSE_JOURNEY_FIXTURE.path / "Lakehouse", root / "Lakehouse")
    client = CreatingInventoryClient(
        "Demo", [("Warehouse", "Catalogue"), ("Lakehouse", "Sales_Dev")]
    )
    session._resolver.client = client
    session._default_workspace = given_workspace(catalogue="Warehouse/Catalogue")
    session.require_build_runtime = lambda **options: pytest.fail(
        "Invalid mixed request reached runtime admission"
    )
    with session:
        with pytest.raises(BuildError, match="separate step"):
            weaver.build(
                root,
                items=[
                    "Lakehouse/Sales=Lakehouse/Sales_Dev",
                    "SemanticModel/Reporting=SemanticModel/Reporting_Dev",
                ],
                session=session,
            )
    assert not client.created


@weaver_test()
@pytest.mark.parametrize("mode", ["dry_run", "bundle_only", "warehouse"])
def test_planning_and_warehouse_only_builds_skip_hosted_runtime_admission(
    tmp_path, monkeypatch, mode
):
    from types import SimpleNamespace

    from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE, WAREHOUSE_ESTATE_FIXTURE

    import weaver
    from weaver.fabric import powerbi_items
    from weaver.operations import build as operation
    from weaver.sessions.testing import TestSession

    fixture = (
        WAREHOUSE_ESTATE_FIXTURE if mode == "warehouse" else LAKEHOUSE_JOURNEY_FIXTURE
    )
    source = fixture.disposable(tmp_path / "source")

    class Session(TestSession):
        def require_build_runtime(self, **options):
            pytest.fail("Planning or a no-Spark Build required hosted execution")

    marker = SimpleNamespace(succeeded=True)
    monkeypatch.setattr(operation, "_preflight", lambda *a, **k: None)
    monkeypatch.setattr(operation, "_run_build", lambda *a, **k: marker)
    monkeypatch.setattr(powerbi_items, "create_powerbi_items", lambda *a, **k: None)
    options = {} if mode == "warehouse" else {mode: True}
    with Session(workspace=given_workspace()) as session:
        assert (
            weaver.build(
                str(source.path),
                items=[f"{item}={item}" for item in fixture.items],
                session=session,
                **options,
            )
            is marker
        )
