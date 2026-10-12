import importlib
import json
from contextlib import contextmanager
from dataclasses import FrozenInstanceError

import pytest
from support.build_preview_guards import guard_preview
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import project
from test_semantic_model_catalogue_free_cycle import SELECTOR, session_without_catalogue

import weaver
from weaver.errors import CommandError
from weaver_cli.main import build_parser


@weaver_test()
def test_public_dry_run_returns_the_real_plan_without_execution(tmp_path, monkeypatch):
    root = project(tmp_path, False)
    session = session_without_catalogue()
    guards = guard_preview(session, monkeypatch)

    def forbidden(*args, **kwargs):
        pytest.fail("dry run reached mutation or dispatch")

    monkeypatch.setattr(session, "execute_mutation", forbidden)
    monkeypatch.setattr(session, "execute_run", forbidden)
    monkeypatch.setattr(session, "execute_python", forbidden)
    result = weaver.build(root, items=SELECTOR, session=session, dry_run=True)
    assert result.succeeded and result.dry_run and not result.installation
    assert result.bundle_path is None and result.report_path is None
    assert result.preview.plan.bundle_id == result.bundle_id
    mapping = json.loads(json.dumps(result.to_mapping()))
    assert mapping["dry_run"] is True
    assert mapping["preview"]["plan"]["bundle_id"] == result.bundle_id
    assert not session.semantic_model("Reporting_Dev").calls
    assert not session.tsql and not session.spark_sql
    assert not guards["forbidden_calls"]


@weaver_test()
def test_cli_accepts_dry_run_and_json():
    args = build_parser().parse_args(["build", ".", "--dry-run", "--json"])
    assert args.dry_run and args.json and not args.bundle_only


@weaver_test()
def test_dry_run_refuses_bundle_export_before_resolution(tmp_path):
    with pytest.raises(CommandError, match="dry_run.*bundle_only"):
        weaver.build(tmp_path, dry_run=True, bundle_only=True)


@pytest.mark.parametrize("native", [False, True])
@weaver_test()
def test_preview_matches_the_real_execution_plan_at_unchanged_inputs(
    tmp_path, monkeypatch, native
):
    from test_semantic_model_build_cycle import ITEM, engine_model

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location
    from weaver.semantic_models.definition import encode_definition

    root = project(tmp_path, False)
    session = session_without_catalogue()
    session._executes_here = native
    session.semantic_model("Reporting_Dev").definition = encode_definition(
        engine_model(parse_item_repository(Location(str(root))))
    )
    operation = importlib.import_module("weaver.operations.build")
    execute = operation.execute_bundle
    plans = []

    def observe(bundle, opened, **options):
        plans.append(bundle.plan)
        return execute(bundle, opened, **options)

    monkeypatch.setattr(operation, "execute_bundle", observe)
    first = weaver.build(root, items=SELECTOR, session=session, dry_run=True)
    assert not plans
    assert not first.preview.to_mapping()["certification"]["publish_after_success"]
    second = weaver.build(root, items=SELECTOR, session=session)
    assert second.succeeded, second.errors
    assert plans == [first.preview.plan]
    assert second.bundle_id == first.bundle_id
    assert str(ITEM) in {o["identity"] for o in first.preview.to_mapping()["objects"]}
    assert first.preview.to_mapping()["destructive"]
    with pytest.raises(FrozenInstanceError):
        first.preview.plan = plans[0]


@weaver_test()
def test_missing_powerbi_target_is_refused_without_creation(tmp_path, monkeypatch):
    from weaver.errors import BuildError
    from weaver.fabric.resources import ItemNotFoundError

    def forbidden(*args, **kwargs):
        pytest.fail("dry run created a Power BI target")

    monkeypatch.setattr("weaver.fabric.powerbi_items.create_powerbi_items", forbidden)
    with pytest.raises((BuildError, ItemNotFoundError)):
        weaver.build(
            project(tmp_path, False),
            items=SELECTOR,
            session=session_without_catalogue(()),
            dry_run=True,
        )


@pytest.mark.parametrize("json_output", [False, True])
@weaver_test()
def test_cli_drives_the_public_api_and_prints_preview(
    tmp_path, monkeypatch, capsys, json_output
):
    cli = importlib.import_module("weaver_cli.main")

    root = project(tmp_path, False)
    session = session_without_catalogue()

    @contextmanager
    def running(*args, **kwargs):
        yield session

    monkeypatch.setattr(cli, "_resolve_workspace", lambda args: session.workspace)
    monkeypatch.setattr(cli, "_running_session", running)
    args = build_parser().parse_args(
        ["build", str(root), "--item", SELECTOR, "--dry-run"]
        + (["--json"] if json_output else [])
    )
    assert cli.handle_build(args) == 0
    output = capsys.readouterr().out
    if json_output:
        mapping = json.loads(output)
        assert (
            mapping["dry_run"]
            and mapping["preview"]["plan"]["bundle_id"] == mapping["bundle_id"]
        )
    else:
        assert "Build plan (dry run)" in output and "Reporting_Dev" in output
    assert not session.semantic_model("Reporting_Dev").calls


@weaver_test()
def test_cli_conflicting_plan_modes_refused_before_opening_session(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")

    monkeypatch.setattr(
        cli, "_running_session", lambda *a, **k: pytest.fail("opened session")
    )
    args = build_parser().parse_args(["build", ".", "--dry-run", "--bundle-only"])
    with pytest.raises(CommandError, match="--dry-run.*--bundle-only"):
        cli.handle_build(args)


@pytest.mark.parametrize(
    "boundary",
    [
        "execute_mutation",
        "execute_run",
        "execute_python",
        "execute_tsql",
        "create_delta_table",
        "flusher",
        "rest",
        "onelake",
        "spark_sql",
    ],
)
@weaver_test()
def test_preview_observer_catches_writes_and_dispatch_offline(monkeypatch, boundary):
    from weaver.fabric.client import FabricClient
    from weaver.fabric.onelake import OneLakeDfsClient

    session = session_without_catalogue()
    guards = guard_preview(session, monkeypatch)
    with pytest.raises(AssertionError, match="write or action-dispatch"):
        if boundary == "rest":
            FabricClient.request(None, "POST", "workspaces/items")
        elif boundary == "onelake":
            OneLakeDfsClient.write(None, None, b"data")
        elif boundary == "spark_sql":
            session.execute_spark_sql("DROP TABLE sales.customer")
        else:
            getattr(session, boundary)()
    assert guards["forbidden_calls"] == ["write_or_dispatch"]
