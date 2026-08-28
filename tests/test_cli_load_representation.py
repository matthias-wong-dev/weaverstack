"""``weaver load``, the command-line surface, and what it delegates to.

Two claims, and they pull in opposite directions on purpose.

The first is that every option reaches :func:`weaver.load` unchanged: the CLI is
an adapter, so what a user types and what the API is called with must be the
same request. These are asserted against a recorded call rather than a real run,
because what is under test is the mapping, not the load.

The second is that the CLI owns nothing else. It resolves a workspace, crosses
the host boundary when it has to, renders what comes back and chooses an exit
code. It does not plan, validate targets, order a graph or decide what failed,
and the last test here says so by reading the source, because that is the kind of
rule that decays without saying so.

Pure Python throughout: no Spark, no workspace, no Livy. The desktop-to-Fabric
crossing is proved against a recording double, since what matters on this side
of the boundary is what was submitted and what was made of the answer.
"""

from __future__ import annotations

import json

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.errors import CommandError, LoadError
from weaver.load_report import (
    FAILED,
    SUCCEEDED,
    TASK_FAILED,
    TASK_SUCCEEDED,
    LoadNodeReport,
    LoadResult,
    LoadRunReport,
)
from weaver_cli.main import build_parser, main


def _cli_module():
    """The command module itself.

    ``weaver_cli.main`` is a function on the package as well as a submodule, and
    the function is what attribute access finds, so the module is asked for by
    name.
    """

    import sys

    import weaver_cli.main  # noqa: F401 - imported for its side effect on sys.modules

    return sys.modules["weaver_cli.main"]


@pytest.fixture
def recorded(monkeypatch, desktop_credential):
    """Capture the call ``weaver load`` makes, and answer it with a report."""

    calls: list[dict] = []

    def fake_load(targets, **kwargs):
        calls.append({"targets": targets, **kwargs})
        return _report()

    monkeypatch.setattr(weaver, "load", fake_load)
    return calls


def _report(
    *, status: str = TASK_SUCCEEDED, node_status: str = SUCCEEDED, **extra
) -> LoadRunReport:
    return LoadRunReport(
        requested=("Lakehouse/Sales",),
        status=status,
        dry_run=False,
        fault_tolerant=False,
        nodes=(
            LoadNodeReport(
                node_id="load:Lakehouse/Sales/Sales.Customer",
                logical_id="Lakehouse/Sales/Sales.Customer",
                physical_target="Lakehouse/Sales",
                primitive_kind="python_table",
                dispatch_location="/x/Sales__Customer.py",
                status=node_status,
                executed=True,
                result=LoadResult(succeeded=True, rows_read=5, rows_inserted=5),
            ),
        ),
        **extra,
    )


def _command(*args: str) -> list[str]:
    return ["load", "--target", "Lakehouse/Sales", "--workspace", "Demo", *args]


# --- the options the contract names -------------------------------------------


@weaver_test()
def test_the_command_exposes_every_option_the_contract_names():
    parser = build_parser()
    load = parser.parse_args(
        [
            "load",
            "--target",
            "Lakehouse/Sales",
            "--workspace",
            "My Workspace",
            "--catalogue",
            "Warehouse/Weaver",
            "--workspace-config",
            "environment.yml",
            "--fault-tolerant",
            "--dry-run",
            "--name",
            "Sales.Customer",
            "--name",
            "Sales.Order",
        ]
    )

    assert load.targets == ["Lakehouse/Sales"]
    assert load.workspace == "My Workspace"
    assert load.catalogue == "Warehouse/Weaver"
    assert load.workspace_config == "environment.yml"
    assert load.fault_tolerant
    assert load.dry_run
    assert load.names == ["Sales.Customer", "Sales.Order"]


@weaver_test()
def test_more_than_one_target_is_one_request():
    parser = build_parser()

    load = parser.parse_args(
        ["load", "--target", "Lakehouse/Sales", "--target", "Warehouse/Reporting"]
    )

    assert load.targets == ["Lakehouse/Sales", "Warehouse/Reporting"]


@weaver_test()
def test_targets_are_required():
    """Refused by the operation, so a notebook call and a command line agree.

    ``--target`` repeats, so argparse accepts none of it. What a load needs is
    core's claim, and stating it there is what makes
    ``weaver.load([])`` refuse the same way.
    """

    from weaver.errors import CommandError
    from weaver.operations.load import load as load_operation

    with pytest.raises(CommandError, match="load needs at least one target"):
        load_operation([], workspace="Demo")


@pytest.mark.parametrize("command", ["load", "test"])
@weaver_test()
def test_naming_no_target_is_a_weaver_sentence(command, capsys):
    """argparse gives ``None`` for a repeated option nobody wrote.

    Passed through to the operation, which refuses it in one sentence. Handing
    the CLI a list to build first turned it into a TypeError traceback.
    """

    exit_code = main(
        [command, "--workspace", "Demo", "--catalogue", "Warehouse/Weaver"]
    )

    assert exit_code == 1
    assert f"{command} needs at least one target" in capsys.readouterr().err


# --- what reaches the API -----------------------------------------------------


@weaver_test()
def test_the_targets_reach_the_api_as_written(recorded):
    main(_command())

    assert recorded[0]["targets"] == ["Lakehouse/Sales"]


@weaver_test()
def test_fault_tolerance_and_dry_run_reach_the_api(recorded):
    main(_command("--fault-tolerant", "--dry-run"))

    assert recorded[0]["fault_tolerant"] is True
    assert recorded[0]["dry_run"] is True


@weaver_test()
def test_repeated_names_reach_the_api_as_one_exact_selection(recorded):
    main(
        _command(
            "--name",
            "Sales.Customer",
            "--name",
            "Sales.Order",
        )
    )

    assert recorded[0]["names"] == ["Sales.Customer", "Sales.Order"]


@weaver_test()
def test_the_cautious_answers_are_the_defaults(recorded):
    main(_command())

    assert recorded[0]["fault_tolerant"] is False
    assert recorded[0]["dry_run"] is False
    assert recorded[0]["names"] is None


@weaver_test()
def test_an_explicit_catalogue_needs_no_configuration_file(recorded):
    """The case the CLI must not require ceremony for.

    Naming both the workspace and its catalogue is a complete request.
    Insisting on a configuration file to carry the second would make the first
    argument useless.
    """

    main(_command("--catalogue", "Warehouse/Weaver"))

    assert recorded[0]["session"].workspace.catalogue == "Warehouse/Weaver"


@weaver_test()
def test_workspace_configuration_is_still_supported(recorded, tmp_path):
    config = tmp_path / "environment.yml"
    config.write_text(
        "workspace: Demo\ncatalogue: Warehouse/Configured\n",
        encoding="utf-8",
    )

    main(["load", "--target", "Lakehouse/Sales", "--workspace-config", str(config)])

    assert recorded[0]["session"].workspace.catalogue == "Warehouse/Configured"


@weaver_test()
def test_an_explicit_argument_overrides_the_configured_value(recorded, tmp_path):
    config = tmp_path / "environment.yml"
    config.write_text(
        "workspace: Demo\ncatalogue: Warehouse/Configured\n",
        encoding="utf-8",
    )

    main(
        [
            "load",
            "--target",
            "Lakehouse/Sales",
            "--workspace-config",
            str(config),
            "--catalogue",
            "Warehouse/Explicit",
        ]
    )

    assert recorded[0]["session"].workspace.catalogue == "Warehouse/Explicit"


@weaver_test()
def test_naming_no_workspace_at_all_fails_saying_which_value_is_missing(capsys):
    exit_code = main(["load", "--target", "Lakehouse/Sales"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "--workspace" in captured.err


# --- what is made of the answer -----------------------------------------------


@weaver_test()
def test_a_successful_run_renders_its_nodes_and_exits_zero(recorded, capsys):
    exit_code = main(_command())
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "load:Lakehouse/Sales/Sales.Customer" in captured.out
    assert "succeeded" in captured.out


@weaver_test()
def test_json_renders_the_whole_report(recorded, capsys):
    main(_command("--json"))
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == TASK_SUCCEEDED
    assert payload["nodes"][0]["node_id"] == "load:Lakehouse/Sales/Sales.Customer"


@weaver_test()
def test_a_tolerant_run_that_reports_failure_renders_and_exits_non_zero(
    monkeypatch, capsys, desktop_credential
):
    """Tolerance returns a report; a report of failure is still a failure.

    The distinction the exit code has to keep is between how a failure was
    surfaced and whether there was one.
    """

    monkeypatch.setattr(
        weaver,
        "load",
        lambda targets, **kwargs: _report(status=TASK_FAILED, node_status=FAILED),
    )

    exit_code = main(_command("--fault-tolerant"))

    assert exit_code == 1
    assert "failed" in capsys.readouterr().out


@weaver_test()
def test_an_intolerant_failure_exits_non_zero_showing_what_it_carried(
    monkeypatch, capsys, desktop_credential
):
    partial = _report(status=TASK_FAILED, node_status=FAILED, workflow_id="0f8b2c1d")

    def raising(targets, **kwargs):
        raise LoadError(
            "load:Lakehouse/Sales/Sales.Customer failed: rows were rejected",
            report=partial,
            workflow_id="0f8b2c1d",
        )

    monkeypatch.setattr(weaver, "load", raising)

    exit_code = main(_command())
    captured = capsys.readouterr()

    assert exit_code == 1
    # The partial report is the useful half of the answer, so it is rendered
    # rather than discarded in favour of the message.
    assert "load:Lakehouse/Sales/Sales.Customer" in captured.out
    assert "rows were rejected" in captured.err
    assert "Workflow: 0f8b2c1d" in captured.err


@weaver_test()
def test_a_command_error_from_the_api_becomes_a_non_zero_exit(
    monkeypatch, capsys, desktop_credential
):
    def raising(targets, **kwargs):
        raise CommandError("load needs a Weaver catalogue")

    monkeypatch.setattr(weaver, "load", raising)

    exit_code = main(_command())

    assert exit_code == 1
    assert "Weaver catalogue" in capsys.readouterr().err


# --- the host boundary --------------------------------------------------------


class _FakeTds:
    """The catalogue Warehouse, doubled beneath a real Session.

    The catalogue moved to a Warehouse, so a desktop load reads it over TDS
    before it crosses anywhere. These tests are about what crosses and what is
    resolved first, so the catalogue is answered here rather than stood up.
    """

    #: What the estate says is installed: one Lakehouse item, one object.
    ITEM = ("Lakehouse", "Sales")
    OBJECT = ("Sales", "Customer")

    @classmethod
    def answer(cls, statement: str) -> list[dict]:
        from weaver.catalogue.tables import (
            INSTALLATION,
            PROJECTED_TABLES,
            REGISTRY,
        )

        if "INFORMATION_SCHEMA.COLUMNS" in statement:
            return [
                {"TABLE_NAME": table.name, "COLUMN_NAME": table.public_name_of(column)}
                for table in PROJECTED_TABLES
                for column in table.physical_columns
            ]
        item_type, item_name = cls.ITEM
        schema, name = cls.OBJECT
        if f"[{INSTALLATION.name}]" in statement:
            return [
                {
                    "item_type": item_type,
                    "item_name": item_name,
                    "target_name": item_name,
                    "weaver_version": "0",
                    "signature": "sig",
                }
            ]
        if f"[{REGISTRY.name}]" in statement:
            return [
                {
                    "item_type": item_type,
                    "item_name": item_name,
                    "schema_name": schema,
                    "object_name": name,
                    "object_type": "table",
                    "object_role": "data",
                    "signature": "sig",
                    "build_datetime": None,
                },
                # The deployed module that loads it. Without one the object has
                # no load primitive, the graph is empty, and nothing crosses.
                {
                    "item_type": item_type,
                    "item_name": item_name,
                    "schema_name": "_/Load",
                    "object_name": f"{schema}__{name}.py",
                    "object_type": "file",
                    "object_role": "load",
                    "signature": "sig",
                    "build_datetime": None,
                },
            ]
        return []


class _FakeLivy:
    """A Livy session that records the program and answers with a payload.

    Reached through a real :class:`~weaver.sessions.console.ConsoleSession`,
    because that is how the command reaches it: the double is the transport,
    not the Session, so what these tests exercise is the crossing the product
    performs rather than one arranged for them.

    ``submitted`` holds the programs the command sent. The Session's own version
    probe is answered but not recorded. It is the Session's business, it
    happens once per workspace context, and a test about a load should not have
    to know it exists.
    """

    submitted: list[str] = []
    answer: dict = {}
    started: int = 0

    @classmethod
    def for_workspace(cls, workspace, **kwargs):
        instance = cls()
        instance.workspace = workspace
        return instance

    def start(self) -> None:
        type(self).started += 1

    def ensure_weaver(self, *args, **kwargs) -> None:
        """A load imports the published wheel; the double has nothing to check."""

    def close(self, **kwargs) -> None:
        pass

    def run(self, code, **kwargs):
        answer = type(self).answer
        if "weaver.__version__" in code:
            from weaver import __version__

            answer = __version__
        else:
            type(self).submitted.append(code)

        class Result:
            # ``returned`` is whether the program called ``emit`` at all, which
            # is a different question from what it emitted, and the one the
            # Session uses to tell "ran and said nothing" from "ran and said no".
            returned = answer is not None
            payload = answer

        return Result()


class _FakeCredential:
    """Enough of a credential for a token to exist, and no network at all."""

    def get_token(self, *scopes, **kwargs):
        from types import SimpleNamespace

        return SimpleNamespace(token="token", expires_on=2**31 - 1)


class _FakeResolver:
    """A REST resolver that answers from a set of names it pretends exist."""

    present: set = set()
    asked: list = []

    def __init__(self, workspace, **kwargs) -> None:
        self.workspace = workspace

    def spark_destination(self, item):
        from weaver.spark import FabricSparkTarget

        return FabricSparkTarget(workspace="My Workspace", lakehouse=item.name)

    def resolve(self, item, *, item_type):
        from weaver.fabric import ItemNotFoundError

        type(self).asked.append((item.name, item_type))
        if item.name not in type(self).present:
            raise ItemNotFoundError(f"no {item_type} named {item.name!r} in workspace")
        return object()


@pytest.fixture
def livy(monkeypatch):
    """The transport a desktop crosses on, doubled beneath a real Session.

    Everything is replaced before a Session is constructed: a ``Resource``
    binds its acquisition at construction, so patching the scope afterwards
    leaves the original in place and the credential is asked for anyway.
    """

    from weaver.sessions.console import ConsoleScope

    _FakeLivy.submitted = []
    _FakeLivy.started = 0
    # One node's load result: the catalogue is read over TDS now, so the only
    # thing that crosses Livy is the primitive that fills a table.
    _FakeLivy.answer = {
        "succeeded": True,
        "rows_read": 5,
        "rows_inserted": 5,
        "rows_updated": 0,
        "rows_deleted": 0,
        "rows_rejected": 0,
        "error_message": None,
    }
    _FakeResolver.present = {"Sales", "Reporting"}
    _FakeResolver.asked = []
    monkeypatch.setattr("weaver.fabric.auth.credential", _FakeCredential)
    monkeypatch.setattr(
        "weaver.fabric.LivySession.for_workspace",
        classmethod(lambda cls, *args, **kwargs: _FakeLivy.for_workspace(*args)),
    )
    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: _FakeResolver(self.workspace))
    )
    # The catalogue is a Warehouse now, so a load reads it over TDS before it
    # crosses. Doubled at the Session's own capability, for the same reason the
    # Livy transport is: what is under test is the crossing, not the engine.
    from weaver.sessions.console import ConsoleSession

    monkeypatch.setattr(
        ConsoleSession,
        "query_tsql",
        lambda self, statement, **kwargs: _FakeTds.answer(statement),
    )
    monkeypatch.setattr(
        ConsoleSession, "execute_tsql", lambda self, statement, **kwargs: None
    )
    monkeypatch.setattr(_cli_module(), "_prefer_desktop_credential", lambda: None)
    return _FakeLivy


def _fabric(*args: str) -> list[str]:
    return [
        "load",
        "--target",
        "Lakehouse/Sales",
        "--workspace",
        "My Workspace",
        "--catalogue",
        "Warehouse/Weaver",
        "--environment",
        "weaver",
        *args,
    ]


@weaver_test()
def test_a_session_that_returns_nothing_is_an_error_rather_than_a_success(livy, capsys):
    livy.answer = None

    exit_code = main(_fabric())

    assert exit_code == 1
    assert "returned nothing" in capsys.readouterr().err


# --- the CLI owns no semantics ------------------------------------------------


@weaver_test()
def test_the_command_module_contains_no_orchestration_of_its_own():
    """The rule that decays without saying so, so it is read off the source.

    Everything a load decides, which nodes exist, in what order, what a failure
    means, what the run added up to, belongs to one implementation that runs on
    both sides of the host boundary. A CLI that reached for any of it would be
    the second place that knows, and the two would drift.
    """

    from pathlib import Path

    source = Path(_cli_module().__file__).read_text(encoding="utf-8")
    forbidden = (
        "load_dag",
        "resolve_load_plan",
        "execute_load_plan",
        "dispatch_load_node",
        "InstalledDag",
        "final_status",
        "open_task_log",
        "read_installed_catalogue",
    )

    assert [name for name in forbidden if name in source] == []


@weaver_test()
def test_a_missing_physical_target_is_left_to_dispatch(livy):
    _FakeResolver.present = set()

    main(_fabric())

    assert livy.submitted
    assert _FakeResolver.asked == []
