"""``weaver load`` — the command-line surface, and what it delegates to.

Two claims, and they pull in opposite directions on purpose.

The first is that every option reaches :func:`weaver.load` unchanged: the CLI is
an adapter, so what a user types and what the API is called with must be the
same request. These are asserted against a recorded call rather than a real run,
because what is under test is the mapping, not the load.

The second is that the CLI owns *nothing else*. It resolves a workspace, crosses
the host boundary when it has to, renders what comes back and chooses an exit
code. It does not plan, validate targets, order a graph or decide what failed —
and the last test here says so by reading the source, because that is the kind of
rule that decays quietly.

Pure Python throughout: no Spark, no workspace, no Livy. The desktop-to-Fabric
crossing is proved against a recording double, since what matters on this side
of the boundary is what was submitted and what was made of the answer.
"""

from __future__ import annotations

import importlib

import json

import pytest

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
    the function is what attribute access finds — so the module is asked for by
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
    # The desktop preflight resolves each named target over REST before the
    # operation runs. What this file claims is what the CLI parses and hands
    # on, so the crossing is stubbed rather than made. `weaver_cli.main` is
    # also the entry point function's name, so the module is imported.
    monkeypatch.setattr(
        importlib.import_module("weaver_cli.main"),
        "_refuse_absent_targets",
        lambda *_a, **_k: None,
    )
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
    return ["load", "Lakehouse/Sales", "--workspace", "Demo", *args]


# --- the options the contract names -------------------------------------------


def test_the_command_exposes_every_option_the_contract_names():
    parser = build_parser()
    load = parser.parse_args(
        [
            "load",
            "Lakehouse/Sales",
            "--workspace",
            "My Workspace",
            "--weaver-lakehouse",
            "Weaver",
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
    assert load.weaver_lakehouse == "Weaver"
    assert load.workspace_config == "environment.yml"
    assert load.fault_tolerant
    assert load.dry_run
    assert load.names == ["Sales.Customer", "Sales.Order"]


def test_more_than_one_target_is_one_request():
    parser = build_parser()

    load = parser.parse_args(
        ["load", "Lakehouse/Sales", "Warehouse/Reporting"]
    )

    assert load.targets == ["Lakehouse/Sales", "Warehouse/Reporting"]


def test_targets_are_required():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["load", "--workspace", "My Workspace"])


# --- what reaches the API -----------------------------------------------------


def test_the_targets_reach_the_api_as_written(recorded):
    main(_command())

    assert recorded[0]["targets"] == ["Lakehouse/Sales"]


def test_fault_tolerance_and_dry_run_reach_the_api(recorded):
    main(_command("--fault-tolerant", "--dry-run"))

    assert recorded[0]["fault_tolerant"] is True
    assert recorded[0]["dry_run"] is True


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


def test_the_cautious_answers_are_the_defaults(recorded):
    main(_command())

    assert recorded[0]["fault_tolerant"] is False
    assert recorded[0]["dry_run"] is False
    assert recorded[0]["names"] is None


def test_an_explicit_weaver_lakehouse_needs_no_configuration_file(recorded):
    """The case the CLI must not require ceremony for.

    Naming both the workspace and its control Lakehouse is a complete request.
    Insisting on a configuration file to carry the second would make the first
    argument useless.
    """

    main(_command("--weaver-lakehouse", "Weaver"))

    assert recorded[0]["workspace"].weaver_lakehouse == "Weaver"


def test_workspace_configuration_is_still_supported(recorded, tmp_path):
    config = tmp_path / "environment.yml"
    config.write_text(
        "workspace: Demo\nweaver_lakehouse: Configured\n",
        encoding="utf-8",
    )

    main(["load", "Lakehouse/Sales", "--workspace-config", str(config)])

    assert recorded[0]["workspace"].weaver_lakehouse == "Configured"


def test_an_explicit_argument_overrides_the_configured_value(recorded, tmp_path):
    config = tmp_path / "environment.yml"
    config.write_text(
        "workspace: Demo\nweaver_lakehouse: Configured\n",
        encoding="utf-8",
    )

    main(
        [
            "load",
            "Lakehouse/Sales",
            "--workspace-config",
            str(config),
            "--weaver-lakehouse",
            "Explicit",
        ]
    )

    assert recorded[0]["workspace"].weaver_lakehouse == "Explicit"


def test_naming_no_workspace_at_all_fails_saying_which_value_is_missing(capsys):
    exit_code = main(["load", "Lakehouse/Sales"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "--workspace" in captured.err


# --- what is made of the answer -----------------------------------------------


def test_a_successful_run_renders_its_nodes_and_exits_zero(recorded, capsys):
    exit_code = main(_command())
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "load:Lakehouse/Sales/Sales.Customer" in captured.out
    assert "succeeded" in captured.out


def test_json_renders_the_whole_report(recorded, capsys):
    main(_command("--json"))
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == TASK_SUCCEEDED
    assert payload["nodes"][0]["node_id"] == "load:Lakehouse/Sales/Sales.Customer"


def test_a_tolerant_run_that_reports_failure_renders_and_exits_non_zero(
    monkeypatch, capsys, desktop_credential, no_target_preflight
):
    """Tolerance returns a report; a report of failure is still a failure.

    The distinction the exit code has to keep is between *how* a failure was
    surfaced and *whether* there was one.
    """

    monkeypatch.setattr(
        weaver,
        "load",
        lambda targets, **kwargs: _report(status=TASK_FAILED, node_status=FAILED),
    )

    exit_code = main(_command("--fault-tolerant"))

    assert exit_code == 1
    assert "failed" in capsys.readouterr().out


def test_an_intolerant_failure_exits_non_zero_showing_what_it_carried(
    monkeypatch, capsys, desktop_credential, no_target_preflight
):
    partial = _report(status=TASK_FAILED, node_status=FAILED, task_log="Files/_/Log/x")

    def raising(targets, **kwargs):
        raise LoadError(
            "load:Lakehouse/Sales/Sales.Customer failed: rows were rejected",
            report=partial,
            task_log="Files/_/Log/x",
        )

    monkeypatch.setattr(weaver, "load", raising)

    exit_code = main(_command())
    captured = capsys.readouterr()

    assert exit_code == 1
    # The partial report is the useful half of the answer, so it is rendered
    # rather than discarded in favour of the message.
    assert "load:Lakehouse/Sales/Sales.Customer" in captured.out
    assert "rows were rejected" in captured.err
    assert "Files/_/Log/x" in captured.err


def test_a_command_error_from_the_api_becomes_a_non_zero_exit(
    monkeypatch, capsys, desktop_credential, no_target_preflight
):
    def raising(targets, **kwargs):
        raise CommandError("load needs a Weaver control Lakehouse")

    monkeypatch.setattr(weaver, "load", raising)

    exit_code = main(_command())

    assert exit_code == 1
    assert "control Lakehouse" in capsys.readouterr().err


# --- the host boundary --------------------------------------------------------


class _FakeLivy:
    """A Livy session that records the program and answers with a payload.

    Reached through a real :class:`~weaver.session.console.ConsoleSession`,
    because that is how the command reaches it: the double is the *transport*,
    not the Session, so what these tests exercise is the crossing the product
    performs rather than one arranged for them.

    ``submitted`` holds the programs the command sent. The Session's own version
    probe is answered but not recorded — it is the Session's business, it
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

    def close(self, **kwargs) -> None:
        pass

    def run(self, code, **kwargs):
        answer = type(self).answer
        if "weaver.__version__" in code:
            from weaver import __version__

            answer = __version__
        else:
            type(self).submitted.append(code)
            if answer is not None and "_statements" in code:
                # The estate is read as Spark SQL, and an empty answer is an
                # empty catalogue — which these tests are content with, because
                # what they assert is which targets were resolved and when.
                answer = []

        class Result:
            # ``returned`` is whether the program called ``emit`` at all, which
            # is a different question from what it emitted — and the one the
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
            raise ItemNotFoundError(
                f"no {item_type} named {item.name!r} in workspace"
            )
        return object()


@pytest.fixture
def livy(monkeypatch):
    """The transport a desktop crosses on, doubled beneath a real Session.

    Everything is replaced *before* a Session is constructed: a ``Resource``
    binds its acquisition at construction, so patching the scope afterwards
    leaves the original in place and the credential is asked for anyway.
    """

    from weaver.session.console import ConsoleScope

    _FakeLivy.submitted = []
    _FakeLivy.started = 0
    _FakeLivy.answer = {"failed": False, "report": _report().to_mapping()}
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
    monkeypatch.setattr(_cli_module(), "_prefer_desktop_credential", lambda: None)
    return _FakeLivy


def _fabric(*args: str) -> list[str]:
    return [
        "load",
        "Lakehouse/Sales",
        "--workspace",
        "My Workspace",
        "--weaver-lakehouse",
        "Weaver",
        "--environment",
        "weaver",
        *args,
    ]


def test_a_session_that_returns_nothing_is_an_error_rather_than_a_success(livy, capsys):
    livy.answer = None

    exit_code = main(_fabric())

    assert exit_code == 1
    assert "returned nothing" in capsys.readouterr().err


# --- the CLI owns no semantics ------------------------------------------------


def test_the_command_module_contains_no_orchestration_of_its_own():
    """The rule that decays quietly, so it is read off the source.

    Everything a load decides — which nodes exist, in what order, what a failure
    means, what the run added up to — belongs to one implementation that runs on
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
        "InstalledEstate",
        "final_status",
        "open_task_log",
        "read_installed_catalogue",
    )

    assert [name for name in forbidden if name in source] == []


# --- the cheap guard in front of the session ----------------------------------
#
# Starting a Livy session costs tens of seconds and a capacity's only session
# slot. Resolving a name over REST costs one call. So a request that can already
# be rejected is rejected before any of that is spent.


def test_the_requested_targets_are_resolved_before_a_session_is_opened(livy, capsys):
    main(_fabric())

    assert _FakeResolver.asked == [("Sales", "Lakehouse")]
    assert livy.submitted, "the session should still have been used"


def test_a_target_that_does_not_exist_is_refused_without_opening_a_session(
    livy, capsys
):
    """The whole point: nothing is spent on a request already known to be bad."""

    _FakeResolver.present = set()

    exit_code = main(_fabric())
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Lakehouse/Sales was not found" in captured.err
    assert "Lakehouse/Sales" in captured.err
    assert livy.submitted == [], "no session should have been started"


def test_every_requested_target_is_checked_not_only_the_first(livy, capsys):
    _FakeResolver.present = {"Sales"}

    exit_code = main(
        [
            "load",
            "Lakehouse/Sales",
            "Warehouse/Reporting",
            "--workspace",
            "My Workspace",
            "--weaver-lakehouse",
            "Weaver",
            "--environment",
            "weaver",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Warehouse/Reporting" in captured.err
    assert ("Reporting", "Warehouse") in _FakeResolver.asked
    assert livy.submitted == []


def test_a_lakehouse_and_a_warehouse_are_resolved_by_their_own_types(livy):
    """Identity is workspace + type + name, so a bare name is never asked
    "what are you?" — and a Lakehouse and its SQL endpoint share a name."""

    main(
        [
            "load",
            "Lakehouse/Sales",
            "Warehouse/Reporting",
            "--workspace",
            "My Workspace",
            "--weaver-lakehouse",
            "Weaver",
            "--environment",
            "weaver",
        ]
    )

    assert _FakeResolver.asked == [
        ("Sales", "Lakehouse"),
        ("Reporting", "Warehouse"),
    ]


def test_a_resolver_failure_that_is_not_a_missing_item_keeps_its_own_diagnosis(
    livy, capsys
):
    """"Your Lakehouse is gone" is a bad answer to "your token expired"."""

    from weaver.errors import CommandError

    def expired(self, item, *, item_type):
        raise CommandError("the credential could not be refreshed")

    _FakeResolver.resolve = expired

    try:
        exit_code = main(_fabric())
        captured = capsys.readouterr()

        assert exit_code == 1
        assert "credential could not be refreshed" in captured.err
        assert "no such item" not in captured.err
    finally:
        del _FakeResolver.resolve


def test_the_guard_reads_no_catalogue_and_builds_no_graph():
    """It checks exactly what the user typed, and nothing that needs the estate.

    The catalogue, the graph, upstream discovery and inventories all live on the
    far side of the boundary; reaching for any of them here would be doing the
    remote run's work in the wrong place — and before a session exists to do it
    in.
    """

    import ast
    import inspect

    # The body only. The docstring explains what is deliberately *not* reached,
    # so scanning it would flag the very sentence that promises the rule.
    tree = ast.parse(inspect.getsource(_cli_module()._refuse_absent_targets).strip())
    body = tree.body[0].body
    if isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    source = "\n".join(ast.dump(node) for node in body)

    for reached_too_far in (
        "read_catalogue",
        "InstalledEstate",
        "load_dag",
        "inventory",
        "LivySession",
    ):
        assert reached_too_far not in source
