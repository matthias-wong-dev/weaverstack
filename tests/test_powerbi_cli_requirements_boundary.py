import pytest
from support.weaver_test import weaver_test
from test_session_requirements_declaration import _scope

from weaver.sessions.console import ConsoleSession
from weaver.sessions.requirements import AUTH, LIVY, ONELAKE, RESOLVER, TDS
from weaver.workspaces import TargetDeclaration, Workspace
from weaver_cli.main import build_parser, command_requirements
from weaver_cli.shell import _prepare_for


@pytest.mark.parametrize(
    "selector",
    [
        "PowerBI",
        "PowerBI/Sales",
        "Report",
        "Report/Executive",
        "Report/Executive=Report/Executive_Dev",
    ],
)
@weaver_test()
def test_persistent_powerbi_preparation_starts_no_livy(monkeypatch, selector):
    scope = _scope(monkeypatch, lakehouses=())
    scope.workspace = Workspace(
        workspace="W",
        catalogue="Warehouse/Weaver",
        targets={
            "Lakehouse/Landing": TargetDeclaration("Landing_Dev"),
            "SemanticModel/Revenue": TargetDeclaration("Revenue_Dev"),
        },
    )
    assert scope.workspace.configured_lakehouses
    session = ConsoleSession.__new__(ConsoleSession)
    monkeypatch.setattr(session, "scope", lambda workspace=None: scope)
    args = build_parser().parse_args(
        ["build", ".", "--item", selector, "--workspace", "W"]
    )
    required = command_requirements(args)
    _prepare_for(session, args)
    assert LIVY not in required
    assert ONELAKE not in required
    assert {AUTH, RESOLVER, TDS} <= required
    assert scope.auth.started == 1
    assert scope.livy.started == 0


@pytest.mark.parametrize(
    "selector,spark",
    [
        (None, True),
        ("Lakehouse", True),
        ("Lakehouse/Landing", True),
        ("Warehouse", False),
        ("Warehouse/Serving", False),
    ],
)
@weaver_test()
def test_persistent_source_preparation_retains_requirements(
    monkeypatch, selector, spark
):
    scope = _scope(monkeypatch)
    session = ConsoleSession.__new__(ConsoleSession)
    monkeypatch.setattr(session, "scope", lambda workspace=None: scope)
    words = ["build", ".", "--workspace", "W"] + (
        ["--item", selector] if selector else []
    )
    args = build_parser().parse_args(words)
    _prepare_for(session, args)
    assert (LIVY in command_requirements(args)) == spark
    assert (ONELAKE in command_requirements(args)) == spark
    assert scope.livy.started == int(spark)
