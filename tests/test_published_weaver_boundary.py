"""Which crossings wait on `weaver install`, and which only need a session.

A Fabric Environment and a published Weaver are two separate needs. Attaching
an Environment is how a Livy session starts at all; publishing a wheel into it
is how a submitted body can `import weaver`. Conflating them put a five-minute
publish in front of a build that submits nothing but Spark SQL.

So the requirement is derived from the work: a `RemoteProgram` is Python that
imports Weaver where Spark is, and it is the only crossing that asserts the
install.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from weaver.errors import CommandError
from weaver.sessions.console import ConsoleScope, ConsoleSession
from weaver.sessions.program import RemoteProgram
from weaver.workspaces import Workspace


class _Livy:
    """A Livy session double: records what it was asked to do, interprets none of it."""

    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.weaver_asserted = 0

    def run(self, code, **kwargs):
        if "weaver.__version__" in code:
            from weaver import __version__

            return SimpleNamespace(returned=True, payload=__version__)
        self.submitted.append(code)
        return SimpleNamespace(returned=True, payload=[])

    def ensure_weaver(self) -> None:
        self.weaver_asserted += 1

    def start(self) -> None:
        pass

    def close(self, **kwargs) -> None:
        pass


@pytest.fixture
def desktop(monkeypatch):
    """A console reaching into Fabric, with the transport recorded."""

    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: SimpleNamespace(workspace=None))
    )
    livy = _Livy()
    session = ConsoleSession(
        workspace=Workspace(
            workspace="Analytics", catalogue="Warehouse/Weaver", environment="weaver"
        ),
        livy=livy,
    )
    with session:
        yield session, livy


def test_spark_sql_crosses_without_asserting_the_install(desktop):
    session, livy = desktop

    session.execute_spark_sql("SELECT 1")

    assert livy.submitted, "the statement crossed"
    assert livy.weaver_asserted == 0


def test_a_program_asserts_the_install_before_it_runs(desktop):
    session, livy = desktop

    session.execute_python(
        RemoteProgram(
            name="dispatch",
            call=lambda: None,
            source="import weaver\nemit(None)\n",
        )
    )

    assert livy.weaver_asserted == 1


def test_the_import_is_submitted_once_per_livy_session(monkeypatch):
    """It is an import, and an import stays imported: one statement, not one per crossing."""

    from weaver.fabric.livy import LivySession

    session = LivySession.__new__(LivySession)
    session._weaver_asserted = False
    session.environment_id = "env99"
    submitted: list[str] = []
    monkeypatch.setattr(
        type(session), "run", lambda self, code, **kw: submitted.append(code)
    )

    session.ensure_weaver()
    session.ensure_weaver()

    assert len(submitted) == 1
    assert "import weaver" in submitted[0]


# --- and the two sentences stay apart ----------------------------------------


def test_a_workspace_naming_no_environment_is_told_where_weaver_comes_from():
    """A missing Environment names the workspace and the publish that fills it."""

    from weaver.fabric.livy import missing_environment

    message = missing_environment(Workspace(workspace="Analytics"))

    assert "Analytics" in message
    assert "weaver install" in message
    assert "--environment" in message


def test_a_body_that_cannot_import_weaver_is_told_to_install():
    from weaver.fabric.livy import environment_bootstrap

    source = environment_bootstrap()

    assert "import weaver" in source
    assert "weaver install --workspace <ws> --environment <env>" in source


def test_a_scope_with_no_livy_says_so_rather_than_asserting_nothing(monkeypatch):
    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: SimpleNamespace(workspace=None))
    )
    session = ConsoleSession(
        workspace=Workspace(
            workspace="Analytics", catalogue="Warehouse/Weaver", environment="weaver"
        )
    )
    scope = session.scope(None)
    scope.livy = None

    with pytest.raises(CommandError, match="no Livy session"):
        scope.ensure_weaver()
