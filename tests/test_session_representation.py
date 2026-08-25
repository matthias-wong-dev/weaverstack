"""What a Session is, before it has acquired anything physical.

A Session is a process scope, not a workspace binding: ``weaver session`` starts
without a workspace, commands bring their own, and resources are cached per
workspace context. These are the rules that shape it, provable with no Fabric,
no Spark and no credentials.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.sessions import ConsoleSession, workspace_context
from weaver.sessions.console import ConsoleScope
from weaver.workspaces import TargetDeclaration, Workspace


@pytest.fixture
def console():
    with ThreadPoolExecutor(max_workers=2) as pool:
        with ConsoleSession(executor=pool) as session:
            yield session


def _other(name="B_Workspace") -> Workspace:
    """A second workspace, so a scope claim is about two of them."""

    return _fabric(name)


def _fabric(name="A_Workspace") -> Workspace:
    return Workspace(workspace=name, catalogue="Warehouse/Weaver", environment="weaver")


# --- identity ---------------------------------------------------------------


@weaver_test()
def test_a_console_session_needs_no_workspace_to_exist(console):
    assert console.workspace is None


@weaver_test()
def test_a_command_without_a_workspace_says_what_is_missing(console):
    with pytest.raises(CommandError, match="A Workspace is required for this command"):
        console.scope(None)


@weaver_test()
def test_a_command_may_name_a_workspace_the_session_did_not(console):
    scope = console.scope(_other())

    assert scope.workspace == _other()


@weaver_test()
def test_two_commands_naming_one_workspace_share_its_resources(console):
    first = console.scope(_other())
    second = console.scope(_other())

    assert first is second


@weaver_test()
def test_two_workspaces_are_two_contexts(console):
    assert console.scope(_other("one")) is not console.scope(_other("two"))


@weaver_test()
def test_a_default_workspace_is_a_default_and_not_an_identity():
    with ConsoleSession(workspace=_other("default")) as session:
        assert session.scope(None).workspace == _other("default")
        assert session.scope(_other("other")).workspace == _other("other")


@weaver_test()
def test_the_default_is_what_the_session_started_with_and_never_accumulates():
    """A command naming a workspace does not make it the session's default.

    Inheritance is only ever from what ``weaver session`` was started with.
    Learning a default from whichever command ran last would mean the next
    command silently inherited another workspace's Environment and control
    Lakehouse, a plausible-looking build into the wrong place.
    """

    with ConsoleSession(workspace=_other("default")) as session:
        session.scope(_other("elsewhere"))

        assert session.workspace == _other("default")
        assert session.scope(None).workspace == _other("default")


@weaver_test()
def test_a_session_started_without_a_workspace_never_gains_one():
    with ConsoleSession() as session:
        session.scope(_other("named-by-a-command"))

        assert session.workspace is None
        with pytest.raises(
            CommandError, match="A Workspace is required for this command"
        ):
            session.scope(None)


@weaver_test()
def test_context_identity_ignores_which_targets_were_declared():
    plain = _fabric()
    with_targets = Workspace(
        workspace="A_Workspace",
        catalogue="Warehouse/Weaver",
        environment="weaver",
        lakehouses={
            "Sales": TargetDeclaration(item=WeaverItemId.parse("Lakehouse/Sales")),
        },
    )

    # The same place, the same catalogue and the same Environment: one
    # Livy session serves both, so they must not be two contexts.
    assert workspace_context(plain) == workspace_context(with_targets)


@weaver_test()
def test_a_different_control_lakehouse_is_a_different_context():
    other = Workspace(
        workspace="A_Workspace", catalogue="Warehouse/Other", environment="weaver"
    )

    assert workspace_context(_fabric()) != workspace_context(other)


# --- position ---------------------------------------------------------------


@weaver_test()
def test_a_console_reaching_into_fabric_does_not_execute_here(console):
    assert console.executes_here(_fabric()) is False


@weaver_test()
def test_a_console_has_no_spark_object_at_all(console):
    """Not a Spark session that refuses: nothing to reach for.

    A console prepares work and crosses; Spark is on the other side of that
    crossing. Anything here holding a SparkSession would be a second execution
    position hidden inside the desktop one.
    """

    assert not hasattr(console, "spark")
    assert console.executes_here(_fabric()) is False


# --- reporting context ------------------------------------------------------


@weaver_test()
def test_reporting_frames_nest_and_unwind(console):
    console.task_started("build")
    console.step_started("install")

    assert [frame.name for frame in console.frames] == ["build", "install"]

    console.step_completed("install")
    assert [frame.name for frame in console.frames] == ["build"]

    console.task_completed("build")
    assert console.frames == ()


@weaver_test()
def test_completing_a_task_unwinds_the_steps_beneath_it(console):
    console.task_started("build")
    console.step_started("install")
    console.substep_started("create_schema")

    console.task_failed("build", RuntimeError("stopped"))

    assert console.frames == ()


# --- lifetime ---------------------------------------------------------------


@weaver_test()
def test_a_closed_session_serves_no_further_commands():
    session = ConsoleSession()
    session.scope(_other())
    session.close()

    with pytest.raises(CommandError, match="closed"):
        session.scope(_other())


@weaver_test()
def test_closing_twice_is_not_an_error():
    session = ConsoleSession()
    session.close()
    session.close()


# --- an acquired resource is a running one -----------------------------------


@weaver_test()
def test_the_livy_resource_is_started_before_anyone_is_handed_it(monkeypatch):
    """Acquiring means the expensive part is over.

    ``for_workspace`` builds the object; ``start`` is what asks Fabric for the
    session. A resource that returned the unstarted object looked acquired to
    everything above it, ready state, shared by the next caller, and the first
    statement failed with "the Livy session has not been started" while no
    session had ever appeared in the workspace.
    """

    class FakeLivy:
        def __init__(self) -> None:
            self.started = False

        def start(self) -> None:
            self.started = True

        def run(self, source, **kwargs):
            assert self.started, "a statement reached an unstarted session"

        def close(self) -> None:
            pass

    class FakeCredential:
        """Enough of a credential for a token to exist, and no network at all."""

        def get_token(self, *scopes, **kwargs):
            return SimpleNamespace(token="token", expires_on=2**31 - 1)

    built = FakeLivy()
    # Everything is replaced before the scope exists, because a Resource binds
    # its acquisition at construction: patching a method on the scope afterwards
    # leaves the Resource holding the original, and this test would reach a real
    # credential, which on a build agent means asking Azure.
    monkeypatch.setattr("weaver.fabric.auth.credential", FakeCredential)
    monkeypatch.setattr(
        "weaver.fabric.LivySession.for_workspace",
        classmethod(lambda cls, *args, **kwargs: built),
    )
    monkeypatch.setattr(ConsoleScope, "resolver", property(lambda self: object()))

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()

        assert scope.livy.get() is built
        assert built.started, "the resource handed out a session that was never started"


# --- the published wheel and this checkout -----------------------------------


@weaver_test()
def test_a_version_difference_warns_and_names_the_fix(monkeypatch):
    """A difference is worth saying; it is not worth refusing over.

    The console prepares work locally and runs it against the published wheel,
    so the two drift the moment either moves. During rapid development that is
    usually harmless, putting a publish in front of every experiment is not.
    """

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()
        monkeypatch.setattr(scope, "livy_run", lambda *a, **k: "9.9.9-elsewhere")

        scope.check_published_version(session.warn)

        assert session.warnings
        assert "9.9.9-elsewhere" in session.warnings[0]
        assert "weaver fabric environment publish" in session.warnings[0]


@weaver_test()
def test_a_matching_version_says_nothing(monkeypatch):
    from weaver import __version__

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()
        monkeypatch.setattr(scope, "livy_run", lambda *a, **k: __version__)

        scope.check_published_version(session.warn)

        assert session.warnings == []


@weaver_test()
def test_the_check_is_asked_once_per_workspace_not_once_per_command(monkeypatch):
    asked = []

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()
        monkeypatch.setattr(
            scope, "livy_run", lambda *a, **k: asked.append(1) or "9.9.9"
        )

        for _ in range(3):
            scope.check_published_version(session.warn)

        assert asked == [1], "one statement, and a warning nobody has to reread"
        assert len(session.warnings) == 1


@weaver_test()
def test_a_check_that_cannot_run_never_fails_the_work(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("the probe itself failed")

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()
        monkeypatch.setattr(scope, "livy_run", broken)

        scope.check_published_version(session.warn)  # does not raise

        assert session.warnings == []


# --- a statement that failed is not a session that died ----------------------


class _Livy:
    """Stands in for a Fabric Spark session that is up and stays up."""

    def __init__(self, raises=None) -> None:
        self.raises = raises
        self.runs = 0

    def start(self) -> None:
        pass

    def run(self, source, **kwargs):
        self.runs += 1
        if self.raises is not None:
            raise self.raises
        from weaver.fabric.livy import StatementResult

        return StatementResult(text="", payload={"ran": True})

    def close(self) -> None:
        pass


def _scope_with(livy):
    session = ConsoleSession(workspace=_fabric(), livy=livy)
    return session, session.scope()


@weaver_test()
def test_a_failed_statement_leaves_the_spark_session_up():
    """One bad command must not cost the next one a minute of startup."""

    from weaver.fabric import LivyStatementError
    from weaver.sessions.resources import ResourceState

    livy = _Livy(raises=LivyStatementError("boom", ename="ValueError", evalue="boom"))
    session, scope = _scope_with(livy)

    with pytest.raises(Exception):
        scope.livy_run("emit(1)", name="work")

    assert scope.livy.state is ResourceState.READY
    session.close()


@weaver_test()
def test_a_session_that_died_is_marked_failed():
    from weaver.fabric import LivyError
    from weaver.sessions.resources import ResourceState

    session, scope = _scope_with(_Livy(raises=LivyError("the session is dead")))

    with pytest.raises(LivyError):
        scope.livy_run("emit(1)", name="work")

    assert scope.livy.state is ResourceState.FAILED
    session.close()


@weaver_test()
def test_a_wheel_too_old_to_import_weaver_says_to_publish():
    """The raw ModuleNotFoundError points at a missing package.

    What is actually true is that the published wheel predates the console that
    submitted the program, and the fix is a publish.
    """

    from weaver.fabric import LivyStatementError

    livy = _Livy(
        raises=LivyStatementError(
            "ModuleNotFoundError: No module named 'weaver.session'",
            ename="ModuleNotFoundError",
            evalue="No module named 'weaver.session'",
        )
    )
    session, scope = _scope_with(livy)

    with pytest.raises(
        CommandError, match="weaver fabric environment publish"
    ) as raised:
        scope.livy_run("emit(1)", name="read_build_state")

    assert "older than this console" in str(raised.value)
    session.close()


@weaver_test()
def test_an_ordinary_remote_failure_is_passed_through_as_it_came():
    """Guessing at causes would bury the real ones."""

    from weaver.fabric import LivyStatementError

    livy = _Livy(
        raises=LivyStatementError(
            "ValueError: that column is not there",
            ename="ValueError",
            evalue="that column is not there",
        )
    )
    session, scope = _scope_with(livy)

    with pytest.raises(LivyStatementError, match="that column is not there"):
        scope.livy_run("emit(1)", name="work")

    session.close()
