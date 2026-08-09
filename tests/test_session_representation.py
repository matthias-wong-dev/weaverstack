"""What a Session is, before it has acquired anything physical.

A Session is a process scope, not a workspace binding: ``weaver session`` starts
without a workspace, commands bring their own, and resources are cached per
workspace context. These are the rules that shape it, provable with no Fabric,
no Spark and no credentials.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.session import ConsoleSession, workspace_context
from weaver.workspaces import FabricWorkspace, LocalWorkspace, TargetDeclaration


@pytest.fixture
def console():
    with ThreadPoolExecutor(max_workers=2) as pool:
        with ConsoleSession(executor=pool) as session:
            yield session


def _local(root="./emulator") -> LocalWorkspace:
    return LocalWorkspace(workspace=Path(root), weaver_lakehouse="Weaver")


def _fabric(name="A_Workspace") -> FabricWorkspace:
    return FabricWorkspace(
        workspace=name, weaver_lakehouse="Weaver", environment="weaver"
    )


# --- identity ---------------------------------------------------------------


def test_a_console_session_needs_no_workspace_to_exist(console):
    assert console.workspace is None


def test_a_command_without_a_workspace_says_what_is_missing(console):
    with pytest.raises(CommandError, match="needs a workspace"):
        console.scope(None)


def test_a_command_may_name_a_workspace_the_session_did_not(console):
    scope = console.scope(_local())

    assert scope.workspace == _local()


def test_two_commands_naming_one_workspace_share_its_resources(console):
    first = console.scope(_local())
    second = console.scope(_local())

    assert first is second


def test_two_workspaces_are_two_contexts(console):
    assert console.scope(_local("./one")) is not console.scope(_local("./two"))


def test_a_default_workspace_is_a_default_and_not_an_identity():
    with ConsoleSession(workspace=_local("./default")) as session:
        assert session.scope(None).workspace == _local("./default")
        assert session.scope(_local("./other")).workspace == _local("./other")


def test_the_default_is_what_the_session_started_with_and_never_accumulates():
    """A command naming a workspace does not make it the session's default.

    Inheritance is only ever from what ``weaver session`` was started with.
    Learning a default from whichever command ran last would mean the *next*
    command silently inherited another workspace's Environment and control
    Lakehouse — a plausible-looking build into the wrong place.
    """

    with ConsoleSession(workspace=_local("./default")) as session:
        session.scope(_local("./elsewhere"))

        assert session.workspace == _local("./default")
        assert session.scope(None).workspace == _local("./default")


def test_a_session_started_without_a_workspace_never_gains_one():
    with ConsoleSession() as session:
        session.scope(_local("./named-by-a-command"))

        assert session.workspace is None
        with pytest.raises(CommandError, match="needs a workspace"):
            session.scope(None)


def test_context_identity_ignores_which_targets_were_declared():
    plain = _fabric()
    with_targets = FabricWorkspace(
        workspace="A_Workspace",
        weaver_lakehouse="Weaver",
        environment="weaver",
        lakehouses={
            "Sales": TargetDeclaration(item=WeaverItemId.parse("Lakehouse/Sales")),
        },
    )

    # The same place, the same control Lakehouse and the same Environment: one
    # Livy session serves both, so they must not be two contexts.
    assert workspace_context(plain) == workspace_context(with_targets)


def test_a_different_control_lakehouse_is_a_different_context():
    other = FabricWorkspace(
        workspace="A_Workspace", weaver_lakehouse="Other", environment="weaver"
    )

    assert workspace_context(_fabric()) != workspace_context(other)


# --- position ---------------------------------------------------------------


def test_a_console_against_the_emulator_executes_here(console):
    assert console.executes_here(_local()) is True


def test_a_console_reaching_into_fabric_does_not_execute_here(console):
    assert console.executes_here(_fabric()) is False


def test_a_console_reaching_into_fabric_has_no_spark_of_its_own(console):
    with pytest.raises(CommandError, match="cross with execute_python"):
        console.spark(_fabric())


def test_a_console_cannot_address_a_workspace_kind_it_has_no_host_for(console):
    class Elsewhere:
        workspace = "somewhere"
        workspace_type = "elsewhere"
        weaver_lakehouse = None
        environment = None

    with pytest.raises(CommandError, match="cannot address"):
        console.scope(Elsewhere())


# --- reporting context ------------------------------------------------------


def test_reporting_frames_nest_and_unwind(console):
    console.task_started("build")
    console.step_started("install")

    assert [frame.name for frame in console.frames] == ["build", "install"]

    console.step_completed("install")
    assert [frame.name for frame in console.frames] == ["build"]

    console.task_completed("build")
    assert console.frames == ()


def test_completing_a_task_unwinds_the_steps_beneath_it(console):
    console.task_started("build")
    console.step_started("install")
    console.substep_started("create_schema")

    console.task_failed("build", RuntimeError("stopped"))

    assert console.frames == ()


# --- lifetime ---------------------------------------------------------------


def test_a_closed_session_serves_no_further_commands():
    session = ConsoleSession()
    session.scope(_local())
    session.close()

    with pytest.raises(CommandError, match="closed"):
        session.scope(_local())


def test_closing_twice_is_not_an_error():
    session = ConsoleSession()
    session.close()
    session.close()


# --- an acquired resource is a running one -----------------------------------


def test_the_livy_resource_is_started_before_anyone_is_handed_it(monkeypatch):
    """Acquiring means the expensive part is over.

    ``for_workspace`` builds the object; ``start`` is what asks Fabric for the
    session. A resource that returned the unstarted object looked acquired to
    everything above it — ready state, shared by the next caller — and the first
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

    built = FakeLivy()
    monkeypatch.setattr(
        "weaver.fabric.LivySession.for_workspace",
        classmethod(lambda cls, *args, **kwargs: built),
    )

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()
        monkeypatch.setattr(scope, "_acquire_token_provider", lambda: (lambda: "t"))
        monkeypatch.setattr(type(scope), "resolver", property(lambda self: object()))

        assert scope.livy.get() is built
        assert built.started, "the resource handed out a session that was never started"


# --- the published wheel and this checkout -----------------------------------


def test_a_version_difference_warns_and_names_the_fix(monkeypatch):
    """A difference is worth saying; it is not worth refusing over.

    The console prepares work locally and runs it against the published wheel,
    so the two drift the moment either moves. During rapid development that is
    usually harmless — putting a publish in front of every experiment is not.
    """

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()
        monkeypatch.setattr(scope, "livy_run", lambda *a, **k: "9.9.9-elsewhere")

        scope.check_published_version(session.warn)

        assert session.warnings
        assert "9.9.9-elsewhere" in session.warnings[0]
        assert "weaver install" in session.warnings[0]


def test_a_matching_version_says_nothing(monkeypatch):
    from weaver import __version__

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()
        monkeypatch.setattr(scope, "livy_run", lambda *a, **k: __version__)

        scope.check_published_version(session.warn)

        assert session.warnings == []


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


def test_a_check_that_cannot_run_never_fails_the_work(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("the probe itself failed")

    with ConsoleSession(workspace=_fabric()) as session:
        scope = session.scope()
        monkeypatch.setattr(scope, "livy_run", broken)

        scope.check_published_version(session.warn)  # does not raise

        assert session.warnings == []
