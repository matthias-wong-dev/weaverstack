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
from weaver.session.protocol import PROTOCOL_ERROR, PROTOCOL_VERSION, check, guarded
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


# --- remote protocol --------------------------------------------------------


def test_a_guarded_program_refuses_a_weaver_that_speaks_another_protocol():
    source = guarded("emit({'ran': True})", version=PROTOCOL_VERSION)
    emitted = []
    namespace = {"emit": emitted.append}

    # Stand in for a stale Fabric Environment: one whose Weaver has no protocol.
    exec(  # noqa: S102 - the program is the thing under test
        source.replace(
            "from weaver.session.protocol import PROTOCOL_VERSION as _weaver_protocol",
            "raise ImportError('no such module')",
        ),
        namespace,
    )

    assert emitted == [{PROTOCOL_ERROR: {"remote": 0, "local": PROTOCOL_VERSION}}]


def test_a_guarded_program_runs_against_a_current_weaver():
    emitted = []
    exec(  # noqa: S102 - the program is the thing under test
        guarded("emit({'ran': True})"), {"emit": emitted.append}
    )

    assert emitted == [{"ran": True}]


def test_a_protocol_refusal_becomes_a_sentence_about_publishing():
    payload = {PROTOCOL_ERROR: {"remote": 0, "local": 4}}

    with pytest.raises(CommandError, match="weaver install"):
        check(payload, workspace="A_Workspace")


def test_an_ordinary_payload_passes_through_untouched():
    assert check({"status": "succeeded"}) == {"status": "succeeded"}


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
