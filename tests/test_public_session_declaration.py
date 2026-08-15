"""``weaver.session()`` — the reusable context, and what opening one costs.

Two claims. The first is that it exists as a callable at all: ``weaver.session``
used to be the session *package*, so the name a caller would reach for was
already taken by a module. That collision is why the package is now
``weaver.sessions``.

The second is that opening one is cheap. Everything a Session holds is expensive
and everything is lazy, so a caller who opens one and does nothing has paid for
a credential object and nothing else — no resolved items, no Livy session, no
TDS connection, nothing published.
"""

from __future__ import annotations

import pytest

import weaver
from weaver.errors import CommandError, ConfigError
from weaver.sessions import ConsoleSession
from weaver.workspaces import Workspace


def test_the_public_name_is_a_callable_rather_than_a_module():
    """The collision the rename removed.

    ``import weaver; weaver.session(...)`` is the documented form, and a module
    is not callable — so this is the whole reason ``weaver.session`` could not
    stay the package's name.
    """

    assert callable(weaver.session)


def test_a_named_workspace_is_enough(desktop_credential):
    with weaver.session(workspace="Demo", catalogue="Warehouse/Weaver") as opened:
        assert isinstance(opened, ConsoleSession)
        assert opened.workspace.workspace == "Demo"
        assert opened.workspace.catalogue == "Warehouse/Weaver"


def test_a_resolved_workspace_is_taken_as_it_is(desktop_credential):
    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Weaver")

    with weaver.session(workspace=workspace) as opened:
        assert opened.workspace is workspace


def test_a_resolved_workspace_and_a_configuration_file_is_refused(
    tmp_path, desktop_credential
):
    """One of them describes the workspace. Two would leave which one unclear."""

    config = tmp_path / "workspace.yml"
    config.write_text("workspace: Other\n", encoding="utf-8")

    with pytest.raises(CommandError, match="nothing to add"):
        weaver.session(
            workspace=Workspace(workspace="Demo"),
            workspace_config=config,
        )


def test_configuration_supplies_what_was_not_named(tmp_path, desktop_credential):
    config = tmp_path / "workspace.yml"
    config.write_text(
        "workspace: Demo\ncatalogue: Warehouse/Configured\nenvironment: weaver\n",
        encoding="utf-8",
    )

    with weaver.session(workspace_config=config) as opened:
        assert opened.workspace.catalogue == "Warehouse/Configured"
        assert opened.workspace.environment == "weaver"


def test_an_explicit_value_wins_over_the_configured_one(tmp_path, desktop_credential):
    config = tmp_path / "workspace.yml"
    config.write_text(
        "workspace: Demo\ncatalogue: Warehouse/Configured\n", encoding="utf-8"
    )

    with weaver.session(
        workspace_config=config, catalogue="Warehouse/Explicit"
    ) as opened:
        assert opened.workspace.catalogue == "Warehouse/Explicit"


def test_naming_no_workspace_says_which_value_is_missing():
    with pytest.raises(ConfigError, match="--workspace"):
        weaver.session()


def test_opening_a_session_acquires_nothing(desktop_credential):
    """The property that makes one Session per script the right default.

    A Session that resolved items or started Livy on open would make holding one
    expensive, and callers would go back to opening one per operation — which is
    the cost this exists to remove.
    """

    with weaver.session(workspace="Demo", catalogue="Warehouse/Weaver") as opened:
        assert opened.telemetry.counters.get("session.scopes", 0) == 0
        assert opened.telemetry.counters.get("resolve.item", 0) == 0


def test_a_closed_session_says_so_rather_than_reopening(desktop_credential):
    opened = weaver.session(workspace="Demo", catalogue="Warehouse/Weaver")
    opened.close()

    assert opened.closed
    with pytest.raises(CommandError, match="closed"):
        opened.scope()
