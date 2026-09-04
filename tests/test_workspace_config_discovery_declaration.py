"""Finding `workspace-config.yml` in the directory a command was run from.

A generated project answers to `weaver build`, `weaver load` and `weaver test`
from its own root, which is what the initialise output tells a new user to type.
That works because a command naming no workspace and inheriting none reads the
configuration file beside it.

It is the last resort. An explicit `--workspace` or `--workspace-config` and a
Session's own workspace are each consulted first, so nothing already working
starts reading a different file.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.config import (
    DEFAULT_FILE,
    discovered_workspace_config,
    resolve_workspace,
)
from weaver.errors import ConfigError
from weaver.operations.workspace import operation_workspace
from weaver.workspaces import Workspace

PROJECT = """\
workspace: Weaver Example
environment: Weaver
catalogue: Warehouse/Catalogue

targets:
  Lakehouse/Landing: Landing
"""

OTHER = """\
workspace: Somewhere Else
catalogue: Warehouse/Other
"""


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A project directory, with the process running inside it."""

    (tmp_path / DEFAULT_FILE).write_text(PROJECT, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return tmp_path


@weaver_test()
def test_a_project_directory_is_found(project):
    assert discovered_workspace_config() == project / DEFAULT_FILE


@weaver_test()
def test_a_directory_without_one_is_not(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert discovered_workspace_config() is None


@weaver_test()
def test_a_command_naming_nothing_reads_the_project_it_is_run_in(project):
    resolved = resolve_workspace()

    assert resolved.workspace == "Weaver Example"
    assert resolved.catalogue == "Warehouse/Catalogue"
    assert [str(item) for item in resolved.targets] == ["Lakehouse/Landing"]


@weaver_test()
def test_an_operation_naming_nothing_reads_it_too(project):
    resolved = operation_workspace("build")

    assert resolved.workspace == "Weaver Example"


@weaver_test()
def test_a_named_configuration_file_wins(project, tmp_path):
    other = tmp_path / "other.yml"
    other.write_text(OTHER, encoding="utf-8")

    resolved = resolve_workspace(workspace_config=str(other))

    assert resolved.workspace == "Somewhere Else"


@weaver_test()
def test_a_named_workspace_wins(project):
    """An explicit name is the whole answer, and brings no targets with it."""

    resolved = resolve_workspace(workspace="Somewhere Else")

    assert resolved.workspace == "Somewhere Else"
    assert resolved.targets == {}


@weaver_test()
def test_a_sessions_own_workspace_wins(project):
    """Inside `weaver session` the workspace is the session's, wherever it runs."""

    session = type("Session", (), {"workspace": Workspace(workspace="Session's Own")})()

    resolved = operation_workspace("build", session=session, needs_catalogue=False)

    assert resolved.workspace == "Session's Own"


@weaver_test()
def test_a_directory_with_no_project_says_what_to_do(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ConfigError, match=DEFAULT_FILE):
        resolve_workspace()


@weaver_test()
def test_an_invalid_discovered_file_is_reported_against_itself(tmp_path, monkeypatch):
    """A file that was found still has to parse, and says so as itself."""

    (tmp_path / DEFAULT_FILE).write_text("targets: []\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ConfigError, match="must define 'workspace'"):
        resolve_workspace()
