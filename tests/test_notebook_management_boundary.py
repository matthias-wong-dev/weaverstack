"""Notebook and CLI management requests share names and configuration."""

from __future__ import annotations

from support.weaver_test import weaver_test

import weaver


@weaver_test()
def test_notebook_environment_publication_uses_the_cli_workspace_configuration(
    tmp_path, monkeypatch
):
    from weaver.fabric import environment

    config = tmp_path / "workspace-development.yml"
    config.write_text(
        "workspace: Parcel Development\nenvironment: Weaver\n", encoding="utf-8"
    )
    client = object()
    observed = {}
    result = object()

    def publish(workspace_name, environment=None, **kwargs):
        observed.update(workspace=workspace_name, environment=environment, **kwargs)
        return result

    monkeypatch.setattr(environment, "publish_environment", publish)
    actual = weaver.publish_environment(workspace_config=config, client=client)
    assert actual is result
    assert observed["workspace"] == "Parcel Development"
    assert str(observed["environment"]) == "Weaver"
    assert observed["client"] is client


@weaver_test()
def test_notebook_environment_publication_respects_a_qualified_owner(monkeypatch):
    from weaver.fabric import environment

    observed = {}

    def publish(workspace_name, environment=None, **kwargs):
        observed.update(workspace=workspace_name, environment=environment)
        return observed

    monkeypatch.setattr(environment, "publish_environment", publish)
    result = weaver.publish_environment(
        "Platform Runtimes/ParcelRuntime", client=object()
    )
    assert result["workspace"] == "Platform Runtimes"
    assert str(result["environment"]) == "Platform Runtimes/ParcelRuntime"


@weaver_test()
def test_notebook_environment_refuses_conflicting_definition_forms():
    import pytest

    from weaver.errors import CommandError

    with pytest.raises(CommandError, match="cannot be used together"):
        weaver.publish_environment(
            "Weaver",
            path="Environment/Other.Environment",
            workspace="Parcel Development",
            client=object(),
        )


@weaver_test()
def test_notebook_push_preserves_names_and_explicit_workspace(tmp_path, monkeypatch):
    from weaver.fabric import notebooks

    config = tmp_path / "workspace-development.yml"
    config.write_text("workspace: Parcel Development\n", encoding="utf-8")
    observed = {}

    def push(source, **kwargs):
        observed.update(source=source, **kwargs)
        return observed

    monkeypatch.setattr(notebooks, "push_notebook", push)
    client = object()
    result = weaver.push_notebook(
        "notebooks/ParcelRefresh.py",
        name="ParcelRefresh",
        description="Refresh parcel landing data",
        workspace="Parcel Production",
        workspace_config=config,
        client=client,
    )
    assert result["source"] == "notebooks/ParcelRefresh.py"
    assert result["workspace"] == "Parcel Production"
    assert result["name"] == "ParcelRefresh"
    assert result["description"] == "Refresh parcel landing data"
    assert result["client"] is client


@weaver_test()
def test_notebook_run_inherits_attachments_and_preserves_wait_controls(
    tmp_path, monkeypatch
):
    from weaver.fabric import notebooks

    config = tmp_path / "workspace-development.yml"
    config.write_text(
        "workspace: Parcel Development\nenvironment: Weaver\ntargets:\n  Lakehouse/Landing: ParcelLanding_Dev\n",
        encoding="utf-8",
    )
    observed = {}

    def run(name, **kwargs):
        observed.update(name=name, **kwargs)
        return observed

    monkeypatch.setattr(notebooks, "run_notebook", run)
    result = weaver.run_notebook(
        "ParcelRefresh",
        workspace_config=config,
        wait=False,
        timeout=7200,
        poll_interval=10,
        client=object(),
    )
    assert result["name"] == "ParcelRefresh"
    assert result["workspace"] == "Parcel Development"
    assert result["lakehouse"] == "ParcelLanding_Dev"
    assert str(result["environment"]) == "Weaver"
    assert result["wait"] is False
    assert result["timeout"] == 7200
    assert result["poll_interval"] == 10


@weaver_test()
def test_notebook_run_defaults_match_the_terminal_parser():
    import inspect

    from weaver_cli.main import build_parser

    args = build_parser().parse_args(["fabric", "notebook", "run", "ParcelRefresh"])
    defaults = inspect.signature(weaver.run_notebook).parameters
    assert defaults["timeout"].default == args.timeout
    assert defaults["poll_interval"].default == args.poll_interval
