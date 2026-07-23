"""Shipping and bootstrapping, in the parts that need no tenant."""

from __future__ import annotations

import pytest

from weaver import FabricHost
from weaver.errors import ConfigError
from weaver.fabric.livy import RESULT_PREFIX, StatementResult, _payload, sessions_url
from weaver.fabric.runtime import (
    _package_files,
    bootstrap_source,
    mounted_package_parent,
    package_root,
)


def host(**kwargs) -> FabricHost:
    defaults = {"workspace": "Analytics", "weaver_lakehouse": "Weaver"}
    return FabricHost(**{**defaults, **kwargs})


# --- what gets shipped -------------------------------------------------------


def test_the_package_is_found_and_has_files():
    files = _package_files(package_root())
    names = {relative for relative, _ in files}
    assert "__init__.py" in names
    assert "fabric/onelake.py" in names


def test_caches_are_never_shipped():
    assert not any(
        "__pycache__" in relative for relative, _ in _package_files(package_root())
    )


def test_the_cli_is_not_shipped():
    """A session has no use for it, and the core must not depend on it."""
    assert not any(
        relative.startswith("weaver_cli") for relative, _ in _package_files(package_root())
    )


# --- where it lands, and what imports it -------------------------------------


def test_sys_path_gets_the_parent_of_the_package_directory():
    """`import weaver` searches sys.path for a directory named weaver, so
    shipping to Files/weaver means inserting Files."""
    parent = mounted_package_parent(host(weaver_install="Weaver/Files/weaver"))
    assert parent == "/lakehouse/default/Files"


def test_a_nested_install_path_keeps_its_parents():
    parent = mounted_package_parent(host(weaver_install="Weaver/Files/runtime/weaver"))
    assert parent == "/lakehouse/default/Files/runtime"


def test_the_convention_applies_when_no_install_is_named():
    assert mounted_package_parent(host()) == "/lakehouse/default/Files"


def test_the_bootstrap_prefers_an_installed_package():
    """The day Weaver comes from a Fabric Environment, the fallback is dead."""
    source = bootstrap_source("/lakehouse/default/Files")
    assert source.index("import weaver") < source.index("sys.path.insert")
    assert "except ImportError" in source


def test_an_install_path_must_name_a_lakehouse_and_a_path():
    with pytest.raises(ConfigError, match="Lakehouse and a path"):
        host(weaver_install="weaver")


def test_the_install_path_is_carried_on_the_host():
    assert host(weaver_install="Weaver/Files/weaver").weaver_install == "Weaver/Files/weaver"


def test_a_hosts_file_can_declare_it(tmp_path):
    from weaver import load_hosts

    config = tmp_path / "env.yml"
    config.write_text(
        "hosts:\n  MyFabric:\n    type: Fabric\n    workspace: Analytics\n"
        "    weaver_lakehouse: Weaver\n    weaver_install: Weaver/Files/weaver\n",
        encoding="utf-8",
    )
    assert load_hosts(config)["MyFabric"].weaver_install == "Weaver/Files/weaver"


# --- livy plumbing -----------------------------------------------------------


def test_the_sessions_url_names_workspace_and_lakehouse():
    url = sessions_url("ws-id", "lh-id")
    assert "/workspaces/ws-id/lakehouses/lh-id/livyapi/" in url
    assert url.endswith("/sessions")


def test_a_returned_value_is_told_from_printed_output():
    text = f"some log line\n{RESULT_PREFIX}" + '{"removed": 2}\n' + "another line"
    assert _payload(text) == {"removed": 2}


def test_output_with_no_returned_value():
    assert _payload("just logging\n") is None
    assert StatementResult(text="x").returned is False


def test_the_last_returned_value_wins():
    text = f"{RESULT_PREFIX}" + '{"n": 1}\n' + f"{RESULT_PREFIX}" + '{"n": 2}\n'
    assert _payload(text) == {"n": 2}


def test_malformed_json_is_not_a_result():
    assert _payload(f"{RESULT_PREFIX}not json\n") is None
