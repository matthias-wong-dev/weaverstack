"""The wipe command. Destructive, so it asks before acting."""

from __future__ import annotations

import pytest

from weaver_cli import main


@pytest.fixture
def hosts_file(tmp_path, populated_folders):
    config = tmp_path / "env.yml"
    config.write_text(
        f"hosts:\n  MyLocal:\n    type: Local\n    root: {populated_folders.root}\n"
        f"    weaver_lakehouse: Weaver\n",
        encoding="utf-8",
    )
    return config


def files_root(lakehouses):
    return lakehouses.resolver.files_root(lakehouses.target).path


# --- naming the target -------------------------------------------------------


def test_an_item_name_clears_the_whole_item(populated_folders, capsys):
    assert main(["wipe", "--target", "Sales_LH", "--root", str(populated_folders.root), "--yes"]) == 0
    assert list(files_root(populated_folders).iterdir()) == []


def test_a_folder_path_narrows_to_that_root(populated_folders, capsys):
    exit_code = main([
        "wipe", "--target", "Sales_LH/Files/Sales", "--root", str(populated_folders.root), "--yes",
    ])
    assert exit_code == 0
    assert (files_root(populated_folders) / "notes.txt").exists()
    assert list((files_root(populated_folders) / "Sales").iterdir()) == []


def test_several_targets_at_once(populated_folders, capsys):
    exit_code = main([
        "wipe", "--target", "Sales_LH", "--target", "Weaver",
        "--root", str(populated_folders.root), "--yes",
    ])
    assert exit_code == 0
    printed = capsys.readouterr().out
    assert "Sales_LH" in printed and "Weaver" in printed


def test_an_unknown_item_is_reported(populated_folders, capsys):
    exit_code = main(["wipe", "--target", "Nope", "--root", str(populated_folders.root), "--yes"])
    assert exit_code == 1
    assert "no Lakehouse named" in capsys.readouterr().err


# --- safety ------------------------------------------------------------------


def test_a_dry_run_changes_nothing(populated_folders, capsys):
    exit_code = main(["wipe", "--target", "Sales_LH", "--root", str(populated_folders.root), "--dry-run"])
    assert exit_code == 0
    assert "Nothing was changed" in capsys.readouterr().out
    assert (files_root(populated_folders) / "notes.txt").exists()


def test_it_refuses_to_act_unattended_without_yes(populated_folders, capsys):
    """A non-interactive caller must say --yes, or nothing happens."""
    exit_code = main(["wipe", "--target", "Sales_LH", "--root", str(populated_folders.root)])
    assert exit_code == 1
    assert "Refusing to remove" in capsys.readouterr().err
    assert (files_root(populated_folders) / "notes.txt").exists()


def test_a_declined_confirmation_changes_nothing(populated_folders, capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "n")
    exit_code = main(["wipe", "--target", "Sales_LH", "--root", str(populated_folders.root)])
    assert exit_code == 1
    assert "Cancelled" in capsys.readouterr().out
    assert (files_root(populated_folders) / "notes.txt").exists()


def test_an_accepted_confirmation_acts(populated_folders, capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "y")
    assert main(["wipe", "--target", "Sales_LH", "--root", str(populated_folders.root)]) == 0
    assert list(files_root(populated_folders).iterdir()) == []


def test_an_already_empty_target_needs_no_confirmation(lakehouses, capsys):
    """Sales_LH exists with both areas but nothing in them."""
    exit_code = main(["wipe", "--target", "Sales_LH", "--root", str(lakehouses.root)])
    assert exit_code == 0
    assert "Nothing to remove" in capsys.readouterr().out


def test_the_plan_is_printed_before_anything_is_removed(populated_folders, capsys):
    main(["wipe", "--target", "Sales_LH", "--root", str(populated_folders.root), "--yes"])
    printed = capsys.readouterr().out
    assert printed.index("folder:Sales_LH/Files") < printed.index("removed")


# --- host resolution ---------------------------------------------------------


def test_a_host_comes_from_the_config(hosts_file, populated_folders, capsys):
    exit_code = main([
        "wipe", "--target", "Sales_LH", "--host", "MyLocal", "--hosts", str(hosts_file), "--yes",
    ])
    assert exit_code == 0
    assert "MyLocal" in capsys.readouterr().out
    assert list(files_root(populated_folders).iterdir()) == []


def test_an_unknown_host_lists_what_there_is(hosts_file, capsys):
    exit_code = main([
        "wipe", "--target", "Sales_LH", "--host", "Absent", "--hosts", str(hosts_file), "--yes",
    ])
    assert exit_code == 1
    error = capsys.readouterr().err
    assert "no host 'Absent'" in error and "MyLocal" in error


def test_a_host_needs_a_config_to_be_looked_up_in(capsys):
    assert main(["wipe", "--target", "Sales_LH", "--host", "MyLocal", "--yes"]) == 1
    assert "--hosts" in capsys.readouterr().err


def test_root_and_config_are_alternatives(hosts_file, capsys):
    exit_code = main([
        "wipe", "--target", "Sales_LH", "--root", "/tmp", "--host", "MyLocal",
        "--hosts", str(hosts_file), "--yes",
    ])
    assert exit_code == 1
    assert "drop --host and --hosts" in capsys.readouterr().err


def test_a_fabric_host_resolves_to_the_fabric_implementation(tmp_path):
    """Dispatch happens on host type, without any network call."""
    from weaver import FabricHost, LocalHost
    from weaver.fabric import FabricResolver, FabricStore
    from weaver.resolution import LocalResolver, resolver_for, store_for
    from weaver.store import LocalStore

    fabric = FabricHost(workspace="Analytics", weaver_lakehouse="Weaver")
    assert isinstance(resolver_for(fabric), FabricResolver)
    assert isinstance(store_for(fabric), FabricStore)

    local = LocalHost(root=tmp_path)
    assert isinstance(resolver_for(local), LocalResolver)
    assert isinstance(store_for(local), LocalStore)


def test_wipe_needs_a_target(populated_folders, capsys):
    assert main(["wipe", "--root", str(populated_folders.root), "--yes"]) == 1
    assert "at least one --target" in capsys.readouterr().err
