"""The wipe command. Destructive, so it asks before acting."""

from __future__ import annotations

import importlib
from dataclasses import dataclass

import pytest

from weaver_cli import main
from weaver_cli.main import build_parser


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


@dataclass(frozen=True)
class _Report:
    target: str
    location: str
    removed: tuple[str, ...] = ("object",)
    dry_run: bool = False

    @property
    def count(self):
        return len(self.removed)


def test_typed_flags_accept_hyphens_and_the_documented_underscore_aliases():
    args = build_parser().parse_args(
        [
            "wipe",
            "--lakehouse-target",
            "Sales",
            "--warehouse_target",
            "Sales",
            "--folder_target",
            "Sales/Files/Inbound",
            "--root",
            ".local",
        ]
    )

    assert args.lakehouse_targets == ["Sales"]
    assert args.warehouse_targets == ["Sales"]
    assert args.folder_targets == ["Sales/Files/Inbound"]


def test_typed_targets_route_to_core_without_name_inference(
    tmp_path, monkeypatch, capsys
):
    config = tmp_path / "env.yml"
    config.write_text(
        "hosts:\n  Weaver:\n    type: Fabric\n    workspace: Analytics\n",
        encoding="utf-8",
    )
    cli = importlib.import_module("weaver_cli.main")
    store = object()
    sql = object()
    calls = []

    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(cli, "_desktop_store", lambda host: store)

    def lakehouse_wipe(target, host, *, store, dry_run=False):
        calls.append(("lakehouse", target.name, store, dry_run))
        return (
            _Report(
                target=f"lakehouse:{target.name}",
                location=f"/{target.name}",
                dry_run=dry_run,
            ),
        )

    def folder_wipe(target, host, *, store, dry_run=False):
        calls.append(("folder", str(target), store, dry_run))
        return _Report(
            target=f"folder:{target}",
            location=f"/{target}",
            dry_run=dry_run,
        )

    class DesktopExecutor:
        def __enter__(self):
            return sql

        def __exit__(self, *exc):
            return False

    def desktop_executor(target, host):
        calls.append(("desktop sql", target.warehouse.name))
        return DesktopExecutor()

    def warehouse_wipe(target, host, *, sql):
        calls.append(("warehouse", target.warehouse.name, sql))

    monkeypatch.setattr("weaver.wipe_lakehouse", lakehouse_wipe)
    monkeypatch.setattr("weaver.wipe_folder_target", folder_wipe)
    monkeypatch.setattr("weaver.wipe_sql_target", warehouse_wipe)
    monkeypatch.setattr("weaver.fabric.desktop_sql_executor", desktop_executor)

    assert main(
        [
            "wipe",
            "--lakehouse_target",
            "Shared",
            "--warehouse_target",
            "Shared",
            "--folder_target",
            "Shared/Files/Inbound",
            "--host",
            "Weaver",
            "--hosts",
            str(config),
            "--yes",
        ]
    ) == 0

    assert calls == [
        ("lakehouse", "Shared", store, True),
        ("folder", "Shared/Files/Inbound", store, True),
        ("lakehouse", "Shared", store, False),
        ("folder", "Shared/Files/Inbound", store, False),
        ("desktop sql", "Shared"),
        ("warehouse", "Shared", sql),
    ]
    output = capsys.readouterr().out
    assert "lakehouse:Shared" in output
    assert "warehouse:Shared" in output
    assert "folder:Shared/Files/Inbound" in output


def test_a_warehouse_target_requires_a_fabric_host(tmp_path, capsys):
    assert main(
        [
            "wipe",
            "--warehouse-target",
            "Reporting",
            "--root",
            str(tmp_path),
            "--yes",
        ]
    ) == 1
    assert "require a Fabric host" in capsys.readouterr().err


def test_warehouse_only_routing_never_constructs_a_store(
    tmp_path, monkeypatch
):
    config = tmp_path / "env.yml"
    config.write_text(
        "hosts:\n  Weaver:\n    type: Fabric\n    workspace: Analytics\n",
        encoding="utf-8",
    )
    cli = importlib.import_module("weaver_cli.main")
    sql = object()
    calls = []

    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(
        cli,
        "_desktop_store",
        lambda host: (_ for _ in ()).throw(
            AssertionError("Warehouse-only wipe constructed a Store")
        ),
    )

    class DesktopExecutor:
        def __enter__(self):
            return sql

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(
        "weaver.fabric.desktop_sql_executor",
        lambda target, host: DesktopExecutor(),
    )
    monkeypatch.setattr(
        "weaver.wipe_sql_target",
        lambda target, host, *, sql: calls.append(
            (target.warehouse.name, sql)
        ),
    )

    assert main(
        [
            "wipe",
            "--warehouse-target",
            "Reporting",
            "--host",
            "Weaver",
            "--hosts",
            str(config),
            "--yes",
        ]
    ) == 0
    assert calls == [("Reporting", sql)]


# --- naming the target -------------------------------------------------------


def test_an_item_name_clears_the_whole_item(populated_folders, capsys):
    assert main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--root", str(populated_folders.root), "--yes",
    ]) == 0
    assert list(files_root(populated_folders).iterdir()) == []


def test_a_folder_path_narrows_to_that_root(populated_folders, capsys):
    exit_code = main([
        "wipe", "--folder-target", "Sales_LH/Files/Sales",
        "--root", str(populated_folders.root), "--yes",
    ])
    assert exit_code == 0
    assert (files_root(populated_folders) / "notes.txt").exists()
    assert list((files_root(populated_folders) / "Sales").iterdir()) == []


def test_several_targets_at_once(populated_folders, capsys):
    exit_code = main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--lakehouse-target", "Weaver",
        "--root", str(populated_folders.root), "--yes",
    ])
    assert exit_code == 0
    printed = capsys.readouterr().out
    assert "Sales_LH" in printed and "Weaver" in printed


def test_an_unknown_item_is_reported(populated_folders, capsys):
    exit_code = main([
        "wipe", "--lakehouse-target", "Nope",
        "--root", str(populated_folders.root), "--yes",
    ])
    assert exit_code == 1
    assert "no Lakehouse named" in capsys.readouterr().err


# --- safety ------------------------------------------------------------------


def test_a_dry_run_changes_nothing(populated_folders, capsys):
    exit_code = main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--root", str(populated_folders.root), "--dry-run",
    ])
    assert exit_code == 0
    assert "Nothing was changed" in capsys.readouterr().out
    assert (files_root(populated_folders) / "notes.txt").exists()


def test_it_refuses_to_act_unattended_without_yes(populated_folders, capsys):
    """A non-interactive caller must say --yes, or nothing happens."""
    exit_code = main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--root", str(populated_folders.root),
    ])
    assert exit_code == 1
    assert "Refusing to remove" in capsys.readouterr().err
    assert (files_root(populated_folders) / "notes.txt").exists()


def test_a_declined_confirmation_changes_nothing(populated_folders, capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "n")
    exit_code = main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--root", str(populated_folders.root),
    ])
    assert exit_code == 1
    assert "Cancelled" in capsys.readouterr().out
    assert (files_root(populated_folders) / "notes.txt").exists()


def test_an_accepted_confirmation_acts(populated_folders, capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "y")
    assert main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--root", str(populated_folders.root),
    ]) == 0
    assert list(files_root(populated_folders).iterdir()) == []


def test_an_already_empty_target_needs_no_confirmation(lakehouses, capsys):
    """Sales_LH exists with both areas but nothing in them."""
    exit_code = main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--root", str(lakehouses.root),
    ])
    assert exit_code == 0
    assert "Nothing to remove" in capsys.readouterr().out


def test_the_plan_is_printed_before_anything_is_removed(populated_folders, capsys):
    main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--root", str(populated_folders.root), "--yes",
    ])
    printed = capsys.readouterr().out
    assert printed.index("folder:Sales_LH/Files") < printed.index("removed")


# --- host resolution ---------------------------------------------------------


def test_a_host_comes_from_the_config(hosts_file, populated_folders, capsys):
    exit_code = main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--host", "MyLocal", "--hosts", str(hosts_file), "--yes",
    ])
    assert exit_code == 0
    assert "MyLocal" in capsys.readouterr().out
    assert list(files_root(populated_folders).iterdir()) == []


def test_an_unknown_host_lists_what_there_is(hosts_file, capsys):
    exit_code = main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--host", "Absent", "--hosts", str(hosts_file), "--yes",
    ])
    assert exit_code == 1
    error = capsys.readouterr().err
    assert "no host 'Absent'" in error and "MyLocal" in error


def test_a_host_needs_a_config_to_be_looked_up_in(capsys):
    assert main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--host", "MyLocal", "--yes",
    ]) == 1
    assert "--hosts" in capsys.readouterr().err


def test_root_and_config_are_alternatives(hosts_file, capsys):
    exit_code = main([
        "wipe", "--lakehouse-target", "Sales_LH",
        "--root", "/tmp", "--host", "MyLocal",
        "--hosts", str(hosts_file), "--yes",
    ])
    assert exit_code == 1
    assert "drop --host and --hosts" in capsys.readouterr().err


def test_local_host_has_a_within_host_store(tmp_path):
    from weaver import LocalHost
    from weaver.resolution import LocalResolver, resolver_for, store_for
    from weaver.store import LocalStore

    local = LocalHost(root=tmp_path)
    assert isinstance(resolver_for(local), LocalResolver)
    assert isinstance(store_for(local), LocalStore)


def test_a_fabric_store_is_only_available_inside_fabric():
    """Desktop DFS is not the default Fabric storage path — it is injected."""
    from weaver import FabricHost
    from weaver.errors import CommandError
    from weaver.resolution import store_for

    with pytest.raises(CommandError, match="OneLakeDfsClient"):
        store_for(FabricHost(workspace="Analytics", weaver_lakehouse="Weaver"))


def test_wipe_needs_a_target(populated_folders, capsys):
    assert main(["wipe", "--root", str(populated_folders.root), "--yes"]) == 1
    assert "at least one --lakehouse-target" in capsys.readouterr().err
