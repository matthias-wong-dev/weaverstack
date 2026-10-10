"""Unbind reads the catalogue and the workspace, and changes only the catalogue."""

from __future__ import annotations

import pytest
from support.sessions import given_session
from support.weaver_test import weaver_test
from support.workspaces import given_resolver, given_workspace
from test_unbind_cycle import _Catalogue

import weaver
from weaver.errors import CommandError
from weaver.store import FilesystemStore

ROWS = (
    {"item_type": "Lakehouse", "item_name": "Sales", "target_name": "Sales_Dev"},
    {
        "item_type": "SemanticModel",
        "item_name": "Reporting",
        "target_name": "Reporting_Dev",
    },
    {"item_type": "Report", "item_name": "Executive", "target_name": "Executive_Dev"},
)


@pytest.fixture
def estate(monkeypatch):
    """Sales_Dev still exists; Reporting_Dev was deleted from the workspace."""

    from weaver.catalogue import connection

    catalogue = _Catalogue(ROWS)
    monkeypatch.setattr(connection, "catalogue_connection", lambda *_a, **_k: catalogue)
    workspace = given_workspace(catalogue="Warehouse/Weaver")
    session = given_session(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=given_resolver(
            workspace=workspace, lakehouses=("Sales_Dev",), warehouses=("Weaver",)
        ),
    )
    return session


def plan(session, *targets):
    return weaver.plan_unbind(
        targets, workspace="Demo", catalogue="Warehouse/Weaver", session=session
    )


@weaver_test()
def test_plan_names_the_logical_items_and_whether_each_target_exists(estate):
    settled = plan(estate, "Lakehouse/Sales_Dev", "SemanticModel/Reporting_Dev")
    assert settled.still_in_fabric == ("Lakehouse/Sales_Dev",)
    lines = [" ".join(line.split()) for line in settled.describe().splitlines()]
    assert lines[2:5] == [
        "Forget",
        "Lakehouse/Sales → Lakehouse/Sales_Dev still in Fabric",
        "SemanticModel/Reporting → SemanticModel/Reporting_Dev not in Fabric",
    ]


@weaver_test()
def test_unbind_runs_only_the_claim_deletion(estate, monkeypatch):
    captured = []
    execute = estate.execute_mutation

    def record(mutation, payloads):
        captured.append(mutation)
        return execute(mutation, payloads)

    monkeypatch.setattr(estate, "execute_mutation", record)
    result = weaver.unbind(
        plan=plan(estate, "SemanticModel/Reporting_Dev"), session=estate
    )
    (mutation,) = captured
    assert [
        a.kind for s in mutation.sequences for b in s.batches for a in b.actions
    ] == ["unbind_catalogue_claims"]
    assert result.logical_items == ("SemanticModel/Reporting",)
    deletes = [s for s in estate.tsql if "DELETE" in s.upper()]
    assert deletes and all("Reporting" in s for s in deletes)
    assert not any("Sales" in s for s in deletes)


@weaver_test()
def test_a_report_claim_unbinds_like_any_other(estate):
    result = weaver.unbind(plan=plan(estate, "Report/Executive_Dev"), session=estate)

    assert result.logical_items == ("Report/Executive",)
    deletes = [s for s in estate.tsql if "DELETE" in s.upper()]
    assert deletes and all("Executive" in s for s in deletes)
    assert not any("Reporting" in s or "Sales" in s for s in deletes)


@weaver_test()
def test_dry_run_changes_nothing(estate):
    result = weaver.unbind(
        "SemanticModel/Reporting_Dev",
        workspace="Demo",
        catalogue="Warehouse/Weaver",
        dry_run=True,
        session=estate,
    )
    assert result.dry_run and not result.logical_items
    assert not any("DELETE" in s.upper() for s in estate.tsql)


@pytest.mark.parametrize(
    "targets,message",
    [
        ((), "at least one target"),
        (("Lakehouse/Unknown",), "records no item in Lakehouse/Unknown"),
        (("Warehouse/Weaver",), "is the catalogue"),
    ],
)
@weaver_test()
def test_refusals_precede_any_change(estate, targets, message):
    with pytest.raises(CommandError, match=message):
        plan(estate, *targets)
    assert not any("DELETE" in s.upper() for s in estate.tsql)


@pytest.mark.parametrize(
    "target,answer,unbound",
    [
        ("SemanticModel/Reporting_Dev", None, True),
        ("Lakehouse/Sales_Dev", None, False),
        ("Lakehouse/Sales_Dev", "--yes", True),
    ],
)
@weaver_test()
def test_cli_confirms_only_an_item_that_still_exists(
    estate, monkeypatch, capsys, target, answer, unbound
):
    import importlib
    from contextlib import nullcontext

    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: estate.workspace)
    monkeypatch.setattr(
        cli, "_running_session", lambda _args, _workspace: nullcontext(estate)
    )
    arguments = [
        "unbind",
        target,
        "--workspace",
        "Demo",
        "--catalogue",
        "Warehouse/Weaver",
        "--non-interactive",
    ]
    code = cli.main(arguments + ([answer] if answer else []))
    output = capsys.readouterr()
    assert (code == 0) is unbound
    assert any("DELETE" in s.upper() for s in estate.tsql) is unbound
    if not unbound:
        assert "Lakehouse/Sales_Dev is still in Fabric" in output.err + output.out
