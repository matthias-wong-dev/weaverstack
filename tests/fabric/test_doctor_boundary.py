"""The crossings `weaver doctor` claims, made against a real workspace.

Doctor is the command a user runs when something will not connect, so what has
to be true is that its checks cross what they say they cross. A check that
passed without reaching Fabric would be worse than no check.

Nothing here creates or deletes an item. The fixed estate is what it points at.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

from weaver.operations.doctor import doctor


def _configuration(
    path, *, workspace, environment, catalogue, warehouse=None, lakehouse=None
):
    """One workspace configuration naming the fixed items this test uses.

    The Lakehouse entry is written as `Lakehouse/Sales`, which is what the
    suite's own `fabric_workspace` declares. A configuration that parses to the
    same Workspace value resolves to the same Session scope, so a check reuses
    the shared Livy session instead of asking the capacity for a second one.
    """

    lines = [
        f"workspace: {workspace}",
        f"environment: {environment}",
        f"catalogue: Warehouse/{catalogue}",
        "targets:",
    ]
    if lakehouse:
        lines.append(f"  Lakehouse/Sales: {lakehouse}")
    if warehouse:
        lines.append(f"  Warehouse/Reporting: {warehouse}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@weaver_test(remote=True, resources={"rest"})
def test_signing_in_and_reaching_fabric_is_proven_by_one_listing(
    session_fabric_client,
):
    report = doctor(client=session_fabric_client)

    assert report.succeeded, [check.detail for check in report.failures]
    assert [check.name for check in report.checks] == ["Fabric REST"]


@weaver_test(remote=True, resources={"rest"})
def test_a_workspace_that_is_there_resolves(fabric_workspace, session_fabric_client):
    report = doctor(workspace=fabric_workspace.workspace, client=session_fabric_client)

    assert report.succeeded, [check.detail for check in report.failures]
    assert report.workspace == fabric_workspace.workspace


@weaver_test(remote=True, resources={"rest"})
def test_a_workspace_that_is_not_there_fails_without_a_traceback(
    session_fabric_client,
):
    report = doctor(
        workspace="weavertest_no_such_workspace", client=session_fabric_client
    )

    assert report.succeeded is False
    failure = report.failures[0]
    assert failure.name == "Workspace weavertest_no_such_workspace"
    assert "was not found" in failure.detail
    assert failure.remedy


@weaver_test(remote=True, resources={"rest", "tds"})
def test_a_warehouse_project_opens_tds_and_starts_no_spark(
    tmp_path,
    fabric_workspace,
    environment_name,
    fabric_catalogue,
    disposable_warehouse,
    weaver_session,
):
    configuration = _configuration(
        tmp_path / "workspace-config.yml",
        workspace=fabric_workspace.workspace,
        environment=environment_name,
        catalogue=fabric_catalogue.name,
        warehouse=disposable_warehouse.item.name,
    )

    report = doctor(workspace_config=str(configuration), session=weaver_session)

    assert report.succeeded, [check.detail for check in report.failures]
    names = [check.name for check in report.checks]
    assert f"Warehouse/{fabric_catalogue.name} TDS" in names
    assert "Spark session" not in names


@weaver_test(remote=True, resources={"livy", "onelake", "rest", "tds"})
def test_a_lakehouse_project_reads_onelake_and_starts_spark(
    tmp_path,
    fabric_workspace,
    environment_name,
    fabric_catalogue,
    fabric_target_lakehouse,
    weaver_session,
):
    """Every surface a Lakehouse estate is reached through, in one report."""

    configuration = _configuration(
        tmp_path / "workspace-config.yml",
        workspace=fabric_workspace.workspace,
        environment=environment_name,
        catalogue=fabric_catalogue.name,
        lakehouse=fabric_target_lakehouse.name,
    )
    from weaver.config import load_workspace

    # The shared Livy session belongs to this workspace value, and a different
    # one would open a second session against the capacity's only slot.
    assert load_workspace(configuration) == fabric_workspace

    report = doctor(workspace_config=str(configuration), session=weaver_session)

    assert report.succeeded, [check.detail for check in report.failures]
    assert [check.name for check in report.checks] == [
        "Fabric REST",
        f"Workspace {fabric_workspace.workspace}",
        f"Warehouse/{fabric_catalogue.name} TDS",
        f"Lakehouse/{fabric_target_lakehouse.name} OneLake",
        "Spark session",
    ]
