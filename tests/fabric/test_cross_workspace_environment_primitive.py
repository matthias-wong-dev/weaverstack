"""A shared Environment attached to Spark in another Fabric workspace."""

from __future__ import annotations

from support.weaver_test import weaver_test


@weaver_test(remote=True)
def test_a_qualified_environment_runs_weaver_in_the_consumer_workspace(
    fabric_workspace,
    fabric_external_workspace_item,
    fabric_external_lakehouse,
):
    """The ID-only Livy attachment contract for a cross-workspace Environment."""

    from weaver.fabric.livy import LivySession
    from weaver.workspaces import Workspace

    environment = f"{fabric_workspace.workspace}/{fabric_workspace.environment.name}"
    consumer = Workspace(
        workspace=fabric_external_workspace_item.name,
        environment=environment,
    )
    session = LivySession.for_workspace(
        consumer, lakehouse=fabric_external_lakehouse.name
    )
    try:
        session.start()
        observed = session.run(
            "from importlib.metadata import version\n"
            "import mssql_python\n"
            "import weaver\n"
            "emit({\n"
            "  'mssql_python': version('mssql-python'),\n"
            "  'weaver': weaver.__version__,\n"
            "  'workspace_id': spark.conf.get('trident.workspace.id'),\n"
            "})\n"
        ).payload
    finally:
        session.close()

    assert observed["weaver"]
    assert observed["mssql_python"]
    assert observed["workspace_id"] == fabric_external_workspace_item.id
