"""The ``tsql`` executor — mechanical, running a script through the SQL stack.

Behavioural verification of the generated scripts runs against the Play Warehouse
under Fabric. Here we prove the executor's own contract without a database: it
hands the whole payload to the environment's SQL executor as one script, and
fails clearly when no SQL executor was supplied.
"""

from __future__ import annotations

import pytest

from weaver.build_bundle.executors.base import InstallationContext
from weaver.build_bundle.executors.tsql import TSqlExecutor
from weaver.build_bundle.models import BuildAction
from weaver.errors import InstallError


class _FakeSql:
    def __init__(self) -> None:
        self.scripts: list[str] = []

    def execute_script(self, script: str) -> None:
        self.scripts.append(script)


ACTION = BuildAction(
    id="build-table-Wh.CustomerReport",
    kind="build_table",
    resource_node_id="sql:Wh.CustomerReport",
    executor="tsql",
    payload="payload/x.sql",
    payload_sha256="x",
)


def _context(sql):
    return InstallationContext(
        spark=None, resolver=None, store=None, snapshot=None, target=None, sql=sql
    )


def test_the_whole_script_is_run_as_one_script():
    sql = _FakeSql()
    script = "set nocount on;\ncreate table [Wh].[CustomerReport] (...);\n"
    details = TSqlExecutor().execute(ACTION, script.encode("utf-8"), _context(sql))

    assert sql.scripts == [script]
    assert details["statement_first_line"] == "set nocount on;"


def test_a_missing_sql_executor_is_a_clear_install_error():
    with pytest.raises(InstallError, match="needs a SQL executor"):
        TSqlExecutor().execute(ACTION, b"create table x;", _context(None))


def test_a_missing_payload_is_a_clear_install_error():
    with pytest.raises(InstallError, match="has no payload"):
        TSqlExecutor().execute(ACTION, None, _context(_FakeSql()))
