"""Session-owned Delta table creation in both execution positions."""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.sessions.console import ConsoleScope, ConsoleSession
from weaver.sessions.notebook import NotebookSession
from weaver.workspaces import Workspace

CASE_KEY = "spark.sql.caseSensitive"
TARGET = "`Demo`.`Sales LH`.`DWG`.`Customer Order`"
COLUMNS = (
    ("Customer key", "bigint", True),
    ("Customer id", "string", True),
    ("Amount", "decimal(18,2)", False),
)


class _Conf:
    def __init__(self):
        self.values = {CASE_KEY: "false"}

    def get(self, key):
        return self.values[key]

    def set(self, key, value):
        self.values[key] = str(value)


class _Builder:
    def __init__(self, spark, *, fail=False):
        self.spark = spark
        self.fail = fail
        self.calls = []

    def tableName(self, name):  # noqa: N802 - Delta's API
        self.calls.append(("tableName", name))
        return self

    def addColumn(self, name, type_, **kwargs):  # noqa: N802 - Delta's API
        self.calls.append(("addColumn", name, type_, kwargs))
        return self

    def property(self, name, value):
        self.calls.append(("property", name, value))
        return self

    def execute(self):
        self.calls.append(("execute", self.spark.conf.get(CASE_KEY)))
        if self.fail:
            raise RuntimeError("create failed")
        return "created"


class _DeltaTable:
    builders = []
    fail = False

    @classmethod
    def create(cls, spark):
        builder = _Builder(spark, fail=cls.fail)
        cls.builders.append(builder)
        return builder


class _IdentityGenerator:
    pass


@pytest.fixture
def delta_module(monkeypatch):
    delta = ModuleType("delta")
    tables = ModuleType("delta.tables")
    tables.DeltaTable = _DeltaTable
    tables.IdentityGenerator = _IdentityGenerator
    delta.tables = tables
    monkeypatch.setitem(sys.modules, "delta", delta)
    monkeypatch.setitem(sys.modules, "delta.tables", tables)
    _DeltaTable.builders = []
    _DeltaTable.fail = False
    return tables


@pytest.fixture
def notebook():
    def make(spark):
        return NotebookSession(
            workspace=Workspace(workspace="Demo"),
            spark=spark,
            resolver=object(),
            store=object(),
        )

    return make


@weaver_test()
def test_notebook_creates_an_ordinary_table_without_identity_support(
    notebook, delta_module
):
    del delta_module.IdentityGenerator
    spark = SimpleNamespace(conf=_Conf())

    result = notebook(spark).create_delta_table(TARGET, COLUMNS[1:])

    assert result == "created"
    (builder,) = _DeltaTable.builders
    assert builder.calls == [
        ("tableName", TARGET),
        ("addColumn", "Customer id", "string", {"nullable": False}),
        ("addColumn", "Amount", "decimal(18,2)", {"nullable": True}),
        ("property", "delta.columnMapping.mode", "name"),
        ("execute", "true"),
    ]
    assert spark.conf.get(CASE_KEY) == "false"


@weaver_test()
def test_notebook_marks_only_the_identity_column_as_generated(notebook, delta_module):
    spark = SimpleNamespace(conf=_Conf())

    notebook(spark).create_delta_table(TARGET, COLUMNS, identity_column="Customer key")

    (builder,) = _DeltaTable.builders
    identity = builder.calls[1]
    business = builder.calls[2]
    assert identity[:3] == ("addColumn", "Customer key", "bigint")
    assert identity[3]["nullable"] is False
    assert isinstance(identity[3]["generatedAlwaysAs"], _IdentityGenerator)
    assert business == (
        "addColumn",
        "Customer id",
        "string",
        {"nullable": False},
    )


@weaver_test()
def test_notebook_restores_exact_case_after_a_create_error(notebook, delta_module):
    _DeltaTable.fail = True
    spark = SimpleNamespace(conf=_Conf())

    with pytest.raises(RuntimeError, match="create failed"):
        notebook(spark).create_delta_table(TARGET, COLUMNS[1:])

    assert spark.conf.get(CASE_KEY) == "false"


@weaver_test()
def test_an_unsupported_identity_names_the_target_before_creation(
    notebook, delta_module
):
    del delta_module.IdentityGenerator
    spark = SimpleNamespace(conf=_Conf())

    with pytest.raises(CommandError, match="Customer Order.*IdentityGenerator"):
        notebook(spark).create_delta_table(
            TARGET,
            COLUMNS,
            identity_column="Customer key",
        )

    assert _DeltaTable.builders == []


class _Livy:
    def __init__(self):
        self.submitted = []

    def run(self, code, **kwargs):
        self.submitted.append(code)
        return SimpleNamespace(returned=True, payload={"created": True})

    def start(self):
        pass

    def close(self, **kwargs):
        pass


@weaver_test()
def test_console_submits_a_self_contained_serialised_creator(monkeypatch):
    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: SimpleNamespace(workspace=None))
    )
    livy = _Livy()
    session = ConsoleSession(
        workspace=Workspace(workspace="Demo"),
        livy=livy,
    )

    result = session.create_delta_table(TARGET, COLUMNS, identity_column="Customer key")

    assert result == {"created": True}
    (source,) = livy.submitted
    compile(source, "<delta-table>", "exec")
    assert "import weaver" not in source
    assert "DeltaTable.create(spark)" in source
    assert "IdentityGenerator" in source
    assert "delta.columnMapping.mode" in source
    assert "spark.sql.caseSensitive" in source


@weaver_test()
def test_console_program_restores_exact_case_after_a_create_error(delta_module):
    from weaver.sessions.delta_table import remote_delta_table_program

    _DeltaTable.fail = True
    spark = SimpleNamespace(conf=_Conf())
    source = remote_delta_table_program(
        TARGET,
        COLUMNS,
        identity_column="Customer key",
        column_mapping=True,
    )

    with pytest.raises(RuntimeError, match="create failed"):
        exec(source, {"spark": spark, "emit": lambda _value: None})

    assert spark.conf.get(CASE_KEY) == "false"


@weaver_test()
def test_console_program_names_the_target_when_identity_is_unsupported(delta_module):
    from weaver.sessions.delta_table import remote_delta_table_program

    del delta_module.IdentityGenerator
    spark = SimpleNamespace(conf=_Conf())
    source = remote_delta_table_program(
        TARGET,
        COLUMNS,
        identity_column="Customer key",
        column_mapping=True,
    )

    with pytest.raises(RuntimeError, match="Customer Order.*IdentityGenerator"):
        exec(source, {"spark": spark, "emit": lambda _value: None})

    assert _DeltaTable.builders == []
