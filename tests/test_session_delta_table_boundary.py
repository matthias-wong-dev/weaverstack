"""Session-owned Delta table creation in both execution positions."""

from __future__ import annotations

import sys
import threading
from types import ModuleType, SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.sessions.console import ConsoleScope, ConsoleSession
from weaver.sessions.direct_delta import run_direct_delta_actions
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
@pytest.mark.parametrize("remote", [False, True])
def test_spark_creation_honours_explicit_protocol_minimums(remote, delta_module):
    from weaver.sessions.delta_table import (
        create_delta_table_in_session,
        remote_delta_table_program,
    )

    spark = SimpleNamespace(conf=_Conf())
    minima = {"minReaderVersion": 2, "minWriterVersion": 5}
    if remote:
        exec(
            remote_delta_table_program(
                TARGET,
                COLUMNS[1:],
                identity_column=None,
                column_mapping=True,
                protocol_minima=minima,
            ),
            {"spark": spark, "emit": lambda _: None},
        )
    else:
        create_delta_table_in_session(
            spark,
            TARGET,
            COLUMNS[1:],
            identity_column=None,
            column_mapping=True,
            protocol_minima=minima,
        )
    (builder,) = _DeltaTable.builders
    assert ("property", "delta.minReaderVersion", "2") in builder.calls
    assert ("property", "delta.minWriterVersion", "5") in builder.calls


@weaver_test()
def test_base_direct_fallback_preserves_authored_protocol_policy():
    from weaver.sessions.testing import TestSession

    minima = {"minReaderVersion": 2, "minWriterVersion": 5}
    session = TestSession(workspace=Workspace(workspace="Demo"))
    outcomes = session.create_direct_delta_table_actions(
        [("ordinary", TARGET, COLUMNS[1:], None, minima)]
    )
    assert outcomes[0]["succeeded"] is True
    assert session.calls[0].body["protocol_minima"] == minima


@weaver_test()
def test_test_session_retains_protocol_policy_through_the_base_action_fallback():
    from weaver.sessions.testing import TestSession

    minima = {"minReaderVersion": 2, "minWriterVersion": 5}
    session = TestSession(workspace=Workspace(workspace="Demo"))
    outcomes = session.create_delta_table_actions(
        [("ordinary", TARGET, COLUMNS[1:], None, True, minima)]
    )
    assert outcomes[0]["succeeded"] is True
    assert session.calls[0].body["protocol_minima"] == minima


@weaver_test()
def test_notebook_labelled_spark_actions_preserve_authored_protocol_minima(
    notebook, delta_module
):
    spark = SimpleNamespace(conf=_Conf())
    minima = {"minReaderVersion": 2, "minWriterVersion": 5}
    outcomes = notebook(spark).create_delta_table_actions(
        [("ordinary", TARGET, COLUMNS[1:], None, True, minima)]
    )
    assert outcomes[0]["succeeded"] is True
    (builder,) = _DeltaTable.builders
    assert ("property", "delta.minReaderVersion", "2") in builder.calls
    assert ("property", "delta.minWriterVersion", "5") in builder.calls


@weaver_test()
def test_remote_table_batch_preserves_each_explicit_policy(delta_module):
    from weaver.sessions.delta_table import remote_delta_table_actions_program

    spark = SimpleNamespace(conf=_Conf())
    actions = [
        ("first", TARGET, COLUMNS[1:], None, True),
        (
            "second",
            TARGET + "Two",
            COLUMNS[1:],
            None,
            True,
            {"minReaderVersion": 2, "minWriterVersion": 5},
        ),
    ]
    exec(
        remote_delta_table_actions_program(actions),
        {"spark": spark, "emit": lambda _: None},
    )
    for builder, expected in zip(
        _DeltaTable.builders, [("3", "7"), ("2", "5")], strict=True
    ):
        assert ("property", "delta.minReaderVersion", expected[0]) in builder.calls
        assert ("property", "delta.minWriterVersion", expected[1]) in builder.calls


@weaver_test()
def test_remote_table_batch_preserves_the_common_floor_for_every_action(delta_module):
    from weaver.sessions.delta_table import remote_delta_table_actions_program

    spark = SimpleNamespace(conf=_Conf())
    actions = [
        ("first", TARGET, COLUMNS[1:], None, True),
        ("second", TARGET + "Two", COLUMNS[1:], None, True),
    ]
    exec(
        remote_delta_table_actions_program(actions),
        {"spark": spark, "emit": lambda _: None},
    )
    assert len(_DeltaTable.builders) == 2
    for builder in _DeltaTable.builders:
        assert ("property", "delta.minReaderVersion", "3") in builder.calls
        assert ("property", "delta.minWriterVersion", "7") in builder.calls


@weaver_test()
@pytest.mark.parametrize("remote", [False, True])
def test_spark_creation_uses_weaver_common_protocol_floor(remote, delta_module):
    from weaver.sessions.delta_table import (
        create_delta_table_in_session,
        remote_delta_table_program,
    )

    spark = SimpleNamespace(conf=_Conf())
    if remote:
        exec(
            remote_delta_table_program(
                TARGET, COLUMNS[1:], identity_column=None, column_mapping=True
            ),
            {"spark": spark, "emit": lambda _: None},
        )
    else:
        create_delta_table_in_session(
            spark, TARGET, COLUMNS[1:], identity_column=None, column_mapping=True
        )
    (builder,) = _DeltaTable.builders
    assert ("property", "delta.minReaderVersion", "3") in builder.calls
    assert ("property", "delta.minWriterVersion", "7") in builder.calls


@weaver_test()
def test_notebook_creates_an_ordinary_table_without_identity_support(
    notebook, delta_module
):
    del delta_module.IdentityGenerator
    spark = SimpleNamespace(conf=_Conf())

    minima = {"minReaderVersion": 2, "minWriterVersion": 5}
    result = notebook(spark).create_delta_table(
        TARGET, COLUMNS[1:], protocol_minima=minima
    )

    assert result == "created"
    (builder,) = _DeltaTable.builders
    assert ("property", "delta.minReaderVersion", "2") in builder.calls
    assert ("property", "delta.minWriterVersion", "5") in builder.calls
    assert builder.calls == [
        ("tableName", TARGET),
        ("addColumn", "Customer id", "string", {"nullable": False}),
        ("addColumn", "Amount", "decimal(18,2)", {"nullable": True}),
        ("property", "delta.minReaderVersion", "2"),
        ("property", "delta.minWriterVersion", "5"),
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
def test_notebook_batches_labelled_delta_creates_without_changing_table_semantics(
    notebook, delta_module
):
    spark = SimpleNamespace(conf=_Conf())
    actions = [
        ("first", TARGET, COLUMNS[1:], None, True),
        ("second", TARGET + "Two", COLUMNS, "Customer key", False),
    ]

    outcomes = notebook(spark).create_delta_table_actions(actions)

    assert [(outcome["label"], outcome["succeeded"]) for outcome in outcomes] == [
        ("first", True),
        ("second", True),
    ]
    assert len(_DeltaTable.builders) == 2
    assert _DeltaTable.builders[0].calls[-2:] == [
        ("property", "delta.columnMapping.mode", "name"),
        ("execute", "true"),
    ]
    assert ("property", "delta.columnMapping.mode", "name") not in (
        _DeltaTable.builders[1].calls
    )
    assert isinstance(
        _DeltaTable.builders[1].calls[1][3]["generatedAlwaysAs"], _IdentityGenerator
    )
    assert spark.conf.get(CASE_KEY) == "false"


@weaver_test()
def test_notebook_reports_one_failed_delta_create_and_continues_siblings(
    notebook, delta_module, monkeypatch
):
    spark = SimpleNamespace(conf=_Conf())

    def create(_cls, attached):
        builder = _Builder(attached, fail=not _DeltaTable.builders)
        _DeltaTable.builders.append(builder)
        return builder

    monkeypatch.setattr(_DeltaTable, "create", classmethod(create))
    outcomes = notebook(spark).create_delta_table_actions(
        [
            ("failed", TARGET, COLUMNS[1:], None, True),
            ("sibling", TARGET + "Two", COLUMNS[1:], None, True),
        ]
    )

    assert [(o["label"], o["succeeded"]) for o in outcomes] == [
        ("failed", False),
        ("sibling", True),
    ]
    assert outcomes[0]["error_type"] == "RuntimeError"
    assert outcomes[0]["error_message"] == "create failed"
    assert all(o["duration_seconds"] >= 0 for o in outcomes)
    assert len(_DeltaTable.builders) == 2
    assert spark.conf.get(CASE_KEY) == "false"


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
        self.kwargs = []

    def run(self, code, **kwargs):
        self.submitted.append(code)
        self.kwargs.append(kwargs)
        return SimpleNamespace(returned=True, payload={"created": True})

    def start(self):
        pass

    def close(self, **kwargs):
        pass


@weaver_test()
def test_console_submits_a_self_contained_serialised_creator(monkeypatch, delta_module):
    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: SimpleNamespace(workspace=None))
    )
    livy = _Livy()
    session = ConsoleSession(
        workspace=Workspace(workspace="Demo"),
        livy=livy,
    )

    minima = {"minReaderVersion": 2, "minWriterVersion": 5}
    result = session.create_delta_table(
        TARGET, COLUMNS, identity_column="Customer key", protocol_minima=minima
    )

    assert result == {"created": True}
    (source,) = livy.submitted
    compile(source, "<delta-table>", "exec")
    assert "import weaver" not in source
    assert "DeltaTable.create(spark)" in source
    assert "IdentityGenerator" in source
    assert "delta.columnMapping.mode" in source
    assert "spark.sql.caseSensitive" in source
    exec(source, {"spark": SimpleNamespace(conf=_Conf()), "emit": lambda _: None})
    (builder,) = _DeltaTable.builders
    assert ("property", "delta.minReaderVersion", "2") in builder.calls
    assert ("property", "delta.minWriterVersion", "5") in builder.calls


@weaver_test()
@pytest.mark.parametrize("host", ("console", "notebook"))
def test_labelled_direct_actions_preserve_each_tables_protocol_policy(
    host, notebook, monkeypatch
):
    session = (
        ConsoleSession(workspace=Workspace(workspace="Demo"), livy=_Livy())
        if host == "console"
        else notebook(SimpleNamespace(conf=_Conf()))
    )
    captured = []

    def create(_qualified, _columns, **kwargs):
        captured.append(kwargs["protocol_minima"])

    monkeypatch.setattr(session, "create_direct_delta_table", create)
    minima = {"minReaderVersion": 2, "minWriterVersion": 5}
    outcomes = session.create_direct_delta_table_actions(
        [("first", TARGET, COLUMNS, None, minima)]
    )
    assert outcomes[0]["succeeded"] is True
    assert captured == [minima]


@weaver_test()
def test_console_direct_creation_does_not_acquire_livy(monkeypatch):
    from weaver.sessions import direct_delta

    captured = []
    store = SimpleNamespace(rename_directory=lambda *_: None)
    monkeypatch.setattr(
        direct_delta,
        "create_bound_delta_table",
        lambda **kw: captured.append(kw) or "allocated",
    )
    monkeypatch.setattr(
        ConsoleScope,
        "resolver",
        property(lambda self: "resolved"),
    )
    monkeypatch.setattr(
        ConsoleScope,
        "transport_store",
        property(lambda self: store),
    )
    session = ConsoleSession(workspace=Workspace(workspace="Demo"), livy=_Livy())
    assert (
        session.create_direct_delta_table(
            TARGET,
            COLUMNS,
            identity_column="Customer key",
            protocol_minima={"minReaderVersion": 2, "minWriterVersion": 5},
        )
        == "allocated"
    )
    assert captured[0]["resolver"] == "resolved"
    assert captured[0]["protocol_minima"] == {
        "minReaderVersion": 2,
        "minWriterVersion": 5,
    }
    assert captured[0]["store"] is store
    assert captured[0]["publish"] == store.rename_directory
    assert session.scope().livy.acquired is False


@weaver_test()
def test_notebook_direct_creation_stages_with_native_store(notebook, monkeypatch):
    from weaver.sessions import direct_delta

    captured = []
    monkeypatch.setattr(
        direct_delta,
        "create_bound_delta_table",
        lambda **kw: captured.append(kw) or "allocated",
    )
    credentials = SimpleNamespace(
        getToken=lambda audience: "storage-token" if audience == "storage" else None
    )
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(credentials=credentials)
    )
    session = notebook(SimpleNamespace(conf=_Conf()))
    minima = {"minReaderVersion": 2, "minWriterVersion": 5}
    assert (
        session.create_direct_delta_table(TARGET, COLUMNS, protocol_minima=minima)
        == "allocated"
    )
    assert captured[0]["protocol_minima"] == minima
    assert captured[0]["store"] is session.scope().transport_store
    assert captured[0]["publish"].__self__.token == "storage-token"


@weaver_test()
@pytest.mark.parametrize("host", ("console", "notebook"))
def test_direct_actions_keep_failed_sibling_and_order(host, notebook, monkeypatch):
    if host == "console":
        session = ConsoleSession(workspace=Workspace(workspace="Demo"), livy=_Livy())
    else:
        session = notebook(SimpleNamespace(conf=_Conf()))
    calls = []

    def create(qualified, columns, *, identity_column, workspace):
        calls.append((qualified, columns, identity_column, workspace))
        if qualified == TARGET:
            raise ValueError("profile mismatch")
        return "created"

    monkeypatch.setattr(session, "create_direct_delta_table", create)
    monkeypatch.setattr(
        session,
        "create_delta_table_actions",
        lambda *_args, **_kwargs: pytest.fail("Spark fallback"),
    )
    actions = [
        ("failed", TARGET, COLUMNS, "Customer key"),
        ("sibling", TARGET + "Two", COLUMNS[1:], None),
    ]
    outcomes = session.create_direct_delta_table_actions(actions)
    assert [outcome["label"] for outcome in outcomes] == ["failed", "sibling"]
    assert [outcome["succeeded"] for outcome in outcomes] == [False, True]
    assert outcomes[0]["error_message"] == "profile mismatch"
    assert len(calls) == 2


@weaver_test()
@pytest.mark.parametrize("workers", (4, 8, 16))
def test_direct_delta_actions_upload_independent_tables_concurrently(workers):
    barrier = threading.Barrier(workers)
    calls = []
    lock = threading.Lock()

    def create(qualified, columns, *, identity_column, workspace):
        with lock:
            calls.append(qualified)
        barrier.wait(timeout=10)
        if qualified == "table-0":
            raise ValueError("profile mismatch")
        return qualified

    actions = [(str(i), f"table-{i}", (), None) for i in range(workers)]
    outcomes = run_direct_delta_actions(
        create, actions, workspace=None, max_workers=workers
    )
    assert len(calls) == workers
    assert [result["label"] for result in outcomes] == [str(i) for i in range(workers)]
    assert [result["succeeded"] for result in outcomes] == [False] + [True] * (
        workers - 1
    )
    assert all(result["duration_seconds"] >= 0 for result in outcomes)


@weaver_test()
def test_direct_delta_actions_reject_invalid_worker_counts():
    with pytest.raises(ValueError, match="workers"):
        run_direct_delta_actions(lambda *_args, **_kwargs: None, [], max_workers=0)


@weaver_test()
def test_console_routes_configured_delta_workers_without_spark(monkeypatch):
    barrier = threading.Barrier(4)
    session = ConsoleSession(
        workspace=Workspace(workspace="Demo"),
        direct_delta_workers=4,
        livy=_Livy(),
    )

    def create(qualified, columns, *, identity_column, workspace):
        barrier.wait(timeout=10)
        return qualified

    monkeypatch.setattr(session, "create_direct_delta_table", create)
    actions = [(str(i), f"table-{i}", (), None) for i in range(4)]
    outcomes = session.create_direct_delta_table_actions(actions)
    assert [row["succeeded"] for row in outcomes] == [True] * 4
    assert session.scope().livy.acquired is False


@weaver_test()
def test_console_uses_qualified_direct_delta_upload_bound_by_default():
    session = ConsoleSession(workspace=Workspace(workspace="Demo"))
    assert session.direct_delta_workers == 16
    session.close()


@weaver_test()
def test_console_batches_delta_actions_in_one_submission_without_retry(
    monkeypatch, delta_module
):
    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: SimpleNamespace(workspace=None))
    )
    livy = _Livy()
    session = ConsoleSession(workspace=Workspace(workspace="Demo"), livy=livy)
    actions = [
        ("failed", TARGET, COLUMNS[1:], None, True),
        ("sibling", TARGET + "Two", COLUMNS, "Customer key", False),
    ]

    session.create_delta_table_actions(actions, timeout=12.5)

    assert len(livy.submitted) == 1
    assert livy.kwargs == [{"timeout": 25.0, "retry_submission": False}]
    source = livy.submitted[0]
    assert "import weaver" not in source
    compile(source, "<delta-actions>", "exec")

    def create(_cls, attached):
        builder = _Builder(attached, fail=not _DeltaTable.builders)
        _DeltaTable.builders.append(builder)
        return builder

    monkeypatch.setattr(_DeltaTable, "create", classmethod(create))
    spark = SimpleNamespace(conf=_Conf())
    emitted = []
    exec(source, {"spark": spark, "emit": emitted.append})

    assert [(o["label"], o["succeeded"]) for o in emitted[0]] == [
        ("failed", False),
        ("sibling", True),
    ]
    assert emitted[0][0]["error_message"] == "create failed"
    assert len(_DeltaTable.builders) == 2
    assert ("property", "delta.columnMapping.mode", "name") in _DeltaTable.builders[
        0
    ].calls
    assert ("property", "delta.columnMapping.mode", "name") not in _DeltaTable.builders[
        1
    ].calls
    assert isinstance(
        _DeltaTable.builders[1].calls[1][3]["generatedAlwaysAs"], _IdentityGenerator
    )
    assert spark.conf.get(CASE_KEY) == "false"


@weaver_test()
def test_console_delta_batch_preserves_the_default_timeout_per_table(monkeypatch):
    from weaver.fabric.livy import DEFAULT_STATEMENT_TIMEOUT

    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: SimpleNamespace(workspace=None))
    )
    livy = _Livy()
    session = ConsoleSession(workspace=Workspace(workspace="Demo"), livy=livy)

    session.create_delta_table_actions(
        [
            ("first", TARGET, COLUMNS[1:], None, True),
            ("second", TARGET + "Two", COLUMNS[1:], None, True),
        ]
    )

    assert livy.kwargs == [
        {"timeout": 2 * DEFAULT_STATEMENT_TIMEOUT, "retry_submission": False}
    ]


@weaver_test()
def test_console_delta_batch_names_an_unsupported_identity_and_keeps_siblings(
    delta_module,
):
    from weaver.sessions.delta_table import remote_delta_table_actions_program

    del delta_module.IdentityGenerator
    spark = SimpleNamespace(conf=_Conf())
    emitted = []
    source = remote_delta_table_actions_program(
        [
            ("identity", TARGET, COLUMNS, "Customer key", True),
            ("ordinary", TARGET + "Two", COLUMNS[1:], None, True),
        ]
    )
    exec(source, {"spark": spark, "emit": emitted.append})

    assert [o["succeeded"] for o in emitted[0]] == [False, True]
    assert "Customer Order" in emitted[0][0]["error_message"]
    assert "IdentityGenerator" in emitted[0][0]["error_message"]
    assert len(_DeltaTable.builders) == 1
    assert spark.conf.get(CASE_KEY) == "false"


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
