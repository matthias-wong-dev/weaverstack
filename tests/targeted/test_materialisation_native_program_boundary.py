"""Check the live fault program against the Session's public Python carrier."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.errors import InstallError
from weaver.fabric.store import FabricStore
from weaver.sessions import NotebookSession
from weaver.sessions.program import FabricProgram


@weaver_test()
def test_fault_program_uses_the_typed_nonreplayable_carrier():
    from fabric.test_protected_mirror_materialisation_journey import failure_program

    program = failure_program(
        "abfss://source/Files/project",
        "PYTEST_WORKSPACE",
        "PYTEST_LH_MIRROR",
        "PYTEST_WEAVER_FORK",
    )
    assert isinstance(program, FabricProgram)
    assert program.resubmit is False
    compile(program.source, "materialisation-fault-program", "exec")


@weaver_test()
@pytest.mark.parametrize("position", ["source", "call"])
def test_fault_source_constructs_the_actual_native_session(monkeypatch, position):
    from fabric.test_protected_mirror_materialisation_journey import failure_program

    calls = []
    monkeypatch.setattr(
        FabricStore, "__init__", lambda self, fs=None: setattr(self, "fs", object())
    )

    def refuse(source, *, items, session):
        assert isinstance(session, NotebookSession)
        assert session.workspace.workspace == "PYTEST_WORKSPACE"
        assert session.workspace.catalogue == "Warehouse/PYTEST_WEAVER_FORK"
        calls.append((source, items))
        raise InstallError("Injected protected copy failure")

    monkeypatch.setattr(weaver, "build", refuse)
    program = failure_program(
        "abfss://source/Files/project",
        "PYTEST_WORKSPACE",
        "PYTEST_LH_MIRROR",
        "PYTEST_WEAVER_FORK",
    )
    emitted = []
    source = program.source if isinstance(program, FabricProgram) else program
    if position == "source":
        exec(source, {"spark": object(), "emit": emitted.append})
    else:
        monkeypatch.setitem(
            __import__("sys").modules,
            "pyspark.sql",
            SimpleNamespace(
                SparkSession=SimpleNamespace(getActiveSession=lambda: object())
            ),
        )
        emitted.append(program.call())
    assert calls == [
        ("abfss://source/Files/project", ["Lakehouse/Sales=Lakehouse/PYTEST_LH_MIRROR"])
    ]
    assert emitted == [
        {"succeeded": False, "errors": ["Injected protected copy failure"]}
    ]
