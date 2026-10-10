"""Caller-thread Spark discovery for concurrent notebook source Tests."""

import threading

import pytest
from support.weaver_test import weaver_test
from test_file_test_workspace_boundary import TEST_SOURCE
from test_isolated_spark_boundary import Spark

import weaver
from weaver.runtime.validation_result import TestResult
from weaver.sessions import NotebookSession
from weaver.workspaces import Workspace


@weaver_test()
@pytest.mark.parametrize("total", [1, 2])
def test_fresh_notebook_caches_parent_spark_before_worker_dispatch(
    tmp_path, monkeypatch, total
):
    caller = threading.get_ident()
    parent = Spark()
    acquisition = []
    dispatch = []

    def active():
        acquisition.append(threading.get_ident())
        assert threading.get_ident() == caller, "active Spark is caller-thread only"
        return parent

    monkeypatch.setattr("weaver.sessions.host.active_spark", active)
    paths = []
    for i in range(3):
        path = tmp_path / f"Sales.Match{i}.sql"
        path.write_bytes(TEST_SOURCE.replace("Sales.Match", f"Sales.Match{i}").encode())
        paths.append(path)

    def execute(session, document, target, *, workspace, isolated=False):
        assert isolated
        assert session.spark(workspace) is parent
        dispatch.append(threading.get_ident())
        return TestResult(), ()

    monkeypatch.setattr("weaver.test_file._run_spark", execute)
    with NotebookSession(workspace=Workspace(workspace="Demo")) as session:
        dry = weaver.test(
            "Lakehouse/Source",
            files=paths,
            source=tmp_path / "empty",
            session=session,
            concurrency=total,
            dry_run=True,
        )
        assert dry.status == "planned" and not acquisition and not dispatch
        report = weaver.test(
            "Lakehouse/Source",
            files=paths,
            source=tmp_path / "empty",
            session=session,
            concurrency=total,
        )
        assert report.succeeded and len(report.nodes) == 3
    assert acquisition == [caller]
    assert len(dispatch) == 3 and all(thread != caller for thread in dispatch)
