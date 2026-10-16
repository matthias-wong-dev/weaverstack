"""Documented CLI operations have a public, native notebook entry point."""

from __future__ import annotations

from pathlib import Path
from shutil import copytree

from support.weaver_test import weaver_test

import weaver


@weaver_test()
def test_notebook_check_runs_the_same_source_check_as_the_cli(tmp_path):
    root = tmp_path / "project"
    copytree(Path(__file__).parent / "fixtures" / "build-lakehouse-item", root)
    result = weaver.check(root)
    assert isinstance(result, weaver.CheckResult)
    assert result.project_folder == root.as_posix()


@weaver_test()
def test_notebook_connectivity_and_bundle_install_are_public_operations():
    from weaver.operations.doctor import DoctorReport, doctor
    from weaver.operations.install import install

    assert weaver.doctor is doctor
    assert weaver.DoctorReport is DoctorReport
    assert weaver.install is install
