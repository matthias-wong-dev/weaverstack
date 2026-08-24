"""Source-only repository checking."""

from __future__ import annotations

from pathlib import Path
from shutil import copytree

import pytest
from support.weaver_test import weaver_test

from weaver.errors import DiscoveryError
from weaver.operations.check import check
from weaver_cli.main import build_parser, command_requirements, handle_check


def _repository(root: Path) -> Path:
    fixture = Path(__file__).parent / "fixtures" / "build-lakehouse-item"
    copytree(fixture, root)
    return root


@weaver_test()
def test_check_parses_a_valid_repository_without_running_authored_python(tmp_path):
    root = _repository(tmp_path / "repository")
    source = root / "Lakehouse" / "Raw" / "DWG__Customer.py"
    source.write_text(
        source.read_text(encoding="utf-8")
        + "\nraise RuntimeError('must not execute')\n",
        encoding="utf-8",
    )

    result = check(root)

    # A Location normalises to "/" on every platform, so compare in that form.
    assert result.source == root.as_posix()


@weaver_test()
def test_check_raises_existing_parser_errors(tmp_path):
    root = _repository(tmp_path / "repository")
    (root / "Lakehouse" / "Raw" / "unexpected.txt").write_text("x", encoding="utf-8")

    with pytest.raises(DiscoveryError):
        check(root)


@weaver_test()
def test_check_command_declares_no_fabric_resources_and_renders_success(
    tmp_path, capsys
):
    root = _repository(tmp_path / "repository")
    args = build_parser().parse_args(["check", str(root)])

    assert command_requirements(args) == frozenset()
    assert handle_check(args) == 0
    assert capsys.readouterr().out == "Repository valid.\n"


@weaver_test()
def test_check_command_adapts_parser_error_to_retry_status(tmp_path, capsys):
    root = _repository(tmp_path / "repository")
    (root / "Lakehouse" / "Raw" / "unexpected.txt").write_text("x", encoding="utf-8")
    args = build_parser().parse_args(["check", str(root)])

    assert handle_check(args) == 1
    assert "error:" in capsys.readouterr().err
