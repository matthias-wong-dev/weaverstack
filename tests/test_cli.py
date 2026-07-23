"""The CLI is an empty but working shell at checkpoint 0."""

from __future__ import annotations

import pytest

from weaver_cli import main


def test_bare_invocation_prints_help(capsys):
    assert main([]) == 0
    assert "usage: weaver" in capsys.readouterr().out


def test_help_exits_cleanly(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--help"])
    assert exit_info.value.code == 0
    assert "usage: weaver" in capsys.readouterr().out


def test_version_reports_the_distribution(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])
    assert exit_info.value.code == 0
    assert "weaverstack" in capsys.readouterr().out
