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


def test_doctor_reports_the_local_spark_requirements(capsys):
    exit_code = main(["doctor"])
    printed = capsys.readouterr().out
    for requirement in ("python", "pyspark", "delta-spark", "java"):
        assert requirement in printed
    assert exit_code in (0, 1)


def test_doctor_can_emit_json(capsys):
    main(["doctor", "--json"])
    import json

    report = json.loads(capsys.readouterr().out)
    assert {check["name"] for check in report["checks"]} == {
        "python", "pyspark", "delta-spark", "java",
    }
    assert isinstance(report["ok"], bool)


def test_doctor_exit_status_follows_the_report(capsys):
    """Non-zero when something is missing, so it can gate a script."""
    import json

    exit_code = main(["doctor", "--json"])
    report = json.loads(capsys.readouterr().out)
    assert exit_code == (0 if report["ok"] else 1)


# --- one target grammar ------------------------------------------------------


TARGET_COMMANDS = ("wipe", "load", "test")


@pytest.mark.parametrize("command", TARGET_COMMANDS)
def test_a_target_oriented_command_takes_its_targets_positionally(command):
    """The three commands that operate on named targets spell them one way.

    ``load`` used to want ``--targets`` while its two neighbours took
    positionals, so the same three names had to be typed two ways depending on
    the verb. There is nothing behind the difference to remember — which is
    what made it worth removing rather than documenting.
    """

    from weaver_cli.main import build_parser

    parsed = build_parser().parse_args(
        [command, "Lakehouse/Sales", "Warehouse/Reporting"]
    )

    assert parsed.targets == ["Lakehouse/Sales", "Warehouse/Reporting"]


@pytest.mark.parametrize("command", TARGET_COMMANDS)
def test_a_target_oriented_command_needs_at_least_one_target(command):
    from weaver_cli.main import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args([command])


def test_no_target_oriented_command_still_offers_the_old_spelling(capsys):
    """Pre-alpha, so the inconsistent spelling is gone rather than aliased.

    Asserted because a silently accepted ``--targets`` would be worse than a
    removed one: the sequence would keep working for whoever already typed it
    and stay unlearnable for everyone else.
    """

    from weaver_cli.main import build_parser

    for command in TARGET_COMMANDS:
        with pytest.raises(SystemExit):
            build_parser().parse_args([command, "--targets", "Lakehouse/Sales"])
        assert "--targets" in capsys.readouterr().err
