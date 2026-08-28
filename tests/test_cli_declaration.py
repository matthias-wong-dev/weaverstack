"""The CLI is an empty but working shell at checkpoint 0."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver_cli import main


@weaver_test()
def test_bare_invocation_prints_help(capsys):
    assert main([]) == 0
    assert "usage: weaver" in capsys.readouterr().out


@weaver_test()
def test_help_exits_cleanly(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--help"])
    assert exit_info.value.code == 0
    assert "usage: weaver" in capsys.readouterr().out


@weaver_test()
def test_version_reports_the_distribution(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])
    assert exit_info.value.code == 0
    assert "weaverstack" in capsys.readouterr().out


# --- one target grammar ------------------------------------------------------
#
# The three lifecycle verbs name logical Weaver items with a repeated
# `--target`. `wipe` keeps physical positionals: it empties a physical resource
# whether or not an installation exists, so it has no logical item to name.


LOGICAL_TARGET_COMMANDS = ("build", "load", "test")


@pytest.mark.parametrize("command", LOGICAL_TARGET_COMMANDS)
@weaver_test()
def test_a_lifecycle_command_repeats_one_target_option(command):
    """Build, load and test spell their targets one way.

    An option rather than positionals, because a build target may carry an ``=``
    and one repeated flag reads the same on all three verbs.
    """

    from weaver_cli.main import build_parser

    parsed = build_parser().parse_args(
        [command, "--target", "Lakehouse/Landing", "--target", "Warehouse/Curated"]
    )

    assert parsed.targets == ["Lakehouse/Landing", "Warehouse/Curated"]


@weaver_test()
def test_wipe_still_names_physical_targets_positionally():
    """A wipe addresses a physical resource, so it names one."""

    from weaver_cli.main import build_parser

    parsed = build_parser().parse_args(["wipe", "Lakehouse/Sales_LH"])

    assert parsed.targets == ["Lakehouse/Sales_LH"]


@weaver_test()
def test_wipe_needs_at_least_one_target():
    from weaver_cli.main import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(["wipe"])


@weaver_test()
def test_no_lifecycle_command_still_offers_the_old_spellings(capsys):
    """Pre-alpha, so the retired spellings are gone rather than shortcut.

    Asserted because a silently accepted ``--targets`` would be worse than a
    removed one: the sequence would keep working for whoever already typed it
    and stay unlearnable for everyone else.
    """

    from weaver_cli.main import build_parser

    for command in LOGICAL_TARGET_COMMANDS:
        with pytest.raises(SystemExit):
            build_parser().parse_args([command, "--targets", "Lakehouse/Landing"])
        assert "--targets" in capsys.readouterr().err


@weaver_test()
def test_the_retired_bind_grammar_says_what_replaced_it():
    """``--bind`` parsed the two halves the other way round.

    Recognised so the migration is one message rather than an unknown-option
    error, and refused at the CLI boundary so no old binding reaches core.
    """

    from weaver.errors import CommandError
    from weaver_cli.main import build_parser, handle_build

    parsed = build_parser().parse_args(
        ["build", ".", "--bind", "Lakehouse/Landing_Dev=Landing"]
    )

    with pytest.raises(CommandError, match="--bind is replaced by --target") as raised:
        handle_build(parsed)

    assert "--target Lakehouse/Landing=Lakehouse/Landing_Dev" in str(raised.value)
