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


# --- one item grammar --------------------------------------------------------
#
# The three lifecycle verbs name logical Weaver items. `load` and `test` name
# them positionally, and `build` with a repeated `--item`, because a build's
# positional is the repository source and a build item may carry an `=`.
# `wipe` keeps physical positionals: it empties a physical resource whether or
# not an installation exists, so it has no logical item to name.


LOGICAL_ITEM_COMMANDS = ("build", "load", "test")

RUN_COMMANDS = ("load", "test")


@pytest.mark.parametrize("command", RUN_COMMANDS)
@weaver_test()
def test_a_run_names_its_items_positionally(command):
    """The documented spelling. A run item is one bare ``Kind/Name`` token."""

    from weaver_cli.main import build_parser, run_items

    parsed = build_parser().parse_args(
        [command, "Lakehouse/Landing", "Warehouse/Curated"]
    )

    assert run_items(parsed) == ("Lakehouse/Landing", "Warehouse/Curated")


@pytest.mark.parametrize("command", RUN_COMMANDS)
@weaver_test()
def test_a_run_names_no_item_at_all(command):
    """Every installed item. The Weaver catalogue is what answers which."""

    from weaver_cli.main import build_parser, run_items

    assert run_items(build_parser().parse_args([command])) == ()


@pytest.mark.parametrize("command", RUN_COMMANDS)
@weaver_test()
def test_a_run_still_accepts_the_option_spelling(command):
    from weaver_cli.main import build_parser, run_items

    parsed = build_parser().parse_args(
        [command, "--item", "Lakehouse/Landing", "--item", "Warehouse/Curated"]
    )

    assert run_items(parsed) == ("Lakehouse/Landing", "Warehouse/Curated")


@pytest.mark.parametrize("command", RUN_COMMANDS)
@weaver_test()
def test_both_run_item_spellings_are_one_selection(command):
    """Positional first, then ``--item``: one order, whatever was typed."""

    from weaver_cli.main import build_parser, run_items

    written = build_parser().parse_args(
        [command, "Lakehouse/Landing", "--item", "Warehouse/Curated"]
    )
    reversed_order = build_parser().parse_args(
        [command, "--item", "Warehouse/Curated", "Lakehouse/Landing"]
    )

    assert run_items(written) == ("Lakehouse/Landing", "Warehouse/Curated")
    assert run_items(reversed_order) == run_items(written)


@weaver_test()
def test_a_build_repeats_one_item_option():
    """A build's positional is the repository, so its items stay an option."""

    from weaver_cli.main import build_parser

    parsed = build_parser().parse_args(
        ["build", "--item", "Lakehouse/Landing", "--item", "Warehouse/Curated"]
    )

    assert parsed.items == ["Lakehouse/Landing", "Warehouse/Curated"]


@weaver_test()
def test_wipe_still_names_physical_targets_positionally():
    """A wipe addresses a physical resource, so it names one."""

    from weaver_cli.main import build_parser

    parsed = build_parser().parse_args(["wipe", "Lakehouse/Sales_LH"])

    assert parsed.targets == ["Lakehouse/Sales_LH"]


@weaver_test()
def test_the_retired_target_option_says_what_replaced_it():
    """``--target`` named a physical item on build, load and test."""

    from weaver.errors import CommandError
    from weaver_cli.main import build_parser, handle_load

    parsed = build_parser().parse_args(["load", "--target", "Lakehouse/Landing"])

    with pytest.raises(CommandError, match="--target is replaced by --item"):
        handle_load(parsed)


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

    for command in LOGICAL_ITEM_COMMANDS:
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

    with pytest.raises(CommandError, match="--bind is replaced by --item") as raised:
        handle_build(parsed)

    assert "--item Lakehouse/Landing=Lakehouse/Landing_Dev" in str(raised.value)


# --- what a build failure is ---------------------------------------------------


class _BorrowedSession:
    """A Session the command borrows, so nothing here opens one."""

    closed = False


def _cli():
    """The command module, not the ``main`` function of the same dotted name."""

    import sys

    import weaver_cli.main  # noqa: F401 - imported for its effect on sys.modules

    return sys.modules["weaver_cli.main"]


def _parsed_build(monkeypatch):
    """One parsed ``build``, with a workspace and a Session already settled."""

    from weaver.workspaces import Workspace

    monkeypatch.setattr(
        _cli(), "_resolve_workspace", lambda args: Workspace(workspace="Analytics")
    )
    parsed = _cli().build_parser().parse_args(["build", "."])
    parsed.session = _BorrowedSession()
    return parsed


@pytest.mark.parametrize(
    "error",
    ["DiscoveryError", "GraphError", "IdentityError", "MetadataError"],
)
@weaver_test()
def test_a_rejected_repository_is_a_failed_build(error, monkeypatch, capsys):
    """The four errors a repository parse raises, and one answer to them.

    Each is cleared by editing the repository, and the next attempt re-reads the
    tree, so the build renders the error and returns a failure. That failure is
    what the retry prompt offers to run again.
    """

    import weaver
    from weaver import errors

    parsed = _parsed_build(monkeypatch)

    def refuse(*arguments, **keywords):
        raise getattr(errors, error)("Lakehouse/Sales/Tables/Sales__Order.py: refused")

    monkeypatch.setattr(weaver, "build", refuse)

    assert _cli()._build_once(parsed) == 1
    assert "Sales__Order.py: refused" in capsys.readouterr().err


@weaver_test()
def test_a_workspace_failure_is_not_a_build_failure(monkeypatch):
    """Only the source tree is retryable.

    Configuration, Fabric resolution and the transports reach the same answer on
    the next attempt, so they stay raised and end the command.
    """

    import weaver
    from weaver.errors import ConfigError

    parsed = _parsed_build(monkeypatch)

    def refuse(*arguments, **keywords):
        raise ConfigError("catalogue Warehouse is not configured")

    monkeypatch.setattr(weaver, "build", refuse)

    with pytest.raises(ConfigError):
        _cli()._build_once(parsed)
