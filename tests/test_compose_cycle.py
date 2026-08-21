"""``weaver compose`` — what it accepts, what it asks, and what it runs.

The composition is deliberately small, so what these prove is mostly what it
*refuses*: a file that could run arbitrary programs, a sequence that ran because
nothing answered a prompt, a second grammar for commands that already have one.

The handlers are replaced throughout. What a build or a load does is proved
where builds and loads are proved; what matters here is that the composition
reaches the real parser, hands the real handler the real arguments, in order,
in one Session, and stops when one of them fails.
"""

from __future__ import annotations

import io
from contextlib import contextmanager, nullcontext

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver_cli.compose import (
    _parse,
    composition_words,
    load_composition,
    run_composition,
)
from weaver_cli.main import build_parser


def _write(tmp_path, text: str):
    path = tmp_path / "compose.yml"
    path.write_text(text, encoding="utf-8")
    return path


DEV = """\
compose:
  dev:
    - weaver wipe Lakehouse/Sales Warehouse/Reporting
    - weaver build ./repository --bind Lakehouse/Sales=Sales
    - weaver load Warehouse/Reporting
    - weaver test Warehouse/Reporting
"""

RUNS = """\
compose:
  verify:
    - weaver load Warehouse/Reporting
    - weaver test Warehouse/Reporting
"""


class _Args:
    """The parsed ``compose`` invocation, with no workspace of its own."""

    def __init__(self, name, file=None, session=None, yes=False):
        self.name = name
        self.file = file
        self.session = session
        self.yes = yes
        self.workspace = None
        self.workspace_type = None
        self.workspace_config = None
        self.catalogue = None
        self.environment = None


@pytest.fixture
def recorded(monkeypatch):
    """Every command handler, replaced by one that records and succeeds."""

    calls: list = []
    parser = build_parser()

    def record(parsed):
        parsed.workflow_id = getattr(parsed.session, "workflow_id", None)
        calls.append(parsed)
        return getattr(record, "status", 0)

    for action in parser._subparsers._group_actions[0].choices.values():
        action.set_defaults(handler=record)

    # No workspace: a composition of recorded handlers needs none, and asking
    # for one would put credential resolution in front of every test here.
    monkeypatch.setattr("weaver_cli.shell._default_workspace", lambda args: None)
    return calls, (lambda: parser), record


# --- the file ----------------------------------------------------------------


@weaver_test()
def test_a_composition_resolves_to_the_commands_it_lists(tmp_path):
    path = _write(tmp_path, DEV)

    entries, found = load_composition("dev", file=str(path))

    assert found == path
    assert entries[0] == "weaver wipe Lakehouse/Sales Warehouse/Reporting"
    assert len(entries) == 4


@weaver_test()
def test_a_missing_file_says_so_and_names_the_flag(tmp_path):
    with pytest.raises(CommandError) as raised:
        load_composition("dev", file=str(tmp_path / "nowhere.yml"))

    assert "no composition file" in str(raised.value)


@weaver_test()
def test_an_unknown_name_lists_the_ones_that_exist(tmp_path):
    path = _write(tmp_path, DEV)

    with pytest.raises(CommandError) as raised:
        load_composition("prod", file=str(path))

    assert "dev" in str(raised.value)


@weaver_test()
def test_an_empty_composition_is_refused(tmp_path):
    path = _write(tmp_path, "compose:\n  dev: []\n")

    with pytest.raises(CommandError):
        load_composition("dev", file=str(path))


# --- what an entry may be ----------------------------------------------------


@weaver_test()
def test_an_entry_is_a_weaver_command_line():
    assert composition_words("weaver load Lakehouse/Sales") == [
        "load",
        "Lakehouse/Sales",
    ]


@weaver_test()
def test_the_weaver_prefix_is_optional():
    """A composition holds Weaver commands, so naming the program adds nothing.

    Both spellings work: a line pasted from a terminal keeps the prefix, and a
    line written for the file need not.
    """

    assert composition_words("load Lakehouse/Sales") == ["load", "Lakehouse/Sales"]


@weaver_test()
def test_quoted_arguments_survive_exactly():
    """A workspace with a space in it is ordinary, and must not become two."""

    words = composition_words('weaver build . --workspace-config "my config.yml"')

    assert words == ["build", ".", "--workspace-config", "my config.yml"]


@pytest.mark.parametrize(
    "entry",
    [
        "weaver load Lakehouse/Sales | tee log",
        "weaver load Lakehouse/Sales && weaver test Lakehouse/Sales",
        "weaver load Lakehouse/Sales > out.txt",
        "weaver load $TARGET",
        "weaver load `echo Lakehouse/Sales`",
    ],
)
@weaver_test()
def test_anything_shell_shaped_is_refused(entry):
    """A composition runs Weaver commands. It is not a place to put a script."""

    with pytest.raises(CommandError):
        composition_words(entry)


@pytest.mark.parametrize("entry", ["rm -rf /", "python -c 'print(1)'"])
@weaver_test()
def test_an_entry_naming_another_program_is_refused_by_the_parser(entry, recorded):
    """Whether a word is a command is argparse's answer, so it gives it."""

    _, parser_factory, _ = recorded

    with pytest.raises(CommandError) as raised:
        _parse(parser_factory(), entry)

    assert "not a valid Weaver command" in str(raised.value)


@pytest.mark.parametrize("command", ["session", "compose"])
@weaver_test()
def test_the_commands_a_composition_cannot_contain(command):
    with pytest.raises(CommandError) as raised:
        composition_words(f"weaver {command} dev")

    assert "cannot" in str(raised.value)


# --- confirmation -------------------------------------------------------------


@weaver_test()
def test_the_sequence_is_shown_before_anything_runs(tmp_path, recorded, capsys):
    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)

    run_composition(
        _Args("dev", file=str(path)),
        parser_factory=parser_factory,
        stdin=io.StringIO(""),
    )
    printed = capsys.readouterr().out

    assert "Compose: dev" in printed
    for number in ("1.", "2.", "3.", "4."):
        assert number in printed
    assert not calls, "the sequence ran before it was confirmed"


@weaver_test()
def test_without_a_terminal_nothing_runs(tmp_path, recorded, capsys):
    """Silence is not consent, and the first entry is usually a wipe.

    Non-zero, because this is a refusal rather than a decision: a script that
    piped nothing in and got a success back would be told the composition ran.
    """

    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)

    status = run_composition(
        _Args("dev", file=str(path)),
        parser_factory=parser_factory,
        stdin=io.StringIO("y\n"),
    )

    assert status == 1
    assert not calls
    assert "--yes" in capsys.readouterr().err


@weaver_test()
def test_yes_runs_a_composition_unattended(tmp_path, recorded, capsys):
    """The other half of the refusal above: a script says so and it runs.

    Approving the sequence approves each command in it, so a wipe inside one
    does not ask again — there is nobody to ask.
    """

    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)

    status = run_composition(
        _Args("dev", file=str(path), yes=True),
        parser_factory=parser_factory,
        stdin=io.StringIO(""),
    )

    assert status == 0
    assert len(calls) == 4
    assert all(getattr(parsed, "authorised", False) for parsed in calls)


@weaver_test()
def test_saying_no_is_not_a_failure(tmp_path, recorded, monkeypatch, capsys):
    """Somebody was asked and declined. Nothing went wrong."""

    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)
    monkeypatch.setattr("weaver_cli.compose._interactive", lambda stdin: True)
    monkeypatch.setattr("weaver_cli.compose._confirmed", lambda stdin: False)

    status = run_composition(
        _Args("dev", file=str(path)), parser_factory=parser_factory
    )

    assert status == 0
    assert not calls
    assert "Composition cancelled." in capsys.readouterr().out


@weaver_test()
def test_a_bad_entry_is_found_before_the_first_command_runs(tmp_path, recorded):
    """A sequence that is displayed and agreed to should be one that can run."""

    calls, parser_factory, _ = recorded
    path = _write(
        tmp_path,
        "compose:\n  dev:\n    - weaver load Lakehouse/Sales\n    - weaver frobnicate\n",
    )

    with pytest.raises(CommandError):
        run_composition(
            _Args("dev", file=str(path)),
            parser_factory=parser_factory,
            stdin=io.StringIO("y\n"),
        )
    assert not calls


# --- execution ---------------------------------------------------------------


@pytest.fixture
def confirmed(monkeypatch):
    """An operator who says yes, without a terminal to type it into."""

    monkeypatch.setattr("weaver_cli.compose._interactive", lambda stdin: True)
    monkeypatch.setattr("weaver_cli.compose._confirmed", lambda stdin: True)


@weaver_test()
def test_every_command_runs_in_order_with_its_arguments(
    tmp_path, recorded, confirmed, capsys
):
    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)

    status = run_composition(
        _Args("dev", file=str(path)), parser_factory=parser_factory
    )

    assert status == 0
    assert len(calls) == 4
    assert calls[0].targets == ["Lakehouse/Sales", "Warehouse/Reporting"]
    assert calls[1].repository == "./repository"
    assert calls[1].item_bindings == ["Lakehouse/Sales=Sales"]
    assert calls[2].targets == ["Warehouse/Reporting"]
    assert calls[3].targets == ["Warehouse/Reporting"]


@weaver_test()
def test_one_session_serves_the_whole_sequence(tmp_path, recorded, confirmed):
    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)

    run_composition(_Args("dev", file=str(path)), parser_factory=parser_factory)

    sessions = {id(parsed.session) for parsed in calls}
    assert len(sessions) == 1
    assert all(parsed.session is not None for parsed in calls)


@weaver_test()
def test_one_workflow_id_correlates_the_whole_composition(
    tmp_path, recorded, confirmed
):
    calls, parser_factory, _ = recorded
    path = _write(tmp_path, RUNS)

    run_composition(_Args("verify", file=str(path)), parser_factory=parser_factory)

    assert [parsed.command for parsed in calls] == ["load", "test"]
    assert len({parsed.workflow_id for parsed in calls}) == 1
    assert calls[0].workflow_id


@weaver_test()
def test_commands_can_supply_the_composition_workspace(
    tmp_path, recorded, confirmed, monkeypatch
):
    calls, parser_factory, _ = recorded
    config = tmp_path / "workspace.yml"
    config.write_text(
        "workspace: Sales Dev\ncatalogue: Warehouse/Weaver\n", encoding="utf-8"
    )
    path = _write(
        tmp_path,
        "compose:\n"
        "  dev:\n"
        f'    - wipe Lakehouse/Sales --workspace-config "{config}"\n'
        f'    - load Lakehouse/Sales --workspace-config "{config}"\n',
    )
    opened = []

    class Session:
        closed = False

        def __init__(self, workspace):
            self.workspace = workspace

        def workflow(self, _workflow_id):
            return nullcontext()

        def offer_spark_home(self, lakehouses, *, workspace=None):
            pass

        def prepare(self, required, *, workspace=None):
            return type("W", (), {"started": ()})()

    @contextmanager
    def use_session(session, *, workspace=None):
        opened.append(workspace)
        yield Session(workspace)

    monkeypatch.setattr("weaver.sessions.host.use_or_create_session", use_session)

    assert (
        run_composition(
            _Args("dev", file=str(path), yes=True), parser_factory=parser_factory
        )
        == 0
    )
    assert len(opened) == 1
    assert opened[0].workspace == "Sales Dev"
    assert all(call.session.workspace == opened[0] for call in calls)


@weaver_test()
def test_a_composition_inside_a_session_joins_the_one_already_open(
    tmp_path, recorded, confirmed
):
    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)
    from weaver.sessions import ConsoleSession

    with ConsoleSession(workspace=None) as session:
        run_composition(
            _Args("dev", file=str(path), session=session),
            parser_factory=parser_factory,
        )

        assert all(parsed.session is session for parsed in calls)
        assert not session.closed, "a borrowed Session must outlive the composition"


@weaver_test()
def test_the_sequence_stops_at_the_first_failure(tmp_path, recorded, confirmed, capsys):
    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)

    def fail_on_second(parsed):
        calls.append(parsed)
        return 1 if len(calls) == 2 else 0

    parser = parser_factory()
    for action in parser._subparsers._group_actions[0].choices.values():
        action.set_defaults(handler=fail_on_second)

    status = run_composition(
        _Args("dev", file=str(path)), parser_factory=lambda: parser
    )

    assert status == 1
    assert len(calls) == 2
    assert "Composition stopped at [2]" in capsys.readouterr().err


@weaver_test()
def test_a_raised_weaver_error_stops_the_sequence_too(
    tmp_path, recorded, confirmed, capsys
):
    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)

    def raise_on_first(parsed):
        calls.append(parsed)
        raise CommandError("no such Lakehouse")

    parser = parser_factory()
    for action in parser._subparsers._group_actions[0].choices.values():
        action.set_defaults(handler=raise_on_first)

    status = run_composition(
        _Args("dev", file=str(path)), parser_factory=lambda: parser
    )
    captured = capsys.readouterr()

    assert status == 1
    assert len(calls) == 1
    assert "no such Lakehouse" in captured.err


@weaver_test()
def test_the_confirmed_sequence_is_not_confirmed_again_per_command(
    tmp_path, recorded, confirmed
):
    """Having agreed to four commands, being asked again about the first of
    them is not a second safeguard — it is the first one repeated."""

    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)

    run_composition(_Args("dev", file=str(path)), parser_factory=parser_factory)

    assert all(parsed.authorised for parsed in calls)


# --- what a whole sequence will want ------------------------------------------


@weaver_test()
def test_a_composition_warms_the_union_before_the_first_command(
    tmp_path, recorded, confirmed, monkeypatch
):
    """A sequence ending in a load should not wait for Spark at the end of the
    build in front of it. The resources are shared, so warming the maximum set
    once is warming it correctly."""

    from weaver.sessions.requirements import AUTH, LIVY, ONELAKE, RESOLVER, TDS

    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)
    prepared = []

    monkeypatch.setattr("weaver_cli.shell._default_workspace", lambda args: object())

    offered = []

    class Warmed:
        closed = False

        def workflow(self, _workflow_id):
            return nullcontext()

        def offer_spark_home(self, lakehouses, *, workspace=None):
            offered.append(tuple(lakehouses))

        def prepare(self, required, *, workspace=None):
            prepared.append(set(required))
            return type("W", (), {"started": ()})()

    run_composition(
        _Args("dev", file=str(path), session=Warmed()), parser_factory=parser_factory
    )

    (required,) = prepared
    # wipe and build declare the lot; load and test add nothing new here.
    assert required == {AUTH, RESOLVER, ONELAKE, LIVY, TDS}

    # And the Lakehouse Spark can attach to, gathered across the whole sequence
    # rather than left to whichever command needs Spark first.
    assert offered == [("Sales",)]


@weaver_test()
def test_a_composition_with_no_workspace_warms_nothing(tmp_path, recorded, confirmed):
    """There is nothing to warm against, and asking would put workspace
    resolution in front of a sequence that may name one per command."""

    calls, parser_factory, _ = recorded
    path = _write(tmp_path, DEV)
    prepared = []

    class Watched:
        closed = False

        def workflow(self, _workflow_id):
            return nullcontext()

        def offer_spark_home(self, lakehouses, *, workspace=None):
            raise AssertionError("a composition with no workspace offered a Lakehouse")

        def prepare(self, required, *, workspace=None):
            prepared.append(required)

    run_composition(
        _Args("dev", file=str(path), session=Watched()), parser_factory=parser_factory
    )

    assert prepared == []
