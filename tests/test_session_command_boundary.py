"""One Session, many commands, one Spark session.

``weaver session`` makes one promise, and it is the whole reason the command
exists: a console pays for authentication, item resolution and a Livy session
once, and every command typed at the prompt reuses them. The promise was
already true of ``build`` and ``wipe``. It was quietly false of ``load``,
``test`` and unbinding, which each opened a :class:`LivySession` of their own,
waited a minute for it, and closed it on the way out — so a developer running

.. code-block:: text

    weaver> wipe  Lakehouse/Sales
    weaver> build ./repository --bind Lakehouse/Sales=Sales
    weaver> load  Lakehouse/Sales
    weaver> test  Lakehouse/Sales

started three Spark sessions in a shell whose banner said it had started one.
On a capacity that permits a single concurrent session that is not merely slow;
the second one cannot start at all while the first is up.

Two claims, because they fail differently. The behavioural one below is what a
developer experiences. The source-level one is what stops it coming back: the
bug was not that a crossing was written wrongly, it was that a crossing was
written *somewhere else*, and no test could see the difference.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from weaver.errors import WeaverError
from weaver.sessions.console import ConsoleScope, ConsoleSession
from weaver.workspaces import Workspace
from weaver_cli.main import build_parser


class _FakeCredential:
    def get_token(self, *scopes, **kwargs):
        return SimpleNamespace(token="token", expires_on=2**31 - 1)


class _CountingLivy:
    """A Livy session that counts how many times one was asked for."""

    acquired = 0
    submitted: list[str] = []

    @classmethod
    def for_workspace(cls, *args, **kwargs):
        cls.acquired += 1
        return cls()

    def start(self) -> None:
        pass

    def close(self, **kwargs) -> None:
        pass

    def run(self, code, **kwargs):
        if "weaver.__version__" in code:
            from weaver import __version__

            answer = __version__
        else:
            type(self).submitted.append(code)
            answer = _answer_for(code)

        class Result:
            returned = True
            payload = answer

        return Result()


class _PresentResolver:
    """Everything the commands ask about exists, and costs nothing to say so."""

    def __init__(self, workspace=None) -> None:
        self.workspace = workspace

    def resolve(self, item, *, item_type):
        return SimpleNamespace(id="00000000-0000-0000-0000-000000000000")

    def spark_destination(self, item):
        from weaver.spark import FabricSparkTarget

        return FabricSparkTarget(workspace="My Workspace", lakehouse=item.name)


@pytest.fixture
def transport(monkeypatch):
    """The physical boundary, doubled beneath a real ConsoleSession.

    Replaced before any Session exists: a ``Resource`` binds its acquisition at
    construction, so a patch applied to a scope afterwards leaves the original
    in place and a real credential is asked for anyway.
    """

    _CountingLivy.acquired = 0
    _CountingLivy.submitted = []
    monkeypatch.setattr("weaver.fabric.auth.credential", _FakeCredential)
    monkeypatch.setattr(
        "weaver.fabric.LivySession.for_workspace",
        classmethod(lambda cls, *args, **kwargs: _CountingLivy.for_workspace()),
    )
    monkeypatch.setattr(
        ConsoleScope,
        "resolver",
        property(lambda self: _PresentResolver(self.workspace)),
    )
    # The catalogue is a Warehouse, reached over TDS rather than Livy. Answered
    # with nothing, which is an empty catalogue — see `_answer_for`. Counted, so
    # a command that stopped reading the estate cannot pass for the wrong reason.
    from weaver.sessions.console import ConsoleSession

    _CountingLivy.queried = []
    monkeypatch.setattr(
        ConsoleSession,
        "query_tsql",
        lambda self, statement, **kwargs: _CountingLivy.queried.append(statement) or [],
    )
    monkeypatch.setattr(
        ConsoleSession, "execute_tsql", lambda self, statement, **kwargs: None
    )
    return _CountingLivy


def _answer_for(code: str):
    """What the far side would emit for each program a run submits.

    The catalogue itself is answered over TDS and is empty, deliberately: these
    tests are about which resources a run of commands acquires, not about what
    it finds. A load against an estate that claims nothing stops early, which is
    what is being counted.
    """

    from weaver.unbind import UnbindResult

    if "unbind_targets" in code:
        return UnbindResult(
            targets=("Lakehouse/Sales",), logical_items=(), statements=()
        ).to_mapping()
    return {}


def _weaver_python(session):
    """Which submitted programs imported Weaver rather than running a statement."""

    return [code for code in session.submitted if "import weaver" in code]


def _workspace() -> Workspace:
    return Workspace(
        workspace="My Workspace", catalogue="Warehouse/Weaver", environment="weaver"
    )


def _run(session, parser, words: list[str]) -> None:
    """One command line, through the parser and handler the shell uses.

    Failures are swallowed exactly as the prompt swallows them: an empty estate
    is a perfectly ordinary answer here, and what these tests count is which
    resources were acquired on the way to it — a session that a failed command
    tore down would be the bug, not the failure.
    """

    parsed = parser.parse_args(words)
    parsed.session = session
    try:
        parsed.handler(parsed)
    except WeaverError:
        pass


def test_reading_the_catalogue_starts_no_livy_at_all(transport, capsys):
    """The catalogue is a Warehouse, so asking it what is installed is TDS.

    A Spark session costs a minute to start and a capacity's only slot. Neither
    of these commands finds anything to run, so neither has any Spark work — and
    a run with no Spark work must not pay for a Spark session.
    """

    parser = build_parser()

    with ConsoleSession(workspace=_workspace()) as session:
        _run(session, parser, ["load", "Lakehouse/Sales", "--dry-run"])
        _run(session, parser, ["test", "Lakehouse/Sales", "--dry-run"])

    assert transport.acquired == 0
    assert transport.submitted == []


def test_each_command_still_did_its_own_work(transport, capsys):
    """One session is only worth having if the commands still ran in it.

    Guards the way this test could rot into passing for the wrong reason: a
    command that silently stopped crossing would also acquire no second Livy.
    """

    parser = build_parser()

    with ConsoleSession(workspace=_workspace()) as session:
        _run(session, parser, ["load", "Lakehouse/Sales", "--dry-run"])
        _run(session, parser, ["test", "Lakehouse/Sales", "--dry-run"])

    # Each command reached the estate over TDS, and none of them imported
    # Weaver to do it: reading the catalogue is T-SQL against the Warehouse.
    assert any("SELECT" in statement for statement in transport.queried), (
        "the estate was never read, so this passes for the wrong reason"
    )
    assert _weaver_python(transport) == []


def test_no_command_ships_its_whole_run_across(transport):
    """What the decomposition removed, asserted so it cannot come back.

    A desktop load once submitted one program that called `weaver.load` on the
    far side and ran the entire graph there. Now the estate is read across and
    every decision is made here.
    """

    parser = build_parser()

    with ConsoleSession(workspace=_workspace()) as session:
        _run(session, parser, ["load", "Lakehouse/Sales", "--dry-run"])
        _run(session, parser, ["test", "Lakehouse/Sales", "--dry-run"])

    assert not any("weaver.load(" in code for code in transport.submitted)
    assert not any("weaver.test(" in code for code in transport.submitted)


def test_a_command_given_no_session_still_works_on_its_own(transport):
    """A one-shot invocation owns the Session it opens, and closes it."""

    parser = build_parser()
    parsed = parser.parse_args(
        [
            "load",
            "Lakehouse/Sales",
            "--dry-run",
            "--workspace",
            "My Workspace",
            "--catalogue",
            "Warehouse/Weaver",
            "--environment",
            "weaver",
        ]
    )
    try:
        parsed.handler(parsed)
    except WeaverError:
        pass

    # It opened one and closed it, and needed no Spark to read the catalogue.
    assert transport.acquired == 0
    assert transport.queried


# --- the invariant that keeps it true ----------------------------------------


def test_only_the_session_opens_a_livy_session():
    """Who may reach for the transport, read off the source.

    The bug this replaces was not a crossing written wrongly — each of the three
    worked. It was a crossing written *outside* the Session, where nothing could
    see that it duplicated one. So the rule is positional: acquiring Livy is the
    Session's, and a command that wants Spark asks for a capability.
    """

    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "src"
    allowed = {
        # Where a Livy session is legitimately acquired and owned.
        root / "weaver" / "sessions" / "console.py",
        # The module that defines it, and the package that exports it.
        root / "weaver" / "fabric" / "livy.py",
        root / "weaver" / "fabric" / "__init__.py",
    }

    offenders = []
    for module in sorted(root.rglob("*.py")):
        if module in allowed:
            continue
        source = module.read_text(encoding="utf-8")
        if "LivySession.for_workspace" in source:
            offenders.append(str(module.relative_to(root)))

    assert not offenders, (
        "these modules open a Livy session of their own instead of asking the "
        f"Session for one: {offenders}"
    )
