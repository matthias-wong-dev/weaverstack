"""One Session, many commands, one Spark session.

``weaver session`` makes one promise, and it is the whole reason the command
exists: a console pays for authentication, item resolution and a Livy session
once, and every command typed at the prompt reuses them. The promise was
already true of ``build`` and ``wipe``. It was quietly false of ``load``,
``test`` and ``unbind``, which each opened a :class:`LivySession` of their own,
waited a minute for it, and closed it on the way out — so a developer running

.. code-block:: text

    weaver> wipe  Lakehouse/Sales
    weaver> build ./repository --bind Lakehouse/Sales=Lakehouse/Sales
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

from weaver.session.console import ConsoleScope, ConsoleSession
from weaver.workspaces import FabricWorkspace
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
        type(self).submitted.append(code)
        if "weaver.__version__" in code:
            from weaver import __version__

            answer = __version__
        else:
            answer = _CountingLivy.answers.pop(0)

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


@pytest.fixture
def transport(monkeypatch):
    """The physical boundary, doubled beneath a real ConsoleSession.

    Replaced before any Session exists: a ``Resource`` binds its acquisition at
    construction, so a patch applied to a scope afterwards leaves the original
    in place and a real credential is asked for anyway.
    """

    _CountingLivy.acquired = 0
    _CountingLivy.submitted = []
    _CountingLivy.answers = []
    monkeypatch.setattr("weaver.fabric.auth.credential", _FakeCredential)
    monkeypatch.setattr(
        "weaver.fabric.LivySession.for_workspace",
        classmethod(lambda cls, *args, **kwargs: _CountingLivy.for_workspace()),
    )
    monkeypatch.setattr(
        ConsoleScope, "resolver", property(lambda self: _PresentResolver(self.workspace))
    )
    return _CountingLivy


def _workspace() -> FabricWorkspace:
    return FabricWorkspace(
        workspace="My Workspace", weaver_lakehouse="Weaver", environment="weaver"
    )


def _run(session, parser, words: list[str]) -> None:
    """One command line, through the parser and handler the shell uses."""

    parsed = parser.parse_args(words)
    parsed.session = session
    parsed.handler(parsed)


def _load_report() -> dict:
    from weaver.load_report import LoadRunReport

    return {
        "failed": False,
        "report": LoadRunReport(
            requested=("Lakehouse/Sales",),
            status="succeeded",
            dry_run=True,
            fault_tolerant=False,
        ).to_mapping(),
    }


def _unbind_result() -> dict:
    from weaver.unbind import UnbindResult

    return UnbindResult(
        targets=("Lakehouse/Sales",), logical_items=(), statements=()
    ).to_mapping()


def _test_report() -> dict:
    from weaver.test_report import ValidationRunReport

    return {
        "failed": False,
        "report": ValidationRunReport(status="planned").to_mapping(),
        "diagnostics": {},
    }


def test_a_run_of_commands_in_one_session_starts_one_livy(transport, capsys):
    """The claim the console's banner makes, asserted rather than assumed."""

    transport.answers = [_load_report(), _test_report(), _unbind_result()]
    parser = build_parser()

    with ConsoleSession(workspace=_workspace()) as session:
        _run(session, parser, ["load", "Lakehouse/Sales", "--dry-run"])
        _run(session, parser, ["test", "Lakehouse/Sales", "--dry-run"])
        _run(session, parser, ["unbind", "Lakehouse/Sales"])

    assert transport.acquired == 1


def test_each_command_still_did_its_own_work(transport, capsys):
    """One session is only worth having if the commands still ran in it.

    Guards the way this test could rot into passing for the wrong reason: a
    command that silently stopped crossing would also acquire no second Livy.
    """

    transport.answers = [_load_report(), _test_report(), _unbind_result()]
    parser = build_parser()

    with ConsoleSession(workspace=_workspace()) as session:
        _run(session, parser, ["load", "Lakehouse/Sales", "--dry-run"])
        _run(session, parser, ["test", "Lakehouse/Sales", "--dry-run"])
        _run(session, parser, ["unbind", "Lakehouse/Sales"])

    crossings = [
        code for code in transport.submitted if "weaver.__version__" not in code
    ]
    assert len(crossings) == 3
    assert "weaver.load(" in crossings[0]
    assert "weaver.test(" in crossings[1]
    assert "unbind_targets(" in crossings[2]


def test_a_command_given_no_session_still_works_on_its_own(transport):
    """A one-shot invocation owns the Session it opens, and closes it."""

    transport.answers = [_load_report()]
    parser = build_parser()
    parsed = parser.parse_args(
        [
            "load",
            "Lakehouse/Sales",
            "--dry-run",
            "--workspace",
            "My Workspace",
            "--weaver-lakehouse",
            "Weaver",
            "--environment",
            "weaver",
        ]
    )
    parsed.handler(parsed)

    assert transport.acquired == 1


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
        root / "weaver" / "session" / "console.py",
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
