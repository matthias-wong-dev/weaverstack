"""What a command says it will need, and what the Session does with it.

The direction is the point. A Session cannot work out what a build wants — it
has no idea what a build *is*, and a Session that decided would be a second
place deciding what an operation does. So commands declare and the Session
prepares.

Two levels, because arguments cannot know everything:

.. code-block:: text

    command requirements    from parsed args, coarse, a superset  → warm-up
    execution requirements  from the BuildBundle or RunGraph      → routing

And one rule that ties them together and is easy to lose: **preparing is not
using.** A declaration buys a head start for an acquisition that is coming
anyway. It never causes one. A run that declares Livy and turns out to be all
T-SQL opens no Spark session — which is asserted where it can actually go wrong,
in `tests/test_run_remote_boundary.py`.
"""

from __future__ import annotations

import pytest

from weaver.session.requirements import (
    AUTH,
    LIVY,
    ONELAKE,
    RESOLVER,
    TDS,
    requirements,
    union,
)
from weaver_cli.main import build_parser, command_requirements


def _declared(*words: str) -> set[str]:
    return set(command_requirements(build_parser().parse_args(list(words))))


# --- the vocabulary -----------------------------------------------------------


def test_a_declaration_is_checked_against_the_vocabulary():
    """A typo must be a failure, not a requirement nobody honours."""

    with pytest.raises(ValueError, match="unknown resource requirement"):
        requirements(AUTH, "sparkle")


def test_the_union_is_what_a_sequence_will_want_between_its_commands():
    assert union({AUTH, TDS}, {AUTH, LIVY}, ()) == frozenset({AUTH, TDS, LIVY})


def test_the_union_of_nothing_is_nothing():
    assert union() == frozenset()


# --- what each command declares -----------------------------------------------


def test_a_warehouse_load_asks_for_tds_and_not_for_spark():
    """The case the whole mechanism is for: T-SQL work should not wait on a
    Spark session, and on a small capacity should not queue for one."""

    declared = _declared("load", "Warehouse/Reporting")

    assert TDS in declared
    assert LIVY not in declared


def test_a_lakehouse_load_asks_for_spark_and_files():
    declared = _declared("load", "Lakehouse/Sales")

    assert {ONELAKE, LIVY} <= declared
    assert TDS not in declared


def test_a_mixed_request_asks_for_both():
    declared = _declared("test", "Lakehouse/Sales", "Warehouse/Reporting")

    assert {TDS, LIVY, ONELAKE} <= declared


def test_a_build_asks_for_everything_it_might_touch():
    """Coarse on purpose: a repository can hold files, DDL and Warehouse tables,
    and which of them this one holds is not knowable from the arguments."""

    assert _declared("build", ".") == {AUTH, RESOLVER, ONELAKE, LIVY, TDS}


def test_every_command_that_reaches_a_workspace_asks_for_a_credential():
    for words in (
        ("load", "Lakehouse/Sales"),
        ("test", "Lakehouse/Sales"),
        ("build", "."),
        ("wipe", "Lakehouse/Sales"),
        ("unbind", "Lakehouse/Sales"),
    ):
        assert AUTH in _declared(*words), words


def test_a_command_that_declares_nothing_asks_for_nothing():
    """`capacity` manages the capacity itself, not anything inside a workspace,
    so it must not warm one."""

    assert _declared(
        "capacity",
        "status",
        "--resource-group",
        "rg",
        "--capacity-name",
        "cap",
    ) == set()


# --- what the Session does with it --------------------------------------------


class _Resource:
    def __init__(self):
        self.started = 0

    def start(self, speculative=False):
        self.started += 1


def _scope(monkeypatch):
    from weaver.session.console import ConsoleScope
    from weaver.workspaces import FabricWorkspace

    monkeypatch.setattr(
        "weaver.fabric.auth.credential",
        lambda: (_ for _ in ()).throw(AssertionError("no credential in this test")),
    )
    scope = ConsoleScope.__new__(ConsoleScope)
    scope.workspace = FabricWorkspace(
        workspace="W", weaver_lakehouse="Weaver", environment="weaver"
    )
    scope.auth = _Resource()
    scope.livy = _Resource()
    scope.local_spark = None
    return scope


def test_preparing_starts_only_what_was_declared(monkeypatch):
    scope = _scope(monkeypatch)

    scope.warm({AUTH})

    assert scope.auth.started == 1
    assert scope.livy.started == 0


def test_preparing_for_spark_starts_it(monkeypatch):
    scope = _scope(monkeypatch)

    scope.warm({AUTH, LIVY})

    assert scope.livy.started == 1


def test_declaring_nothing_expensive_starts_nothing_expensive(monkeypatch):
    scope = _scope(monkeypatch)

    scope.warm({RESOLVER})

    assert scope.auth.started == 0
    assert scope.livy.started == 0


def test_an_undeclared_warm_up_still_starts_everything(monkeypatch):
    """`weaver session` warms before any command is typed, so it has nothing to
    go on and wants the lot."""

    scope = _scope(monkeypatch)

    scope.warm()

    assert scope.auth.started == 1
    assert scope.livy.started == 1
