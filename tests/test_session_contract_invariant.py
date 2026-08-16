"""Every Session host implements the same contract, signature for signature.

``TestSession`` is what the fast suite decides against, so a boundary it spells
differently from the real hosts is a test suite proving something Fabric never
does. Compared mechanically rather than by review: the drift that prompted this
was one iterable parameter where the contract says one string, and reading it
did not catch it.
"""

from __future__ import annotations

import inspect

import pytest
from support.weaver_test import weaver_test

from weaver.sessions.base import Session
from weaver.sessions.console import ConsoleSession
from weaver.sessions.notebook import NotebookSession
from weaver.sessions.testing import TestSession

HOSTS = (ConsoleSession, NotebookSession, TestSession)

#: The capabilities an operation reaches for. Every host answers all of them,
#: and a caller may pass the same arguments to any of them.
CAPABILITIES = (
    "execute_python",
    "execute_spark_sql_batch",
    "execute_tsql",
    "query_tsql",
    "executes_here",
)


def _signature(implementation, name: str) -> inspect.Signature:
    return inspect.signature(getattr(implementation, name))


@pytest.mark.parametrize("host", HOSTS, ids=lambda host: host.__name__)
@pytest.mark.parametrize("capability", CAPABILITIES)
@weaver_test()
def test_each_host_matches_the_session_contract(host, capability):
    contract = _signature(Session, capability)
    implemented = _signature(host, capability)

    assert list(implemented.parameters) == list(contract.parameters), (
        f"{host.__name__}.{capability} takes {list(implemented.parameters)}, "
        f"and the contract says {list(contract.parameters)}"
    )
    for name, declared in contract.parameters.items():
        found = implemented.parameters[name]
        assert found.kind == declared.kind, (
            f"{host.__name__}.{capability} passes {name} as {found.kind}, "
            f"and the contract says {declared.kind}"
        )
        assert found.default == declared.default, (
            f"{host.__name__}.{capability} defaults {name} to {found.default!r}, "
            f"and the contract says {declared.default!r}"
        )


@weaver_test()
def test_no_session_capability_is_left_unlisted():
    """The list above is the contract, so it cannot fall behind the contract."""

    declared = {
        name
        for name, value in vars(Session).items()
        if getattr(value, "__isabstractmethod__", False)
    }
    unlisted = declared - set(CAPABILITIES) - {"_new_scope"}
    assert not unlisted, (
        "Session declares capabilities this invariant does not compare: "
        + ", ".join(sorted(unlisted))
    )


@weaver_test()
def test_the_test_host_records_rather_than_interprets():
    """``TestSession`` answers from configuration and never reads a statement.

    The line it must not cross: a double that parsed SQL would become a second
    engine, and the suite would be proving Weaver against it rather than
    against Fabric.
    """

    source = inspect.getsource(TestSession)
    for forbidden in ("sqlparse", "import re", "CREATE ", "SELECT ", "startswith("):
        assert forbidden not in source, (
            f"TestSession mentions {forbidden!r}, which suggests it has started "
            "interpreting statements rather than recording them"
        )
