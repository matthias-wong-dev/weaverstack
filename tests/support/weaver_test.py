"""The declarative test contract and its telemetry registry.

This is pytest infrastructure.  Production Sessions know nothing about pytest;
fixtures register the Sessions they hand to a test, and the hooks in the root
``conftest`` consume their normal ``Session.telemetry`` after the test body.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Callable

import pytest

from weaver.sessions.telemetry import RESOURCES


@dataclass(frozen=True)
class WeaverTestDeclaration:
    """The claimed position and external resources for one test function."""

    scope: str = "core"
    resources: frozenset[str] = frozenset()


def weaver_test(
    *,
    remote: bool = False,
    hosted: bool = False,
    integration: bool = False,
    provision: bool = False,
    resources=frozenset(),
) -> Callable:
    """Declare a test's Weaver position and necessary external resources."""

    selected = [
        scope
        for scope, enabled in (
            ("remote", remote),
            ("hosted", hosted),
            ("integration", integration),
            ("provision", provision),
        )
        if enabled
    ]
    if len(selected) > 1:
        raise ValueError("a Weaver test has one scope")
    scope = selected[0] if selected else "core"
    declared = frozenset(resources)
    unknown = declared - RESOURCES
    if unknown:
        raise ValueError(f"unknown Weaver test resource(s): {sorted(unknown)}")
    declaration = WeaverTestDeclaration(
        scope=scope,
        resources=declared,
    )

    def apply(function):
        setattr(function, "__weaver_test_declaration__", declaration)
        marked = function
        if scope != "core":
            marked = pytest.mark.fabric(marked)
        if scope in {"remote", "hosted"}:
            marked = getattr(pytest.mark, scope)(marked)
        if scope == "integration":
            marked = pytest.mark.full_integration(marked)
        if scope == "provision":
            marked = pytest.mark.provision(marked)
        return marked

    return apply


_sessions: ContextVar[tuple[object, ...]] = ContextVar(
    "weaver_test_sessions", default=()
)


def begin_test() -> object:
    """Start a fresh registry for the current pytest test body."""

    return _sessions.set(())


def end_test(token) -> None:
    """Discard a test's registered Sessions."""

    _sessions.reset(token)


def register_session(session) -> object:
    """Register a fixture-provided Session with the active test, once."""

    current = _sessions.get()
    if session not in current:
        _sessions.set((*current, session))
    return session


def registered_sessions() -> tuple[object, ...]:
    """The Sessions attributed to the active test body."""

    return _sessions.get()


def observed_resources(before: dict[int, int]) -> frozenset[str]:
    """The resource union from events emitted since the test body began."""

    events = []
    for session in registered_sessions():
        events.extend(session.telemetry.events()[before.get(id(session), 0) :])
    return frozenset(event.resource for event in events)


def event_snapshot() -> dict[int, int]:
    """The per-Session event positions at the start of a test body."""

    return {
        id(session): len(session.telemetry.events()) for session in registered_sessions()
    }


__all__ = [
    "WeaverTestDeclaration",
    "begin_test",
    "end_test",
    "event_snapshot",
    "observed_resources",
    "register_session",
    "registered_sessions",
    "weaver_test",
]
