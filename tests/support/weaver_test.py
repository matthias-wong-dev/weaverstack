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

    position: str = "core"
    resources: frozenset[str] = frozenset()
    integration: bool = False
    provision: bool = False

    @property
    def scope(self) -> str:
        if self.integration:
            return "integration"
        if self.provision:
            return "provision"
        return self.position


def weaver_test(
    *,
    remote: bool = False,
    hosted: bool = False,
    integration: bool = False,
    provision: bool = False,
    resources=frozenset(),
) -> Callable:
    """Declare a test's Weaver position and necessary external resources."""

    if remote and hosted:
        raise ValueError("a Weaver test cannot be both remote and hosted")
    position = "remote" if remote else "hosted" if hosted else "core"
    if (integration or provision) and position == "core":
        raise ValueError("integration and provision tests need remote=True or hosted=True")
    declared = frozenset(resources)
    unknown = declared - RESOURCES
    if unknown:
        raise ValueError(f"unknown Weaver test resource(s): {sorted(unknown)}")
    declaration = WeaverTestDeclaration(
        position=position,
        resources=declared,
        integration=integration,
        provision=provision,
    )

    def apply(function):
        setattr(function, "__weaver_test_declaration__", declaration)
        marked = function
        if position != "core":
            marked = pytest.mark.fabric(marked)
            marked = getattr(pytest.mark, position)(marked)
        if integration:
            marked = pytest.mark.full_integration(marked)
        if provision:
            marked = pytest.mark.provision(marked)
        return marked

    return apply


_sessions: ContextVar[tuple[object, ...]] = ContextVar(
    "weaver_test_sessions", default=()
)
_known_sessions: list[object] = []


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
    if session not in _known_sessions:
        _known_sessions.append(session)
    return session


def registered_sessions() -> tuple[object, ...]:
    """The Sessions attributed to the active test body."""

    return tuple(_known_sessions)


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
