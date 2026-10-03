"""Exact-case Spark analysis scoped to one executor call."""

from __future__ import annotations

import threading
import weakref
from contextlib import contextmanager
from typing import Iterator

_CASE_SENSITIVE = "spark.sql.caseSensitive"


class _CaseGate:
    """One Spark session's identifier-case mode, shared by concurrent callers.

    The setting is session-global, so callers in one mode run together and a
    caller in the other mode waits until they have all left.
    """

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.exact = False
        self.active = 0
        self.previous = None


_GATES: "weakref.WeakKeyDictionary[object, _CaseGate]" = weakref.WeakKeyDictionary()
_GATES_LOCK = threading.Lock()


def _gate(spark) -> _CaseGate:
    with _GATES_LOCK:
        gate = _GATES.get(spark)
        if gate is None:
            gate = _GATES[spark] = _CaseGate()
        return gate


@contextmanager
def exact_identifier_case(spark, *, enabled: bool) -> Iterator[None]:
    """Make both analysis and DDL honour Weaver identifier case while inside."""

    gate = _gate(spark)
    with gate.condition:
        while gate.active and gate.exact != enabled:
            gate.condition.wait()
        if not gate.active:
            gate.exact = enabled
            gate.previous = None
            if enabled:
                previous = spark.conf.get(_CASE_SENSITIVE)
                if str(previous).lower() != "true":
                    spark.conf.set(_CASE_SENSITIVE, "true")
                    gate.previous = previous
        gate.active += 1
    try:
        yield
    finally:
        with gate.condition:
            gate.active -= 1
            if not gate.active:
                if gate.previous is not None:
                    spark.conf.set(_CASE_SENSITIVE, gate.previous)
                    gate.previous = None
                gate.condition.notify_all()
