"""Runtime scopes held by name, for the life of one interpreter.

A deployed Python primitive is a *module imported inside the session*, and the
object owning those imports — :class:`~weaver.runtime.python_context.RuntimeScope`
— cannot cross a process boundary. So a run orchestrated from elsewhere opens a
scope here, dispatches against it by name, and closes it when it is done.

**A registry rather than one scope.** Two runs can overlap — a notebook and a
desktop against one session, or a retry begun before its predecessor was cleaned
up — and a shared scope would let the second run use modules the first had
already imported. Across runs nothing is shared at all, which is what makes a
rebuilt module take effect on the next run rather than the next session.

**A leaked scope dies with the interpreter.** If whoever opened one never closes
it, the entry survives until the session ends and then goes with it. That is the
right failure mode, and the reason this is module state rather than anything
durable: a scope's lifetime is at most the interpreter's.
"""

from __future__ import annotations

import threading

from ..errors import RuntimeScopeError

#: run_id → RuntimeScope, for scopes currently open in this interpreter.
_SCOPES: dict[str, object] = {}
_LOCK = threading.Lock()


def open_scope(run_id: str) -> str:
    """Open a runtime scope under one name, and return the name.

    Idempotent deliberately: a resubmitted statement — a dropped response, a
    retried call — must not replace a scope whose modules are already imported
    and in use by the run that is still going.
    """

    from .python_context import RuntimeScope

    with _LOCK:
        if run_id not in _SCOPES:
            _SCOPES[run_id] = RuntimeScope.new()
    return run_id


def close_scope(run_id: str) -> bool:
    """Close one scope and forget it. True if there was one to close."""

    with _LOCK:
        scope = _SCOPES.pop(run_id, None)
    if scope is None:
        return False
    scope.close()
    return True


def open_scopes() -> tuple[str, ...]:
    """Which names this interpreter is currently holding scopes for."""

    with _LOCK:
        return tuple(sorted(_SCOPES))


def get_scope(run_id: str):
    """The scope these imports belong to, or a diagnosis of its absence."""

    with _LOCK:
        scope = _SCOPES.get(run_id)
    if scope is None:
        raise RuntimeScopeError(
            f"run {run_id!r} has no runtime scope in this session; either "
            "open_scope was never called, or the Spark session was replaced "
            "underneath the run"
        )
    return scope


__all__ = ["close_scope", "get_scope", "open_scope", "open_scopes"]
