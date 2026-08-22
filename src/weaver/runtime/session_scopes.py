"""Runtime scopes held by name, for the life of one interpreter.

A run orchestrated from elsewhere opens a scope here, dispatches against it by
name, and closes it when it is done.

A registry rather than one scope, because two runs can overlap and a shared one
would let the second use modules the first imported. Module state rather than
anything durable, because a scope's lifetime is at most the interpreter's: a
leaked one dies with the session.
"""

from __future__ import annotations

import threading

from ..errors import RuntimeScopeError

#: run_id → RuntimeScope, for scopes currently open in this interpreter.
_SCOPES: dict[str, object] = {}
#: run_id → the run's bookmarks, as installed identity text to ISO instant. Held
#: beside the scope because they have the same lifetime and the same reason to
#: exist: a run reads them once, and every object it dispatches reads them here
#: rather than asking the Warehouse again.
_BOOKMARKS: dict[str, dict] = {}
_LOCK = threading.Lock()


def open_scope(run_id: str, bookmarks: dict | None = None) -> str:
    """Open a runtime scope under one name, and return the name.

    Idempotent: a resubmitted statement must not replace a scope whose modules
    are in use by the run that is still going.
    """

    from .python_context import RuntimeScope

    with _LOCK:
        if run_id not in _SCOPES:
            _SCOPES[run_id] = RuntimeScope.new()
            _BOOKMARKS[run_id] = dict(bookmarks or {})
    return run_id


def scope_bookmarks(run_id: str) -> dict:
    """The bookmarks this run opened with, as identity text to ISO instant."""

    with _LOCK:
        return dict(_BOOKMARKS.get(run_id, {}))


def close_scope(run_id: str) -> bool:
    """Close one scope and forget it. True if there was one to close."""

    with _LOCK:
        scope = _SCOPES.pop(run_id, None)
        _BOOKMARKS.pop(run_id, None)
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


__all__ = [
    "close_scope",
    "get_scope",
    "open_scope",
    "open_scopes",
    "scope_bookmarks",
]
