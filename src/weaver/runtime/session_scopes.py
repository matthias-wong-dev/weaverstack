"""Session-local runtime scopes, isolated by run."""

from __future__ import annotations

import threading

from ..errors import RuntimeScopeError

#: run_id → RuntimeScope, for scopes currently open in this interpreter.
_SCOPES: dict[str, object] = {}
#: run_id → the run's catalogue, as the payload it crossed as. Held beside the
#: scope because it has the same lifetime and the same reason to exist: a run
#: reads it once, and every object it dispatches reads it here rather than asking
#: the Warehouse again.
_CATALOGUES: dict[str, dict] = {}
_LOCK = threading.Lock()


def open_scope(run_id: str, catalogue: dict | None = None) -> str:
    """Open a runtime scope under one name, and return the name.

    Idempotent: a resubmitted statement must not replace a scope whose modules
    are in use by the run that is still going.
    """

    from .python_context import RuntimeScope

    with _LOCK:
        if run_id not in _SCOPES:
            _SCOPES[run_id] = RuntimeScope.new()
            _CATALOGUES[run_id] = catalogue
    return run_id


def scope_catalogue(run_id: str):
    from ..catalogue.state import Catalogue

    with _LOCK:
        payload = _CATALOGUES.get(run_id)
    return None if payload is None else Catalogue.from_mapping(payload)


def close_scope(run_id: str) -> bool:
    with _LOCK:
        scope = _SCOPES.pop(run_id, None)
        _CATALOGUES.pop(run_id, None)
    if scope is None:
        return False
    scope.close()
    return True


def open_scopes() -> tuple[str, ...]:
    with _LOCK:
        return tuple(sorted(_SCOPES))


def get_scope(run_id: str):
    with _LOCK:
        scope = _SCOPES.get(run_id)
    if scope is None:
        raise RuntimeScopeError(
            f"run {run_id!r} has no runtime scope in this Spark session; rerun the load"
        )
    return scope


__all__ = [
    "close_scope",
    "get_scope",
    "open_scope",
    "open_scopes",
    "scope_catalogue",
]
