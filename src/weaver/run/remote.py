"""The far side of a decomposed run: one RuntimeScope per run, held open.

When the Runner moves to the desktop, one thing cannot come with it. A Python
primitive is a *deployed module*, imported inside the Fabric session against
that session's Spark, through Weaver's own import machinery — so the object that
owns those imports, :class:`~weaver.runtime.python_context.RuntimeScope`, has to
live where the imports do.

The scope therefore stays remote and only its **name** crosses:

.. code-block:: text

    desktop                          Fabric session

    begin_run(run_id)        ──►     RuntimeScope.new(), stored under run_id
    dispatch(run_id, node A) ──►     same scope → existing import path
    dispatch(run_id, node B) ──►     same scope
    end_run(run_id)          ──►     scope.close(), forgotten

Nothing here reimplements what importing a deployed module means. Each entry
point calls the same function an in-session run calls, with the same arguments.
What the decomposition moved is *where the orchestration decides*; what a
primitive does is one implementation, and stays one.

**Why a registry rather than one global scope.** Two runs can overlap — a
notebook and a desktop against one session, or a retry begun before its
predecessor was cleaned up — and a scope shared between them would let the
second import modules the first had already loaded. Across runs nothing may be
shared at all: that is what makes a rebuilt module take effect on the next load
rather than the next session.

**A leaked scope dies with the interpreter.** If a desktop vanishes before
``end_run``, the entry survives until the Livy session ends and then goes with
it. That is the right failure mode, and the reason the registry lives in module
state rather than anywhere durable: a scope's lifetime is at most the
interpreter's.
"""

from __future__ import annotations

import threading

from .result import RunError

#: run_id → RuntimeScope, for runs currently open in this interpreter.
_SCOPES: dict[str, object] = {}
_LOCK = threading.Lock()


def begin_run(run_id: str) -> str:
    """Open a runtime scope for one logical run, and name it.

    Idempotent deliberately: a resubmitted statement — a dropped response, a
    retried call — must not replace a scope whose modules are already imported
    and in use by the run that is still going.
    """

    from ..runtime.python_context import RuntimeScope

    with _LOCK:
        if run_id not in _SCOPES:
            _SCOPES[run_id] = RuntimeScope.new()
    return run_id


def end_run(run_id: str) -> bool:
    """Close one run's scope and forget it. True if there was one to close."""

    with _LOCK:
        scope = _SCOPES.pop(run_id, None)
    if scope is None:
        return False
    scope.close()
    return True


def open_runs() -> tuple[str, ...]:
    """Which runs this interpreter is currently holding scopes for."""

    with _LOCK:
        return tuple(sorted(_SCOPES))


def scope_for(run_id: str):
    """The scope this run's imports belong to, or a diagnosis of its absence."""

    with _LOCK:
        scope = _SCOPES.get(run_id)
    if scope is None:
        raise RunError(
            f"run {run_id!r} has no runtime scope in this session; either "
            "begin_run was never called, or the Spark session was replaced "
            "underneath the run"
        )
    return scope


def dispatch_python(
    *,
    run_id: str,
    node_id: str,
    item: str,
    target: str,
    schema: str,
    object: str,
    expected_class: str,
    fault_tolerant: bool = False,
    session=None,
    workspace=None,
) -> dict:
    """Run one deployed Python primitive in this run's scope, and report rows.

    The arguments are flat and small on purpose. A ``RunNode`` carries typed
    Weaver identities and an opaque description of what is installed, none of
    which the far side needs — so what crosses is the handful of strings the
    import and the construction actually use, rather than a serialisation of the
    Runner's internal model that would then have to be kept in step with it.
    """

    from ..declaration.model import WeaverItemId
    from ..load_plan import LAKEHOUSE_TARGET, PhysicalTargetRef
    from .dispatch import python_primitive

    return python_primitive(
        node_id=node_id,
        logical_item=WeaverItemId.parse(item),
        physical_target=PhysicalTargetRef(kind=LAKEHOUSE_TARGET, name=target),
        schema=schema,
        object=object,
        expected_class=expected_class,
        fault_tolerant=fault_tolerant,
        runtime_scope=scope_for(run_id),
        session=_session(session, workspace),
        workspace=workspace,
    ).as_row()


def dispatch_validation(
    *,
    run_id: str,
    installed: dict,
    collect: bool = False,
    session=None,
    workspace=None,
) -> dict:
    """Run one installed Lakehouse validation in this run's scope.

    A validation crosses as the description the estate gave of it, because that
    is what a validation *is* here — the Registry row saying where the primitive
    lives and what it compares. Reconstructed on this side into the same value
    an in-session run holds, and handed to the same runtime.
    """

    from ..test_execution import run_installed_validation
    from ..test_plan import InstalledValidation

    carried = run_installed_validation(
        InstalledValidation.from_mapping(installed),
        session=_session(session, workspace),
        workspace=workspace,
        runtime_scope=scope_for(run_id),
        collect_diagnostics=collect,
    )
    return {
        "result": carried.result.to_mapping(),
        "diagnostics": list(carried.diagnostics or ()),
    }


def _session(session, workspace):
    """The Session these entry points run against.

    Given one, that is the answer: the submitted body constructs a
    :class:`~weaver.session.notebook.NotebookSession` around the interpreter's
    own ``spark`` global, exactly as every other crossing does, because a
    Session built in here would have to go looking for an active Spark session
    rather than being handed the one this statement is running in.

    Absent — a notebook calling these directly — the host decides.
    """

    if session is not None:
        return session
    from ..session.host import session_for

    return session_for(workspace)


__all__ = [
    "begin_run",
    "dispatch_python",
    "dispatch_validation",
    "end_run",
    "open_runs",
    "scope_for",
]
