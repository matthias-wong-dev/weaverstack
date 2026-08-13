"""Named entry points a submitted statement calls, and the arguments they take.

A run orchestrated from a desktop still has to import its deployed Python
primitives where Spark is, so a statement is submitted per node and these are
what it calls. Each one unpacks flat arguments into the values an in-session run
already holds, and then calls the same function that run calls: nothing here
reimplements what a primitive does.

The arguments are flat and small on purpose. A ``RunNode`` carries typed Weaver
identities and an opaque description of what is installed, none of which this
side needs — so what crosses is the handful of strings the import and the
construction actually use, rather than a serialisation of the Runner's model
that would then have to be kept in step with it.

These are functions rather than text inside a submitted body because the wheel
and the desktop are two independently versioned halves of one contract, and a
named function is versioned, testable and greppable. Widening this surface is the
coupling that has caused Fabric failures, so it stays two functions.
"""

from __future__ import annotations

from ..runtime.session_scopes import get_scope


def run_python_primitive(
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
    """Run one deployed Python primitive in a named scope, and report rows."""

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
        runtime_scope=get_scope(run_id),
        session=_session(session, workspace),
        workspace=workspace,
    ).as_row()


def run_validation_primitive(
    *,
    run_id: str,
    installed: dict,
    collect: bool = False,
    session=None,
    workspace=None,
) -> dict:
    """Run one installed Lakehouse validation in a named scope.

    A validation crosses as the description the estate gave of it, because that
    is what a validation *is* here — the Registry row saying where the primitive
    lives and what it compares. Reconstructed on this side into the same value an
    in-session run holds, and handed to the same runtime.
    """

    from ..test_execution import run_installed_validation
    from ..test_plan import InstalledValidation

    carried = run_installed_validation(
        InstalledValidation.from_mapping(installed),
        session=_session(session, workspace),
        workspace=workspace,
        runtime_scope=get_scope(run_id),
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
    own ``spark`` global, exactly as every other crossing does, because a Session
    built in here would have to go looking for an active Spark session rather
    than being handed the one this statement is running in.

    Absent — a notebook calling these directly — the host decides.
    """

    if session is not None:
        return session
    from ..session.host import session_for

    return session_for(workspace)


__all__ = ["run_python_primitive", "run_validation_primitive"]
