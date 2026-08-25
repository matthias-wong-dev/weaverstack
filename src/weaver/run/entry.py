"""What a submitted statement calls: the far side of a decomposed run.

A deployed Python primitive is imported where Spark is, so a run orchestrated
elsewhere submits a statement per node and these are what it calls. Each unpacks
flat arguments into the values an in-session run already holds and calls the same
function that run calls.

This is the surface the published wheel exports, so it stays two named functions:
the wheel and the desktop are independently versioned, and widening the surface
between them is what has caused Fabric failures.
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
    identity: str | None = None,
    session=None,
    workspace=None,
) -> dict:
    """Run one deployed Python primitive in a named scope, and report rows."""

    from ..declaration.model import WeaverItemId, parse_installed_identity
    from ..load_plan import LAKEHOUSE_TARGET, PhysicalTargetRef
    from ..runtime.session_scopes import scope_catalogue
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
        # Read where the run opened its scope, not here: the catalogue crossed
        # once, with the scope, and this is one node of the run that carried it.
        catalogue=scope_catalogue(run_id),
        node_identity=parse_installed_identity(identity) if identity else None,
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

    It crosses as the estate's own description of it, the Registry row saying
    where the primitive lives and what it compares.
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
    """The Session these run against.

    The submitted body builds one around the interpreter's own ``spark`` global;
    one built here would have to go looking for an active session instead. A
    notebook calling these directly supplies none, and the host decides.
    """

    if session is not None:
        return session
    from ..sessions.host import session_for

    return session_for(workspace)


__all__ = ["run_python_primitive", "run_validation_primitive"]
