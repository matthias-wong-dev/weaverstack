"""Stable entry points for runtime work submitted to Fabric.

Keep this surface narrow because the published wheel and desktop may have
different versions.
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
    reload: bool = False,
    identity: str | None = None,
    session=None,
    workspace=None,
) -> dict:

    from ..declaration.model import WeaverItemId, parse_installed_identity
    from ..runtime.session_scopes import scope_catalogue
    from ..targets import LAKEHOUSE_TARGET, PhysicalTargetRef
    from .dispatch import python_primitive

    return python_primitive(
        node_id=node_id,
        logical_item=WeaverItemId.parse(item),
        physical_target=PhysicalTargetRef(kind=LAKEHOUSE_TARGET, name=target),
        schema=schema,
        object=object,
        expected_class=expected_class,
        fault_tolerant=fault_tolerant,
        reload=reload,
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

    if session is not None:
        return session
    from ..sessions.host import session_for

    return session_for(workspace)


__all__ = ["run_python_primitive", "run_validation_primitive"]
