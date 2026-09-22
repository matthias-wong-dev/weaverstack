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
    ignore_stability_threshold: bool = False,
    identity: str | None = None,
    session=None,
    workspace=None,
) -> dict:

    from ..declaration.model import WeaverItemId, parse_installed_identity
    from ..runtime.load_refusal import refusal_envelope
    from ..runtime.session_scopes import scope_catalogue
    from ..targets import LAKEHOUSE_TARGET, PhysicalTargetRef
    from .dispatch import python_primitive

    try:
        loaded = python_primitive(
            node_id=node_id,
            logical_item=WeaverItemId.parse(item),
            physical_target=PhysicalTargetRef(kind=LAKEHOUSE_TARGET, name=target),
            schema=schema,
            object=object,
            expected_class=expected_class,
            fault_tolerant=fault_tolerant,
            reload=reload,
            ignore_stability_threshold=ignore_stability_threshold,
            runtime_scope=get_scope(run_id),
            session=_session(session, workspace),
            workspace=workspace,
            # Read where the run opened its scope, not here: the catalogue crossed
            # once, with the scope, and this is one node of the run that carried it.
            catalogue=scope_catalogue(run_id),
            node_identity=parse_installed_identity(identity) if identity else None,
        )
    except Exception as exc:  # noqa: BLE001 - re-raised unless it is a refusal
        # Which failures carry a settled result is the runtime's judgement, not
        # this surface's. Anything it does not recognise crosses as the failure
        # it is, traceback and all.
        envelope = refusal_envelope(exc)
        if envelope is None:
            raise
        return envelope
    return loaded.as_row()


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
