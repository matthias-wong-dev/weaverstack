"""Stable entry points for runtime work submitted to Fabric.

Keep this surface narrow because the published wheel and desktop may have
different versions.
"""

from __future__ import annotations

from ..runtime.session_scopes import get_scope


def run_staged(entry, *, session, workspace, stage: str, workflow_id=None) -> dict:
    """Run ``entry`` with the arguments a client staged, writing its progress.

    The report is left in the stage. Its size and hash are what cross back.
    ``workflow_id`` is the client's workflow, if it runs in one.
    """

    import hashlib
    import json
    from contextlib import nullcontext

    from ..fabric.store import FabricStore
    from ..locations import Location
    from ..sessions.run_in_fabric import PROGRESS, REQUEST, RESULT, progress_written

    store = FabricStore()
    root = Location(stage)
    arguments = json.loads(store.read(root / REQUEST))
    within = session.workflow(workflow_id) if workflow_id else nullcontext()
    with within, progress_written(session, store, root / PROGRESS):
        report = entry(session=session, workspace=workspace, **arguments)
    data = json.dumps({"report": report, "warnings": list(session.warnings)})
    store.write(root / RESULT, data.encode("utf-8"))
    return {
        "bytes": len(data.encode("utf-8")),
        "sha256": hashlib.sha256(data.encode("utf-8")).hexdigest(),
    }


def run_load_in_fabric(
    *, session, workspace, catalogue: dict, request: dict, started: str
) -> dict:
    """Plan and execute a load a client sent, against the catalogue it read."""

    from datetime import datetime

    from ..catalogue.state import Catalogue
    from ..catalogue.writer import writer_for
    from ..operations.load import execute_load, load_runner
    from .runner import RunRequest
    from .state import RunState

    read = Catalogue.from_mapping(
        catalogue, writer=writer_for(session, workspace), session=session
    )
    runner = load_runner(
        session,
        workspace,
        RunState(catalogue=read),
        RunRequest.from_mapping(request),
    )
    report = execute_load(
        session,
        workspace=workspace,
        runner=runner,
        started=datetime.fromisoformat(started),
    )
    return report.to_mapping()


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


__all__ = [
    "run_load_in_fabric",
    "run_staged",
    "run_validation_primitive",
]
