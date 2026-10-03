"""Stable entry points for runtime work submitted to Fabric.

Keep this surface narrow because the published wheel and desktop may have
different versions.
"""

from __future__ import annotations


def run_staged(entry, *, session, workspace, stage: str, workflow_id=None) -> dict:
    """Run ``entry`` with the arguments a client staged, writing its progress.

    The report is left in the stage. Its size and hash are what cross back.
    ``workflow_id`` is the client's workflow, if it runs in one.
    """

    import hashlib
    import json
    from contextlib import nullcontext

    from ..fabric.store import FabricStore
    from ..lakehouse import release_mounts
    from ..locations import Location
    from ..sessions.run_in_fabric import PROGRESS, REQUEST, RESULT, progress_written

    # The interpreter outlives the run; an earlier run's mounts can be stale.
    release_mounts()
    store = FabricStore()
    root = Location(stage)
    arguments = json.loads(store.read(root / REQUEST))
    within = session.workflow(workflow_id) if workflow_id else nullcontext()
    with within, progress_written(session, store, root / PROGRESS):
        report = entry(session=session, workspace=workspace, **arguments)
    # Diagnostic rows carry whatever a check selected, so they cross as text.
    data = json.dumps(
        {"report": report, "warnings": list(session.warnings)}, default=str
    ).encode("utf-8")
    store.write(root / RESULT, data)
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def run_load_in_fabric(
    *, session, workspace, catalogue: dict, request: dict, started: str
) -> dict:
    """Plan and execute a load a client sent, against the catalogue it read."""

    from datetime import datetime

    from ..operations.load import execute_load, load_runner

    runner = load_runner(
        session, workspace, *_planned(session, workspace, catalogue, request)
    )
    report = execute_load(
        session,
        workspace=workspace,
        runner=runner,
        started=datetime.fromisoformat(started),
    )
    return report.to_mapping()


def run_test_in_fabric(
    *, session, workspace, catalogue: dict, request: dict, started: str
) -> dict:
    """Plan and execute a test run a client sent, against the catalogue it read.

    A named run's diagnostic rows cross beside the report, which never holds them.
    """

    from datetime import datetime

    from ..operations.test import execute_test, validation_runner

    runner = validation_runner(
        workspace, *_planned(session, workspace, catalogue, request)
    )
    report = execute_test(
        session,
        workspace=workspace,
        runner=runner,
        started=datetime.fromisoformat(started),
    )
    carried = report.to_mapping()
    carried["diagnostics"] = {
        node.logical_id: list(node.diagnostics)
        for node in report.nodes
        if node.diagnostics
    }
    return carried


def _planned(session, workspace, catalogue: dict, request: dict):
    """The state and request a client planned against, writing through here."""

    from ..catalogue.state import Catalogue
    from ..catalogue.writer import writer_for
    from .runner import RunRequest
    from .state import RunState

    read = Catalogue.from_mapping(
        catalogue, writer=writer_for(session, workspace), session=session
    )
    return RunState(catalogue=read), RunRequest.from_mapping(request)


__all__ = ["run_load_in_fabric", "run_staged", "run_test_in_fabric"]
