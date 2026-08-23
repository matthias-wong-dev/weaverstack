"""Run — what runs against an installed estate, and what happened.

The runtime half of Weaver's architecture:

.. code-block:: text

    RunState + RunRequest  →  Runner  →  RunGraph  →  RunResult

One Runner for load, test and whatever runtime work comes next. The kinds differ
in which nodes are selected and which primitive runs; they do not differ in how
a run behaves.

Eight modules, and each is a step of that line rather than a fragment of one:

.. code-block:: text

    state       the installed catalogue snapshot
    graph       what the catalogue says runs, and in what order
    runner      what runs next — and what a run was asked for
    resolution  the dispatch address a catalogue node names
    dispatch    the one place a run crosses into a real engine
    outcome     what a primitive's answer means, raised or returned
    result      what happened: the contract, the messages, the statuses
    record      what the estate is told about it, in the runtime tables
"""

from __future__ import annotations

from .dispatch import can_refresh, dispatch_primitive
from .graph import RunGraph, RunNode, graph_for
from .record import RunRecord, new_workflow_id, open_run_record
from .result import (
    RunError,
    RunFailure,
    RunNodeResult,
    RunResult,
    reports_outcome,
    run_status,
)
from .runner import LOAD, TEST, Runner, RunRequest
from .state import RunState

__all__ = [
    "LOAD",
    "TEST",
    "RunError",
    "RunFailure",
    "RunGraph",
    "RunNode",
    "RunNodeResult",
    "RunRequest",
    "RunResult",
    "RunState",
    "Runner",
    "can_refresh",
    "reports_outcome",
    "dispatch_primitive",
    "graph_for",
    "RunRecord",
    "new_workflow_id",
    "open_run_record",
    "run_status",
]
