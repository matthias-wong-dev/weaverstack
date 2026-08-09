"""Run — what runs against an installed estate, and what happened.

The runtime half of Weaver's architecture:

.. code-block:: text

    RunState + RunRequest  →  Runner  →  RunGraph  →  RunResult

One Runner for load, test and whatever runtime work comes next. The kinds differ
in which nodes are selected and which primitive runs; they do not differ in how
a run behaves.
"""

from __future__ import annotations

from .dispatch import can_refresh, dispatch_primitive
from .graph import RunGraph, RunNode, graph_for
from .observe import open_run_log, read_run_state
from .request import LOAD, TEST, RunRequest
from .result import RunNodeResult, RunResult, run_status
from .runner import Runner
from .state import RunState

__all__ = [
    "LOAD",
    "TEST",
    "RunGraph",
    "RunNode",
    "RunNodeResult",
    "RunRequest",
    "RunResult",
    "RunState",
    "Runner",
    "can_refresh",
    "dispatch_primitive",
    "graph_for",
    "open_run_log",
    "read_run_state",
    "run_status",
]
