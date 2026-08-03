"""Load mechanics — the half of Weaver that runs where the data is.

Everything else in the core decides *what should exist* and freezes it into a
bundle. This package is what happens afterwards, when an installed artefact is
executed against a real target: the contract a running module reads out of its
own docstring, the result every primitive reports, and the machinery a Python
table or folder loads through.

It is deliberately reachable without any of that upstream: a load primitive
takes a session and a target and nothing more. No repository, no catalogue, no
planner and no orchestrator — see :mod:`weaver.runtime.load_contract` for why
that boundary is the point rather than an omission.

Nothing here imports PySpark or Delta. A session arrives from the caller and is
used through its ordinary API, and Delta operations are issued as SQL text, so
the core stays importable on a machine with no JVM.
"""

from __future__ import annotations

from .load_contract import FolderLoadContract, LoadContract, document_for_module
from .load_result import RESULT_COLUMNS, LoadResult

__all__ = [
    "RESULT_COLUMNS",
    "FolderLoadContract",
    "LoadContract",
    "LoadResult",
    "document_for_module",
]
