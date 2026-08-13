"""Load mechanics — the half of Weaver that runs where the data is.

What happens after a bundle is installed: the contract a running module reads
out of its own docstring, the result every primitive reports, and the machinery
a Python table or folder loads through.

Reachable without any of the upstream — a load primitive takes a session and a
target and nothing more. See :mod:`weaver.runtime.load_contract`.

Nothing here imports PySpark or Delta. A session arrives from the caller and
Delta operations are issued as SQL text, so the core stays importable on a
machine with no JVM.
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
