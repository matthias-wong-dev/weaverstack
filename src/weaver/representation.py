"""Shared public result representation."""

from __future__ import annotations

from typing import Any


class ReportAlias:
    """Give mapping-backed public results one Notebook display method."""

    def report(self) -> dict[str, Any]:
        mapping = getattr(self, "to_mapping", None)
        if mapping is None:
            mapping = getattr(self, "as_dict")
        return mapping()
