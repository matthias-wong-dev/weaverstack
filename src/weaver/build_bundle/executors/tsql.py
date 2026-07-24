"""T-SQL execution — the future Warehouse seam, deliberately refused in v1.

The class and its registry entry exist so the architecture shows the shape a
Warehouse installer will take, but executing raises: v1 plans never carry a
T-SQL action (generation refuses it), and this is the second, defensive line.
"""

from __future__ import annotations

from typing import Any

from ..models import BuildAction
from .base import InstallationContext


class TSqlExecutor:
    name = "tsql"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        raise NotImplementedError(
            "T-SQL and Warehouse installation are not supported by build bundle v1"
        )
