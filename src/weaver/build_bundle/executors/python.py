"""Python execution — run a generated wrapper against the certified snapshot.

The payload is the small wrapper ``create_ddl`` produced: it imports the object
module and calls :func:`weaver.build_bundle.runtime.materialise`. This executor binds
the ambient installation the runtime reads, then executes the wrapper. The object
module is imported from the bundle's certified snapshot — the installer puts that
snapshot at the front of the import path — never from the mutable repository copy
in the Weaver Lakehouse.

The payload is compiled with its bundle path as the filename, so a traceback
from a failing object points at the payload rather than an anonymous ``<string>``.
"""

from __future__ import annotations

from typing import Any

from ...errors import InstallError
from ..models import BuildAction
from ..runtime import Installation, installing
from .base import InstallationContext


class PythonExecutor:
    name = "python"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        if payload is None:
            raise InstallError(f"python action {action.id!r} has no payload")

        installation = Installation(
            spark=context.spark,
            resolver=context.resolver,
            lakehouse=context.target.lakehouse,
        )
        filename = action.payload or f"<{action.id}>"
        code = compile(payload.decode("utf-8"), filename, "exec")
        namespace: dict[str, Any] = {"__name__": "__weaver_payload__", "__file__": filename}
        with installing(installation):
            exec(code, namespace)
        return {"object": action.resource_node_id}
