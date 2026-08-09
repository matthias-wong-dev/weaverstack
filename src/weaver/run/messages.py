"""What a run has to say about a node, or about itself.

Typed rather than written out, because a message is read by two audiences with
different needs. A person wants the sentence; a task log, a report renderer and
anything filtering evidence want the *code* — and a code survives rewording in a
way a sentence does not.

.. code-block:: text

    severity    error, warning, info
    code        target_missing, dependency_blocked, ...
    message     the sentence a person reads
    source      who noticed — the primitive, or the orchestration around it

``source`` matters more than it looks. A primitive and the Runner both write
into one stream deliberately: a caller reading a node's findings should not have
to know which layer wrote each one in order to see everything that was wrong
with it.

These live here, with the Runner, because they are runtime vocabulary rather
than load vocabulary. A load, a validation and whatever runtime work comes next
all report through them, and none of them should have to import another
operation's module to say "the target was not there".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

# --- severity -----------------------------------------------------------------

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"

# --- what a run can find ------------------------------------------------------

#: The primitive ran and refused rows.
PRIMITIVE_REJECTS = "primitive_rejects"
#: The primitive ran and reported failure in its own result.
PRIMITIVE_FAILURE = "primitive_failure"
#: Dispatch raised something the primitive did not normalise.
DISPATCH_EXCEPTION = "dispatch_exception"
#: The installed primitive could not be located.
DISPATCH_LOCATION_MISSING = "dispatch_location_missing"
#: The physical target this node runs against is not there.
TARGET_MISSING = "target_missing"
#: A deployed Python module could not be imported, or carries no expected class.
MODULE_IMPORT_FAILURE = "module_import_failure"
#: A primitive returned something that does not report whether it succeeded.
RESULT_CONTRACT_INVALID = "result_contract_invalid"
#: The endpoint refresh could not be performed.
ENDPOINT_REFRESH_FAILURE = "endpoint_refresh_failure"
#: An upstream node failed or could not be resolved, so this one may not run.
DEPENDENCY_BLOCKED = "dependency_blocked"
#: The catalogue's physical binding is missing, ambiguous or malformed.
CATALOGUE_BINDING_INVALID = "catalogue_binding_invalid"
#: The planned graph contains a cycle.
DAG_CYCLE = "dag_cycle"
#: A dependency named in the catalogue could not be resolved to anything.
DEPENDENCY_UNRESOLVED = "dependency_unresolved"
#: A reference Weaver deliberately does not follow — a fully qualified physical
#: read that names something outside the estate's own logical graph.
DEPENDENCY_EXTERNAL = "dependency_external"


@dataclass(frozen=True)
class RunMessage:
    """One finding about one node, or about the run as a whole."""

    severity: str
    code: str
    message: str
    detail: str | None = None
    source: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "detail": self.detail,
            "source": self.source,
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "RunMessage":
        return cls(
            severity=payload["severity"],
            code=payload["code"],
            message=payload["message"],
            detail=payload.get("detail"),
            source=payload.get("source"),
        )


def error(code: str, message: str, **extra: str | None) -> RunMessage:
    return RunMessage(SEVERITY_ERROR, code, message, **extra)


def warning(code: str, message: str, **extra: str | None) -> RunMessage:
    return RunMessage(SEVERITY_WARNING, code, message, **extra)


def info(code: str, message: str, **extra: str | None) -> RunMessage:
    return RunMessage(SEVERITY_INFO, code, message, **extra)


__all__ = [
    "CATALOGUE_BINDING_INVALID",
    "DAG_CYCLE",
    "DEPENDENCY_BLOCKED",
    "DEPENDENCY_EXTERNAL",
    "DEPENDENCY_UNRESOLVED",
    "DISPATCH_EXCEPTION",
    "DISPATCH_LOCATION_MISSING",
    "ENDPOINT_REFRESH_FAILURE",
    "MODULE_IMPORT_FAILURE",
    "PRIMITIVE_FAILURE",
    "PRIMITIVE_REJECTS",
    "RESULT_CONTRACT_INVALID",
    "SEVERITY_ERROR",
    "SEVERITY_INFO",
    "SEVERITY_WARNING",
    "TARGET_MISSING",
    "RunMessage",
    "error",
    "info",
    "warning",
]
