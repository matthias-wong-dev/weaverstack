"""Session owns workspace resources and execution capabilities.

One of Weaver's four doers:

.. code-block:: text

    Session     acquire resources and execute physical work
    Builder     what should be installed
    Installer   install that decision
    Runner      what runs next, and what happened

Import the contract from here.
"""

from __future__ import annotations

from .base import ReportingFrame, Session, WorkspaceScope, workspace_context
from .console import ConsoleScope, ConsoleSession
from .host import (
    active_spark,
    inside_fabric_session,
    session_for,
    use_or_create_session,
)
from .notebook import NotebookScope, NotebookSession
from .program import RemoteProgram
from .public import session
from .resources import Resource, ResourceError, ResourceState
from .telemetry import (
    RESOURCES,
    Measure,
    SessionTelemetry,
    TelemetryContext,
    TelemetryEvent,
)
from .testing import RecordedCall, TestSession

__all__ = [
    "ConsoleScope",
    "ConsoleSession",
    "Measure",
    "RESOURCES",
    "NotebookScope",
    "NotebookSession",
    "RemoteProgram",
    "session",
    "ReportingFrame",
    "RecordedCall",
    "Resource",
    "ResourceError",
    "ResourceState",
    "Session",
    "SessionTelemetry",
    "TelemetryContext",
    "TelemetryEvent",
    "TestSession",
    "WorkspaceScope",
    "active_spark",
    "inside_fabric_session",
    "session_for",
    "use_or_create_session",
    "workspace_context",
]
