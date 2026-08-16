"""Session — where Weaver is running, and what it already has.

One of Weaver's four doers:

.. code-block:: text

    Session     where and how work reaches physical systems
    Builder     what should be installed
    Installer   install that decision
    Runner      what runs next, and what happened

Import the contract from here; the host implementations decide how each
capability is met.
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
