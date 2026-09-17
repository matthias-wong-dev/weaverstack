"""The Weaver error hierarchy."""

from __future__ import annotations

from typing import Any


class WeaverError(Exception):
    """Base class for every Weaver error."""

    executor: str | None = None

    def __init__(self, message: object, *, executor: str | None = None) -> None:
        super().__init__(message)
        if executor is not None:
            self.executor = executor


def reported_message(value: object) -> str | None:
    """Return a provider's nested message without rendering its container."""

    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, BaseException):
        return reported_message(value.args) or str(value).strip() or None
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).casefold() == "message":
                found = reported_message(nested)
                if found:
                    return found
        for nested in value.values():
            found = reported_message(nested)
            if found:
                return found
        return None
    if isinstance(value, (list, tuple)):
        for nested in value:
            found = reported_message(nested)
            if found:
                return found
    return None


class CommandError(WeaverError):
    """Raised when an explicitly requested operation is invalid."""


class ConfigError(WeaverError):
    """Raised when workspace configuration is invalid."""


class IdentityError(WeaverError):
    """Raised when a target, item or repository identity is malformed."""


class MetadataError(WeaverError):
    """Raised when a Weaver document's metadata is missing, malformed or contradictory."""


class LoadError(WeaverError):
    """Raised when an object cannot be executed or its context is unavailable.

    ``result`` carries load counts, ``report`` the partial run and
    ``workflow_id`` the location of durable evidence when each is available.
    """

    def __init__(
        self,
        message: str,
        *,
        result: object | None = None,
        report: object | None = None,
        workflow_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.result = result
        self.report = report
        self.workflow_id = workflow_id


class ValidationError(WeaverError):
    """Raised when a Test or Assumption cannot be evaluated.

    ``result`` carries a failed-to-run result when available. ``report`` carries
    a completed run rejected by strict mode.
    """

    def __init__(
        self,
        message: str,
        *,
        result: object | None = None,
        report: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.result = result
        self.report = report


class DiscoveryError(WeaverError):
    """Raised when a repository or source file breaks a structural rule."""


class GraphError(WeaverError):
    """Raised for cycles or an unorderable dependency graph."""


class CatalogueStateError(WeaverError):
    """Raised when catalogue state cannot form a managed installed graph."""


class BuildError(WeaverError):
    """Raised when a build bundle cannot be planned, written or validated."""


class InstallError(WeaverError):
    """Raised when a build bundle cannot be installed."""


class RuntimeScopeError(WeaverError):
    """Raised when this interpreter holds no runtime scope under a given name."""
