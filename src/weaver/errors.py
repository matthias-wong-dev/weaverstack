"""The single Weaver error hierarchy.

Every error Weaver raises derives from :class:`WeaverError`, including errors
raised by the CLI adapter. Subclasses are added at the checkpoint that first
raises them rather than declared in advance.
"""

from __future__ import annotations


class WeaverError(Exception):
    """Base class for every Weaver error."""


class CommandError(WeaverError):
    """Raised when an explicitly requested operation is invalid."""


class ConfigError(WeaverError):
    """Raised when workspace configuration is invalid."""


class IdentityError(WeaverError):
    """Raised when a target, item or repository identity is malformed."""


class MetadataError(WeaverError):
    """Raised when an Weaver document document's metadata is missing, malformed or contradictory."""


class LoadError(WeaverError):
    """Raised when an object cannot be executed or its context is unavailable.

    ``result`` carries the load's counts when one was under way, because a
    failure is still an outcome worth reporting: how many rows were read and how
    many refused is exactly what a caller wants, and losing it to an exception
    would force a second query against the reject table to find out.

    It is optional, so the many places that raise this before a load has begun
    stay unchanged.
    """

    def __init__(self, message: str, *, result: object | None = None) -> None:
        super().__init__(message)
        self.result = result


class DiscoveryError(WeaverError):
    """Raised when a repository or source file breaks a structural rule."""


class GraphError(WeaverError):
    """Raised for cycles or an unorderable dependency graph."""


class BuildError(WeaverError):
    """Raised when a build bundle cannot be planned, written or validated."""


class InstallError(WeaverError):
    """Raised when a build bundle cannot be installed."""
