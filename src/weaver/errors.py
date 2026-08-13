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
    """Raised when a Weaver document's metadata is missing, malformed or contradictory."""


class LoadError(WeaverError):
    """Raised when an object cannot be executed or its context is unavailable.

    Three optional pieces of context, each present only where the failure knew
    it:

    ``result``
        the load's counts when one was under way, so a caller need not query the
        reject table to find out how many rows were refused.
    ``report``
        the run as far as it got, when orchestration raised: which nodes
        succeeded, failed, or never started.
    ``task_log``
        where that run's durable evidence was written.
    """

    def __init__(
        self,
        message: str,
        *,
        result: object | None = None,
        report: object | None = None,
        task_log: str | None = None,
    ) -> None:
        super().__init__(message)
        self.result = result
        self.report = report
        self.task_log = task_log


class ValidationError(WeaverError):
    """Raised when a Test or Assumption cannot be evaluated.

    Distinct from the validation *failing*: a Test that found discrepancies did
    its job and reports rows. This is the other outcome — a key that does not
    identify rows, two sides that cannot be compared, a missing primitive.

    ``result`` carries the validation's own failed-to-run result where there is
    one, so a reader need not ask the estate what the counts were.
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


class RuntimeScopeError(WeaverError):
    """Raised when this interpreter holds no runtime scope under a given name."""
