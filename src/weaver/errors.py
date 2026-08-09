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

    Three optional pieces of context, each the answer to a question the message
    alone cannot answer, and each present only where the failure knew it:

    ``result``
        the load's counts when one was under way. A failure is still an outcome
        worth reporting — how many rows were read and how many refused is
        exactly what a caller wants, and losing it to an exception would force a
        second query against the reject table to find out.
    ``report``
        the whole run as far as it got, when orchestration raised. Which nodes
        succeeded, which failed and which never started is what somebody
        restarting the run needs, and it is gone the moment the exception
        replaces it.
    ``task_log``
        where the durable evidence for that run was written, so the answer
        outlives the process that produced it.

    All optional, so the many places that raise this before a load has begun
    stay unchanged.
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

    Deliberately distinct from the validation *failing*. A Test that found
    discrepancies did its job and its evidence is rows, not an exception; this is
    the other outcome — a declared key that does not identify rows, two sides
    that cannot be compared, a missing installed primitive. Collapsing the two
    would report a Test nobody could run as a Test that passed.

    ``result`` carries the validation's own failed-to-run result where there is
    one, for the reason :class:`LoadError` carries a load's: a reader handed
    only an exception has to go and ask the estate what the counts were, and the
    counts are what they came for. Optional, so the many places that raise this
    before anything has run stay unchanged.
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
