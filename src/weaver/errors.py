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
    """Raised when host configuration is invalid."""


class IdentityError(WeaverError):
    """Raised when a target, item or repository identity is malformed."""


class MetadataError(WeaverError):
    """Raised when an SES document's metadata is missing, malformed or contradictory."""
