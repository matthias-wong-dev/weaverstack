"""Installed catalogue state for run planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Mapping

from ..catalogue.state import Catalogue
from ..catalogue.tables import BOOKMARK_SENTINEL
from ..declaration.model import WeaverDocumentId, parse_installed_identity
from .result import RunError


@dataclass(frozen=True)
class RunState:
    """The catalogue snapshot handed to a Runner.

    ``bookmarks`` is how far each loadable object has been loaded, read once for
    the whole run and keyed by the identity the Registry uses. Read here rather
    than asked for per node: a run of two hundred objects would otherwise be two
    hundred Warehouse round trips for one table's contents.

    An object with no row has never had a clean load, so it reads as the
    sentinel. There is no third state — a bookmark is a datetime, and "unknown"
    would have to be handled by every reader.
    """

    catalogue: Catalogue
    bookmarks: Mapping[WeaverDocumentId, datetime] = field(default_factory=dict)

    def bookmark(self, identity: WeaverDocumentId) -> datetime:
        """How far ``identity`` has been loaded, or the sentinel."""

        return self.bookmarks.get(identity, BOOKMARK_SENTINEL)

    def to_mapping(self) -> dict:
        return {
            "format_version": 1,
            "catalogue": self.catalogue.to_mapping(),
            "bookmarks": {
                str(identity): at.isoformat()
                for identity, at in sorted(
                    self.bookmarks.items(), key=lambda pair: str(pair[0])
                )
            },
        }

    @classmethod
    def from_mapping(cls, mapping) -> "RunState":
        from .result import RunError

        version = mapping.get("format_version")
        if version != 1:
            raise RunError(
                f"unsupported run state format_version {version!r}; expected 1"
            )
        return cls(
            catalogue=Catalogue.from_mapping(mapping["catalogue"]),
            bookmarks={
                parse_installed_identity(text): _instant(value)
                for text, value in (mapping.get("bookmarks") or {}).items()
            },
        )


def _instant(value) -> datetime:
    """One bookmark read back from a payload, always aware and always UTC."""

    at = datetime.fromisoformat(str(value))
    return at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)


def read_installed_catalogue(*, session, workspace=None):
    """What Weaver knows it installed, read from the catalogue Warehouse.

    The catalogue is Warehouse tables under ``_``, so reading it is T-SQL over
    TDS. The statements go through the Session and the rows are assembled here:
    above this, nothing knows which side of a boundary they came from.
    """

    from ..catalogue.state import read_installed_catalogue as read

    return read(_connection(session, workspace))


def read_installed_bookmarks(*, session, workspace=None):
    """How far each installed object has been loaded, read from the catalogue.

    The companion read to :func:`read_installed_catalogue`, and taken at the same
    moment: a run plans against one description of the estate.
    """

    from ..catalogue.state import read_installed_bookmarks as read

    return read(_connection(session, workspace))


def _connection(session, workspace):
    from ..catalogue.connection import catalogue_connection

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise RunError("a run needs a Workspace with a Weaver catalogue")
    return catalogue_connection(session, workspace)


__all__ = [
    "RunState",
    "read_installed_bookmarks",
    "read_installed_catalogue",
]
