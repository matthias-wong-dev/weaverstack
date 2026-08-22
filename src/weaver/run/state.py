"""Installed catalogue state for run planning."""

from __future__ import annotations

from dataclasses import dataclass

from ..catalogue.state import Catalogue
from .result import RunError


@dataclass(frozen=True)
class RunState:
    """The catalogue snapshot handed to a Runner.

    One catalogue, carrying whatever the run needs of it: the Installation and
    Registry rows the graph is built from, and the bookmarks its loads read and
    advance. Read once, because a run of two hundred objects would otherwise be
    two hundred round trips for one table's contents.

    Nothing beside it. A bookmark is a catalogue row, so it travels the way every
    other catalogue row travels.
    """

    catalogue: Catalogue

    def to_mapping(self) -> dict:
        return {
            "format_version": 1,
            "catalogue": self.catalogue.to_mapping(),
        }

    @classmethod
    def from_mapping(cls, mapping) -> "RunState":
        version = mapping.get("format_version")
        if version != 1:
            raise RunError(
                f"unsupported run state format_version {version!r}; expected 1"
            )
        return cls(catalogue=Catalogue.from_mapping(mapping["catalogue"]))


def read_installed_catalogue(*, session, workspace=None) -> Catalogue:
    """The installed catalogue a run plans against, and records itself in.

    Readable and writable: one catalogue answers what is installed and how far
    each object has been loaded, and carries the run's own rows back.
    """

    from ..catalogue.state import catalogue_for

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise RunError("a run needs a Workspace with a Weaver catalogue")
    return catalogue_for(session, workspace)


__all__ = ["RunState", "read_installed_catalogue"]
