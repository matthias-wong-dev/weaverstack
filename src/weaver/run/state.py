"""Installed catalogue state for run planning."""

from __future__ import annotations

from dataclasses import dataclass

from ..catalogue.state import Catalogue
from .result import RunError


@dataclass(frozen=True)
class RunState:
    """The catalogue snapshot read once for a Runner."""

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
                f"Run state version {version!r} is unsupported; this Weaver version "
                "supports version 1. Recreate the run state with this Weaver version."
            )
        return cls(catalogue=Catalogue.from_mapping(mapping["catalogue"]))


def read_installed_catalogue(*, session, workspace=None, tables=None) -> Catalogue:
    """Read the installed catalogue used for planning and recording.

    ``tables`` widens the default read without adding another round trip.
    """

    from ..catalogue.state import READABLE_TABLES, catalogue_for

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise RunError(
            "This run needs a Workspace with a Weaver catalogue. Pass a Workspace "
            "that names its catalogue Warehouse."
        )
    return catalogue_for(
        session, workspace, tables=READABLE_TABLES if tables is None else tuple(tables)
    )


__all__ = ["RunState", "read_installed_catalogue"]
