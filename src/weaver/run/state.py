"""Installed catalogue state for run planning."""

from __future__ import annotations

from dataclasses import dataclass

from ..catalogue.state import Catalogue
from .result import RunError


@dataclass(frozen=True)
class RunState:
    """The catalogue snapshot handed to a Runner."""

    catalogue: Catalogue

    def to_mapping(self) -> dict:
        return {
            "format_version": 1,
            "catalogue": self.catalogue.to_mapping(),
        }

    @classmethod
    def from_mapping(cls, mapping) -> "RunState":
        from .result import RunError

        version = mapping.get("format_version")
        if version != 1:
            raise RunError(
                f"unsupported run state format_version {version!r}; expected 1"
            )
        return cls(catalogue=Catalogue.from_mapping(mapping["catalogue"]))


def read_installed_catalogue(*, session, workspace=None):
    """What Weaver knows it installed, read from the catalogue Warehouse.

    The catalogue is Warehouse tables under ``_``, so reading it is T-SQL over
    TDS. The statements go through the Session and the rows are assembled here:
    above this, nothing knows which side of a boundary they came from.
    """

    from ..catalogue.connection import catalogue_connection
    from ..catalogue.state import read_installed_catalogue as read

    workspace = workspace if workspace is not None else session.workspace
    if workspace is None or not workspace.catalogue:
        raise RunError("a run needs a Workspace with a Weaver catalogue")

    return read(catalogue_connection(session, workspace))


__all__ = [
    "RunState",
    "read_installed_catalogue",
]
