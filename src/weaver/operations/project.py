"""Find and read the project folder an operation was given."""

from __future__ import annotations

from functools import cached_property
from pathlib import Path

from ..errors import CommandError
from ..locations import Location
from ..sessions.host import inside_fabric_session
from ..store import FilesystemStore, Store
from ..workspaces import Workspace


def repository_source(source, workspace: Workspace) -> tuple[Location, Store]:
    """Where a project is read from: ``source``, or the current directory."""

    if source is None:
        if not inside_fabric_session(workspace):
            source = "."
        else:
            # Notebook Resources are exposed as the process-local working tree.
            source = Path.cwd()
    location = source if isinstance(source, Location) else Location(str(source))
    if location.value.startswith("abfss://"):
        if not inside_fabric_session(workspace):
            raise CommandError("an abfss source requires a Fabric session")
        from ..fabric.store import FabricStore

        return location, FabricStore()
    return location, FilesystemStore()


class Project:
    """A project folder, read only when a request needs it."""

    def __init__(self, source, workspace: Workspace) -> None:
        self._source = source
        self._workspace = workspace

    @cached_property
    def location(self) -> Location:
        return repository_source(self._source, self._workspace)[0]

    @cached_property
    def repository(self):
        from ..build_bundle.workflow import prepare_repository

        location, store = repository_source(self._source, self._workspace)
        with prepare_repository(
            location,
            source_store=store,
            catalogue_dashboard=self._workspace.catalogue_dashboard is not None,
        ) as prepared:
            return prepared.repository


__all__ = ["Project", "repository_source"]
