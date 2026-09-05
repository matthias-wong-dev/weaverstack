"""Source-only project checking."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..errors import CommandError
from ..locations import Location
from ..store import FilesystemStore


@dataclass(frozen=True)
class CheckResult:
    """The small successful result of checking a project folder."""

    project_folder: str


def check(project_folder=None) -> CheckResult:
    """Parse and validate a project folder without contacting Fabric."""

    from ..build_bundle.workflow import prepare_repository

    location = Location(str(Path.cwd() if project_folder is None else project_folder))
    if location.is_url:
        raise CommandError("check needs a local project folder")
    with prepare_repository(location, source_store=FilesystemStore()):
        pass
    return CheckResult(project_folder=location.value)
