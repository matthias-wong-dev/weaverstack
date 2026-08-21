"""Source-only repository checking."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..errors import CommandError
from ..locations import Location
from ..store import FilesystemStore


@dataclass(frozen=True)
class CheckResult:
    """The small successful result of checking a repository."""

    source: str


def check(source=None) -> CheckResult:
    """Parse and validate a repository without contacting Fabric."""

    from ..build_bundle.workflow import prepare_repository

    location = Location(str(Path.cwd() if source is None else source))
    if location.is_url:
        raise CommandError("check needs a local repository directory")
    with prepare_repository(location, source_store=FilesystemStore()):
        pass
    return CheckResult(source=location.value)
