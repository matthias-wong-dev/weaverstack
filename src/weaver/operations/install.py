"""Install an already-built Weaver bundle."""

from __future__ import annotations

from pathlib import Path

from ..errors import CommandError
from ..locations import Location
from ..store import FilesystemStore
from .workspace import operation_workspace


def install(
    bundle,
    *,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    session=None,
):
    """Validate and install a frozen bundle without rereading its repository."""

    from ..build_bundle import Installer, load_bundle, materialise_bundle_archive
    from ..build_bundle.workflow import ARCHIVE_SUFFIX
    from ..declaration.model import LAKEHOUSE
    from ..sessions.host import use_or_create_session

    location = bundle if isinstance(bundle, Location) else Location(str(bundle))
    if location.is_url:
        raise CommandError(
            "install needs a local bundle directory or .weaver.zip archive"
        )
    store = FilesystemStore()
    resolved_workspace = operation_workspace(
        "install",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
        needs_catalogue=False,
    )

    def run(loaded):
        lakehouses = tuple(
            target.name
            for target in loaded.plan.targets
            if target.kind == LAKEHOUSE.lower()
        )
        with use_or_create_session(session, workspace=resolved_workspace) as opened:
            opened.offer_spark_home(lakehouses)
            with opened.task("Install", loaded.bundle_id):
                return Installer(opened, workspace=resolved_workspace).install(loaded)

    if location.name.endswith(ARCHIVE_SUFFIX):
        with materialise_bundle_archive(location, store=store) as loaded:
            return run(loaded)
    return run(load_bundle(location, store=store))
