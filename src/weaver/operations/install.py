"""Install an already-built Weaver bundle.

The bundle is the whole deployment intent: the workspace, the physical
destinations, the catalogue, the Environment and the Spark attachment are all
frozen in its manifest. A Session contributes credentials, transport and
reusable resources. It supplies no planning decision and overrides none.
"""

from __future__ import annotations

from ..errors import CommandError
from ..locations import Location
from ..store import FilesystemStore


def install(bundle, *, session=None):
    """Validate and install a frozen bundle without rereading its repository."""

    from ..build_bundle import Installer, load_bundle, materialise_bundle_archive
    from ..build_bundle.execution import execution_workspace
    from ..build_bundle.workflow import ARCHIVE_SUFFIX
    from ..sessions.host import use_or_create_session

    location = bundle if isinstance(bundle, Location) else Location(str(bundle))
    if location.is_url:
        raise CommandError(
            "install needs a local bundle directory or .weaver.zip archive"
        )
    store = FilesystemStore()

    def run(loaded):
        # The bundle is loaded and validated before a Session exists, so a
        # damaged or incompatible bundle never acquires a Fabric resource.
        workspace = execution_workspace(loaded.plan.execution, loaded.plan)
        with use_or_create_session(session, workspace=workspace) as opened:
            with opened.task("Install", loaded.bundle_id) as frame:
                report = Installer(opened).install(loaded)
                frame.failed = not report.succeeded
                return report

    if location.name.endswith(ARCHIVE_SUFFIX):
        with materialise_bundle_archive(location, store=store) as loaded:
            return run(loaded)
    return run(load_bundle(location, store=store))
