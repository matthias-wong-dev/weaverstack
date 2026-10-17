"""Deploy the Catalogue Dashboard over one Weaver catalogue.

The Dashboard observes a catalogue and takes no part in its lifecycle. Its model
and Report deploy through an ordinary Power BI Build without a catalogue, with
the source catalogue Warehouse as a configured target, so the catalogue is only
read: no catalogue table is built and no catalogue row is written.
"""

from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from ..errors import CommandError
from ..workspaces import Workspace

CREATED = "created"
UPDATED = "updated"
#: The Dashboard model's measure table and the measure every page ends with.
_FINGERPRINT = ("Dashboard measures", "Renderer")


@dataclass(frozen=True)
class DashboardItem:
    kind: str
    name: str
    #: ``created`` or ``updated``; a dry run says what it would do.
    status: str
    item_id: str | None = None

    def to_mapping(self) -> dict:
        return {
            "kind": self.kind,
            "name": self.name,
            "status": self.status,
            "item_id": self.item_id,
        }


@dataclass(frozen=True)
class CatalogueDashboardResult:
    workspace: str
    #: The source catalogue, as ``Workspace/Warehouse/Name``.
    catalogue: str
    name: str
    items: tuple[DashboardItem, ...] = ()
    #: The catalogue tables the model reads over Direct Lake.
    sources: tuple[str, ...] = ()
    dry_run: bool = False
    status: str = "succeeded"
    errors: tuple[str, ...] = field(default=())

    @property
    def succeeded(self) -> bool:
        return self.status == "succeeded"

    def to_mapping(self) -> dict:
        return {
            "workspace": self.workspace,
            "catalogue": self.catalogue,
            "name": self.name,
            "items": [item.to_mapping() for item in self.items],
            "sources": list(self.sources),
            "dry_run": self.dry_run,
            "status": self.status,
            "errors": list(self.errors),
        }


def deploy_catalogue_dashboard(
    *,
    catalogue: str | Sequence[str] | None = None,
    name: str | None = None,
    workspace: str | None = None,
    workspace_config: str | Path | None = None,
    dry_run: bool = False,
    session=None,
) -> CatalogueDashboardResult:
    """Create or update the Catalogue Dashboard's SemanticModel and Report.

    ``catalogue`` is the source catalogue, ``Workspace/Warehouse/Name`` or
    ``Warehouse/Name`` in the Dashboard's own workspace; it defaults to the
    configured ``catalogue:``. ``name`` is the Fabric item the model and Report
    deploy to; it defaults to ``catalogue_dashboard:``. ``dry_run`` resolves
    the targets and the source and changes nothing.
    """

    from .workspace import operation_workspace

    base = operation_workspace(
        "catalogue-dashboard",
        workspace=workspace,
        workspace_config=workspace_config,
        session=session,
        needs_catalogue=False,
    )
    source_workspace, source = _source(catalogue, base)
    reference = f"{source_workspace}/Warehouse/{source}"
    name = name or base.catalogue_dashboard
    if not name:
        raise CommandError(
            "Name the Catalogue Dashboard: pass --name, or set catalogue_dashboard: "
            "in workspace-config.yml."
        )
    if source_workspace != base.workspace:
        raise CommandError(
            f"The Catalogue Dashboard deploys in the workspace of the catalogue it "
            f"reads, and {reference} is not in {base.workspace}. Pass --workspace "
            f"{source_workspace}."
        )
    _refuse_configured_targets(base, name)
    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=base) as opened:
        with opened.task("Catalogue Dashboard", name):
            with opened.step("Check the source catalogue", reference):
                sources = _require_source(opened, base, source, reference)
            with opened.step("Resolve the Dashboard's items", name):
                model, report = _existing_targets(opened, base, name)
            if dry_run:
                return CatalogueDashboardResult(
                    workspace=base.workspace,
                    catalogue=reference,
                    name=name,
                    items=_outcomes(name, model, report, planned=True),
                    sources=sources,
                    dry_run=True,
                )
            result = _deploy(opened, base, name, source)
            if not result.succeeded:
                return CatalogueDashboardResult(
                    workspace=base.workspace,
                    catalogue=reference,
                    name=name,
                    sources=sources,
                    status=result.status,
                    errors=tuple(
                        error.message or error.action_id for error in result.errors
                    ),
                )
            with opened.step("Refresh the Dashboard model", name):
                # Direct Lake frames the catalogue tables on refresh.
                opened.semantic_model(name, workspace=base).refresh()
            deployed = (
                _find(opened, base, name, "SemanticModel"),
                _find(opened, base, name, "Report"),
            )
    return CatalogueDashboardResult(
        workspace=base.workspace,
        catalogue=reference,
        name=name,
        items=tuple(
            DashboardItem(
                item.kind,
                name,
                UPDATED if before is not None else CREATED,
                after.id,
            )
            for item, before, after in zip(
                _outcomes(name, None, None, planned=False),
                (model, report),
                deployed,
            )
        ),
        sources=sources,
    )


def _source(catalogue, base: Workspace) -> tuple[str, str]:
    """The source catalogue's workspace and Warehouse name."""

    if not isinstance(catalogue, str) and catalogue is not None:
        values = list(catalogue)
        if len(values) > 1:
            raise CommandError(
                "The Catalogue Dashboard reads one catalogue. Pass --catalogue once."
            )
        catalogue = values[0] if values else None
    text = catalogue or base.catalogue
    if not text:
        raise CommandError(
            "Name the catalogue the Dashboard reads: pass --catalogue "
            "Workspace/Warehouse/Name, or set catalogue: in workspace-config.yml."
        )
    parts = text.split("/")
    if len(parts) == 3 and parts[1] == "Warehouse" and parts[0] and parts[2]:
        return parts[0], parts[2]
    if len(parts) == 2 and parts[0] == "Warehouse" and parts[1]:
        return base.workspace, parts[1]
    raise CommandError(
        f"--catalogue must be Workspace/Warehouse/Name or Warehouse/Name, got {text!r}."
    )


def _refuse_configured_targets(base: Workspace, name: str) -> None:
    for item, declaration in base.targets.items():
        if (
            item.item_type in {"SemanticModel", "Report"}
            and declaration.physical.casefold() == name.casefold()
        ):
            raise CommandError(
                f"targets: deploys {item} to {declaration.physical}. Choose another "
                "name for the Catalogue Dashboard."
            )


def required_tables() -> tuple[str, ...]:
    """The catalogue tables the Dashboard model reads."""

    from ..catalogue_dashboard import DASHBOARD
    from ..fragments import DASHBOARD as FRAGMENT
    from ..fragments import fragment_files

    model = fragment_files(FRAGMENT)[f"{DASHBOARD}.tmdl"].decode("utf-8")
    return tuple(
        sorted(
            set(
                re.findall(
                    r"annotation 'Weaver\.Source' = Warehouse/_weaver/_\.(\w+)", model
                )
            )
        )
    )


def _require_source(opened, base, source: str, reference: str) -> tuple[str, ...]:
    from ..fabric.resources import ItemNotFoundError
    from ..targets import ItemRef, WarehouseTarget

    try:
        opened.resolve_item(source, item_type="Warehouse", workspace=base)
    except ItemNotFoundError as exc:
        raise CommandError(f"{reference} was not found. {exc}") from exc
    rows = opened.query_tsql(
        "select name from sys.tables where schema_name(schema_id) = N'_'",
        target=WarehouseTarget(warehouse=ItemRef(source)),
        workspace=base,
    )
    present = {str(row.get("name")) for row in rows}
    wanted = required_tables()
    missing = [table for table in wanted if table not in present]
    if len(missing) == len(wanted):
        raise CommandError(
            f"{reference} holds no Weaver catalogue. Build a project into it first."
        )
    if missing:
        listed = ", ".join(f"_.{table}" for table in missing)
        raise CommandError(
            f"{reference} has no {listed}. Build a project with this version of "
            "Weaver into that catalogue first."
        )
    return tuple(f"_.{table}" for table in wanted)


def _find(opened, base, name: str, kind: str):
    from ..fabric.resources import ItemNotFoundError

    try:
        return opened.resolve_item(name, item_type=kind, workspace=base)
    except ItemNotFoundError:
        return None


def _existing_targets(opened, base, name: str):
    """The model and Report already named ``name``, refusing any that is not ours."""

    model = _find(opened, base, name, "SemanticModel")
    report = _find(opened, base, name, "Report")
    if model is not None and not _is_dashboard(opened, base, model):
        raise CommandError(
            f"SemanticModel {name} is not a Catalogue Dashboard. Choose another name."
        )
    if report is not None:
        bound = (
            opened.report_item(report.name, workspace=base).get_binding() or {}
        ).get("datasetId")
        if model is None or str(bound).casefold() != str(model.id).casefold():
            raise CommandError(
                f"Report {name} does not read the Catalogue Dashboard model. Choose "
                "another name."
            )
    return model, report


def _is_dashboard(opened, base, model) -> bool:
    from urllib.parse import unquote

    from ..semantic_models.definition import decode_parts

    definition = opened.semantic_model(model.name, workspace=base).get_definition(
        format="TMDL"
    )
    table, measure = _FINGERPRINT
    return any(
        unquote(path) == f"definition/tables/{table}.tmdl"
        and re.search(rf"^\s*measure '?{measure}'?\s*=", data.decode("utf-8"), re.M)
        for path, data in decode_parts(definition).items()
    )


def _outcomes(name, model, report, *, planned: bool) -> tuple[DashboardItem, ...]:
    def status(existing):
        return UPDATED if existing is not None else CREATED

    return tuple(
        DashboardItem(
            kind,
            name,
            status(existing),
            getattr(existing, "id", None) if planned else None,
        )
        for kind, existing in (("SemanticModel", model), ("Report", report))
    )


def write_dashboard_project(root: Path) -> None:
    """Write the Dashboard as the ordinary Power BI project it deploys as."""

    from ..catalogue_dashboard import DASHBOARD_PROJECT, dashboard_files

    for relative, data in dashboard_files().items():
        path = root / DASHBOARD_PROJECT / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


def _deploy(opened, base: Workspace, name: str, source: str):
    """Build the Dashboard project, catalogue-free, with the source as a target."""

    import yaml

    from ..catalogue.builtin import BUILTIN_ITEM
    from ..catalogue_dashboard import DASHBOARD_ITEMS
    from . import build as building

    with tempfile.TemporaryDirectory(prefix="weaver-dashboard-") as temporary:
        root = Path(temporary)
        write_dashboard_project(root)
        config = root / "workspace-config.yml"
        config.write_text(
            yaml.safe_dump(
                {
                    "workspace": base.workspace,
                    "targets": {
                        **{str(item): name for item in DASHBOARD_ITEMS},
                        str(BUILTIN_ITEM): source,
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return building.build(
            root,
            items=[str(item) for item in DASHBOARD_ITEMS],
            workspace_config=config,
            session=opened,
        )


__all__ = [
    "CatalogueDashboardResult",
    "DashboardItem",
    "deploy_catalogue_dashboard",
    "write_dashboard_project",
]
