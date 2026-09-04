"""Build the package-owned Weaver catalogue item on its own.

Test infrastructure. A Fabric module that wipes the estate needs `_` back before
its own claim runs, and the ordinary way to get it is a build of the repository
under test. This selects the built-in `Warehouse/_weaver` and nothing else, so a
module with no repository of its own can still stand a catalogue up.

It owns no catalogue DDL and no publication: it names the built-in item that
every build composes in, and the ordinary build path does the rest.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from weaver.build_bundle.models import BuildPlan
from weaver.build_bundle.report import InstallationReport
from weaver.build_bundle.targets import ItemBinding, ItemBindings, WarehouseBinding
from weaver.build_bundle.workflow import build_item_repository_source
from weaver.catalogue.tables import CATALOGUE_TABLES
from weaver.declaration.model import WeaverItemId
from weaver.locations import Location
from weaver.store import FilesystemStore, Store
from weaver.targets import ItemRef

#: The item every build composes in, and the only one this builds.
BUILTIN = "Warehouse/_weaver"


@dataclass(frozen=True)
class CatalogueBuildResult:
    """What building the catalogue item did."""

    item: str
    catalogue: str
    plan: BuildPlan
    report: InstallationReport

    @property
    def succeeded(self) -> bool:
        return self.report.status == "succeeded"

    @property
    def tables(self) -> tuple[str, ...]:
        """Every `_` table this created, however each one is maintained."""

        return tuple(table.qualified for table in CATALOGUE_TABLES)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "item": self.item,
            "catalogue": self.catalogue,
            "bundle_id": self.plan.bundle_id,
            "status": self.report.status,
            "tables": list(self.tables),
        }


def build_catalogue_item(
    *,
    catalogue: ItemRef,
    workspace,
    store: Store,
    session,
    output: Location | None = None,
) -> CatalogueBuildResult:
    """Build `Warehouse/_weaver` alone, through the ordinary build path.

    An empty source directory is the input, because the built-in item is
    composed into a parsed repository rather than authored into one: there is
    nothing for a caller to supply, and a real repository here would be ignored.
    """

    control = WarehouseBinding(warehouse=catalogue)
    bindings = ItemBindings((ItemBinding(WeaverItemId.parse(BUILTIN), control),))
    with tempfile.TemporaryDirectory(prefix="weaver-catalogue-") as temporary:
        root = Path(temporary) / "repository"
        root.mkdir()
        result = build_item_repository_source(
            Location(root.as_posix()),
            source_store=FilesystemStore(),
            bindings=bindings,
            session=session,
            workspace=workspace,
            catalogue_binding=control,
            output=output,
        )
    return CatalogueBuildResult(
        item=BUILTIN,
        catalogue=catalogue.name,
        plan=result.plan,
        report=result.report,
    )
