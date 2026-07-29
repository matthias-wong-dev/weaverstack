"""Bootstrapping the Weaver Lakehouse — Weaver installing its own control plane.

Initialisation composes the package-owned catalogue item into the parsed
repository in memory and builds it through the *ordinary* planner and installer. There is
deliberately no second "create the control tables" path: if the catalogue needed
privileged machinery to exist, the claim that a catalogue table is an ordinary
Weaver object would be false, and every later assumption resting on that claim
would be resting on nothing.

The bootstrap looks circular and is not. One bundle does the whole of it, because
the barriers already order it correctly:

.. code-block:: text

    sequence 10    create schema `_`
    sequence 40    create the ten catalogue tables
    sequence 9000  describe them in their own dictionaries
    sequence 9010  record the installation
    sequence 9020  certify them in their own registry

The catalogue's own DML runs after the tables it writes to exist, so no special
first-run mode is needed and generation reads nothing — the statements are
rendered from the projection and are correct against an absent catalogue as much
as a populated one.

**Initialisation never prunes.** The Weaver Lakehouse belongs to the installation,
not to the built-in item — a reconciling build would treat every schema that item
does not declare as an orphan, including anything a user put there. So prune is
off, and initialisation only ever adds.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .build_bundle.bundle import BuildBundle, load_bundle
from .build_bundle.installer import InstallationEnvironment, install_bundle
from .build_bundle.planner import generate_item_build_bundle
from .build_bundle.report import InstallationReport
from .build_bundle.targets import ItemBinding, ItemBindings, LakehouseBinding
from .build_bundle.workflow import read_reconciled_catalogue, read_target_inventories
from .catalogue.tables import CATALOGUE_TABLES
from .declaration import parse_item_repository
from .declaration.model import WeaverItemId
from .errors import CommandError
from .locations import Location
from .resolution import resolver_for
from .store import Store
from .targets import ItemRef
from .workspaces import FabricWorkspace, LocalWorkspace

#: The bundle directory initialisation writes under the Weaver Lakehouse's
#: build_bundles area. A fixed name, because initialisation is idempotent and
#: there is no value in accumulating one bundle per run.
INITIALISE_BUNDLE_NAME = "weaver-initialise"


@dataclass(frozen=True)
class InitialiseResult:
    """What initialisation did, in terms a caller can print or assert on."""

    item: str
    weaver_lakehouse: str
    bundle: BuildBundle
    report: InstallationReport

    @property
    def succeeded(self) -> bool:
        return self.report.status == "succeeded"

    @property
    def tables(self) -> tuple[str, ...]:
        return tuple(table.qualified for table in CATALOGUE_TABLES)

    def to_mapping(self) -> dict[str, Any]:
        """A plain structure, for a CLI to serialise. The CLI owns no semantics."""

        return {
            "item": self.item,
            "weaver_lakehouse": self.weaver_lakehouse,
            "bundle_id": self.bundle.plan.bundle_id,
            "status": self.report.status,
            "tables": list(self.tables),
        }


@dataclass(frozen=True)
class PreparedWeaverLakehouse:
    workspace: str
    weaver_lakehouse: str
    created: bool


def prepare_weaver_lakehouse(
    workspace,
    *,
    exists_ok: bool = False,
    store: Store | None = None,
    client=None,
) -> PreparedWeaverLakehouse:
    """Create the configured Weaver Lakehouse and its required Files areas."""

    if not workspace.weaver_lakehouse:
        raise CommandError("initialise requires a configured Weaver Lakehouse")
    name = workspace.weaver_lakehouse
    if isinstance(workspace, LocalWorkspace):
        from .store import LocalStore

        store = store or LocalStore()
        resolver = resolver_for(workspace)
        existed = store.exists(resolver.weaver_lakehouse)
        if existed and not exists_ok:
            raise CommandError(
                f"Weaver Lakehouse {name!r} already exists; pass --exists-ok"
            )
        store.make_directory(resolver.files_root(ItemRef(name)))
        store.make_directory(resolver.tables_root(ItemRef(name)))
        store.make_directory(resolver.weaver_items_root)
        return PreparedWeaverLakehouse(str(workspace.workspace), name, not existed)

    if isinstance(workspace, FabricWorkspace):
        from .fabric.resources import (
            LAKEHOUSE,
            ItemNotFoundError,
            create_lakehouse,
            find_item,
            find_workspace,
        )

        physical_workspace = find_workspace(workspace.workspace, client=client)
        try:
            find_item(physical_workspace, name, item_type=LAKEHOUSE, client=client)
        except ItemNotFoundError:
            create_lakehouse(physical_workspace, name, client=client)
            created = True
        else:
            if not exists_ok:
                raise CommandError(
                    f"Weaver Lakehouse {name!r} already exists; pass --exists-ok"
                )
            created = False
        return PreparedWeaverLakehouse(workspace.workspace, name, created)

    raise CommandError(f"unsupported Workspace type: {type(workspace).__name__}")


def initialise_weaver_lakehouse(
    *,
    weaver_lakehouse: ItemRef,
    workspace,
    store: Store,
    spark: Any = None,
    output: Location | None = None,
) -> InitialiseResult:
    """Install Weaver's catalogue into the Weaver Lakehouse, through the normal build.

    Idempotent to re-run in *shape*: the same package produces the same bundle, and
    the catalogue's own reconciliation is a no-op when nothing changed.

    An unchanged incremental plan emits no physical table work, so re-running
    initialisation preserves existing catalogue rows while its catalogue tail
    reconciles the built-in item.
    """

    resolver = resolver_for(workspace)
    if not store.exists(resolver.weaver_items_root):
        store.make_directory(resolver.weaver_items_root)
    repository = parse_item_repository(resolver.weaver_items_root, store=store)
    control = LakehouseBinding(lakehouse=weaver_lakehouse)
    bindings = ItemBindings(
        (
            ItemBinding(
                WeaverItemId.parse("Lakehouse/_weaver"),
                control,
            ),
        )
    )
    environment = InstallationEnvironment(
        store=store, resolver=resolver, spark=spark, workspace=workspace
    )
    inventories = read_target_inventories(bindings, environment=environment)
    reconciled_catalogue = read_reconciled_catalogue(
        bindings, inventories=inventories, environment=environment
    )
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=output or resolver.build_bundle(INITIALISE_BUNDLE_NAME),
        store=store,
        # Never: the Weaver Lakehouse belongs to the installation, not to this
        # repository, so a reconciling build would treat a user's own schema as an
        # orphan. Initialisation only adds.
        prune=False,
        control_lakehouse=control,
        target_inventories=inventories,
        reconciled_catalogue=reconciled_catalogue,
    )

    report = install_bundle(
        load_bundle(bundle.location, store=store),
        environment=environment,
    )

    return InitialiseResult(
        item="Lakehouse/_weaver",
        weaver_lakehouse=weaver_lakehouse.name,
        bundle=bundle,
        report=report,
    )
