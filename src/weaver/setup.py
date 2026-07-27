"""Bootstrapping the Weaver Lakehouse — Weaver installing its own control plane.

Setup materialises the package-owned catalogue item inside the workspace source
tree and builds it through the *ordinary* planner and installer. There is
deliberately no second "create the control tables" path: if the catalogue needed
privileged machinery to exist, the claim that a catalogue table is an ordinary
Weaver object would be false, and every later assumption resting on that claim
would be resting on nothing.

The bootstrap looks circular and is not. One bundle does the whole of it, because
the barriers already order it correctly:

.. code-block:: text

    sequence 20    create schema `_`
    sequence 40    create the ten catalogue tables
    sequence 9000  describe them in their own dictionaries
    sequence 9010  record the installation
    sequence 9020  certify them in their own registry

The catalogue's own DML runs after the tables it writes to exist, so no special
first-run mode is needed and generation reads nothing — the statements are
rendered from the projection and are correct against an absent catalogue as much
as a populated one.

**Setup never prunes.** The Weaver Lakehouse belongs to the installation, not to
the built-in item — a reconciling build would treat every schema that item does
not declare as an orphan, including anything a user put there. So prune is off,
and setup only ever adds.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .build_bundle.bundle import BuildBundle, load_bundle
from .build_bundle.installer import InstallationEnvironment, install_bundle
from .build_bundle.planner import generate_item_build_bundle
from .build_bundle.report import InstallationReport
from .build_bundle.targets import ItemBinding, ItemBindings, LakehouseBinding
from .catalogue.builtin import materialise_builtin_item
from .catalogue.tables import CATALOGUE_TABLES
from .locations import Location
from .resolution import resolver_for
from .store import Store
from .declaration import read_weaver_repository
from .declaration.model import WeaverItemId
from .targets import ItemRef

#: The bundle directory setup writes under the Weaver Lakehouse's build_bundles
#: area. A fixed name, because setup is idempotent and there is no value in
#: accumulating one bundle per run.
BUNDLE_NAME = "weaver-setup"


@dataclass(frozen=True)
class SetupResult:
    """What setup did, in terms a caller can print or assert on."""

    item: str
    weaver_lakehouse: str
    bundle: BuildBundle
    report: InstallationReport
    #: Source-root-relative paths written into ``Files/weaver_items``.
    materialised: tuple[str, ...]

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
            "materialised": list(self.materialised),
        }


def materialise_catalogue_item(
    *, weaver_lakehouse: ItemRef, host, store: Store
) -> tuple[str, ...]:
    """Write the package-owned catalogue item into ``Files/weaver_items``.

    Deterministic and repeatable: the same package always writes the same bytes, so
    an unchanged Weaver produces an unchanged repository signature and therefore an
    unchanged bundle. Only the SES files travel — see
    :func:`weaver.catalogue.builtin.repository_files`.
    """

    resolver = resolver_for(host)
    return materialise_builtin_item(resolver.weaver_items_root, store=store)


def initialise_weaver_lakehouse(
    *,
    weaver_lakehouse: ItemRef,
    host,
    store: Store,
    spark: Any = None,
    output: Location | None = None,
) -> SetupResult:
    """Install Weaver's catalogue into the Weaver Lakehouse, through the normal build.

    Idempotent to re-run in *shape*: the same package produces the same bundle, and
    the catalogue's own reconciliation is a no-op when nothing changed.

    It is **not** yet idempotent in rows. Build still emits
    ``CREATE OR REPLACE TABLE``, so a re-run empties the catalogue tables before
    repopulating the built-in repository's own rows — and any *other* repository's
    rows are lost with them. Dropping only what changed needs the signatures this
    catalogue exists to hold, plus the drop policy that reads them; until then,
    setup is a bootstrap operation rather than a routine one.
    """

    resolver = resolver_for(host)
    materialised = materialise_catalogue_item(
        weaver_lakehouse=weaver_lakehouse, host=host, store=store
    )

    repository = read_weaver_repository(resolver.weaver_items_root, store=store)
    control = LakehouseBinding(lakehouse=weaver_lakehouse)
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings(
            (
                ItemBinding(
                    WeaverItemId.parse("Lakehouse/_weaver"),
                    control,
                ),
            )
        ),
        output=output or resolver.build_bundle(BUNDLE_NAME),
        store=store,
        # Never: the Weaver Lakehouse belongs to the installation, not to this
        # repository, so a reconciling build would treat a user's own schema as an
        # orphan. Setup only adds.
        prune=False,
        catalogue=True,
        control_lakehouse=control,
        resolver=resolver,
        spark=spark,
        host=host,
    )

    report = install_bundle(
        load_bundle(bundle.location, store=store),
        environment=InstallationEnvironment(
            store=store, resolver=resolver, spark=spark, host=host
        ),
    )

    return SetupResult(
        item="Lakehouse/_weaver",
        weaver_lakehouse=weaver_lakehouse.name,
        bundle=bundle,
        report=report,
        materialised=materialised,
    )
