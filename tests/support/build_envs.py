"""Shared build-environment wiring for the transport-neutral build tests.

Both the environment fixtures (in ``conftest``) and the test bodies draw from
here, so a fixture path or the local/Fabric parametrisation is defined exactly
once. This is a plain helper module (imported like ``sql_support``), not a
second conftest.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
import shutil
from typing import Mapping

_FIXTURES = Path(__file__).parent.parent / "fixtures"

#: What never belongs in a copied estate. Bytecode caches are the working tree's,
#: not the fixture's, and copying them would carry one run's compilation into the
#: next test's supposedly pristine estate.
_NOT_COPIED = shutil.ignore_patterns("__pycache__", "*.pyc")


@dataclass(frozen=True)
class SesFixture:
    """One declaration a build env installs, and the items it binds.

    The items are part of the fixture because binding *is* the build input: an
    environment cannot derive them, and which items a fixture leaves unbound is
    sometimes the whole subject — the mixed estate binds only its Lakehouse item
    so that the Warehouse leaves must be omitted.

    ``lakehouse_names`` gives a named item its own Lakehouse instead of the
    environment's single default. Only a cross-item alias needs it, and it needs
    it essentially: an alias is one item's name for what another item owns, so
    with both items in one Lakehouse there is nothing for the alias to cross.
    """

    path: Path
    items: tuple[str, ...]
    lakehouse_names: Mapping[str, str] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def extra_lakehouses(self) -> tuple[str, ...]:
        """Lakehouse names this fixture needs beyond the environment's default."""

        return tuple(sorted(set(self.lakehouse_names.values())))

    def disposable(self, root: Path) -> "SesFixture":
        """The same fixture, over a copy of its tree under ``root``.

        A checked-in fixture is repository source, and a test that edits one is
        editing the repository — so the next test reads an estate the last one
        left behind, and every later run is working from a checkout it quietly
        modified. A test that needs to edit an estate takes a copy and edits
        that.

        Copied rather than restored afterwards: a restore is only as good as the
        teardown that runs it, and the run that fails before teardown is exactly
        the run whose fixture damage matters most.
        """

        destination = root / self.path.name
        if not destination.exists():
            shutil.copytree(self.path, destination, ignore=_NOT_COPIED)
        return replace(self, path=destination)


#: The declaration fixtures a build env can install. One place, so no test
#: hard-codes a path and both transports draw the same source.
BUILD_FIXTURE = SesFixture(_FIXTURES / "build-lakehouse-item", ("Lakehouse/Raw",))
SQL_TABLE_FIXTURE = SesFixture(
    _FIXTURES / "sql-table-build-item", ("Lakehouse/Sales",)
)
#: Three documents and nothing else — two Python tables and a Python folder — so
#: an authored-object test builds the smallest thing that has one of each.
AUTHORED_OBJECTS_FIXTURE = SesFixture(
    _FIXTURES / "authored-objects-item", ("Lakehouse/Sales",)
)
MIXED_ESTATE_FIXTURE = SesFixture(
    _FIXTURES / "mixed-estate-item", ("Lakehouse/Sales",)
)
WAREHOUSE_ESTATE_FIXTURE = SesFixture(
    _FIXTURES / "warehouse-estate-item", ("Warehouse/Reporting",)
)
#: The smallest estate a load run can be orchestrated over: a Folder that
#: produces files, the Python table that reads them, and the Spark SQL table that
#: reads *that*. Three objects, three dispatch kinds, one dependency chain — so a
#: failure names the orchestration layer rather than an unrelated transition.
#:
#: Every object really loads, which is what separates it from the other Lakehouse
#: fixtures: several of them raise from ``read()`` on purpose, to prove a build
#: never calls one.
LOAD_ORCHESTRATION_FIXTURE = SesFixture(
    _FIXTURES / "load-orchestration", ("Lakehouse/Sales",)
)
#: The canonical *physical* load scenario, and the one no single-target estate
#: can hold: a Delta table published into a Warehouse through an alias, read
#: there across a SQL analytics endpoint, and consumed by a Warehouse table with
#: a generated load procedure of its own. That crossing is where the
#: endpoint-refresh barrier lives.
#:
#: Its logical names are its own. The catalogue is keyed by logical item, so an
#: item name shared with another module would describe the same registered
#: objects and make one estate's rows look like the other's.
LOAD_ORCHESTRATION_WAREHOUSE_FIXTURE = SesFixture(
    _FIXTURES / "load-orchestration-warehouse",
    ("Lakehouse/Producer", "Warehouse/Consumer"),
)
#: The one Lakehouse estate a journey drives, and deliberately the only Fabric
#: fixture that declares ``Lakehouse/Sales``.
#:
#: Its logical name matters as much as its content. The catalogue is keyed by
#: logical item — the physical target is never identity — so two fixtures naming
#: the same item describe the *same registered objects*, and building one makes
#: the other's rows look rebuilt. ``BUILD_FIXTURE`` declared ``Lakehouse/Raw``,
#: which is exactly what the alias fixtures declare; this one does not collide
#: with anything.
#:
#: It carries one of each shape a Lakehouse build has to handle, so a single
#: estate can answer for all of them: a Folder, two Python tables (the second
#: importing the first, so there is a real within-item dependency), and a view
#: over a view.
LAKEHOUSE_JOURNEY_FIXTURE = SesFixture(
    _FIXTURES / "lakehouse-journey", ("Lakehouse/Sales",)
)
#: A producer and the consumer that aliases it, in two Lakehouses. The emulator
#: materialises the alias as a filesystem link where Fabric makes a OneLake
#: shortcut, so the same body proves incremental alias behaviour either side.
CROSS_ITEM_ALIAS_FIXTURE = SesFixture(
    _FIXTURES / "cross-item-alias",
    ("Lakehouse/Raw", "Lakehouse/Curated"),
    lakehouse_names={
        "Lakehouse/Raw": "Producer_LH",
        "Lakehouse/Curated": "Consumer_LH",
    },
)
