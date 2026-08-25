"""Shared build-environment wiring for the transport-neutral build tests.

Both the environment fixtures (in ``conftest``) and the test bodies draw from
here, so a fixture path is defined exactly once. This is a plain helper module
(imported like ``sql_support``), not a second conftest.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field, replace
from pathlib import Path
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
    environment's single default. Only a cross-item shortcut needs it, and it needs
    it essentially: a shortcut is one item's name for what another item owns, so
    with both items in one Lakehouse there is nothing for the shortcut to cross.
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

    def substituted(self, root: Path, values: Mapping[str, str]) -> "SesFixture":
        """The same declaration, with ``{{TOKEN}}`` placeholders resolved.

        A physical shortcut names a real Fabric workspace and item, which is this
        tenant's business rather than the repository's. The fixture on disk holds
        placeholders so it stays readable and neutral, and the caller supplies
        the names it resolved.
        """

        copied = self.disposable(root)
        for path in sorted(copied.path.rglob("*")):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:  # a fixture's own binary data
                continue
            rewritten = text
            for token, value in values.items():
                rewritten = rewritten.replace("{{" + token + "}}", value)
            if rewritten != text:
                path.write_text(rewritten, encoding="utf-8")
        remaining = sorted(
            path.relative_to(copied.path).as_posix()
            for path in copied.path.rglob("*")
            if path.is_file() and "{{" in path.read_text(errors="ignore")
        )
        if remaining:
            raise AssertionError(
                f"{copied.name}: unresolved placeholders in {', '.join(remaining)}"
            )
        return copied

    def renamed(self, root: Path, names: Mapping[str, str]) -> "SesFixture":
        """The same declaration, under different logical item names.

        The catalogue is keyed by logical item, so two estates sharing an item
        name describe the same registered objects and building one makes the
        other's rows look rebuilt. An estate that needs an identity of its own
        takes it here rather than by checking the same documents in twice.

        ``names`` maps whole item ids — ``{"Lakehouse/Sales": "Lakehouse/Stock"}``.
        Directories are renamed and every text file is rewritten, because a
        document may name another item: a shortcut says which item it crosses to.
        """

        copied = self.disposable(root)
        for old, new in names.items():
            source = copied.path / old
            if source.is_dir():
                source.rename(copied.path / new)
        for path in sorted(copied.path.rglob("*")):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:  # a fixture's own binary data
                continue
            rewritten = text
            for old, new in names.items():
                rewritten = rewritten.replace(old, new)
            if rewritten != text:
                path.write_text(rewritten, encoding="utf-8")
        return replace(
            copied,
            items=tuple(names.get(item, item) for item in copied.items),
            lakehouse_names={
                names.get(item, item): lakehouse
                for item, lakehouse in copied.lakehouse_names.items()
            },
        )


#: The declaration fixtures a build env can install. One place, so no test
#: hard-codes a path and both transports draw the same source.
BUILD_FIXTURE = SesFixture(_FIXTURES / "build-lakehouse-item", ("Lakehouse/Raw",))
SQL_TABLE_FIXTURE = SesFixture(_FIXTURES / "sql-table-build-item", ("Lakehouse/Sales",))
#: Three documents and nothing else — two Python tables and a Python folder — so
#: an authored-object test builds the smallest thing that has one of each.
AUTHORED_OBJECTS_FIXTURE = SesFixture(
    _FIXTURES / "authored-objects-item", ("Lakehouse/Sales",)
)
MIXED_ESTATE_FIXTURE = SesFixture(_FIXTURES / "mixed-estate-item", ("Lakehouse/Sales",))
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
#: can hold: a Delta table published into a Warehouse through a shortcut, read
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
#: which is exactly what the shortcut fixtures declare; this one does not collide
#: with anything.
#:
#: It carries one of each shape a Lakehouse build has to handle, so a single
#: estate can answer for all of them: a Folder, two Python tables (the second
#: importing the first, so there is a real within-item dependency), and a view
#: over a view.
LAKEHOUSE_JOURNEY_FIXTURE = SesFixture(
    _FIXTURES / "lakehouse-journey", ("Lakehouse/Sales",)
)
#: The journey estate, plus the Warehouse that reports on it. Byte for byte the
#: same Lakehouse as ``LAKEHOUSE_JOURNEY_FIXTURE``, so the Lakehouse claims are
#: the same claims, and what the composition adds is a second physical side: an
#: shortcut publishing a Delta table into the Warehouse, a table materialised from
#: it, a view over that, and a Test that reconciles the two.
#:
#: The composition is what neither half can state alone. Each side is
#: self-consistent while the shortcut between them is stale, so only a claim
#: spanning both catches it — and the endpoint-refresh barrier that keeps them
#: in step exists nowhere else.
#:
#: A Warehouse, so a real workspace is the only place this can run.
CROSS_ITEM_JOURNEY_FIXTURE = SesFixture(
    _FIXTURES / "cross-item-journey", ("Lakehouse/Sales", "Warehouse/Reporting")
)
#: The same estate under its own logical item names, for the desktop journey.
#: Identity rather than content is what it needs: the two journeys drive the
#: same documents from opposite positions, and sharing item names would make
#: each look to the catalogue like a rebuild of the other.
DESKTOP_JOURNEY_NAMES = {
    "Lakehouse/Sales": "Lakehouse/Stock",
    "Warehouse/Reporting": "Warehouse/Analysis",
}
#: A producer and the consumer that shortcuts it, in two Lakehouses — the one
#: thing a single destination cannot express, since a shortcut needs something to
#: point across to.
CROSS_ITEM_SHORTCUT_FIXTURE = SesFixture(
    _FIXTURES / "cross-item-shortcut",
    ("Lakehouse/Raw", "Lakehouse/Curated"),
    lakehouse_names={
        "Lakehouse/Raw": "Producer_LH",
        "Lakehouse/Curated": "Consumer_LH",
    },
)


#: The acceptance estate: a realistic architecture, from the foreign workspace
#: through to a Lakehouse that reads the Warehouse's own output.
#:
#: External → Landing → Curated → Serving → Published. Its physical shortcuts
#: name real Fabric items, so the tree on disk holds ``{{TOKEN}}`` placeholders
#: and a caller resolves them with :meth:`SesFixture.substituted`.
ACCEPTANCE_FIXTURE = SesFixture(
    _FIXTURES / "acceptance",
    (
        "Lakehouse/Landing",
        "Lakehouse/Curated",
        "Warehouse/Serving",
        "Lakehouse/Published",
    ),
)

#: What ``substituted`` resolves in the acceptance estate.
ACCEPTANCE_TOKENS = (
    "EXTERNAL_WORKSPACE",
    "EXTERNAL_LAKEHOUSE",
    "EXTERNAL_WAREHOUSE",
)
