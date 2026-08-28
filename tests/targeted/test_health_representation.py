"""What the installed estate's operational state adds up to.

Pure Python throughout: a `Catalogue` is rows, so these hand-write the rows a
real estate would hold and never open a session, a target or a Warehouse. The
subject is the arithmetic that turns installed state and current status into
Green, Amber and Red.

The graph these read is the same `Catalogue.dag()` load planning reads, so a
freshness claim here is a claim about the estate's own dependency model rather
than about a second one.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from factories import (
    bookmark_row_for,
    dependency_row,
    document_id,
    installation_row,
    load_status_row,
    registry_row,
    shortcut_row,
    target_inventory,
    validation_row,
    validation_status_row,
)
from support.weaver_test import weaver_test

from weaver.catalogue.history import LoadHistory
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    BOOKMARK,
    DEPENDENCY,
    INSTALLATION,
    LOAD_STATUS,
    REGISTRY,
    SHORTCUT,
    TABLE_DICTIONARY,
    TEST_DICTIONARY,
    TEST_STATUS,
)
from weaver.declaration.model import WeaverItemId
from weaver.etl import validation_artefact_id
from weaver.health import (
    AMBER,
    AMBIGUOUS_INSTALLATION,
    CERTIFIED_MISSING,
    FORMAT_VERSION,
    GREEN,
    LOAD_FAILED,
    LOAD_PENDING,
    LOAD_REJECTED,
    LOAD_STALE_ANCESTOR,
    LOAD_STALE_TIME,
    MISSING_VALIDATION_ARTEFACT,
    NOT_INSTALLED,
    RED,
    TEST_FAILED,
    TEST_PENDING,
    TEST_STALE_DEPENDENCY,
    assess,
    worst,
)
from weaver.targets import PhysicalTargetRef

RAW = "Lakehouse/Raw"
CURATED = "Lakehouse/Curated"
REPORTING = "Warehouse/Reporting"

RAW_LH = PhysicalTargetRef("lakehouse", "Raw_LH")
CURATED_LH = PhysicalTargetRef("lakehouse", "Curated_LH")
REPORTING_WH = PhysicalTargetRef("warehouse", "Reporting_WH")

TARGET_FOR = {RAW: "Raw_LH", CURATED: "Curated_LH", REPORTING: "Reporting_WH"}

NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
YESTERDAY = NOW - timedelta(hours=24)


def at(hours: float) -> datetime:
    """An instant, in hours before ``NOW``."""

    return NOW - timedelta(hours=hours)


# --- building an estate out of rows -------------------------------------------


class _Estate:
    """Rows for one estate, gathered per item and handed to a `Catalogue`."""

    def __init__(self) -> None:
        self._rows: dict[WeaverItemId, dict[str, list[dict]]] = {}

    def _tables(self, item: WeaverItemId):
        return self._rows.setdefault(
            item,
            {
                INSTALLATION.name: [installation_row(item, TARGET_FOR[str(item)])],
                REGISTRY.name: [],
                DEPENDENCY.name: [],
                SHORTCUT.name: [],
                TABLE_DICTIONARY.name: [],
                TEST_DICTIONARY.name: [],
                LOAD_STATUS.name: [],
                TEST_STATUS.name: [],
                BOOKMARK.name: [],
            },
        )

    def table(
        self,
        identity: str,
        *,
        object_type: str = "table",
        loadable: bool = True,
        loaded=None,
        result: str = "succeeded",
        moved=None,
        declared: bool = True,
        is_static: bool = False,
    ) -> "_Estate":
        """One installed object, with the state its most recent load left.

        ``loaded`` is when that load settled and ``moved`` is where it left the
        bookmark, which is what says the object's data changed.
        """

        parsed = document_id(identity)
        tables = self._tables(parsed.item)
        tables[REGISTRY.name].append(registry_row(parsed, object_type=object_type))
        if declared and object_type in ("table", "view"):
            tables[TABLE_DICTIONARY.name].append(
                _dictionary_row(parsed, object_type=object_type, is_static=is_static)
            )
        if loadable:
            tables[REGISTRY.name].append(_load_artefact(parsed))
        if loaded is not None or result != "succeeded":
            tables[LOAD_STATUS.name].append(
                load_status_row(parsed, result=result, completed_at=loaded)
            )
        if moved is not None:
            tables[BOOKMARK.name].append(bookmark_row_for(parsed, moved))
        return self

    def view(self, identity: str, **how) -> "_Estate":
        return self.table(identity, object_type="view", loadable=False, **how)

    def declared_not_installed(self, identity: str) -> "_Estate":
        """A TableDictionary row Registry does not certify."""

        parsed = document_id(identity)
        self._tables(parsed.item)[TABLE_DICTIONARY.name].append(_dictionary_row(parsed))
        return self

    def reads(self, consumer: str, reference: str, **referenced) -> "_Estate":
        parsed = document_id(consumer)
        self._tables(parsed.item)[DEPENDENCY.name].append(
            dependency_row(parsed, reference, **referenced)
        )
        return self

    def shortcut(self, destination: str, source: str) -> "_Estate":
        parsed = document_id(destination)
        tables = self._tables(parsed.item)
        tables[SHORTCUT.name].append(shortcut_row(parsed, source))
        tables[REGISTRY.name].append(
            registry_row(
                parsed,
                object_type="view" if parsed.item.item_type == "Warehouse" else "table",
                object_role="shortcut",
            )
        )
        return self

    def validation(
        self,
        logical: str,
        *,
        kind: str = "Test",
        installed: bool = True,
        result: str | None = None,
        ran=None,
        failure_count=None,
    ) -> "_Estate":
        parsed = document_id(logical)
        tables = self._tables(parsed.item)
        tables[TEST_DICTIONARY.name].append(
            validation_row(parsed, test_type=kind.casefold())
        )
        if installed:
            tables[REGISTRY.name].append(_validation_artefact(parsed, kind))
        if result is not None:
            tables[TEST_STATUS.name].append(
                validation_status_row(
                    parsed,
                    result=result,
                    test_type=kind.casefold(),
                    completed_at=ran,
                    failure_count=failure_count,
                )
            )
        return self

    def catalogue(self, *, load_history=None) -> Catalogue:
        return Catalogue(
            rows={
                item: {name: tuple(rows) for name, rows in tables.items()}
                for item, tables in self._rows.items()
            },
            load_history=load_history,
        )

    def report(self, **asked):
        return assess(
            self.catalogue(load_history=asked.pop("load_history", None)),
            as_of=asked.pop("as_of", YESTERDAY),
            generated_at=asked.pop("generated_at", NOW),
            **asked,
        )


def _dictionary_row(
    identity, *, object_type: str = "table", is_static: bool = False
) -> dict:
    from weaver.catalogue.claims import catalogue_schema

    return {
        "item_type": identity.item.item_type,
        "item_name": identity.item.item_name,
        "schema_name": catalogue_schema(identity),
        "object_name": identity.object_id.object,
        "object_type": object_type,
        "description": "A declared object.",
        "description_reference": None,
        "lineage": None,
        "lineage_reference": None,
        "primary_key": None,
        "not_null_columns": None,
        "identity_column": None,
        "comparison_columns": None,
        "is_incremental": None,
        "is_static": is_static,
        "prohibit_rebuild": None,
        "signature": "declaration",
    }


def _load_artefact(identity) -> dict:
    from weaver.declaration.model import WeaverDocumentId

    if identity.item.item_type == "Warehouse":
        return registry_row(
            WeaverDocumentId.parse(
                f"{identity.item}/procedure:_/Load {identity.object_id.qualified}"
            ),
            object_type="stored_procedure",
            object_role="load",
        )
    schema, name = identity.object_id.schema, identity.object_id.object
    return registry_row(
        WeaverDocumentId.parse(f"{identity.item}/file:_/Load/{schema}__{name}.py"),
        object_type="file",
        object_role="load",
    )


def _validation_artefact(logical, kind: str) -> dict:
    artefact = validation_artefact_id(logical.item, kind, logical.object_id)
    return registry_row(
        artefact,
        object_type="file" if artefact.shape == "file" else "stored_procedure",
        object_role=kind.casefold(),
    )


def codes(section) -> tuple[str, ...]:
    return tuple(finding.code for finding in section.findings)


def about(section, code: str):
    return tuple(finding for finding in section.findings if finding.code == code)


# --- the vocabulary -----------------------------------------------------------


@weaver_test()
def test_the_worst_of_nothing_is_green():
    assert worst(()) == GREEN


@weaver_test()
def test_amber_beats_green_and_red_beats_amber():
    assert worst((GREEN, AMBER)) == AMBER
    assert worst((AMBER, RED, GREEN)) == RED


@weaver_test()
def test_overall_status_is_the_worst_section():
    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
        .validation(f"{RAW}/Sales.Integrity", result="failed", ran=at(1))
        .report()
    )

    assert report.load.status == GREEN
    assert report.tests.status == RED
    assert report.build.status == GREEN
    assert report.status == RED
    assert not report.is_healthy


@weaver_test()
def test_a_green_estate_is_green_everywhere():
    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
        .validation(f"{RAW}/Sales.Integrity", result="succeeded", ran=at(0.5))
        .reads(f"{RAW}/Sales.Integrity", "Sales.Order")
        .report()
    )

    assert report.status == GREEN
    assert report.is_healthy
    assert report.findings == ()


# --- load health --------------------------------------------------------------


@weaver_test()
def test_a_recent_success_is_green():
    report = _Estate().table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1)).report()

    assert report.load.status == GREEN
    assert report.load.counts == {"succeeded": 1}


@weaver_test()
def test_a_success_older_than_as_of_is_amber():
    report = _Estate().table(f"{RAW}/Sales.Order", loaded=at(30), moved=at(30)).report()

    assert report.load.status == AMBER
    assert codes(report.load) == (LOAD_STALE_TIME,)


@weaver_test()
def test_a_loadable_with_no_status_is_amber_and_pending():
    """A freshly rebuilt loadable has no status row."""

    report = _Estate().table(f"{RAW}/Sales.Order").report()

    assert report.load.status == AMBER
    assert codes(report.load) == (LOAD_PENDING,)
    assert about(report.load, LOAD_PENDING)[0].status == "pending"


@weaver_test()
def test_a_rejecting_load_is_amber():
    report = (
        _Estate().table(f"{RAW}/Sales.Order", loaded=at(1), result="rejected").report()
    )

    assert report.load.status == AMBER
    assert codes(report.load) == (LOAD_REJECTED,)


@pytest.mark.parametrize("result", ["failed", "error", "blocked"])
@weaver_test()
def test_a_load_that_did_not_succeed_is_red(result):
    report = _Estate().table(f"{RAW}/Sales.Order", loaded=at(1), result=result).report()

    assert report.load.status == RED
    assert codes(report.load) == (LOAD_FAILED,)
    assert about(report.load, LOAD_FAILED)[0].status == result


@weaver_test()
def test_a_view_is_not_a_load_subject():
    """A View owns no load work, so no status row is expected of it."""

    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
        .view(f"{RAW}/Sales.Live")
        .reads(f"{RAW}/Sales.Live", "Sales.Order")
        .report()
    )

    assert report.load.subjects == 1
    assert report.load.status == GREEN


@weaver_test()
def test_a_generated_runtime_artefact_is_not_a_load_subject():
    report = _Estate().table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1)).report()

    assert [finding.object_id for finding in report.findings] == []
    assert report.load.subjects == 1


# --- freshness against the graph ----------------------------------------------


@weaver_test()
def test_a_node_loaded_before_its_ancestor_is_stale():
    """A loaded 08:00, B loaded 07:00, A -> B."""

    report = (
        _Estate()
        .table(f"{RAW}/Sales.A", loaded=at(4), moved=at(4))
        .table(f"{RAW}/Sales.B", loaded=at(5), moved=at(5))
        .reads(f"{RAW}/Sales.B", "Sales.A")
        .report()
    )

    assert report.load.status == AMBER
    stale = about(report.load, LOAD_STALE_ANCESTOR)
    assert [finding.object_id for finding in stale] == [f"{RAW}/Sales.B"]
    assert f"{RAW}/Sales.A" in stale[0].message


@weaver_test()
def test_staleness_reaches_through_a_view():
    """A -> View -> B, where the View owns no load of its own."""

    report = (
        _Estate()
        .table(f"{RAW}/Sales.A", loaded=at(4), moved=at(4))
        .view(f"{RAW}/Sales.Live")
        .table(f"{RAW}/Sales.B", loaded=at(5), moved=at(5))
        .reads(f"{RAW}/Sales.Live", "Sales.A")
        .reads(f"{RAW}/Sales.B", "Sales.Live")
        .report()
    )

    assert codes(report.load) == (LOAD_STALE_ANCESTOR,)


@weaver_test()
def test_staleness_is_transitive():
    """A -> B -> C, where only A moved."""

    report = (
        _Estate()
        .table(f"{RAW}/Sales.A", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Sales.B", loaded=at(5), moved=at(5))
        .table(f"{RAW}/Sales.C", loaded=at(6), moved=at(6))
        .reads(f"{RAW}/Sales.B", "Sales.A")
        .reads(f"{RAW}/Sales.C", "Sales.B")
        .report()
    )

    assert sorted(
        finding.object_id for finding in about(report.load, LOAD_STALE_ANCESTOR)
    ) == [f"{RAW}/Sales.B", f"{RAW}/Sales.C"]


@weaver_test()
def test_staleness_crosses_a_logical_shortcut():
    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
        .shortcut(f"{REPORTING}/Sales.Order", f"{RAW}/Sales.Order")
        .table(f"{REPORTING}/Sales.Summary", loaded=at(5), moved=at(5))
        .reads(f"{REPORTING}/Sales.Summary", "Sales.Order")
        .report()
    )

    stale = about(report.load, LOAD_STALE_ANCESTOR)
    assert [finding.object_id for finding in stale] == [f"{REPORTING}/Sales.Summary"]


@weaver_test()
def test_a_blocked_ancestor_is_not_newer_data():
    """A blocked node did nothing, so nothing downstream fell behind it."""

    report = (
        _Estate()
        .table(f"{RAW}/Sales.A", loaded=at(1), result="blocked")
        .table(f"{RAW}/Sales.B", loaded=at(5), moved=at(5))
        .reads(f"{RAW}/Sales.B", "Sales.A")
        .report()
    )

    assert about(report.load, LOAD_STALE_ANCESTOR) == ()


@weaver_test()
def test_a_static_skip_does_not_make_its_descendants_stale():
    """A Static object skipped keeps the bookmark it had, so its data stood still.

    Its ``_.LoadStatus`` still says the load succeeded a moment ago, which is why
    freshness reads the bookmark.
    """

    report = (
        _Estate()
        .table(f"{RAW}/Sales.Reference", loaded=at(0.5), moved=at(40))
        .table(f"{RAW}/Sales.B", loaded=at(5), moved=at(5))
        .reads(f"{RAW}/Sales.B", "Sales.Reference")
        .report()
    )

    assert about(report.load, LOAD_STALE_ANCESTOR) == ()


@weaver_test()
def test_target_filtering_reports_the_selection_and_reads_the_rest():
    """Upstream ancestry is inspected; nothing about it is reported."""

    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1), result="failed")
        .shortcut(f"{REPORTING}/Sales.Order", f"{RAW}/Sales.Order")
        .table(f"{REPORTING}/Sales.Summary", loaded=at(5), moved=at(5))
        .reads(f"{REPORTING}/Sales.Summary", "Sales.Order")
        .report(targets=(REPORTING_WH,))
    )

    assert report.targets == ("Warehouse/Reporting_WH",)
    assert report.load.subjects == 1
    assert codes(report.load) == (LOAD_STALE_ANCESTOR,)


# --- a Static object is loaded once -------------------------------------------
#
# Static is for a small reference dataset that is loaded once, so
# age says nothing about it and neither does an ancestor that moved. Its own
# bookmark still counts against everything downstream: a genuine reload of a
# reference table is what puts its consumers behind.


@weaver_test()
def test_a_static_object_loaded_months_ago_stays_green():
    report = (
        _Estate()
        .table(f"{RAW}/Ref.Country", loaded=at(3000), moved=at(3000), is_static=True)
        .report()
    )

    assert report.load.status == GREEN
    assert report.load.findings == ()


@weaver_test()
def test_a_static_object_is_not_made_stale_by_an_ancestor():
    report = (
        _Estate()
        .table(f"{RAW}/Ref.Source", loaded=at(1), moved=at(1))
        .table(f"{RAW}/Ref.Country", loaded=at(3000), moved=at(3000), is_static=True)
        .reads(f"{RAW}/Ref.Country", "Ref.Source")
        .report()
    )

    assert about(report.load, LOAD_STALE_ANCESTOR) == ()
    assert about(report.load, LOAD_STALE_TIME) == ()


@weaver_test()
def test_a_static_object_that_has_never_loaded_is_amber():
    report = _Estate().table(f"{RAW}/Ref.Country", is_static=True).report()

    assert report.load.status == AMBER
    assert codes(report.load) == (LOAD_PENDING,)


@pytest.mark.parametrize("result", ["failed", "error", "blocked"])
@weaver_test()
def test_a_static_object_that_did_not_load_is_red(result):
    report = (
        _Estate()
        .table(f"{RAW}/Ref.Country", loaded=at(1), result=result, is_static=True)
        .report()
    )

    assert report.load.status == RED
    assert codes(report.load) == (LOAD_FAILED,)


@weaver_test()
def test_a_static_object_that_rejected_rows_is_amber():
    report = (
        _Estate()
        .table(f"{RAW}/Ref.Country", loaded=at(1), result="rejected", is_static=True)
        .report()
    )

    assert report.load.status == AMBER
    assert codes(report.load) == (LOAD_REJECTED,)


@weaver_test()
def test_a_static_bookmark_is_lineage_for_what_reads_it():
    """Ref.Country loaded in January, Fact.Customer in February: Green."""

    report = (
        _Estate()
        .table(f"{RAW}/Ref.Country", loaded=at(1400), moved=at(1400), is_static=True)
        .table(f"{RAW}/Fact.Customer", loaded=at(700), moved=at(700))
        .reads(f"{RAW}/Fact.Customer", "Ref.Country")
        .report(as_of=at(2000))
    )

    assert report.load.status == GREEN


@weaver_test()
def test_a_genuine_static_reload_puts_its_consumers_behind():
    """Ref.Country reloaded in August, Fact.Customer still at February."""

    report = (
        _Estate()
        .table(f"{RAW}/Ref.Country", loaded=at(1), moved=at(1), is_static=True)
        .table(f"{RAW}/Fact.Customer", loaded=at(700), moved=at(700))
        .reads(f"{RAW}/Fact.Customer", "Ref.Country")
        .report(as_of=at(2000))
    )

    assert report.load.status == AMBER
    stale = about(report.load, LOAD_STALE_ANCESTOR)
    assert [finding.object_id for finding in stale] == [f"{RAW}/Fact.Customer"]
    assert f"{RAW}/Ref.Country" in stale[0].message


@weaver_test()
def test_a_static_reload_makes_a_validation_over_it_stale():
    report = (
        _Estate()
        .table(f"{RAW}/Ref.Country", loaded=at(1), moved=at(1), is_static=True)
        .validation(f"{RAW}/Ref.Integrity", result="succeeded", ran=at(700))
        .reads(f"{RAW}/Ref.Integrity", "Ref.Country")
        .report()
    )

    assert codes(report.tests) == (TEST_STALE_DEPENDENCY,)


# --- validation health --------------------------------------------------------


@weaver_test()
def test_a_passing_validation_with_nothing_loaded_since_is_green():
    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(4), moved=at(4))
        .validation(f"{RAW}/Sales.Integrity", result="succeeded", ran=at(3))
        .reads(f"{RAW}/Sales.Integrity", "Sales.Order")
        .report()
    )

    assert report.tests.status == GREEN
    assert report.tests.counts == {"succeeded": 1}


@weaver_test()
def test_a_validation_whose_data_moved_after_it_passed_is_stale():
    """A -> B -> Test, where B was loaded after the Test passed."""

    report = (
        _Estate()
        .table(f"{RAW}/Sales.A", loaded=at(6), moved=at(6))
        .table(f"{RAW}/Sales.B", loaded=at(1), moved=at(1))
        .validation(f"{RAW}/Sales.Integrity", result="succeeded", ran=at(3))
        .reads(f"{RAW}/Sales.B", "Sales.A")
        .reads(f"{RAW}/Sales.Integrity", "Sales.B")
        .report()
    )

    assert report.tests.status == AMBER
    assert codes(report.tests) == (TEST_STALE_DEPENDENCY,)


@weaver_test()
def test_time_alone_does_not_make_a_validation_stale():
    """A validation's freshness is tied to whether the data it reads moved."""

    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(80), moved=at(80))
        .validation(f"{RAW}/Sales.Integrity", result="succeeded", ran=at(70))
        .reads(f"{RAW}/Sales.Integrity", "Sales.Order")
        .report()
    )

    assert report.tests.status == GREEN


@weaver_test()
def test_a_failing_validation_is_red_and_keeps_its_count():
    report = (
        _Estate()
        .validation(
            f"{RAW}/Sales.Integrity", result="failed", ran=at(1), failure_count=7
        )
        .report()
    )

    assert report.tests.status == RED
    assert about(report.tests, TEST_FAILED)[0].failure_count == 7


@pytest.mark.parametrize("result", ["error", "blocked"])
@weaver_test()
def test_a_validation_that_could_not_be_evaluated_is_red(result):
    report = _Estate().validation(f"{RAW}/Sales.Integrity", result=result).report()

    assert report.tests.status == RED
    assert codes(report.tests) == (TEST_FAILED,)


@weaver_test()
def test_a_validation_that_has_not_run_is_amber():
    report = _Estate().validation(f"{RAW}/Sales.Integrity").report()

    assert report.tests.status == AMBER
    assert codes(report.tests) == (TEST_PENDING,)


@weaver_test()
def test_an_assumption_is_a_validation_subject_too():
    report = (
        _Estate()
        .validation(f"{RAW}/Sales.Coverage", kind="Assumption", result="succeeded")
        .report()
    )

    assert report.tests.subjects == 1
    assert report.tests.status == GREEN


# --- build health -------------------------------------------------------------


@weaver_test()
def test_a_validation_with_no_installed_artefact_is_build_red():
    report = (
        _Estate()
        .validation(f"{RAW}/Sales.Integrity", installed=False, result="succeeded")
        .report()
    )

    assert report.build.status == RED
    assert codes(report.build) == (MISSING_VALIDATION_ARTEFACT,)


@weaver_test()
def test_a_declared_object_registry_does_not_certify_is_build_red():
    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
        .declared_not_installed(f"{RAW}/Sales.Missing")
        .report()
    )

    assert report.build.status == RED
    assert about(report.build, NOT_INSTALLED)[0].object_id == f"{RAW}/Sales.Missing"


@weaver_test()
def test_a_certified_object_the_target_does_not_hold_is_build_red():
    estate = _Estate().table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
    empty = target_inventory(kind="lakehouse", target_name="Raw_LH")

    report = estate.report(inventories={RAW_LH: empty})

    assert report.build.status == RED
    assert about(report.build, CERTIFIED_MISSING)[0].object_id == f"{RAW}/Sales.Order"


@weaver_test()
def test_a_certified_object_the_target_holds_is_build_green():
    estate = _Estate().table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
    present = target_inventory(
        kind="lakehouse",
        target_name="Raw_LH",
        schemas=("Sales",),
        tables=("Sales.Order",),
        files=("_/Load/Sales__Order.py",),
    )

    report = estate.report(inventories={RAW_LH: present})

    assert report.build.status == GREEN


@weaver_test()
def test_a_lakehouse_view_is_not_asked_of_storage():
    """A Lakehouse view lives in the Spark catalogue and nowhere in storage."""

    estate = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
        .view(f"{RAW}/Sales.Live")
        .reads(f"{RAW}/Sales.Live", "Sales.Order")
    )
    present = target_inventory(
        kind="lakehouse",
        target_name="Raw_LH",
        schemas=("Sales",),
        tables=("Sales.Order",),
        files=("_/Load/Sales__Order.py",),
    )

    report = estate.report(inventories={RAW_LH: present})

    assert about(report.build, CERTIFIED_MISSING) == ()


@weaver_test()
def test_two_objects_at_one_physical_address_are_build_red():
    catalogue = Catalogue(
        rows={
            WeaverItemId.parse(RAW): {
                INSTALLATION.name: (installation_row(RAW, "Shared_LH"),),
                REGISTRY.name: (registry_row(document_id(f"{RAW}/Sales.Order")),),
            },
            WeaverItemId.parse(CURATED): {
                INSTALLATION.name: (installation_row(CURATED, "Shared_LH"),),
                REGISTRY.name: (registry_row(document_id(f"{CURATED}/Sales.Order")),),
            },
        }
    )

    report = assess(catalogue, as_of=YESTERDAY, generated_at=NOW)

    assert report.build.status == RED
    assert codes(report.build) == (AMBIGUOUS_INSTALLATION,)


@weaver_test()
def test_a_consistent_estate_reports_no_build_findings():
    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1))
        .validation(f"{RAW}/Sales.Integrity", result="succeeded", ran=at(0.5))
        .reads(f"{RAW}/Sales.Integrity", "Sales.Order")
        .report()
    )

    assert report.build.findings == ()
    assert report.build.status == GREEN


# --- ordering and the JSON contract -------------------------------------------


@weaver_test()
def test_findings_are_ordered_worst_first_then_by_code_and_object():
    report = (
        _Estate()
        .table(f"{RAW}/Sales.A", loaded=at(1), result="failed")
        .table(f"{RAW}/Sales.B")
        .table(f"{RAW}/Sales.C", loaded=at(1), result="rejected")
        .report()
    )

    assert codes(report.load) == (LOAD_FAILED, LOAD_PENDING, LOAD_REJECTED)


@weaver_test()
def test_the_same_estate_reports_the_same_thing_twice():
    estate = (
        _Estate()
        .table(f"{RAW}/Sales.A", loaded=at(4), moved=at(4))
        .table(f"{RAW}/Sales.B", loaded=at(5), moved=at(5))
        .reads(f"{RAW}/Sales.B", "Sales.A")
    )

    assert estate.report().to_mapping() == estate.report().to_mapping()


@weaver_test()
def test_the_mapping_carries_the_format_version_and_the_machine_vocabulary():
    report = _Estate().table(f"{RAW}/Sales.Order", loaded=at(1), moved=at(1)).report()
    mapping = report.to_mapping()

    assert mapping["format_version"] == FORMAT_VERSION
    assert mapping["status"] == GREEN
    assert set(mapping["sections"]) == {"load", "tests", "build"}
    assert mapping["load_activity"] == []


@weaver_test()
def test_the_mapping_is_json_safe():
    import json

    report = (
        _Estate()
        .table(f"{RAW}/Sales.Order", loaded=at(30), moved=at(30))
        .validation(f"{RAW}/Sales.Integrity", result="failed", ran=at(1))
        .report(
            load_history=LoadHistory(
                workflow_id="workflow-1",
                started_at=at(30),
                completed_at=at(29),
                counts={"succeeded": 1},
                statistics=(_statistic("Sales.Order", rows_read=5412),),
            )
        )
    )

    recovered = json.loads(json.dumps(report.to_mapping()))

    assert recovered["as_of"] == YESTERDAY.isoformat()
    assert recovered["latest_load"]["workflow_id"] == "workflow-1"
    assert recovered["load_activity"][0]["rows_read"] == 5412


@weaver_test()
def test_arrays_stay_present_when_empty():
    mapping = _Estate().table(f"{RAW}/Sales.Order", loaded=at(1)).report().to_mapping()

    assert mapping["sections"]["tests"]["findings"] == []
    assert mapping["latest_load"] is None


# --- the bounded activity window ----------------------------------------------


def _window(*statistics) -> LoadHistory:
    """One catalogue's bounded window, in the shape its read carries."""

    return LoadHistory(workflow_id="workflow-1", statistics=tuple(statistics))


def _statistic(name: str, *, duration_ms=None, **counts) -> dict:
    return {
        "load_statistic_sk": name,
        "workflow_id": "workflow-1",
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": name.split(".")[0],
        "object_name": name.split(".")[1],
        "started_datetime": None,
        "completed_datetime": None,
        "duration_milliseconds": duration_ms,
        "rows_read": counts.get("rows_read", 0),
        "rows_inserted": counts.get("rows_inserted", 0),
        "rows_updated": counts.get("rows_updated", 0),
        "rows_deleted": counts.get("rows_deleted", 0),
        "rows_rejected": counts.get("rows_rejected", 0),
        "is_reload": False,
        "is_static_skip": False,
    }


@weaver_test()
def test_the_slowest_loads_come_from_the_whole_window():
    report = _Estate().report(
        load_history=_window(
            _statistic("Sales.A", duration_ms=31200),
            _statistic("Sales.B", duration_ms=12800),
            _statistic("Sales.C"),
        )
    )

    assert [each.object_id for each in report.slowest(2)] == [
        f"{RAW}/Sales.A",
        f"{RAW}/Sales.B",
    ]


@weaver_test()
def test_a_load_with_no_duration_is_left_out_of_the_slowest():
    report = _Estate().report(load_history=_window(_statistic("Sales.C")))

    assert report.slowest() == ()
    assert len(report.load_activity) == 1


@weaver_test()
def test_movement_is_what_a_load_changed_rather_than_what_it_read():
    report = _Estate().report(
        load_history=_window(
            _statistic("Sales.A", rows_read=5000),
            _statistic("Sales.B", rows_inserted=12, rows_deleted=3),
        )
    )

    assert [each.object_id for each in report.moved()] == [f"{RAW}/Sales.B"]


@weaver_test()
def test_counts_are_preserved_exactly():
    report = _Estate().report(
        load_history=_window(
            _statistic(
                "Sales.A",
                rows_read=5412,
                rows_inserted=12,
                rows_updated=3,
                rows_deleted=0,
                rows_rejected=4,
            )
        )
    )

    assert report.load_activity[0].to_mapping() == {
        "object_id": f"{RAW}/Sales.A",
        "target": None,
        "workflow_id": "workflow-1",
        "started_at": None,
        "completed_at": None,
        "duration_ms": None,
        "rows_read": 5412,
        "rows_inserted": 12,
        "rows_updated": 3,
        "rows_deleted": 0,
        "rows_rejected": 4,
        "is_reload": False,
        "is_static_skip": False,
    }


@weaver_test()
def test_a_catalogue_read_without_a_window_reports_no_activity():
    report = _Estate().table(f"{RAW}/Sales.Order", loaded=at(1)).report()

    assert report.latest_load is None
    assert report.load_activity == ()
