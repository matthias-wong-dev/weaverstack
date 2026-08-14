"""The planner/executor seam: generate and install from already-prepared state.

`build_item_repository` is the one function whose whole job is composing prepared
inputs correctly, and until now no test named it — it was reached only through
the Fabric bodies, where a defect in it would surface as "the build failed".

What it must do is narrow: generate a bundle from source and state it was handed,
install that bundle, and report both. What it must *not* do is anything that
looks like re-deciding — parsing the repository again, reading the catalogue,
discovering a target. Those are the caller's, already done, and doing them here
would mean a build could disagree with the state it was planned against.

Fake executors throughout: what runs is not the subject, that each action is
reached and reported is.
"""

from __future__ import annotations

import pytest
from support.sessions import given_session
from factories import (
    Catalogue,
    item_bindings,
    lakehouse_table,
    single_document_repository,
    target_inventory,
)

from weaver.targets import ItemRef
from weaver.store import FilesystemStore
from weaver.locations import Location
from weaver.build_bundle import (
    LakehouseBinding,
    build_item_repository,
    effective_item_bindings,
)
from weaver.build_bundle.workflow import BuildState
from weaver.catalogue.state import Catalogue as RealCatalogue
from support.workspaces import given_resolver, given_workspace
from support.workspaces import WORKSPACE


class RecordingExecutor:
    """Accepts any action and remembers it. Nothing here executes anything."""

    def __init__(self, name: str, failing: bool = False) -> None:
        self.name = name
        self.seen: list[str] = []
        self._failing = failing

    def execute(self, action, payload, context):
        self.seen.append(action.id)
        if self._failing:
            raise RuntimeError(f"{self.name} refused {action.id}")
        return {"executor": self.name}


@pytest.fixture
def estate(tmp_path):
    """One repository, one Lakehouse, and an environment that records."""

    root = tmp_path / "repo"
    repository = single_document_repository(
        root,
        item="Lakehouse/Sales",
        documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")},
    )

    workspace = given_workspace(weaver_lakehouse="Weaver")
    store = FilesystemStore()
    resolver = given_resolver(workspace=workspace, root=tmp_path)
    for item in ("Weaver", "Sales_LH"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))

    executors = {
        name: RecordingExecutor(name)
        for name in (
            "spark_sql",
            "spark_sql_batch",
            "spark_table",
            "folder",
            "alias",
            "sql_endpoint_refresh",
            "tsql",
            "tsql_batch",
            "load_file",
        )
    }
    return {
        "repository": repository,
        "workspace": workspace,
        "store": store,
        "resolver": resolver,
        "executors": executors,
        "session": given_session(
            workspace=workspace, store=store, resolver=resolver
        ),
    }


def _bindings():
    return effective_item_bindings(
        item_bindings(("Lakehouse/Sales", "Sales_LH")),
        weaver_lakehouse="Weaver",
        workspace_name=WORKSPACE,
    )


def _inventories(**observed):
    """What each bound target physically holds. Empty unless a test says otherwise."""

    return {
        binding.item: target_inventory(
            target_id=binding.to_bound_target().id,
            target_name=binding.to_bound_target().name,
            **observed,
        )
        for binding in _bindings().entries
    }


def build(estate, **overrides):
    bindings = _bindings()
    inventories = _inventories()
    arguments = {
        "bindings": bindings,
        "state": BuildState(
            catalogue=RealCatalogue(rows={}), target_inventories=inventories
        ),
        "session": estate["session"],
        "executors": estate["executors"],
        "source_store": estate["store"],
        "control_lakehouse": LakehouseBinding(
            lakehouse=ItemRef("Weaver"), workspace_name="Demo"
        ),
    }
    arguments.update(overrides)
    return build_item_repository(estate["repository"], **arguments)


# --- it generates and installs ------------------------------------------------


def test_it_returns_the_plan_it_generated_and_the_report_it_installed(estate):
    result = build(estate)

    assert result.plan.bundle_id
    assert result.report.bundle_id == result.plan.bundle_id
    assert result.report.status == "succeeded"


def test_every_planned_action_is_executed_exactly_once(estate):
    """The seam's whole claim: what was planned is what ran, and once."""

    result = build(estate)

    planned = [action.id for _s, _b, action in result.plan.actions()]
    ran = [
        action_id
        for executor in estate["executors"].values()
        for action_id in executor.seen
    ]
    assert sorted(ran) == sorted(planned)
    assert len(ran) == len(set(ran))


def test_the_declaration_is_built(estate):
    """The document reaches an executor, rather than the build being all
    catalogue bookkeeping."""

    result = build(estate)

    assert any(
        action.kind == "build_table" for _s, _b, action in result.plan.actions()
    )


def test_the_result_carries_the_signatures_a_caller_records(estate):
    """These are what a later build compares against, so they come from the
    repository that was actually built rather than being recomputed."""

    result = build(estate)

    assert result.repository_signature == estate["repository"].signature
    assert result.item_signatures == {
        item.identity: item.signature for item in estate["repository"].items
    }


# --- it plans against the state it was given ----------------------------------


def test_an_inventory_that_already_holds_the_schema_plans_no_create(estate):
    """Prepared state is *used*, not re-read. If the caller says the schema is
    there, the build must believe it — that is what makes the seam a seam."""

    bindings = _bindings()
    inventories = {
        binding.item: target_inventory(
            target_id=binding.to_bound_target().id,
            target_name=binding.to_bound_target().name,
            schemas=("DWG",),
        )
        for binding in bindings.entries
    }

    result = build(
        estate,
        bindings=bindings,
        state=BuildState(
            catalogue=RealCatalogue(rows={}), target_inventories=inventories
        ),
    )

    # Scoped to this item's batch: a repository always carries Weaver's own
    # builtin catalogue item, whose `_` schema this inventory says nothing about,
    # so a whole-plan assertion would be about the wrong thing.
    assert not any(
        action.kind == "create_schema"
        for _s, batch, action in result.plan.actions()
        if "Sales" in batch.id
    )


def test_a_catalogue_that_certifies_the_document_plans_no_rebuild(estate):
    """The unchanged case, reached through the seam rather than the planner.

    A build handed a catalogue that already certifies what the repository
    declares must decide there is nothing to do — which is the incremental claim
    the whole design rests on, and here it is asserted of the *composition*.
    """

    from factories import FixtureCatalogue

    certified = FixtureCatalogue.from_repository(
        estate["repository"], item="Lakehouse/Sales"
    )
    # And the table is physically there. A catalogue certifying an object the
    # target does not hold is a *stale* claim, and reconciling it away so the
    # object rebuilds is correct — which this test would otherwise trip over,
    # because it used to hand the build a reconciliation already made for it.
    present = _inventories(tables=("DWG.Customer",))
    result = build(
        estate,
        state=BuildState(catalogue=certified, target_inventories=present),
    )

    # Again scoped: the builtin catalogue item is not certified by this
    # catalogue and is correctly built, which is not what this is about.
    rebuilt = {
        action.resource_node_id
        for _s, _b, action in result.plan.actions()
        if action.kind.startswith("build_")
    }
    assert "Lakehouse/Sales/DWG.Customer" not in rebuilt


def test_a_binding_naming_an_unknown_item_is_refused(estate):
    """Better to refuse than to plan a build for something the repository has
    never heard of, which would report success having done nothing."""

    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="absent from the repository"):
        build(
            estate,
            bindings=effective_item_bindings(
                item_bindings(("Lakehouse/Absent", "Sales_LH")),
                weaver_lakehouse="Weaver",
            ),
        )


# --- it reports failure rather than raising ------------------------------------


def test_a_failing_action_is_reported_not_raised(estate):
    """A build that failed is a result to read, not an exception to catch.

    The caller needs the plan and the per-action detail to say *what* failed —
    an exception would leave it with a stack trace and no report.
    """

    estate["executors"]["spark_sql"] = RecordingExecutor("spark_sql", failing=True)

    result = build(estate)

    assert result.report.status == "failed"
    assert any(
        action.status == "failed" for action in result.report.action_results()
    )


def test_a_failure_stops_the_rest_of_its_sequence(estate):
    """The barrier semantics, seen from the seam: nothing after a failure runs.

    Catalogue publication follows the physical work, so a build that kept going
    would certify objects whose creation had just failed.
    """

    estate["executors"]["spark_sql"] = RecordingExecutor("spark_sql", failing=True)

    result = build(estate)

    statuses = [action.status for action in result.report.action_results()]
    assert "skipped" in statuses
    assert not any(
        action.status == "succeeded" and action.action_id.startswith("publish-registry")
        for action in result.report.action_results()
    )


# --- the archive --------------------------------------------------------------


def test_no_archive_is_written_unless_one_is_asked_for(estate):
    result = build(estate)

    assert result.archive is None


def test_an_archive_is_persisted_where_it_was_asked_for(estate, tmp_path):
    """The bundle outlives the temporary directory it was built in — which is the
    point of asking for one, since the build itself is ephemeral."""

    archive = Location(str(tmp_path / "handover.weaver.zip"))

    result = build(estate, archive=archive, archive_store=estate["store"])

    assert result.archive == archive
    assert archive.path.is_file()


# --- a capability offered is not a capability acquired -------------------------


def test_installing_through_executors_that_need_no_spark_asks_for_none(estate):
    """A capability offered is not a capability acquired.

    A build whose work needs no Spark must not open a Livy session for it: the
    Installer used to evaluate the session's Spark while assembling every
    batch's context, so a build through recording executors reached for one it
    never used. Asserted by watching what the Session was asked for, because a
    build that quietly acquired a session would still pass an assertion about
    its result.
    """

    result = build(estate)

    assert result.report.status == "succeeded"
    assert not estate["session"].spark_sql


def test_an_executor_that_runs_a_statement_reaches_it_through_the_session(estate):
    """The other half: deferring acquisition must not stop it happening.

    An executor runs its statement through the Session, so what carries it is
    the Session's own transport rather than a proxy standing in for one.
    """

    seen = []

    class Asking:
        name = "spark_sql"

        def execute(self, action, payload, context):
            # Running one is what acquires the transport, which is the point.
            seen.append(context.spark_sql("SELECT 1"))
            return {"ran": action.id}

    build(estate, executors={**estate["executors"], "spark_sql": Asking()})

    assert seen, "no executor ran"
    assert estate["session"].spark_sql == ("SELECT 1",) * len(seen)
