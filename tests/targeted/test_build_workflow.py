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
from factories import (
    Catalogue,
    item_bindings,
    lakehouse_table,
    single_document_repository,
    target_inventory,
)

from weaver import ItemRef, LocalResolver, LocalStore, LocalWorkspace, Location
from weaver.build_bundle import (
    InstallationEnvironment,
    LakehouseBinding,
    build_item_repository,
    effective_item_bindings,
)
from weaver.catalogue.state import Catalogue as RealCatalogue, Reconciliation


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

    workspace = LocalWorkspace(workspace=tmp_path / "ws", weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(workspace)
    for item in ("Weaver", "Sales_LH"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))

    executors = {
        name: RecordingExecutor(name)
        for name in (
            "spark_sql",
            "spark_sql_batch",
            "spark_schema",
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
        "environment": InstallationEnvironment(
            store=store, resolver=resolver, spark=None, workspace=workspace,
            executors=executors,
        ),
    }


def build(estate, **overrides):
    bindings = effective_item_bindings(
        item_bindings(("Lakehouse/Sales", "Sales_LH")),
        weaver_lakehouse="Weaver",
    )
    inventories = {
        binding.item: target_inventory(
            target_id=binding.to_bound_target().id,
            target_name=binding.to_bound_target().name,
        )
        for binding in bindings.entries
    }
    arguments = {
        "bindings": bindings,
        "target_inventories": inventories,
        "reconciliation": Reconciliation(RealCatalogue(rows={}), stale_claims=()),
        "environment": estate["environment"],
        "source_store": estate["store"],
        "control_lakehouse": LakehouseBinding(lakehouse=ItemRef("Weaver")),
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

    bindings = effective_item_bindings(
        item_bindings(("Lakehouse/Sales", "Sales_LH")), weaver_lakehouse="Weaver"
    )
    inventories = {
        binding.item: target_inventory(
            target_id=binding.to_bound_target().id,
            target_name=binding.to_bound_target().name,
            schemas=("DWG",),
        )
        for binding in bindings.entries
    }

    result = build(estate, bindings=bindings, target_inventories=inventories)

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
    result = build(
        estate,
        reconciliation=Reconciliation(certified, stale_claims=()),
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
    estate["environment"].executors = estate["executors"]

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
    estate["environment"].executors = estate["executors"]

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
