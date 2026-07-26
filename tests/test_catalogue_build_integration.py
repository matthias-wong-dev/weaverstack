"""The catalogue as part of a build bundle: ordering, scope, and failure.

Catalogue work concludes a build, and the order is the invariant everything else
rests on. Dictionaries describe what was built, Installation records which item
the repository is bound to, and Registry certifies. Registry is last and is its
own barrier, so a row in it cannot outrun the work it attests to — any earlier
failure, physical or catalogue, stops the install before anything is certified.

Generation needs no Spark: the statements are rendered from the projection alone.
A session only improves the *report*, which is the point of reading the catalogue
at plan time rather than deriving deletes from it.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import ItemRef, LocalHost, LocalResolver, LocalStore, Location
from weaver.build_bundle import (
    LakehouseBinding,
    TargetBindings,
    generate_build_bundle,
    load_bundle,
)
from weaver.build_bundle.models import (
    CATALOGUE_KINDS,
    PUBLISH_REGISTRY,
    RECONCILE_CATALOGUE,
    RECORD_INSTALLATION,
)
from weaver.build_bundle.payloads import (
    CATALOGUE_SEQUENCE,
    INSTALLATION_SEQUENCE,
    OBJECT_SEQUENCE_START,
    REGISTRY_SEQUENCE,
    check_sequence_headroom,
)
from weaver.build_bundle.targets import WarehouseBinding
from weaver.catalogue import CATALOGUE_TABLES, DICTIONARY_TABLES
from weaver.errors import BuildError

FIXTURE = Path(__file__).parent / "fixtures" / "catalogue-estate"


@pytest.fixture
def estate(tmp_path):
    host = LocalHost(root=tmp_path, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(host)
    for item in ("Weaver", "Sales_LH"):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))
    store.make_directory(resolver.repos_root)
    shutil.copytree(FIXTURE, (resolver.repos_root / "Estate").path)
    return host, store, resolver, tmp_path


def _generate(estate, *, targets=None, catalogue=True, prune=False, name="bundle", sql=None):
    host, store, resolver, tmp_path = estate
    if targets is None:
        targets = TargetBindings(lakehouse=LakehouseBinding(lakehouse=ItemRef("Sales_LH")))
    return generate_build_bundle(
        weaver_lakehouse=ItemRef("Weaver"),
        repository_name="Estate",
        targets=targets,
        output=Location(str(tmp_path / name)),
        host=host,
        store=store,
        prune=prune,
        catalogue=catalogue,
        sql=sql,
    )


def _catalogue_actions(plan):
    return [
        (sequence.number, action)
        for sequence, _batch, action in plan.actions()
        if action.kind in CATALOGUE_KINDS
    ]


# --- ordering ------------------------------------------------------------------


def test_catalogue_work_follows_every_physical_action(estate):
    plan = _generate(estate).plan
    physical = [
        sequence.number
        for sequence, _b, action in plan.actions()
        if action.kind not in CATALOGUE_KINDS
    ]
    catalogue = [number for number, _action in _catalogue_actions(plan)]
    assert physical and catalogue
    assert max(physical) < min(catalogue)


def test_dictionaries_precede_installation_which_precedes_registry(estate):
    plan = _generate(estate).plan
    numbers = {
        action.kind: number for number, action in _catalogue_actions(plan)
    }
    assert numbers[RECONCILE_CATALOGUE] == CATALOGUE_SEQUENCE
    assert numbers[RECORD_INSTALLATION] == INSTALLATION_SEQUENCE
    assert numbers[PUBLISH_REGISTRY] == REGISTRY_SEQUENCE
    assert CATALOGUE_SEQUENCE < INSTALLATION_SEQUENCE < REGISTRY_SEQUENCE


def test_registry_is_the_final_sequence_and_nothing_follows_it(estate):
    plan = _generate(estate).plan
    assert plan.sequences[-1].number == REGISTRY_SEQUENCE
    last = plan.sequences[-1]
    assert {action.kind for batch in last.batches for action in batch.actions} == {
        PUBLISH_REGISTRY
    }


def test_registry_is_its_own_barrier(estate):
    """So a dictionary failure prevents certification, not merely reorders it."""

    plan = _generate(estate).plan
    registry = [s for s in plan.sequences if s.number == REGISTRY_SEQUENCE]
    assert len(registry) == 1
    other = [
        action
        for batch in registry[0].batches
        for action in batch.actions
        if action.kind != PUBLISH_REGISTRY
    ]
    assert other == []


def test_each_dictionary_table_is_reconciled(estate):
    plan = _generate(estate).plan
    ids = {action.id for number, action in _catalogue_actions(plan)}
    for table in DICTIONARY_TABLES:
        assert f"catalogue-{table.name}-delete" in ids, table.name


def test_an_action_id_says_which_table_and_which_half(estate):
    """A failure should be understandable from the report without opening a payload."""

    plan = _generate(estate).plan
    ids = {action.id for _number, action in _catalogue_actions(plan)}
    assert "catalogue-Registry-delete" in ids
    assert "catalogue-Registry-merge" in ids
    assert "catalogue-Installation-merge" in ids
    # Installation's key is the scope, so it has no obsolete row to delete.
    assert "catalogue-Installation-delete" not in ids


def test_the_object_layers_cannot_reach_the_catalogue_sequence_numbers():
    check_sequence_headroom(OBJECT_SEQUENCE_START)
    check_sequence_headroom(CATALOGUE_SEQUENCE - 10)
    with pytest.raises(BuildError, match="catalogue sequence range"):
        check_sequence_headroom(CATALOGUE_SEQUENCE)


# --- the control plane is a named target ---------------------------------------


def test_the_catalogue_is_written_to_the_weaver_lakehouse_not_the_destination(estate):
    """A bundle names every physical destination it touches (§9).

    The catalogue is a different item from the destination, so it gets its own
    bound target rather than relying on wherever the installer happens to point.
    """

    plan = _generate(estate).plan
    destination, control = plan.targets
    assert destination.item_id == "Sales_LH"
    assert control.item_id == "Weaver"

    catalogue_batches = {
        batch.target_id
        for sequence in plan.sequences
        for batch in sequence.batches
        if any(action.kind in CATALOGUE_KINDS for action in batch.actions)
    }
    assert catalogue_batches == {control.id}


def test_a_warehouse_build_still_writes_the_catalogue_to_a_lakehouse(estate):
    """The control plane is a Lakehouse whichever side is being built.

    So a Warehouse build carries Spark SQL catalogue actions against the Weaver
    Lakehouse. That is the accepted consequence of one central catalogue.
    """

    plan = _generate(
        estate,
        targets=TargetBindings(warehouse=WarehouseBinding(warehouse=ItemRef("Sales_WH"))),
        name="warehouse",
    ).plan
    destination, control = plan.targets
    assert destination.kind == "warehouse"
    assert control.kind == "lakehouse"
    assert {action.executor for _n, action in _catalogue_actions(plan)} == {"spark_sql"}


def test_the_destination_is_reused_when_it_is_the_weaver_lakehouse(estate):
    """Weaver building its own catalogue: one item, so one bound target.

    Naming the same item twice would be a manifest a reviewer has to reconcile.
    """

    plan = _generate(
        estate,
        targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=ItemRef("Weaver"))),
        name="into-weaver",
    ).plan
    assert len(plan.targets) == 1
    assert plan.targets[0].item_id == "Weaver"


def test_the_installation_row_records_the_resolved_item_name(estate):
    plan = _generate(estate).plan
    payload = [
        action for number, action in _catalogue_actions(plan)
        if action.kind == RECORD_INSTALLATION
    ][0]
    bundle = load_bundle(Location(str(estate[3] / "bundle")), store=estate[1])
    text = estate[1].read(bundle.location.join(*payload.payload.split("/"))).decode()
    assert "'Sales_LH'" in text


# --- scope --------------------------------------------------------------------


def _scope_predicates(text: str) -> set[str]:
    """The installation each scope predicate in a statement names.

    Matched on the predicate rather than on the bare word, because a target type is
    also an ordinary *value*: an Alias row's ``alias_target_type`` names the other
    side by design, so a Warehouse build's statements legitimately contain
    ``\'lakehouse\'``. Only the scope is a claim about which rows are touched.
    """

    import re

    return set(
        re.findall(r"`target_type` (?:=|<=>) (?:CAST\()?\'(\w+)\'", text)
    )


def test_every_catalogue_statement_names_one_installation(estate):
    host, store, resolver, tmp_path = estate
    bundle = _generate(estate)
    for _number, action in _catalogue_actions(bundle.plan):
        text = store.read(bundle.location.join(*action.payload.split("/"))).decode()
        assert "'Estate'" in text
        assert _scope_predicates(text) == {"lakehouse"}


def test_a_warehouse_build_s_statements_name_only_the_warehouse_installation(estate):
    host, store, resolver, tmp_path = estate
    bundle = _generate(
        estate,
        targets=TargetBindings(warehouse=WarehouseBinding(warehouse=ItemRef("Sales_WH"))),
        name="warehouse",
    )
    for _number, action in _catalogue_actions(bundle.plan):
        text = store.read(bundle.location.join(*action.payload.split("/"))).decode()
        assert _scope_predicates(text) == {"warehouse"}


def test_an_alias_row_may_name_the_other_target_type_as_a_value(estate):
    """The distinction the scope check rests on, asserted so it is not an accident.

    A Warehouse object's Lakehouse alias records ``lakehouse`` in
    ``alias_target_type``. That is a fact about the declaration, not a claim about
    which rows the statement touches — which is why scope is checked as a predicate.
    """

    host, store, resolver, tmp_path = estate
    bundle = _generate(
        estate,
        targets=TargetBindings(warehouse=WarehouseBinding(warehouse=ItemRef("Sales_WH"))),
        name="warehouse-alias",
    )
    (alias,) = [
        action
        for _n, action in _catalogue_actions(bundle.plan)
        if action.id == "catalogue-Alias-merge"
    ]
    text = store.read(bundle.location.join(*alias.payload.split("/"))).decode()
    assert "AS STRING) AS `alias_target_type`" in text
    assert "'lakehouse'" in text.split("FROM VALUES")[1]
    assert _scope_predicates(text) == {"warehouse"}


def test_an_omitted_object_is_not_projected_and_is_not_pruned(estate):
    """A Lakehouse build has no opinion about the Warehouse installation.

    The omitted Warehouse objects appear in the plan's omissions, and nowhere in
    the catalogue statements — neither as rows to write nor as rows to remove.
    """

    host, store, resolver, tmp_path = estate
    bundle = _generate(estate)
    omitted = {node.node_id for node in bundle.plan.omitted_nodes}
    assert "sql:Rpt.CustomerView" in omitted

    statements = "\n".join(
        store.read(bundle.location.join(*action.payload.split("/"))).decode()
        for _n, action in _catalogue_actions(bundle.plan)
    )
    assert "CustomerView" not in statements


# --- opting out ---------------------------------------------------------------


def test_a_build_can_be_generated_without_catalogue_work(estate):
    """Needed before setup has run: the statements require the tables to exist."""

    plan = _generate(estate, catalogue=False, name="no-catalogue").plan
    assert _catalogue_actions(plan) == []
    assert len(plan.targets) == 1


def test_generation_needs_no_spark_session(estate):
    """The statements come from the projection; a session only improves the report.

    That is deliberate — deriving deletes from a read would let a failed read widen
    the deletion scope, which is what build-philosophy §6 exists to prevent.
    """

    plan = _generate(estate).plan
    assert _catalogue_actions(plan)
    descriptions = {
        sequence.description
        for sequence in plan.sequences
        if sequence.number >= CATALOGUE_SEQUENCE
    }
    # No row counts, because nothing was read — but the work is planned regardless.
    assert descriptions == {
        "reconcile catalogue dictionaries",
        "record the installation",
        "publish the registry",
    }


# --- determinism ---------------------------------------------------------------


def test_the_same_repository_produces_the_same_bundle_identity(estate):
    first = _generate(estate, name="one")
    second = _generate(estate, name="two")
    assert first.plan.bundle_id == second.plan.bundle_id


def test_a_catalogue_payload_is_hashed_like_any_other(estate):
    host, store, resolver, tmp_path = estate
    bundle = _generate(estate)
    from weaver.build_bundle.payloads import sha256_hex

    for _number, action in _catalogue_actions(bundle.plan):
        data = store.read(bundle.location.join(*action.payload.split("/")))
        assert sha256_hex(data) == action.payload_sha256


# --- the catalogue schema is not an application's to prune ---------------------


def test_an_ordinary_build_cannot_prune_the_catalogue_schema(estate):
    """`_` is reserved, so a repository built into the Weaver Lakehouse leaves it.

    An application build normally cannot see `_` at all — prune is scoped to the
    bound destination's own storage, and the catalogue lives in the Weaver
    Lakehouse. This covers the case where those coincide, where a prune that
    dropped `_` would take the record of every installation with it.
    """

    host, store, resolver, tmp_path = estate
    # A catalogue already present in the Weaver Lakehouse's Tables area.
    tables = resolver.tables_root(ItemRef("Weaver"))
    for table in CATALOGUE_TABLES:
        store.write(tables.join("_", table.name, "part-0.parquet"), b"x")
    # And an ordinary orphan, to prove prune is working at all.
    store.write(tables.join("Legacy", "OldThing", "part-0.parquet"), b"x")

    plan = _generate(
        estate,
        targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=ItemRef("Weaver"))),
        prune=True,
        catalogue=False,
        name="prune",
    ).plan

    drops = {action.id for _s, _b, action in plan.actions() if action.kind.startswith("prune")}
    assert "prune-schema-Legacy" in drops
    assert not [drop for drop in drops if "_" == drop.split("-")[-1]]
    assert not [drop for drop in drops if "Registry" in drop]


# --- failure stops certification -----------------------------------------------


class _Recorder:
    """Executes everything, failing on named action ids, and records the order."""

    def __init__(self, fail_on=()):
        self.calls: list[str] = []
        self.fail_on = set(fail_on)

    def execute(self, action, payload, context):
        self.calls.append(action.id)
        if action.id in self.fail_on:
            raise RuntimeError(f"boom {action.id}")
        return {"ran": action.id}


def _install(estate, bundle, *, fail_on=()):
    from weaver.build_bundle.installer import InstallationEnvironment, install_bundle

    _host, store, resolver, _tmp = estate
    recorder = _Recorder(fail_on=fail_on)
    report = install_bundle(
        load_bundle(bundle.location, store=store),
        environment=InstallationEnvironment(
            store=store,
            resolver=resolver,
            executors={
                name: recorder
                for name in (
                    "spark_sql",
                    "spark_schema",
                    "spark_table",
                    "folder",
                    "tsql",
                )
            },
        ),
    )
    return report, recorder


def _status(report, action_id: str) -> str:
    for sequence in report.sequences:
        for action in sequence.actions:
            if action.action_id == action_id:
                return action.status
    raise AssertionError(f"no action {action_id} in the report")


def test_a_successful_install_runs_the_catalogue_last(estate):
    bundle = _generate(estate)
    report, recorder = _install(estate, bundle)
    assert report.status == "succeeded"
    physical = {
        action.id
        for _s, _b, action in bundle.plan.actions()
        if action.kind not in CATALOGUE_KINDS
    }
    first_catalogue = min(
        index for index, call in enumerate(recorder.calls) if call.startswith("catalogue-")
    )
    last_physical = max(
        index for index, call in enumerate(recorder.calls) if call in physical
    )
    assert last_physical < first_catalogue
    assert recorder.calls[-1] == "catalogue-Registry-merge"


def test_a_physical_failure_prevents_every_catalogue_action(estate):
    """The barrier does the work: nothing is described and nothing is certified."""

    bundle = _generate(estate)
    physical = [
        action.id
        for _s, _b, action in bundle.plan.actions()
        if action.kind not in CATALOGUE_KINDS
    ]
    report, recorder = _install(estate, bundle, fail_on={physical[-1]})
    assert report.status == "failed"
    assert not [call for call in recorder.calls if call.startswith("catalogue-")]


def test_a_dictionary_failure_prevents_registry_publication(estate):
    """Partial dictionary state is acceptable; an unearned certification is not.

    The next successful build repairs the dictionaries by ordinary row comparison,
    which is why they need no all-or-nothing transaction. Registry is the one thing
    that must not run early.
    """

    bundle = _generate(estate)
    report, recorder = _install(estate, bundle, fail_on={"catalogue-TableDictionary-merge"})
    assert report.status == "failed"
    assert not [call for call in recorder.calls if call.startswith("catalogue-Registry")]
    assert _status(report, "catalogue-Installation-merge") == "skipped"


def test_an_installation_failure_prevents_registry_publication(estate):
    bundle = _generate(estate)
    report, recorder = _install(estate, bundle, fail_on={"catalogue-Installation-merge"})
    assert report.status == "failed"
    assert not [call for call in recorder.calls if call.startswith("catalogue-Registry")]


def test_a_registry_failure_is_reported_as_an_installation_failure(estate):
    bundle = _generate(estate)
    report, _recorder = _install(estate, bundle, fail_on={"catalogue-Registry-merge"})
    assert report.status == "failed"
    assert _status(report, "catalogue-Registry-merge") == "failed"
    # And the dictionaries did run — the catalogue's partial state is repairable.
    assert _status(report, "catalogue-TableDictionary-merge") == "succeeded"


def test_the_report_names_catalogue_actions_like_any_other(estate):
    bundle = _generate(estate)
    report, _recorder = _install(estate, bundle)
    descriptions = {
        sequence.description for sequence in report.sequences
    }
    assert "publish the registry" in descriptions
    assert _status(report, "catalogue-Registry-merge") == "succeeded"
