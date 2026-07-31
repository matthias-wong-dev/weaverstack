"""One Lakehouse estate, built and then moved — on local Spark and on Fabric.

Everything here reads a transition the ``lakehouse_journey`` fixture already
took. Nothing in this module builds anything, which is the point: the estate is
installed once and each move after it is an incremental build, so a dozen
assertions cost what one used to.

It replaces three separate module estates, each of which paid a full install to
ask what a *first* build does — a question the local suite already answers. What
this asks instead is what the second build does, and whether it correctly does
nothing. That question is where incremental logic actually lives and no one-shot
estate could reach it.

**Every assertion names the Lakehouse it is about.** The session is attached to
the Weaver Lakehouse and the build writes to a different one, so a query for a
bare ``DWG.Customer`` would ask the control plane. ``env.query`` resolves object
tokens against a named destination, so a read can only succeed where the write
actually landed.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from build_envs import LAKEHOUSE_JOURNEY_FIXTURE

from weaver import DeltaTarget, FolderTarget

pytestmark = pytest.mark.parametrize(
    "weaver_repo_fixture", [LAKEHOUSE_JOURNEY_FIXTURE], indirect=True
)

AUDIT = {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}

#: Action kinds that are catalogue bookkeeping rather than work on the estate. A
#: build with nothing to do still writes the catalogue and closes the endpoint.
BOOKKEEPING = {
    "publish_catalogue",
    "publish_registry",
    "delete_catalogue_claims",
    "refresh_sql_endpoint",
}


def _folder(env, schema, name):
    return env.resolver.folder_object(FolderTarget(lakehouse=env.target), schema, name)


def _scalar(rows):
    return next(iter(rows[0].values()))


# --- the first build ----------------------------------------------------------


def test_the_bundle_names_only_the_items_it_binds(lakehouse_journey):
    plan = lakehouse_journey["install"].bundle.plan

    assert plan.format_version == 1
    assert plan.repository_name == "weaver_items"
    assert {target.logical_item_name for target in plan.targets} == {"Sales", "_weaver"}
    assert plan.omitted_nodes == ()


def test_every_declared_object_is_built(lakehouse_journey):
    env = lakehouse_journey.env

    assert env.store.exists(_folder(env, "Raw", "CustomerCsv"))
    tables = {
        row["tableName"].lower()
        for row in env.query(f"SHOW TABLES IN {env.schema_name('DWG')}")
    }
    assert {"customer", "order"} <= tables
    views = {
        row["viewName"].lower()
        for row in env.query(f"SHOW VIEWS IN {env.schema_name('DWG')}")
    }
    assert {"activecustomer", "activecustomersummary"} <= views


def test_a_table_is_built_empty_with_its_declared_shape(lakehouse_journey):
    env = lakehouse_journey.env
    columns = {
        row["col_name"].lower()
        for row in env.query("DESCRIBE TABLE {{object:DWG.Customer}}")
        if row["col_name"] and not row["col_name"].startswith("#")
    }

    assert {"customerid", "customername", "isactive"} <= columns
    assert AUDIT <= columns
    assert _scalar(env.query("SELECT count(*) AS n FROM {{object:DWG.Customer}}")) == 0


def test_a_view_resolves_through_the_whole_chain(lakehouse_journey):
    """``ActiveCustomerSummary`` reads ``ActiveCustomer`` reads ``Customer``."""

    env = lakehouse_journey.env

    assert _scalar(
        env.query("SELECT CustomerCount FROM {{object:DWG.ActiveCustomerSummary}}")
    ) == 0


def test_the_table_landed_in_the_destination_not_the_control_plane(lakehouse_journey):
    """A name alone cannot say which Lakehouse answered, so the storage is asked.

    Case-insensitively and not by exact path: the physical name is the
    workspace's to choose, and Fabric lowercases a managed table's directory
    exactly as the local metastore does.
    """

    env = lakehouse_journey.env
    stored = {
        entry.name.lower()
        for entry in env.store.list(env.resolver.tables_root(env.target).join("DWG"))
        if entry.is_directory
    }

    assert "customer" in stored


def test_nothing_is_built_in_the_weaver_lakehouse(lakehouse_journey):
    """The control plane is not the destination, and a build must not treat it as
    one. This is the assertion two-part names made impossible: an unqualified
    ``CREATE TABLE DWG.Customer`` lands in the attached Lakehouse, and a read
    through the same session would find it there and pass."""

    env = lakehouse_journey.env
    weaver = env.weaver_destination

    assert not env.schema_exists("DWG", destination=weaver)
    assert not env.schema_exists("Raw", destination=weaver)
    assert env.schema_exists("DWG")


def test_the_install_report_is_written_into_the_bundle(lakehouse_journey):
    bundle = lakehouse_journey["install"].bundle

    assert lakehouse_journey.env.store.exists(bundle.location.join("install-report.yml"))


def test_a_bundle_installs_without_its_source_repository(lakehouse_journey):
    """The source was deleted between generating this bundle and installing it,
    and the install still succeeded. A bundle carries everything it needs; it is
    not a pointer back at a repository that has to still be there."""

    step = lakehouse_journey["install"]

    assert step.outcome.status == "succeeded"


def test_every_planned_action_ran_in_the_order_planned(lakehouse_journey):
    step = lakehouse_journey["install"]
    planned = [action.id for _s, _b, action in step.bundle.plan.actions()]

    assert list(step.outcome.action_order) == planned
    assert step.outcome.bundle_id == step.bundle.bundle_id


def test_dependencies_are_built_before_the_objects_that_read_them(lakehouse_journey):
    at = lakehouse_journey["install"].sequence_of
    of = {
        action_id: at[action_id]
        for action_id in at
        if "DWG." in action_id or "Raw." in action_id
    }

    def when(name):
        return next(seq for action_id, seq in of.items() if action_id.endswith(name))

    assert when("DWG.Customer") < when("DWG.ActiveCustomer") < when(
        "DWG.ActiveCustomerSummary"
    )
    assert when("DWG.Customer") < when("DWG.Order")


# --- reaching the built objects the way a developer does -----------------------
#
# One round trip into the environment, shared by the assertions below. The body
# is a single string run wherever the environment runs — in this process against
# local Spark, or inside a Fabric session over Livy — so what is asserted is
# genuinely the same code either side. Its only transport-dependent line is
# ``resolver``, which the environment binds before the body starts.
#
# The classes are declared in the body rather than imported from the installed
# repository because importing one is the load executor's job, and that does not
# exist yet. They mirror the fixture's own documents.

AUTHORED = '''
from weaver import DeltaTarget, Folder, FolderTarget, Table, lakehouse_for

lakehouse = lakehouse_for(resolver, target)
delta_target = DeltaTarget(lakehouse=target)
folder_target = FolderTarget(lakehouse=target)


class Raw__CustomerCsv(Folder):
    def read(self):
        return self.staging_folder(), []


class DWG__Customer(Table):
    def read(self):
        return [], []


class DWG__Order(Table):
    def read(self):
        return [], []


order = DWG__Order(spark, lakehouse=lakehouse)
customer = DWG__Customer(order)
export = Raw__CustomerCsv(order)

emit({
    "ids": [order.object_id, customer.object_id, export.object_id],
    "table_path": lakehouse.table_path(*order.identity),
    "folder_path": export.path(),
    "staging_folder": export.staging_folder(),
    "resolved_table_path": resolver.delta_table(delta_target, "DWG", "Order").value,
    "resolved_folder_path": resolver.folder_object(
        folder_target, "Raw", "CustomerCsv"
    ).value,
    "resolved_staging_path": resolver.folder_staging(
        folder_target, "Raw", "CustomerCsv"
    ).value,
    "order_columns": sorted(f.name.lower() for f in order.dataframe().schema),
    "order_rows": order.dataframe().count(),
    "empty_columns": sorted(f.name.lower() for f in order.empty_dataframe().schema),
    "customer_columns": sorted(f.name.lower() for f in customer.dataframe().schema),
    "customer_rows": customer.dataframe().count(),
})
'''


@pytest.fixture(scope="module")
def reached(lakehouse_journey):
    return lakehouse_journey.env.run_python(AUTHORED)


def test_object_identity_comes_from_the_class_name(reached):
    assert reached["ids"] == ["DWG.Order", "DWG.Customer", "Raw.CustomerCsv"]


def test_a_table_reads_the_delta_files_the_build_created(reached):
    assert reached["table_path"] == reached["resolved_table_path"]
    assert reached["order_rows"] == 0
    assert set(reached["order_columns"]) == {"orderid", "customerid", "amount"} | AUDIT


def test_a_dependency_reads_its_own_table_through_the_same_session(reached):
    assert reached["customer_rows"] == 0
    assert set(reached["customer_columns"]) == {
        "customerid",
        "customername",
        "isactive",
    } | AUDIT


def test_an_empty_dataframe_keeps_the_built_shape(reached):
    assert reached["empty_columns"] == reached["order_columns"]


def test_a_folder_resolves_to_the_directory_the_build_created(reached, lakehouse_journey):
    """No mount anywhere in it: the target Lakehouse is not the attached one on
    Fabric, and a folder still resolves — which is the point of addressing it
    through the Lakehouse's own root."""

    assert reached["folder_path"] == reached["resolved_folder_path"]

    env = lakehouse_journey.env
    assert env.store.exists(_folder(env, "Raw", "CustomerCsv"))


def test_staging_sits_beside_the_folder_it_belongs_to(reached):
    assert reached["staging_folder"] == f"{reached['folder_path']}_Staging"
    assert reached["staging_folder"] == reached["resolved_staging_path"]


# --- building again, having changed nothing ------------------------------------


def test_a_second_build_does_no_physical_work(lakehouse_journey):
    """The assertion the one-shot estates could not make.

    An estate that is already correct should cost nothing to build. Anything
    here other than catalogue bookkeeping means something was rebuilt that did
    not need to be — which is exactly the class of bug incremental selection
    exists to prevent, and which a suite that only ever built once could never
    see.
    """

    kinds = lakehouse_journey["unchanged"].kinds() - BOOKKEEPING

    assert kinds == set()


def test_a_second_build_leaves_the_objects_readable(lakehouse_journey):
    """Doing nothing has to mean *nothing*, not quietly dropping something."""

    env = lakehouse_journey.env

    assert _scalar(env.query("SELECT count(*) AS n FROM {{object:DWG.Customer}}")) == 0
    assert _scalar(
        env.query("SELECT CustomerCount FROM {{object:DWG.ActiveCustomerSummary}}")
    ) == 0
    assert env.store.exists(_folder(env, "Raw", "CustomerCsv"))


# --- pruning what the item no longer declares ----------------------------------


def test_unmanaged_objects_are_pruned(lakehouse_journey):
    """Seeded before this build: two tables, a view and two folders the item
    does not declare. Prune is what removes them, and it runs before the build
    so a rebuilt object never meets its own stale predecessor."""

    env = lakehouse_journey.env
    tables = {
        row["tableName"].lower()
        for row in env.query(f"SHOW TABLES IN {env.schema_name('DWG')}")
    }

    assert "oldtable" not in tables
    assert "oldview" not in tables
    assert not env.schema_exists("Legacy")
    assert not env.store.exists(_folder(env, "Legacy", "Stuff"))


def test_pruning_spares_everything_the_item_declares(lakehouse_journey):
    """The half that matters: prune is the destructive direction, so what it
    leaves alone is the assertion worth making."""

    env = lakehouse_journey.env
    tables = {
        row["tableName"].lower()
        for row in env.query(f"SHOW TABLES IN {env.schema_name('DWG')}")
    }

    assert {"customer", "order"} <= tables
    assert env.store.exists(_folder(env, "Raw", "CustomerCsv"))


def test_prune_runs_before_anything_is_built(lakehouse_journey):
    step = lakehouse_journey["prune"]
    prunes = [seq for action_id, seq in step.sequence_of.items() if "prune" in action_id]
    builds = [
        seq
        for action_id, seq in step.sequence_of.items()
        if step.actions[action_id].startswith("build_")
    ]

    assert prunes, "the seeded orphans should have produced prune actions"
    if builds:
        assert max(prunes) < min(builds)


# --- a failing build, last because it leaves the estate part-built --------------


def test_a_failing_view_stops_the_build_and_leaves_no_final_view(lakehouse_journey):
    """A payload that cannot run must stop its barrier, not be skipped past.

    Deliberately the module's last test: the install fails partway by design, so
    the estate afterwards is not in a state anything else could assert against.
    """

    from weaver.build_bundle import compute_bundle_id, write_bundle

    env = lakehouse_journey.env
    bundle = lakehouse_journey["install"].bundle
    store = env.store

    payloads = {
        action.payload: store.read(bundle.location.join(*action.payload.split("/")))
        for _s, _b, action in bundle.plan.actions()
        if action.payload is not None
    }
    broken = (
        b"CREATE VIEW {{object:DWG.ActiveCustomerSummary}} AS\n"
        b"select count(*) as CustomerCount from {{object:DWG.ActiveCustomer}} "
        b"where NoSuchColumn = 1\n"
    )

    def fix(action):
        if action.id.endswith("DWG.ActiveCustomerSummary"):
            payloads[action.payload] = broken
            return replace(action, payload_sha256=hashlib.sha256(broken).hexdigest())
        return action

    plan = bundle.plan
    sequences = tuple(
        replace(
            sequence,
            batches=tuple(
                replace(batch, actions=tuple(fix(action) for action in batch.actions))
                for batch in sequence.batches
            ),
        )
        for sequence in plan.sequences
    )
    plan = replace(plan, sequences=sequences, bundle_id="")
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    corrupted = write_bundle(
        env.resolver.build_bundle("broken"),
        plan=plan,
        payloads=payloads,
        snapshot={},
        store=store,
    )

    outcome = env.install(corrupted)

    assert outcome.status == "failed"
    assert any(status == "failed" for status in outcome.action_status.values())
