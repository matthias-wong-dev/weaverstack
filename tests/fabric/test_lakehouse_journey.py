"""One Lakehouse estate, built and then moved — on local Spark and on Fabric.

The estate is installed once and each move after it is an incremental build over
a target that is already correct, so a whole build lifecycle costs roughly what a
single install used to. It replaces three module estates that each paid a full
install to ask what a *first* build does — the question the local suite already
answers, and not the one incremental logic lives in.

**One test, not many, and that is the point.** Each phase asserts the physical
state *immediately* after the transition it is about. Split into separate tests
sharing a fixture that had already run every move, each would instead be reading
the final estate — and a later move could repair what an earlier one broke, so
the earlier assertion would pass on evidence that no longer existed. A build
lifecycle is a sequence; asserting it out of order asserts something else.

The phases, in the order they must run:

``install``    the estate from nothing, the only full build. The source
               repository is deleted between generation and installation, so
               this also proves a bundle installs from itself
``unchanged``  build again having changed nothing — an estate that is already
               correct must cost nothing, which no one-shot estate could check
``prune``      seed objects the item does not declare, then build again
``broken``     last, because a failing install leaves the estate part-built and
               nothing after it could rely on what it found

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

from weaver import FolderTarget

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


def _tables(env):
    return {
        row["tableName"].lower()
        for row in env.query(f"SHOW TABLES IN {env.schema_name('DWG')}")
    }


def _views(env):
    return {
        row["viewName"].lower()
        for row in env.query(f"SHOW VIEWS IN {env.schema_name('DWG')}")
    }


def _readable(env) -> None:
    """Every declared object is present and answers a read.

    Asserted after each transition rather than once at the end, because "the
    estate still works" is a claim about a moment.
    """

    assert {"customer", "order"} <= _tables(env)
    assert {"activecustomer", "activecustomersummary"} <= _views(env)
    assert env.store.exists(_folder(env, "Raw", "CustomerCsv"))
    assert _scalar(env.query("SELECT count(*) AS n FROM {{object:DWG.Customer}}")) == 0
    assert (
        _scalar(env.query("SELECT CustomerCount FROM {{object:DWG.ActiveCustomerSummary}}"))
        == 0
    )


# --- the body a developer's own code runs --------------------------------------
#
# One round trip into the environment. The body is a single string run wherever
# the environment runs — in this process against local Spark, or inside a Fabric
# session over Livy — so what is asserted is genuinely the same code either side.
# Its only transport-dependent line is ``resolver``, which the environment binds
# before the body starts.
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


def _assert_installed(env, step) -> None:
    """The first build: everything declared exists, and nothing landed elsewhere."""

    plan = step.bundle.plan
    assert plan.format_version == 1
    assert plan.repository_name == "weaver_items"
    assert {target.logical_item_name for target in plan.targets} == {"Sales", "_weaver"}
    assert plan.omitted_nodes == ()

    # The source repository was deleted between generating this bundle and
    # installing it. A bundle carries everything it needs; it is not a pointer
    # back at a repository that has to still be there.
    assert step.outcome.status == "succeeded", step.outcome.action_error
    assert list(step.outcome.action_order) == [
        action.id for _s, _b, action in plan.actions()
    ]
    assert step.outcome.bundle_id == step.bundle.bundle_id

    _readable(env)

    columns = {
        row["col_name"].lower()
        for row in env.query("DESCRIBE TABLE {{object:DWG.Customer}}")
        if row["col_name"] and not row["col_name"].startswith("#")
    }
    assert {"customerid", "customername", "isactive"} <= columns
    assert AUDIT <= columns

    # A name alone cannot say which Lakehouse answered, so the storage is asked.
    # Case-insensitively and not by exact path: the physical name is the
    # workspace's to choose, and Fabric lowercases a managed table's directory
    # exactly as the local metastore does.
    stored = {
        entry.name.lower()
        for entry in env.store.list(env.resolver.tables_root(env.target).join("DWG"))
        if entry.is_directory
    }
    assert "customer" in stored

    # The control plane is not the destination. This is the assertion two-part
    # names made impossible: an unqualified CREATE TABLE lands in the attached
    # Lakehouse, and a read through the same session would find it and pass.
    weaver = env.weaver_destination
    assert not env.schema_exists("DWG", destination=weaver)
    assert not env.schema_exists("Raw", destination=weaver)
    assert env.schema_exists("DWG")

    assert env.store.exists(step.bundle.location.join("install-report.yml"))

    at = step.sequence_of

    def when(name):
        return next(seq for action_id, seq in at.items() if action_id.endswith(name))

    assert when("DWG.Customer") < when("DWG.ActiveCustomer") < when(
        "DWG.ActiveCustomerSummary"
    )
    assert when("DWG.Customer") < when("DWG.Order")


def _assert_authored_objects_reach_the_build(env) -> None:
    """A developer's own classes, resolving to what the build just made."""

    reached = env.run_python(AUTHORED)

    assert reached["ids"] == ["DWG.Order", "DWG.Customer", "Raw.CustomerCsv"]
    assert reached["table_path"] == reached["resolved_table_path"]
    assert reached["order_rows"] == 0
    assert set(reached["order_columns"]) == {"orderid", "customerid", "amount"} | AUDIT
    assert reached["customer_rows"] == 0
    assert set(reached["customer_columns"]) == {
        "customerid",
        "customername",
        "isactive",
    } | AUDIT
    assert reached["empty_columns"] == reached["order_columns"]

    # No mount anywhere in it: the target Lakehouse is not the attached one on
    # Fabric, and a folder still resolves — the point of addressing it through
    # the Lakehouse's own root.
    assert reached["folder_path"] == reached["resolved_folder_path"]
    assert reached["staging_folder"] == f"{reached['folder_path']}_Staging"
    assert reached["staging_folder"] == reached["resolved_staging_path"]


def _assert_unchanged(env, step) -> None:
    """Building an already-correct estate must cost nothing and break nothing."""

    assert step.outcome.status == "succeeded", step.outcome.action_error

    physical = step.kinds() - BOOKKEEPING
    assert physical == set(), (
        f"a build with nothing to do performed {sorted(physical)} — something was "
        "rebuilt that did not need to be"
    )

    # Doing nothing has to mean nothing, not quietly dropping something. Read
    # here, immediately after this transition, because that is what it claims.
    _readable(env)


def _assert_pruned(env, step) -> None:
    """What the item no longer declares goes; what it declares stays."""

    assert step.outcome.status == "succeeded", step.outcome.action_error

    tables = _tables(env)
    assert "oldtable" not in tables
    assert "oldview" not in tables
    assert not env.schema_exists("Legacy")
    assert not env.store.exists(_folder(env, "Legacy", "Stuff"))

    # Prune is the destructive direction, so what it spares is the assertion
    # worth making.
    _readable(env)

    prunes = [seq for action_id, seq in step.sequence_of.items() if "prune" in action_id]
    assert prunes, "the seeded orphans should have produced prune actions"
    builds = [
        seq
        for action_id, seq in step.sequence_of.items()
        if step.actions[action_id].startswith("build_")
    ]
    if builds:
        assert max(prunes) < min(builds), "prune must run before anything is built"


#: The summary view, rewritten. Changing it is what gives the failing transition
#: something to build: by this point the estate is correct, so an unchanged
#: repository would plan no work at all and there would be no payload to corrupt.
#: That is incremental build working — and the reason a failure case in a journey
#: has to create its own reason to run.
CHANGED_SUMMARY = """/*
View ID: DWG.ActiveCustomerSummary

Description: How many customers are active.

Lineage: $DWG.ActiveCustomer

Dependencies:
  - DWG.ActiveCustomer
*/
select
    count(1) as CustomerCount
from DWG.ActiveCustomer
"""


def _change_the_summary(env) -> None:
    env.install_repo()
    env.write_repo_file(
        "Lakehouse/Sales/DWG.ActiveCustomerSummary.sql", CHANGED_SUMMARY
    )


def _corrupt(env, bundle):
    """The same bundle with one view's payload made invalid, hashes matching."""

    from weaver.build_bundle import compute_bundle_id, write_bundle

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
    plan = replace(
        plan,
        bundle_id="",
        sequences=tuple(
            replace(
                sequence,
                batches=tuple(
                    replace(batch, actions=tuple(fix(a) for a in batch.actions))
                    for batch in sequence.batches
                ),
            )
            for sequence in plan.sequences
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        env.resolver.build_bundle("broken"),
        plan=plan,
        payloads=payloads,
        snapshot={},
        store=store,
    )


def _assert_failed(journey, step) -> None:
    """A payload that cannot run stops its barrier rather than being skipped past."""

    assert step.outcome is not None, "the failing install should have been recorded"
    assert "build_view" in step.kinds(), (
        "the changed summary should have planned a rebuild for the corruption to hit"
    )
    assert step.outcome.status == "failed"
    assert any(status == "failed" for status in step.outcome.action_status.values())

    # And the journey knows it failed, so anything after it is skipped rather
    # than asserting against a part-built estate.
    after = journey.run("after-failure")
    assert after.error is not None
    assert "broken" in str(after.error)


def test_a_lakehouse_estate_through_a_whole_build_lifecycle(lakehouse_journey):
    """Install, rebuild unchanged, prune, then fail — asserting each in turn."""

    journey = lakehouse_journey
    env = journey.env

    env.install_repo()
    _assert_installed(env, journey.run("install", between=lambda e, _b: e.remove_repo()))
    _assert_authored_objects_reach_the_build(env)

    # The source goes back before every later transition: generation reads it.
    _assert_unchanged(env, journey.run("unchanged", before=lambda e: e.install_repo()))

    _assert_pruned(env, journey.run("prune", before=lambda e: e.seed_orphans()))

    _assert_failed(
        journey,
        journey.run("broken", before=_change_the_summary, between=_corrupt),
    )
