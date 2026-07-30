"""A cross-item alias, built for real in the emulator.

Two Lakehouse items: ``Lakehouse/Raw`` produces ``DWG.Customer``, and
``Lakehouse/Curated`` declares an alias to it and then builds a view over that
alias by its own local name. Nothing in ``Curated``'s source knows where the table
actually lives, which is the whole point of an alias — and nothing in the plan
lets ``Curated`` start before ``Raw`` has finished.

This is the emulator answering AGENTS.md's first question for multi-item build:
does it work locally, with no tenant. Fabric materialises the same frozen alias as
a OneLake shortcut; here it is a filesystem link plus the catalogue registration
Fabric performs for itself.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver import DeltaTarget, ItemRef, LocalResolver, LocalStore, LocalWorkspace
from weaver.build_bundle import (
    InstallationEnvironment,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    build_uploaded_item_repository,
    effective_item_bindings,
)
from weaver.declaration.model import WeaverItemId
from weaver.initialise import initialise_weaver_lakehouse
from weaver.spark import SparkCatalogue

pytestmark = pytest.mark.spark

WEAVER = "Weaver"
PRODUCER_LH = "Raw_LH"
CONSUMER_LH = "Curated_LH"
PRODUCER = WeaverItemId.parse("Lakehouse/Raw")
CONSUMER = WeaverItemId.parse("Lakehouse/Curated")

_SCHEMA = "Schema ID: DWG\n\nDescription: |\n  Shaped business entities.\n"

_CUSTOMER = '''\
"""
Table ID: DWG.Customer

Description: One row per customer.

Lineage: A source system.

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string
"""

from weaver import Table


class DWG__Customer(Table):
    def read(self):
        return [], []
'''

_ALIASED_VIEW = """\
/*
View ID: DWG.CustomerName

Description: Customer names, read through this item's alias.

Lineage: $DWG.PortableCustomer

Dependencies:
  - DWG.PortableCustomer
*/
select CustomerId, CustomerName from DWG.PortableCustomer
"""


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def estate(tmp_path, spark):
    """A Weaver Lakehouse, a producer, a consumer, and one alias between them."""

    workspace = LocalWorkspace(workspace=tmp_path, weaver_lakehouse=WEAVER)
    store, resolver = LocalStore(), LocalResolver(workspace)
    for name in (WEAVER, PRODUCER_LH, CONSUMER_LH):
        store.make_directory(resolver.files_root(ItemRef(name)))
        store.make_directory(resolver.tables_root(ItemRef(name)))
    store.make_directory(resolver.weaver_items_root)

    root = resolver.weaver_items_root.path
    _write(root, "Lakehouse/Raw/schemas/DWG.yml", _SCHEMA)
    _write(root, "Lakehouse/Raw/DWG__Customer.py", _CUSTOMER)
    _write(root, "Lakehouse/Curated/schemas/DWG.yml", _SCHEMA)
    _write(root, "Lakehouse/Curated/DWG.CustomerName.sql", _ALIASED_VIEW)
    _write(
        root,
        "Lakehouse/Curated/alias.yml",
        "aliases:\n  DWG.PortableCustomer: Lakehouse/Raw/DWG.Customer\n",
    )

    initialise_weaver_lakehouse(
        weaver_lakehouse=ItemRef(WEAVER), workspace=workspace, store=store, spark=spark
    )
    try:
        yield workspace, store, resolver
    finally:
        for name in (WEAVER, PRODUCER_LH, CONSUMER_LH):
            place = resolver.spark_destination(ItemRef(name))
            for schema in ("_", "DWG"):
                spark.sql(
                    f"DROP SCHEMA IF EXISTS {place.qualified_schema(schema)} CASCADE"
                )


@pytest.fixture
def built(estate, spark):
    workspace, store, resolver = estate
    selected = ItemBindings(
        (
            ItemBinding(PRODUCER, LakehouseBinding(lakehouse=ItemRef(PRODUCER_LH))),
            ItemBinding(CONSUMER, LakehouseBinding(lakehouse=ItemRef(CONSUMER_LH))),
        )
    )
    result = build_uploaded_item_repository(
        resolver.weaver_items_root,
        bindings=effective_item_bindings(selected, weaver_lakehouse=WEAVER),
        environment=InstallationEnvironment(
            store=store, resolver=resolver, spark=spark, workspace=workspace
        ),
        control_lakehouse=LakehouseBinding(lakehouse=ItemRef(WEAVER)),
    )
    assert result.report.status == "succeeded", [
        f"{action.action_id}: {action.error_message}"
        for sequence in result.report.sequences
        for action in sequence.actions
        if action.status == "failed"
    ]
    return result, resolver, store


def test_the_alias_lands_in_the_consumers_tables_area_without_copying_data(built):
    _result, resolver, _store = built
    linked = resolver.delta_table(
        DeltaTarget(ItemRef(CONSUMER_LH)), "DWG", "PortableCustomer"
    )
    produced = resolver.delta_table(
        DeltaTarget(ItemRef(PRODUCER_LH)), "DWG", "Customer"
    )

    assert linked.path.is_symlink()
    assert linked.path.resolve() == produced.path.resolve()


def test_the_consumer_reads_the_producers_table_through_its_own_name(built, spark):
    _result, resolver, _store = built
    consumer = SparkCatalogue(spark, resolver.spark_destination(ItemRef(CONSUMER_LH)))

    assert consumer.exists("DWG", "PortableCustomer")
    assert consumer.exists("DWG", "CustomerName")
    assert (
        consumer.sql("SELECT count(*) AS n FROM {{object:DWG.CustomerName}}").collect()[0][0]
        == 0
    )


def test_the_producers_table_is_not_registered_in_the_consumers_own_name(built, spark):
    """An alias adds a name; it does not move the object or duplicate it."""

    _result, resolver, _store = built
    producer = SparkCatalogue(spark, resolver.spark_destination(ItemRef(PRODUCER_LH)))
    consumer = SparkCatalogue(spark, resolver.spark_destination(ItemRef(CONSUMER_LH)))

    assert producer.exists("DWG", "Customer")
    assert not producer.exists("DWG", "PortableCustomer")
    assert not consumer.exists("DWG", "Customer")


def test_the_alias_is_planned_after_the_producer_item_and_before_its_consumers(built):
    result, _resolver, _store = built
    at = {
        action.id: sequence.number
        for sequence, _batch, action in result.plan.actions()
    }

    assert (
        at["object-Lakehouse--Raw--DWG.Customer"]
        < at["alias-Lakehouse--Curated--DWG.PortableCustomer"]
        < at["object-Lakehouse--Curated--DWG.CustomerName"]
    )


def test_rebuilding_re_points_the_alias_rather_than_failing_on_it(estate, built, spark):
    """Re-running a build over its own aliases has to work."""

    workspace, store, resolver = estate
    selected = ItemBindings(
        (
            ItemBinding(PRODUCER, LakehouseBinding(lakehouse=ItemRef(PRODUCER_LH))),
            ItemBinding(CONSUMER, LakehouseBinding(lakehouse=ItemRef(CONSUMER_LH))),
        )
    )
    again = build_uploaded_item_repository(
        resolver.weaver_items_root,
        bindings=effective_item_bindings(selected, weaver_lakehouse=WEAVER),
        environment=InstallationEnvironment(
            store=store, resolver=resolver, spark=spark, workspace=workspace
        ),
        control_lakehouse=LakehouseBinding(lakehouse=ItemRef(WEAVER)),
    )

    assert again.report.status == "succeeded", [
        f"{action.action_id}: {action.error_message}"
        for sequence in again.report.sequences
        for action in sequence.actions
        if action.status == "failed"
    ]
    consumer = SparkCatalogue(spark, resolver.spark_destination(ItemRef(CONSUMER_LH)))
    assert consumer.exists("DWG", "PortableCustomer")


def test_the_local_endpoint_refresh_is_skipped_rather_than_faked(built):
    result, _resolver, _store = built
    refreshes = [
        action
        for sequence in result.report.sequences
        for action in sequence.actions
        if action.executor == "sql_endpoint"
    ]

    assert refreshes
    assert all(action.status == "succeeded" for action in refreshes)
    assert all("no SQL analytics endpoint" in action.details["skipped"] for action in refreshes)
