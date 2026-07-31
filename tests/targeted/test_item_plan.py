"""One item's physical plan, from prepared inputs.

The seam between deciding *what* to build and assembling a whole bundle. Ordering
claims live here — prune before drop before schema before build before refresh —
and they are the claims that used to require generating a bundle, or installing
one against Fabric, to see.

Everything above the item stays out: no item layers, no catalogue publication, no
control-plane target, no bundle identity. So a failure here means this item's
plan is wrong, and cannot mean that catalogue reconciliation or bundle assembly
is wrong.
"""

from __future__ import annotations

import pytest
from factories import (
    bound_target,
    document_id,
    folder_document,
    item_id,
    lakehouse_table,
    registered_document,
    single_document_repository,
    spark_view,
    target_inventory,
)

from weaver.build_bundle import plan_item_build


def plan(repository, **overrides):
    """Plan the one item, with everything unselected unless a test says otherwise."""

    item = overrides.pop("item", item_id())
    target = overrides.pop("target", bound_target())
    inventory = overrides.pop("inventory", target_inventory())
    arguments = {
        "selected_documents": set(),
        "selected_aliases": set(),
        "selected_for_drop": set(),
        "selected_for_build": set(),
        "registered": {},
    }
    arguments.update(overrides)
    return plan_item_build(
        repository,
        item=item,
        target=target,
        inventory=inventory,
        target_by_item={item: target},
        **arguments,
    )


def phases(planned) -> list[str]:
    return [stage.phase for stage in planned.stages]


def kinds(planned) -> list[str]:
    return [
        action.kind
        for stage in planned.stages
        for batch in stage.batches
        for action in batch.actions
    ]


@pytest.fixture
def customer(tmp_path):
    return single_document_repository(
        tmp_path, documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )


# --- the ordinary case --------------------------------------------------------


def test_a_new_table_gets_its_schema_created_before_it_is_built(customer):
    """The schema has to exist first, and the inventory says it does not."""

    identity = document_id("DWG.Customer")

    planned = plan(
        customer,
        inventory=target_inventory(),  # nothing exists yet
        selected_documents={identity},
        selected_for_build={identity},
    )

    assert phases(planned).index("schema") < phases(planned).index("build")
    assert kinds(planned)[:2] == ["create_schema", "build_table"]


def test_an_existing_schema_is_not_created_again(customer):
    identity = document_id("DWG.Customer")

    planned = plan(
        customer,
        inventory=target_inventory(schemas=("DWG",)),
        selected_documents={identity},
        selected_for_build={identity},
    )

    assert "create_schema" not in kinds(planned)
    assert "build_table" in kinds(planned)


def test_an_item_with_nothing_selected_plans_no_work_at_all(customer):
    """The unchanged case. An already-correct estate must cost nothing."""

    planned = plan(
        customer,
        inventory=target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
        selected_documents={document_id("DWG.Customer")},
    )

    assert planned.stages == ()


def test_a_delta_mutation_closes_the_item_with_an_endpoint_refresh(customer):
    """A Lakehouse's SQL endpoint lags its Delta tables, so the item ends here."""

    identity = document_id("DWG.Customer")

    planned = plan(
        customer, selected_documents={identity}, selected_for_build={identity}
    )

    assert phases(planned)[-1] == "refresh"
    assert kinds(planned)[-1] == "refresh_sql_endpoint"


# --- prune and drop -----------------------------------------------------------


def test_an_unmanaged_object_is_pruned_before_anything_is_built(customer):
    """Prune is the destructive direction and must precede the constructive one."""

    identity = document_id("DWG.Customer")

    planned = plan(
        customer,
        inventory=target_inventory(schemas=("DWG",), tables=("DWG.OldTable",)),
        selected_documents={identity},
        selected_for_build={identity},
    )

    assert kinds(planned) == ["prune_table", "build_table", "refresh_sql_endpoint"]


def test_a_declared_object_present_in_the_inventory_is_never_pruned(customer):
    """What prune spares is the assertion worth making — it is destructive."""

    identity = document_id("DWG.Customer")

    planned = plan(
        customer,
        inventory=target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
        selected_documents={identity},
        selected_for_build={identity},
    )

    assert "prune_table" not in kinds(planned)


def test_an_object_being_rebuilt_is_dropped_before_it_is_built(customer):
    """A drop clears the way for a rebuild; it is not how a removal is handled.

    The two are different and the difference is easy to lose. A *drop* is Weaver
    removing something it certified, whose type it knows from the Registry, and
    which is still declared — so it is ordered through the repository's own
    dependency graph, dependants first. A *prune* removes something Weaver never
    claimed, found by diffing the inventory. An object that has left the
    declaration entirely is in neither set here: it has no node in the graph, and
    its Registry claim is retired by catalogue reconciliation.
    """

    identity = document_id("DWG.Customer")

    planned = plan(
        customer,
        inventory=target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
        selected_documents={identity},
        selected_for_drop={identity},
        selected_for_build={identity},
        registered={identity: registered_document(identity)},
    )

    assert kinds(planned).index("drop_table") < kinds(planned).index("build_table")


def test_a_drop_needs_the_registry_to_say_what_type_the_object_is(customer):
    """Weaver drops what it certified, by the type it certified — not by guessing.

    A registered type that is not a folder, table or view is a corrupt claim, and
    dropping on a guess would issue the wrong statement against a real object.
    """

    from weaver.errors import BuildError

    identity = document_id("DWG.Customer")

    with pytest.raises(BuildError, match="unsupported type"):
        plan(
            customer,
            selected_documents={identity},
            selected_for_drop={identity},
            registered={
                identity: registered_document(identity, object_type="procedure")
            },
        )


def test_an_object_belonging_to_another_item_is_left_alone(customer):
    """Items are planned one at a time and must not reach across."""

    other = document_id("Lakehouse/Other/DWG.Thing")

    planned = plan(
        customer,
        inventory=target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
        selected_documents={document_id("DWG.Customer"), other},
        selected_for_build={other},
    )

    assert kinds(planned) == []


# --- dependency ordering within the item --------------------------------------


def test_a_view_is_built_after_the_table_it_reads(tmp_path):
    """Inside an item, the document graph orders the work."""

    repository = single_document_repository(
        tmp_path,
        documents={
            "DWG__Customer.py": lakehouse_table("DWG.Customer"),
            "DWG.ActiveCustomer.sql": spark_view(
                "DWG.ActiveCustomer", depends_on="DWG.Customer"
            ),
        },
    )
    table = document_id("DWG.Customer")
    view = document_id("DWG.ActiveCustomer")

    planned = plan(
        repository,
        inventory=target_inventory(schemas=("DWG",)),
        selected_documents={table, view},
        selected_for_build={table, view},
    )

    ordered = [
        action.resource_node_id
        for stage in planned.stages
        for batch in stage.batches
        for action in batch.actions
        if action.kind.startswith("build_")
    ]
    assert ordered.index("Lakehouse/Sales/DWG.Customer") < ordered.index(
        "Lakehouse/Sales/DWG.ActiveCustomer"
    )


def test_a_warehouse_item_orders_its_objects_by_dependency(tmp_path):
    """The same ordering claim on the SQL side, and it needs no Warehouse.

    A Warehouse build used to prove this by installing into Fabric and reading
    sequence numbers off the bundle. The ordering is a property of the item's
    document graph, so it is decided here — what Fabric can say is only whether
    the statements are valid, which is a different test.
    """

    from factories import warehouse_table, warehouse_view

    repository = single_document_repository(
        tmp_path,
        item="Warehouse/Reporting",
        documents={
            "DWG.Customer.sql": warehouse_table("DWG.Customer"),
            "DWG.CustomerOrder.sql": warehouse_table(
                "DWG.CustomerOrder",
                select="select CustomerId from [DWG].[Customer]",
            ),
            "DWG.Summary.sql": warehouse_view(
                "DWG.Summary",
                select="select CustomerId from [DWG].[CustomerOrder]",
                depends_on="DWG.CustomerOrder",
            ),
        },
    )
    item = item_id("Warehouse/Reporting")
    selected = {key for key in repository.source_documents if key.item == item}
    target = bound_target(kind="warehouse", item_id="Reporting_WH")

    planned = plan(
        repository,
        item=item,
        target=target,
        selected_documents=selected,
        selected_for_build=selected,
    )

    ordered = [
        action.resource_node_id
        for stage in planned.stages
        for batch in stage.batches
        for action in batch.actions
        if action.kind.startswith("build_")
    ]
    assert ordered.index("Warehouse/Reporting/DWG.Customer") < ordered.index(
        "Warehouse/Reporting/DWG.CustomerOrder"
    )
    assert ordered.index("Warehouse/Reporting/DWG.CustomerOrder") < ordered.index(
        "Warehouse/Reporting/DWG.Summary"
    )


def test_a_folder_is_planned_without_any_schema_creation(tmp_path):
    """A folder lives under Files and has no catalogue schema to create."""

    repository = single_document_repository(
        tmp_path,
        schemas=("DWG", "Raw"),
        documents={"Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv")},
    )
    identity = document_id("Lakehouse/Sales/Files/Raw.CustomerCsv")

    planned = plan(
        repository,
        selected_documents={identity},
        selected_for_build={identity},
    )

    assert "build_folder" in kinds(planned)
    assert "create_schema" not in kinds(planned)
