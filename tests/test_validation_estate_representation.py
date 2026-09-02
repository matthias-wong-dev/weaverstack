"""Reading the installed validation estate out of the catalogue.

The claim §14 of the design turns on: a validation has **no Registry row**, so
the estate is recovered from `_.TestDictionary` and each declaration is
connected to its installed primitive by computing the artefact identity. The
same function the build claimed it with. Dependency rows are associated against
logical IDs for the same reason.

Everything here is pure. A `Catalogue` is rows, so these hand-write the rows a
real estate would hold and never open a repository, a session or a target.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import DEPENDENCY, INSTALLATION, REGISTRY, TEST_DICTIONARY
from weaver.declaration.metadata import TEST, ObjectId
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.errors import CatalogueStateError, ValidationError
from weaver.etl import validation_artefact_id
from weaver.targets import PhysicalTargetRef
from weaver.test_plan import ValidationEstate, validation_order

LAKEHOUSE = WeaverItemId.parse("Lakehouse/Sales")
WAREHOUSE = WeaverItemId.parse("Warehouse/Reporting")

LAKEHOUSE_TARGET = PhysicalTargetRef(kind="lakehouse", name="Sales_LH")
WAREHOUSE_TARGET = PhysicalTargetRef(kind="warehouse", name="Reporting_WH")


def _installation(item: WeaverItemId, target: str):
    return {
        "item_type": item.item_type,
        "item_name": item.item_name,
        "target_name": target,
        "weaver_version": "1.2.3",
        "signature": "installation",
    }


def _dictionary(
    item,
    schema,
    name,
    *,
    test_type="test",
    primary_key=None,
    description="A validation.",
):
    return {
        "item_type": item.item_type,
        "item_name": item.item_name,
        "schema_name": schema,
        "object_name": name,
        "test_type": test_type,
        "description": description,
        "description_reference": None,
        "primary_key": primary_key,
        "signature": f"signature-{name}",
    }


def _dependency(item, schema, name, dependency):
    referenced_schema, _, referenced_object = dependency.partition(".")
    return {
        "item_type": item.item_type,
        "item_name": item.item_name,
        "referencing_schema_name": schema,
        "referencing_object_name": name,
        "dependency_reference": dependency,
        "referenced_item_type": item.item_type,
        "referenced_item_name": item.item_name,
        "referenced_schema_name": referenced_schema,
        "referenced_object_name": referenced_object,
        "signature": f"signature-{name}",
    }


def _registry(identity, object_type, object_role):
    from weaver.catalogue.claims import catalogue_schema

    return {
        "item_type": identity.item.item_type,
        "item_name": identity.item.item_name,
        "schema_name": catalogue_schema(identity),
        "object_name": identity.object_id.object,
        "object_type": object_type,
        "object_role": object_role,
        "signature": "signature",
    }


def _artefact_row(item, kind, schema, name, object_type):
    from weaver.declaration.metadata import ObjectId

    identity = validation_artefact_id(item, kind, ObjectId(schema=schema, object=name))
    return identity, _registry(identity, object_type, kind.casefold())


@pytest.fixture
def catalogue():
    """One Lakehouse Test and one Warehouse Assumption, both installed."""

    lake_id, lake_row = _artefact_row(
        LAKEHOUSE, "Test", "Sales", "OrdersReconcile", "file"
    )
    house_id, house_row = _artefact_row(
        WAREHOUSE, "Assumption", "Sales", "OrdersHaveCustomers", "stored_procedure"
    )
    return (
        Catalogue(
            {
                LAKEHOUSE: {
                    INSTALLATION.name: (_installation(LAKEHOUSE, "Sales_LH"),),
                    TEST_DICTIONARY.name: (
                        _dictionary(
                            LAKEHOUSE, "Sales", "OrdersReconcile", primary_key="OrderId"
                        ),
                    ),
                    DEPENDENCY.name: (
                        _dependency(
                            LAKEHOUSE, "Sales", "OrdersReconcile", "Sales.Order"
                        ),
                    ),
                    REGISTRY.name: (
                        lake_row,
                        _registry(
                            WeaverDocumentId(
                                LAKEHOUSE, ObjectId(schema="Sales", object="Order")
                            ),
                            "table",
                            "data",
                        ),
                    ),
                },
                WAREHOUSE: {
                    INSTALLATION.name: (_installation(WAREHOUSE, "Reporting_WH"),),
                    TEST_DICTIONARY.name: (
                        _dictionary(
                            WAREHOUSE,
                            "Sales",
                            "OrdersHaveCustomers",
                            test_type="assumption",
                            description="Every order carries a customer.",
                        ),
                    ),
                    REGISTRY.name: (house_row,),
                },
            }
        ),
        lake_id,
        house_id,
    )


# --- what the estate holds ----------------------------------------------------


@weaver_test()
def test_the_estate_comes_from_the_test_dictionary(catalogue):
    rows, _lake, _house = catalogue

    estate = ValidationEstate.from_catalogue(rows)

    assert sorted(str(identity) for identity in estate.validations) == [
        "Lakehouse/Sales/Sales.OrdersReconcile",
        "Warehouse/Reporting/Sales.OrdersHaveCustomers",
    ]


@weaver_test()
def test_each_validation_carries_its_declared_contract(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    test = estate.validations[
        next(k for k in estate.validations if k.item == LAKEHOUSE)
    ]
    assert test.kind == "Test"
    assert test.primary_key == ("OrderId",)
    assert test.is_test


@weaver_test()
def test_an_assumption_reads_back_as_one(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    assumption = estate.validations[
        next(k for k in estate.validations if k.item == WAREHOUSE)
    ]
    assert assumption.kind == "Assumption"
    assert not assumption.is_test
    assert assumption.description == "Every order carries a customer."


@weaver_test()
def test_the_logical_id_is_connected_to_its_computed_artefact(catalogue):
    """The one function that joins TestDictionary to Registry."""

    rows, lake, house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    artefacts = {
        str(validation.logical): str(validation.artefact)
        for validation in estate.validations.values()
    }
    assert artefacts["Lakehouse/Sales/Sales.OrdersReconcile"] == str(lake)
    assert artefacts["Warehouse/Reporting/Sales.OrdersHaveCustomers"] == str(house)


@weaver_test()
def test_the_installed_primitive_supplies_the_object_type(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    types = {
        validation.logical.item.item_type: validation.object_type
        for validation in estate.validations.values()
    }
    assert types == {"Lakehouse": "file", "Warehouse": "stored_procedure"}


@weaver_test()
def test_a_validation_is_a_terminal_node_of_the_installed_graph(catalogue):
    """Not through Registry: there is no Registry row to go through.

    A Test reads what it validates, so its declared read is an edge into it, and
    nothing reads a Test.
    """

    rows, _lake, _house = catalogue
    dag = rows.dag()

    test = dag.node("Lakehouse/Sales/Sales.OrdersReconcile")
    assert [str(edge.upstream) for edge in dag.reads(test.identity)] == [
        "Lakehouse/Sales/Tables/Sales.Order"
    ]
    assert dag.children(test.identity) == ()


# --- a declared validation that was never installed ---------------------------


@weaver_test()
def test_a_missing_primitive_is_reported_rather_than_skipped(catalogue):
    """An estate with one fewer Test in it is the wrong answer."""

    rows, _lake, _house = catalogue
    without_registry = Catalogue(
        {
            item: {
                table: () if table == REGISTRY.name else contents
                for table, contents in tables.items()
            }
            for item, tables in rows.rows.items()
        }
    )

    estate = ValidationEstate.from_catalogue(without_registry)
    validation = next(iter(estate.validations.values()))

    assert not validation.is_installed
    with pytest.raises(ValidationError, match="is not registered"):
        validation.require_installed()


@weaver_test()
def test_an_installed_primitive_passes_the_check(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    for validation in estate.validations.values():
        validation.require_installed()


@weaver_test()
def test_a_validation_with_no_installation_row_is_refused(catalogue):
    """Registry without Installation means nobody knows where it lives."""

    rows, _lake, _house = catalogue
    unbound = Catalogue(
        {
            item: {
                table: () if table == INSTALLATION.name else contents
                for table, contents in tables.items()
            }
            for item, tables in rows.rows.items()
        }
    )

    with pytest.raises(CatalogueStateError, match="has no installation row"):
        ValidationEstate.from_catalogue(unbound)


@weaver_test()
def test_an_unknown_test_type_is_refused_rather_than_guessed(catalogue):
    rows, _lake, _house = catalogue
    broken = Catalogue(
        {
            LAKEHOUSE: {
                INSTALLATION.name: (_installation(LAKEHOUSE, "Sales_LH"),),
                TEST_DICTIONARY.name: (
                    _dictionary(LAKEHOUSE, "Sales", "Odd", test_type="probe"),
                ),
            }
        }
    )

    with pytest.raises(CatalogueStateError, match="unsupported test_type"):
        ValidationEstate.from_catalogue(broken)


# --- selection ----------------------------------------------------------------


@weaver_test()
def test_a_request_names_an_item_and_means_every_validation_it_owns(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    selected = estate.for_items([LAKEHOUSE])

    assert [validation.qualified for validation in selected] == [
        "Sales.OrdersReconcile"
    ]


@weaver_test()
def test_both_items_select_both(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    selected = estate.for_items([LAKEHOUSE, WAREHOUSE])

    assert len(selected) == 2


@weaver_test()
def test_naming_one_selects_only_it(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    found = estate.named("Sales.OrdersReconcile", [LAKEHOUSE])

    assert found.qualified == "Sales.OrdersReconcile"


@weaver_test()
def test_naming_one_that_is_not_installed_is_an_error(catalogue):
    """Reporting nothing would answer a question nobody asked."""

    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    with pytest.raises(ValidationError, match="no validation named 'Sales.Absent'"):
        estate.named("Sales.Absent", [LAKEHOUSE])


@weaver_test()
def test_the_error_lists_what_is_installed(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    with pytest.raises(ValidationError, match="Sales.OrdersReconcile"):
        estate.named("Sales.Absent", [LAKEHOUSE])


@weaver_test()
def test_the_order_is_stable(catalogue):
    rows, _lake, _house = catalogue
    estate = ValidationEstate.from_catalogue(rows)

    ordered = validation_order(estate.for_items([WAREHOUSE, LAKEHOUSE]))

    assert [str(validation.logical) for validation in ordered] == [
        "Lakehouse/Sales/Sales.OrdersReconcile",
        "Warehouse/Reporting/Sales.OrdersHaveCustomers",
    ]


@weaver_test()
def test_two_items_sharing_one_target_do_not_select_each_others_validations():
    """The selection boundary is the logical item, not the physical container.

    Both items are installed in ``Shared_WH``. Naming one runs its own check, and
    the named lookup is scoped the same way, so a name the other item owns is
    reported as not installed rather than run.
    """

    other = WeaverItemId.parse("Warehouse/Inventory")
    rows = Catalogue(
        {
            WAREHOUSE: {
                INSTALLATION.name: (_installation(WAREHOUSE, "Shared_WH"),),
                TEST_DICTIONARY.name: (
                    _dictionary(WAREHOUSE, "Sales", "OrdersHaveCustomers"),
                ),
                REGISTRY.name: (
                    _artefact_row(
                        WAREHOUSE,
                        TEST,
                        "Sales",
                        "OrdersHaveCustomers",
                        "stored_procedure",
                    )[1],
                ),
            },
            other: {
                INSTALLATION.name: (_installation(other, "Shared_WH"),),
                TEST_DICTIONARY.name: (_dictionary(other, "Stock", "LevelsAgree"),),
                REGISTRY.name: (
                    _artefact_row(
                        other, TEST, "Stock", "LevelsAgree", "stored_procedure"
                    )[1],
                ),
            },
        }
    )
    estate = ValidationEstate.from_catalogue(rows)

    assert [validation.qualified for validation in estate.for_items([WAREHOUSE])] == [
        "Sales.OrdersHaveCustomers"
    ]

    with pytest.raises(
        ValidationError, match="no validation named 'Stock.LevelsAgree'"
    ):
        estate.named("Stock.LevelsAgree", [WAREHOUSE])

    # Both installed in one Warehouse, and each request answers for its own item.
    assert len(estate.for_items([WAREHOUSE, other])) == 2
