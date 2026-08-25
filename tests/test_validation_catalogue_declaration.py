"""What the catalogue records about validation, and what it does not.

Two rows describe two different things, and keeping them apart is the whole
point. ``TestDictionary`` describes the **logical** authored declaration,
what a Test is, what it compares, what key correlates it. ``Registry``
certifies the **physical** procedure or module that validation compiles to.
There is no Registry row under the logical Test ID, because nothing is
materialised there.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test
from test_validation_repository_declaration import (
    _python_assumption,
    _python_test,
    _schema,
    _table,
    _write,
)

from weaver.build_bundle.prune import TargetInventory
from weaver.catalogue.projection import project_item_catalogue
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    DEPENDENCY,
    REGISTRY,
    ROLE_ASSUMPTION,
    ROLE_DATA,
    ROLE_LOAD,
    ROLE_TEST,
    SCHEMA_DICTIONARY,
    TABLE_DICTIONARY,
    TEST_DICTIONARY,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.errors import BuildError
from weaver.locations import Location

ITEM = WeaverItemId.parse("Lakehouse/Sales")


@pytest.fixture
def repository(tmp_path):
    """One table, one Test that reads it, and one Assumption over the same."""

    _write(tmp_path, "Lakehouse/Sales/schemas/Sales.yml", _schema("Sales"))
    _write(tmp_path, "Lakehouse/Sales/Sales__Order.py", _table("Sales.Order"))
    _write(
        tmp_path,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile"),
    )
    _write(
        tmp_path,
        "Lakehouse/Sales/assumptions/Sales__OrdersHaveCustomers.py",
        _python_assumption("Sales.OrdersHaveCustomers"),
    )
    return parse_item_repository(Location(str(tmp_path)))


def project(repository):
    retained = [
        identity for identity in repository.source_documents if identity.item == ITEM
    ]
    return project_item_catalogue(repository, item=ITEM, retained=retained)


def rows(projection, table):
    return projection.for_table(table)


def by_name(projection, table):
    return {row["object_name"]: row for row in rows(projection, table)}


# --- the dictionary ---------------------------------------------------------


@weaver_test()
def test_a_test_projects_a_dictionary_row(repository):
    row = by_name(project(repository), TEST_DICTIONARY)["OrdersReconcile"]

    assert row["item_type"] == "Lakehouse"
    assert row["item_name"] == "Sales"
    assert row["schema_name"] == "Sales"
    assert row["test_type"] == "test"
    assert (
        row["description"] == "The materialised rows match the independent calculation."
    )
    assert row["primary_key"] == "Id"


@weaver_test()
def test_an_assumption_projects_a_dictionary_row(repository):
    row = by_name(project(repository), TEST_DICTIONARY)["OrdersHaveCustomers"]

    assert row["test_type"] == "assumption"
    assert row["description"] == "Every row carries a customer."


@weaver_test()
def test_an_assumption_has_no_primary_key(repository):
    """Structurally, not incidentally: there is one side to correlate."""

    assert (
        by_name(project(repository), TEST_DICTIONARY)["OrdersHaveCustomers"][
            "primary_key"
        ]
        is None
    )


@weaver_test()
def test_a_test_without_a_key_projects_a_null_key(tmp_path):
    _write(tmp_path, "Lakehouse/Sales/schemas/Sales.yml", _schema("Sales"))
    _write(tmp_path, "Lakehouse/Sales/Sales__Order.py", _table("Sales.Order"))
    _write(
        tmp_path,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile").replace("\nPrimary key: Id\n", "\n"),
    )
    repository = parse_item_repository(Location(str(tmp_path)))

    assert (
        by_name(project(repository), TEST_DICTIONARY)["OrdersReconcile"]["primary_key"]
        is None
    )


@weaver_test()
def test_a_composite_key_is_projected_in_declared_order(tmp_path):
    _write(tmp_path, "Lakehouse/Sales/schemas/Sales.yml", _schema("Sales"))
    _write(tmp_path, "Lakehouse/Sales/Sales__Order.py", _table("Sales.Order"))
    _write(
        tmp_path,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile").replace(
            "Primary key: Id", "Primary key: Id, Line no"
        ),
    )
    repository = parse_item_repository(Location(str(tmp_path)))

    assert (
        by_name(project(repository), TEST_DICTIONARY)["OrdersReconcile"]["primary_key"]
        == "Id, Line no"
    )


@weaver_test()
def test_a_referenced_description_keeps_its_pointer(tmp_path):
    _write(tmp_path, "Lakehouse/Sales/schemas/Sales.yml", _schema("Sales"))
    _write(tmp_path, "Lakehouse/Sales/Sales__Order.py", _table("Sales.Order"))
    _write(
        tmp_path,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile").replace(
            "Description: The materialised rows match the independent calculation.",
            "Description: $Sales.Order",
        ),
    )
    repository = parse_item_repository(Location(str(tmp_path)))

    row = by_name(project(repository), TEST_DICTIONARY)["OrdersReconcile"]
    assert row["description"] == "A declared table."
    assert row["description_reference"] == "$Sales.Order"


@weaver_test()
def test_one_dictionary_holds_both_kinds(repository):
    """Deliberately not a second AssumptionDictionary."""

    assert {row["test_type"] for row in rows(project(repository), TEST_DICTIONARY)} == {
        "test",
        "assumption",
    }


# --- what validation does not claim -----------------------------------------


@weaver_test()
def test_a_validation_claims_no_registry_row(repository):
    """Registry certifies a physical object, and nothing exists at the Test ID."""

    registered = {row["object_name"] for row in rows(project(repository), REGISTRY)}

    assert "OrdersReconcile" not in registered
    assert "OrdersHaveCustomers" not in registered
    assert "Order" in registered


@weaver_test()
def test_a_validation_claims_no_table_dictionary_row(repository):
    described = {
        row["object_name"] for row in rows(project(repository), TABLE_DICTIONARY)
    }

    assert described == {"Order"}


@weaver_test()
def test_a_validation_puts_its_schema_to_use(repository):
    """It names a schema the item declares, so the schema is described."""

    assert {
        row["schema_name"] for row in rows(project(repository), SCHEMA_DICTIONARY)
    } >= {"Sales"}


# --- dependencies belong to the logical identity ----------------------------


@weaver_test()
def test_a_validation_dependency_names_the_logical_validation(repository):
    """Not the procedure or module it compiles to. See §14 of the design."""

    edges = {
        (row["referencing_object_name"], row["dependency_reference"])
        for row in rows(project(repository), DEPENDENCY)
    }

    # The reference is kept exactly as the author wrote it, and a Python
    # dependency is written as an import.
    assert ("OrdersReconcile", "Sales__Order") in edges
    assert ("OrdersHaveCustomers", "Sales__Order") in edges


@weaver_test()
def test_a_validation_dependency_is_within_the_item(repository):
    row = next(
        row
        for row in rows(project(repository), DEPENDENCY)
        if row["referencing_object_name"] == "OrdersReconcile"
    )

    assert row["referenced_item_type"] == "Lakehouse"
    assert row["referenced_item_name"] == "Sales"


# --- the role survives the round trip ---------------------------------------


#: A Warehouse item, because a stored-procedure identity only belongs to one,
#: and a procedure is the shape a load artefact and a Test artefact share, which
#: is the confusion these tests exist to rule out.
WAREHOUSE_ITEM = WeaverItemId.parse("Warehouse/Reporting")


def _registry_row(object_name: str, *, object_role: str, object_type: str = "table"):
    return {
        "item_type": WAREHOUSE_ITEM.item_type,
        "item_name": WAREHOUSE_ITEM.item_name,
        "schema_name": "_" if object_type == "stored_procedure" else "Sales",
        "object_name": object_name,
        "object_type": object_type,
        "object_role": object_role,
        "signature": f"signature-{object_name}",
    }


def _read_back(*registry_rows):
    return Catalogue({WAREHOUSE_ITEM: {REGISTRY.name: tuple(registry_rows)}}).registered


@pytest.mark.parametrize(
    "role,object_type",
    [
        (ROLE_DATA, "table"),
        (ROLE_LOAD, "stored_procedure"),
        (ROLE_TEST, "stored_procedure"),
        (ROLE_ASSUMPTION, "stored_procedure"),
    ],
)
@weaver_test()
def test_the_object_role_survives_reading_the_registry(role, object_type):
    """It is the only place the answer survives, so it must not be dropped."""

    registered = _read_back(
        _registry_row("Thing", object_role=role, object_type=object_type)
    )

    assert next(iter(registered.values())).object_role == role


@weaver_test()
def test_a_runtime_artefact_is_known_by_its_role_not_its_shape():
    """A Test procedure and a load procedure are the same shape."""

    registered = _read_back(
        _registry_row("Order", object_role=ROLE_DATA),
        _registry_row(
            "Load Sales.Order", object_role=ROLE_LOAD, object_type="stored_procedure"
        ),
        _registry_row(
            "Test Sales.Reconciles",
            object_role=ROLE_TEST,
            object_type="stored_procedure",
        ),
    )
    by_name = {
        identity.object_id.object: document for identity, document in registered.items()
    }

    assert not by_name["Order"].is_runtime_artefact
    assert by_name["Load Sales.Order"].is_runtime_artefact
    assert by_name["Test Sales.Reconciles"].is_runtime_artefact
    assert by_name["Test Sales.Reconciles"].is_validation
    assert not by_name["Load Sales.Order"].is_validation


@weaver_test()
def test_an_unknown_role_is_refused_rather_than_guessed():
    with pytest.raises(BuildError, match="unsupported object_role"):
        _read_back(_registry_row("Order", object_role="whatever"))


@weaver_test()
def test_a_missing_role_is_refused_rather_than_assumed_to_be_data():
    row = _registry_row("Order", object_role=ROLE_DATA)
    del row["object_role"]

    with pytest.raises(BuildError, match="unsupported object_role"):
        _read_back(row)


# --- and it converges ---------------------------------------------------------
#
# A validation is registered as the artefact it compiles to, so what classifies
# its declaration is that artefact's row. Without this, a validation is "new" on
# every build for as long as it exists: an estate that never converges, and a
# build that always reports having selected something.


def _built(repository):
    """The catalogue a successful build of this repository leaves behind."""

    from weaver.build_bundle.catalogue_actions import desired_catalogue
    from weaver.build_bundle.planner import certifiable_identities
    from weaver.build_bundle.targets import LakehouseBinding
    from weaver.targets import ItemRef

    binding = LakehouseBinding(ItemRef("Sales_LH"), workspace_name="Demo")
    by_item = {ITEM: binding}
    return desired_catalogue(
        repository,
        certifiable_identities(repository, by_item),
        {ITEM: binding.to_bound_target()},
    )


def _inventory(catalogue):
    physical = {
        "file": [],
        "folder": [],
        "stored_procedure": [],
        "table": [],
        "view": [],
    }
    for identity, registered in catalogue.registered.items():
        schema = identity.object_id.schema
        name = identity.object_id.object
        if registered.object_type == "file":
            value = f"{schema}/{name}"
        elif registered.object_type == "folder":
            value = f"{schema.removeprefix('Files/')}.{name}"
        else:
            value = f"{schema}.{name}"
        physical[registered.object_type].append(value)
    return {
        ITEM: TargetInventory(
            target_id="sales",
            kind="lakehouse",
            target_name="Sales_LH",
            files=tuple(physical["file"]),
            folders=tuple(physical["folder"]),
            procedures=tuple(physical["stored_procedure"]),
            tables=tuple(physical["table"]),
            views=tuple(physical["view"]),
        )
    }


@weaver_test()
def test_an_unchanged_validation_is_not_selected_again(repository):
    """The property the whole estate's convergence rests on."""

    from weaver.build_bundle.incremental import select_build
    from weaver.build_bundle.planner import certifiable_identities
    from weaver.build_bundle.targets import LakehouseBinding
    from weaver.targets import ItemRef

    installed = _built(repository)
    selectable = certifiable_identities(
        repository,
        {ITEM: LakehouseBinding(ItemRef("Sales_LH"), workspace_name="Demo")},
    )

    selection = select_build(
        repository,
        installed.registered,
        selected=selectable,
        inventories=_inventory(installed),
    )

    assert selection.selected_for_build == ()


@weaver_test()
def test_an_edited_validation_is_selected(repository, tmp_path):
    """Guards the test above from passing by never selecting anything at all."""

    from weaver.build_bundle.incremental import select_build
    from weaver.build_bundle.planner import certifiable_identities
    from weaver.build_bundle.targets import LakehouseBinding
    from weaver.targets import ItemRef

    installed = _built(repository)
    _write(
        tmp_path,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile").replace(
            "Description:", "Description: edited,"
        ),
    )
    edited = parse_item_repository(Location(str(tmp_path)))
    selectable = certifiable_identities(
        edited, {ITEM: LakehouseBinding(ItemRef("Sales_LH"), workspace_name="Demo")}
    )

    selection = select_build(
        edited,
        installed.registered,
        selected=selectable,
        inventories=_inventory(installed),
    )

    assert [
        str(identity)
        for identity in selection.selected_for_build
        if "OrdersReconcile" in str(identity)
    ]
