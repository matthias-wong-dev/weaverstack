"""Projecting catalogue rows from validated SES, for one installation.

The fixture is deliberately awkward in the ways that matter: ``Sales.Customer``
exists as both a Delta table and a Warehouse table, one edge resolves through a
cross-engine alias, one reference is a three-part physical name, and the Delta
table carries an identity column, alternate keys, a self-referencing relationship
and a column note that points at another object's note.

The property under test throughout is that every value comes from the validated
declaration or the repository's resolved graph — never from a physical table, and
never from re-reading a file.
"""

from __future__ import annotations

import pytest

from weaver import LocalStore, Location
from weaver.catalogue import (
    ALIAS,
    COLUMN_DICTIONARY,
    DEPENDENCY,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY,
    INSTALLATION,
    REGISTRY,
    SCHEMA_DICTIONARY,
    TABLE_DICTIONARY,
    InstallationScope,
)
from weaver.catalogue.projection import project_installation
from weaver.ses import IDENTITY_COLUMN_NOTE, read_repository

FIXTURE = "tests/fixtures/catalogue-estate"
LAKEHOUSE = InstallationScope(repository="catalogue-estate", target_type="lakehouse")
WAREHOUSE = InstallationScope(repository="catalogue-estate", target_type="warehouse")


@pytest.fixture(scope="module")
def repository():
    return read_repository(
        Location(value=FIXTURE), store=LocalStore(), name="catalogue-estate"
    )


def _nodes(repository, target_type: str) -> list[str]:
    """Every node that installs into one target type — a whole-side projection."""

    from weaver.catalogue import target_type_for_ses_target

    return [
        document.node_id
        for document in repository.documents
        if target_type_for_ses_target(document.target_kind) == target_type
    ]


@pytest.fixture(scope="module")
def lakehouse(repository):
    return project_installation(
        repository,
        retained=_nodes(repository, "lakehouse"),
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )


@pytest.fixture(scope="module")
def warehouse(repository):
    return project_installation(
        repository,
        retained=_nodes(repository, "warehouse"),
        scope=WAREHOUSE,
        target_name="Sales_WH",
        weaver_version="9.9.9",
    )


def _row(projection, table, **match):
    rows = [
        row
        for row in projection.for_table(table)
        if all(row.get(key) == value for key, value in match.items())
    ]
    assert len(rows) == 1, f"expected one {table.name} row for {match}, got {len(rows)}"
    return rows[0]


# --- installation scope -------------------------------------------------------


def test_every_projected_row_carries_the_installation_scope(lakehouse):
    for name, rows in lakehouse.rows.items():
        for row in rows:
            assert row["repository"] == "catalogue-estate", name
            assert row["target_type"] == "lakehouse", name


def test_the_same_object_name_projects_into_both_installations(lakehouse, warehouse):
    """``Sales.Customer`` is a Delta table and a Warehouse table. Both are real.

    They are distinct rows differing only in ``target_type``, which is the whole
    reason the scope is part of the key.
    """

    delta = _row(lakehouse, REGISTRY, schema_name="Sales", object_name="Customer")
    sql = _row(warehouse, REGISTRY, schema_name="Sales", object_name="Customer")
    assert delta["target_type"] == "lakehouse"
    assert sql["target_type"] == "warehouse"
    assert delta["signature"] != sql["signature"]


def test_a_lakehouse_projection_contains_no_warehouse_object(lakehouse):
    names = {row["object_name"] for row in lakehouse.for_table(REGISTRY)}
    assert "CustomerView" not in names


def test_projecting_an_object_that_installs_elsewhere_is_refused(repository):
    """A subgraph from another build would write rows into the wrong installation."""

    with pytest.raises(ValueError, match="install elsewhere"):
        project_installation(
            repository,
            retained=["sql:Rpt.CustomerView"],
            scope=LAKEHOUSE,
            target_name="Sales_LH",
            weaver_version="9.9.9",
        )


# --- registry and installation ------------------------------------------------


def test_registry_records_what_was_installed_and_what_it_is_for(lakehouse):
    folder = _row(lakehouse, REGISTRY, object_name="CustomerCsv")
    assert folder["object_type"] == "folder"
    assert folder["object_role"] == "data"
    table = _row(lakehouse, REGISTRY, object_name="Region")
    assert table["object_type"] == "table"


def test_a_view_is_registered_as_a_view(warehouse):
    view = _row(warehouse, REGISTRY, object_name="CustomerView")
    assert view["object_type"] == "view"


def test_registry_signatures_are_the_source_file_hashes(lakehouse, repository):
    row = _row(lakehouse, REGISTRY, object_name="Region")
    assert row["signature"] == repository["delta:Sales.Region"].source_hash


def test_the_installation_row_names_the_bound_item_and_the_weaver_version(lakehouse, repository):
    row = _row(lakehouse, INSTALLATION)
    assert row["target_name"] == "Sales_LH"
    assert row["weaver_version"] == "9.9.9"
    # The repository as a whole, not one file — this row is about the installation.
    assert row["signature"] == repository.signature


def test_there_is_exactly_one_installation_row(lakehouse):
    assert len(lakehouse.for_table(INSTALLATION)) == 1


# --- schemas -------------------------------------------------------------------


def test_only_schemas_the_installation_uses_are_projected(lakehouse, warehouse):
    assert {row["schema_name"] for row in lakehouse.for_table(SCHEMA_DICTIONARY)} == {
        "Sales"
    }
    # Rpt is used by the Warehouse side only, and a schema this installation never
    # created must not be claimed as part of it.
    assert {row["schema_name"] for row in warehouse.for_table(SCHEMA_DICTIONARY)} == {
        "Sales",
        "Rpt",
    }


def test_a_schema_row_carries_its_declaration_s_description_and_hash(lakehouse, repository):
    row = _row(lakehouse, SCHEMA_DICTIONARY, schema_name="Sales")
    assert row["description"].startswith("Customers and their regions")
    assert row["signature"] == repository.schemas["Sales"].source_hash
    # A schema's Description is plain text; there is no reference form to record.
    assert row["description_reference"] is None


# --- tables, views and folders -------------------------------------------------


def test_a_table_row_carries_its_declared_contract(lakehouse):
    row = _row(lakehouse, TABLE_DICTIONARY, object_name="Customer")
    assert row["object_type"] == "table"
    assert row["primary_key"] == "Customer key"
    assert row["not_null_columns"] == "Customer id"
    assert row["identity_column"] == "Customer key"
    assert row["comparison_columns"] == "Customer name, Region code"
    assert row["is_incremental"] is False
    assert row["is_static"] is False
    assert row["prohibit_rebuild"] is False


def test_a_static_table_says_so(lakehouse):
    assert _row(lakehouse, TABLE_DICTIONARY, object_name="Region")["is_static"] is True


def test_tables_and_views_share_one_dictionary(warehouse):
    types = {
        row["object_name"]: row["object_type"]
        for row in warehouse.for_table(TABLE_DICTIONARY)
    }
    assert types == {"Customer": "table", "CustomerView": "view"}


def test_a_folder_keeps_its_two_part_identity_and_its_ordered_file_keys(lakehouse):
    row = _row(lakehouse, FOLDER_DICTIONARY, object_name="CustomerCsv")
    assert row["schema_name"] == "Sales"
    # Declared order preserved, comma-separated like every other column set.
    assert row["file_key"] == "customer_*.csv, region_*.csv"
    # A folder defaults to incremental and prohibits rebuild — deleting managed
    # files is not something a build does casually.
    assert row["is_incremental"] is True
    assert row["prohibit_rebuild"] is True


def test_a_folder_is_not_in_the_table_dictionary(lakehouse):
    assert "CustomerCsv" not in {
        row["object_name"] for row in lakehouse.for_table(TABLE_DICTIONARY)
    }


def test_an_inferred_spark_table_projects_without_knowing_its_columns(lakehouse):
    """The whole reason the catalogue can be projected at plan time.

    ``Sales.CustomerSummary`` declares no schema — its shape is settled at install
    time by running its query. Every catalogue value it needs is still declared, so
    nothing waits on the engine.
    """

    row = _row(lakehouse, TABLE_DICTIONARY, object_name="CustomerSummary")
    assert row["primary_key"] == "Region code"
    assert row["comparison_columns"] is None


# --- descriptions copied from a reference --------------------------------------


def test_prose_written_here_is_recorded_with_no_reference(lakehouse):
    row = _row(lakehouse, TABLE_DICTIONARY, object_name="Region")
    assert row["description"] == "One row per sales region."
    assert row["description_reference"] is None


def test_a_referenced_description_is_copied_and_its_pointer_kept(lakehouse):
    row = _row(lakehouse, TABLE_DICTIONARY, object_name="CustomerSummary")
    assert row["description"] == "One row per customer known to the sales system."
    assert row["description_reference"] == "$Sales.Customer"


def test_a_referenced_lineage_is_copied_from_the_target_s_description(lakehouse):
    row = _row(lakehouse, TABLE_DICTIONARY, object_name="Customer")
    assert row["lineage"] == "Raw customer export files."
    assert row["lineage_reference"] == "$Sales.CustomerCsv"


def test_the_warehouse_table_s_lineage_resolves_to_the_delta_table_of_the_same_id(
    warehouse,
):
    """The cross-target case a documentation reference exists for.

    ``$Sales.Customer`` on the Warehouse ``Sales.Customer`` cannot mean itself, so
    it resolves to the Delta table sharing its ID.
    """

    row = _row(warehouse, TABLE_DICTIONARY, object_name="Customer")
    assert row["lineage"] == "One row per customer known to the sales system."
    assert row["lineage_reference"] == "$Sales.Customer"


# --- columns -------------------------------------------------------------------


def test_only_columns_an_author_described_are_projected(lakehouse):
    """Descriptive, not exhaustive — five declared columns, two described."""

    columns = {
        row["column_name"]
        for row in lakehouse.for_table(COLUMN_DICTIONARY)
        if row["object_name"] == "Customer"
    }
    assert columns == {"Customer key", "Customer id", "Last modified"}


def test_the_identity_column_is_projected_with_a_generic_note(lakehouse):
    row = _row(lakehouse, COLUMN_DICTIONARY, object_name="Customer", column_name="Customer key")
    assert row["is_identity"] is True
    assert row["description"] == IDENTITY_COLUMN_NOTE


def test_an_authored_column_note_is_not_marked_as_identity(lakehouse):
    row = _row(lakehouse, COLUMN_DICTIONARY, object_name="Customer", column_name="Customer id")
    assert row["is_identity"] is False
    assert row["description"] == "Natural key from the sales system."


def test_a_column_note_may_itself_be_copied_from_another_object_s_note(lakehouse):
    row = _row(
        lakehouse, COLUMN_DICTIONARY, object_name="Customer", column_name="Last modified"
    )
    assert row["description"] == "Display name, as the sales system spells it."
    assert row["description_reference"] == "$Sales.Region[Region name]"


def test_no_audit_column_is_projected(lakehouse):
    names = {row["column_name"] for row in lakehouse.for_table(COLUMN_DICTIONARY)}
    assert not names & {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}


def test_an_inferred_object_s_note_is_projected_from_its_raw_block(lakehouse):
    row = _row(
        lakehouse,
        COLUMN_DICTIONARY,
        object_name="CustomerSummary",
        column_name="Customer count",
    )
    assert row["description"] == "How many customers the region holds."


# --- logical keys ---------------------------------------------------------------


def test_the_primary_key_and_the_alternate_keys_are_separate_rows(lakehouse):
    rows = [
        row for row in lakehouse.for_table(INDEX_DICTIONARY) if row["object_name"] == "Customer"
    ]
    assert {(row["index_type"], row["column_set"]) for row in rows} == {
        ("primary_key", "Customer key"),
        ("unique", "Customer id"),
        ("unique", "Region code, Customer name"),
    }


def test_a_key_preserves_its_declared_column_order(lakehouse):
    row = _row(
        lakehouse,
        INDEX_DICTIONARY,
        object_name="Customer",
        column_set="Region code, Customer name",
    )
    assert row["index_type"] == "unique"


def test_a_view_s_logical_keys_are_projected_too(warehouse):
    rows = {
        (row["index_type"], row["column_set"])
        for row in warehouse.for_table(INDEX_DICTIONARY)
        if row["object_name"] == "CustomerView"
    }
    assert rows == {("primary_key", "Customer id"), ("unique", "Customer name")}


def test_an_object_with_no_key_projects_no_key_row(lakehouse):
    assert not [
        row
        for row in lakehouse.for_table(INDEX_DICTIONARY)
        if row["object_name"] == "CustomerFeature"
    ]


# --- relationships ---------------------------------------------------------------


def test_a_relationship_pairs_both_column_sets_in_order(lakehouse):
    row = _row(
        lakehouse, FOREIGN_KEY_DICTIONARY, object_name="Customer", column_set="Region code"
    )
    assert row["reference_schema_name"] == "Sales"
    assert row["reference_object_name"] == "Region"
    assert row["reference_column_set"] == "Region code"


def test_an_object_may_reference_itself(lakehouse):
    row = _row(
        lakehouse,
        FOREIGN_KEY_DICTIONARY,
        object_name="Customer",
        column_set="Parent customer id",
    )
    assert row["reference_object_name"] == "Customer"
    assert row["reference_column_set"] == "Customer id"


def test_a_relationship_stays_logical_and_names_the_owner_s_repository(lakehouse):
    """Two-part parents only, so the parent's repository is the owner's own.

    The column exists so the shape does not change when cross-repository
    references arrive.
    """

    for row in lakehouse.for_table(FOREIGN_KEY_DICTIONARY):
        assert row["reference_repository"] == "catalogue-estate"


def test_a_view_declares_relationships_too(warehouse):
    row = _row(warehouse, FOREIGN_KEY_DICTIONARY, object_name="CustomerView")
    assert row["column_set"] == "Customer id"
    assert row["reference_object_name"] == "Customer"


# --- dependencies -----------------------------------------------------------------


def test_a_managed_dependency_records_the_producer_within_the_repository(lakehouse):
    row = _row(
        lakehouse, DEPENDENCY, object_name="CustomerSummary", dependency_object_name="Customer"
    )
    assert row["dependency_repository"] == "catalogue-estate"
    assert row["dependency_schema_name"] == "Sales"
    assert row["is_within_repository"] is True


def test_a_dependency_row_is_scoped_to_the_owner_and_says_nothing_about_the_target(
    lakehouse,
):
    row = _row(lakehouse, DEPENDENCY, object_name="CustomerSummary")
    assert row["target_type"] == "lakehouse"
    assert not {"dependency_target_type", "dependency_target_name"} & set(row)


def test_a_three_part_reference_is_a_dependency_that_says_it_leaves_the_repository(
    warehouse,
):
    """Naming a physical target in three parts is allowed, and is a real dependency.

    The first part is an item, not a repository, so nothing here resolves it —
    which is exactly what ``is_within_repository`` false records.
    """

    row = _row(warehouse, DEPENDENCY, object_name="Customer", dependency_repository="Sales_LH")
    assert row["dependency_schema_name"] == "Sales"
    assert row["dependency_object_name"] == "Customer"
    assert row["is_within_repository"] is False


def test_an_alias_resolved_edge_records_the_alias_not_the_native_producer(repository):
    """Keeping Dependency same-namespace is what makes Alias necessary.

    ``Sales.CustomerFeature`` reads ``Sales.CustomerWarehouse``, a name the
    Warehouse ``Sales.Customer`` publishes into the Lakehouse. The dependency
    records the name that binds in the Lakehouse; Alias says what it points at.
    Joining Dependency, Alias and Registry is what completes the graph.

    A real single-side build would omit both ends of this edge — the consumer
    stands above a Warehouse producer — so the retained set is given explicitly.
    """

    projection = project_installation(
        repository,
        retained=["delta:Sales.CustomerFeature"],
        scope=LAKEHOUSE,
        target_name="Sales_LH",
        weaver_version="9.9.9",
    )
    row = _row(projection, DEPENDENCY, object_name="CustomerFeature")
    assert row["dependency_object_name"] == "CustomerWarehouse"
    assert row["is_within_repository"] is True


def test_a_dependency_whose_consumer_is_out_of_scope_is_not_projected(lakehouse):
    assert not [
        row for row in lakehouse.for_table(DEPENDENCY) if row["object_name"] == "CustomerView"
    ]


# --- aliases -----------------------------------------------------------------------


def test_an_alias_row_belongs_to_the_publishing_object_s_installation(lakehouse):
    """A Lakehouse build records the Warehouse alias its Delta table publishes.

    The alias is a property of the owner's declaration, so it is known and true
    whether or not the Warehouse installation exists yet.
    """

    row = _row(lakehouse, ALIAS, object_name="Customer")
    assert row["target_type"] == "lakehouse"
    assert row["alias_target_type"] == "warehouse"
    assert row["alias_schema_name"] == "Rpt"
    assert row["alias_object_name"] == "CustomerDelta"


def test_the_warehouse_object_publishes_its_own_alias_in_the_other_direction(warehouse):
    row = _row(warehouse, ALIAS, object_name="Customer")
    assert row["target_type"] == "warehouse"
    assert row["alias_target_type"] == "lakehouse"
    assert row["alias_object_name"] == "CustomerWarehouse"


def test_an_object_publishing_no_alias_projects_no_alias_row(lakehouse):
    assert {row["object_name"] for row in lakehouse.for_table(ALIAS)} == {"Customer"}


# --- determinism -------------------------------------------------------------------


def test_projecting_twice_produces_identical_rows(repository):
    scope = LAKEHOUSE
    nodes = _nodes(repository, "lakehouse")
    first = project_installation(
        repository, retained=nodes, scope=scope, target_name="Sales_LH", weaver_version="1"
    )
    second = project_installation(
        repository,
        retained=list(reversed(nodes)),
        scope=scope,
        target_name="Sales_LH",
        weaver_version="1",
    )
    for table_name in first.rows:
        assert sorted(map(repr, first.rows[table_name])) == sorted(
            map(repr, second.rows[table_name])
        ), table_name
