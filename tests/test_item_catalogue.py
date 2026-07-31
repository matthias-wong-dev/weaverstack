"""Pure-Python tests for item-scoped catalogue projection and DML."""

from __future__ import annotations

from weaver.locations import Location
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.catalogue.projection import project_item_installation
from weaver.catalogue.reconcile import reconcile
from weaver.catalogue.tables import (
    ALIAS,
    CATALOGUE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    REGISTRY,
    SCHEMA_DICTIONARY,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
)

from test_item_dependencies import _dependency_estate
from test_item_repository import _estate


def _project(repository, item_text: str, target: str, *, target_kind="lakehouse"):
    item = WeaverItemId.parse(item_text)
    retained = [
        identity for identity in repository.source_documents if identity.item == item
    ]
    retained.extend(
        alias.destination
        for alias in repository.aliases
        if alias.destination.item == item
    )
    return project_item_installation(
        repository,
        item=item,
        retained=retained,
        target_name=target,
        weaver_version="1.2.3",
        target_kind=target_kind,
    )


def _registry_row(projection, schema: str, name: str):
    return next(
        row
        for row in projection.for_table(REGISTRY)
        if row["schema_name"] == schema and row["object_name"] == name
    )


def test_every_catalogue_table_is_keyed_by_exact_item_without_repository():
    for table in CATALOGUE_TABLES:
        assert table.key[:2] == ("item_type", "item_name")
        assert table.column_names[:2] == table.key[:2]
        assert "repository" not in table.column_names
        assert "object_namespace" not in table.column_names


def test_tables_and_files_with_same_name_are_distinct_registry_rows(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    projection = _project(repository, "Lakehouse/Raw", "Raw_Dev")
    rows = projection.for_table(REGISTRY)

    customer = [row for row in rows if row["object_name"] == "Customer"]
    assert {row["schema_name"] for row in customer} == {"Sales", "Files/Sales"}


def test_folder_schema_is_catalogued_as_files_slash_declared_schema(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    projection = _project(repository, "Lakehouse/Raw", "Raw_Dev")

    schemas = {
        row["schema_name"] for row in projection.for_table(SCHEMA_DICTIONARY)
    }
    assert schemas == {"Sales", "Files/Sales"}


def test_no_catalogue_table_keeps_a_hidden_namespace_dimension():
    namespace_columns = {
        "object_namespace",
        "destination_namespace",
        "source_namespace",
        "reference_namespace",
    }
    for table in CATALOGUE_TABLES:
        assert namespace_columns.isdisjoint(table.column_names)


def test_two_items_of_same_type_have_independent_scope_and_dml(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    raw = _project(repository, "Lakehouse/Raw", "Raw_Dev")
    curated = _project(repository, "Lakehouse/Curated", "Curated_Dev")

    raw_sql = "\n".join(reconcile(raw).statements)
    curated_sql = "\n".join(reconcile(curated).statements)
    assert "`item_name` = 'Raw'" in raw_sql
    assert "`item_name` = 'Curated'" not in raw_sql
    assert "`item_name` = 'Curated'" in curated_sql
    assert "`item_name` = 'Raw'" not in curated_sql


def test_rebinding_changes_only_installation_attribute_not_scope(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    first = _project(repository, "Lakehouse/Raw", "Raw_Dev")
    second = _project(repository, "Lakehouse/Raw", "Raw_Prod")

    first_row = first.for_table(INSTALLATION)[0]
    second_row = second.for_table(INSTALLATION)[0]
    assert first.scope == second.scope
    assert first_row["target_name"] == "Raw_Dev"
    assert second_row["target_name"] == "Raw_Prod"
    assert {
        key: value for key, value in first_row.items() if key != "target_name"
    } == {
        key: value for key, value in second_row.items() if key != "target_name"
    }


def test_installation_records_the_item_signature_not_the_repository_signature(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    projection = _project(repository, "Lakehouse/Raw", "Raw_Dev")
    row = projection.for_table(INSTALLATION)[0]

    assert row["signature"] == repository["Lakehouse/Raw"].signature
    assert row["signature"] != repository.signature


def test_alias_rows_reproduce_destination_and_source_canonical_identity(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(repository, "Warehouse/Reporting", "Reporting_Dev")
    row = projection.for_table(ALIAS)[0]

    assert row[SCOPE_ITEM_TYPE] == "Warehouse"
    assert row[SCOPE_ITEM_NAME] == "Reporting"
    assert row["destination_schema_name"] == "Sales"
    assert row["destination_object_name"] == "PortableCustomer"
    assert row["source_item_type"] == "Lakehouse"
    assert row["source_item_name"] == "Curated"
    assert row["source_schema_name"] == "Sales"
    assert row["source_object_name"] == "Customer"


def test_an_alias_destination_is_registered_as_the_object_it_actually_is(tmp_path):
    """No ``shortcut`` type. To every reader of the catalogue an alias in a
    Warehouse is a view, and that is what it is recorded as — its alias-ness
    lives in ``_.Alias`` and nowhere else."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(
        repository, "Warehouse/Reporting", "Reporting_Dev", target_kind="warehouse"
    )
    row = _registry_row(projection, "Sales", "PortableCustomer")

    assert row["object_type"] == "view"
    assert row["object_role"] == "data"


def test_a_lakehouse_alias_is_registered_as_a_table(tmp_path):
    """The same alias against a Lakehouse is a table — a OneLake shortcut is how
    it is made, not what it is."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(
        repository, "Warehouse/Reporting", "Reporting_Dev", target_kind="lakehouse"
    )

    assert _registry_row(projection, "Sales", "PortableCustomer")["object_type"] == "table"


def test_an_alias_signature_is_its_declaration_and_not_its_sources_content(tmp_path):
    """A rebuilt source does not redefine the alias, so it must not change its
    signature — that would replace every downstream shortcut on every reload."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(
        repository, "Warehouse/Reporting", "Reporting_Dev", target_kind="warehouse"
    )
    alias = next(
        alias
        for alias in repository.aliases
        if str(alias.destination) == "Warehouse/Reporting/Sales.PortableCustomer"
    )
    source = repository.source_documents[alias.source]

    registry = _registry_row(projection, "Sales", "PortableCustomer")
    assert registry["signature"] == alias.signature
    assert registry["signature"] != source.effective_signature
    assert projection.for_table(ALIAS)[0]["signature"] == alias.signature


def test_an_alias_describes_nothing_beyond_its_registration(tmp_path):
    """It holds no columns, no keys and no dependencies of its own. Only the two
    rows that say it exists and what it stands for."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(
        repository, "Warehouse/Reporting", "Reporting_Dev", target_kind="warehouse"
    )
    destination = ("Sales", "PortableCustomer")

    for table in CATALOGUE_TABLES:
        if table in (REGISTRY, ALIAS, SCHEMA_DICTIONARY, INSTALLATION):
            continue
        assert not [
            row
            for row in projection.for_table(table)
            if (row.get("schema_name"), row.get("object_name")) == destination
        ], f"{table.name} should hold no row for an alias destination"


def test_dependency_row_belongs_to_consumer_item_and_preserves_authored_name(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(repository, "Warehouse/Reporting", "Reporting_Dev")
    row = projection.for_table(DEPENDENCY)[0]

    assert row["item_type"] == "Warehouse"
    assert row["item_name"] == "Reporting"
    assert row["dependency_name"] == "Sales.PortableCustomer"
    assert row["is_within_item"] is False


def test_registry_merge_is_last_and_item_scoped(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    reconciliation = reconcile(_project(repository, "Lakehouse/Raw", "Raw_Dev"))

    assert reconciliation.registry.table is REGISTRY
    assert reconciliation.statements[-1] == reconciliation.registry.merge
    assert "`repository`" not in reconciliation.registry.merge
    assert "`item_type` = 'Lakehouse'" in reconciliation.registry.merge
    assert "`item_name` = 'Raw'" in reconciliation.registry.merge
