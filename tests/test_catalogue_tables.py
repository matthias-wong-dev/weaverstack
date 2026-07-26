"""The catalogue's fixed shape, asserted so drift is loud.

These tests exist to make an accidental change obvious. The catalogue schema is a
contract between the built-in SES that materialises it, the reader that tolerates
older shapes, the projection that fills it and the DML that writes it — four
places that must agree, and would fail subtly rather than loudly if one drifted.

The invariants that carry architectural weight are asserted structurally rather
than table by table: every key opens with the installation scope, every table
carries a signature, and the columns the plan forbids are absent.
"""

from __future__ import annotations

import pytest

from weaver.catalogue import (
    ALIAS,
    AUDIT_COLUMN_NAMES,
    CATALOGUE_REPOSITORY,
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    COLUMN_DICTIONARY,
    DEPENDENCY,
    DICTIONARY_TABLES,
    FOLDER_DICTIONARY,
    FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY,
    INSTALLATION,
    LAKEHOUSE,
    REGISTRY,
    SCHEMA_DICTIONARY,
    SIGNATURE,
    TABLE_DICTIONARY,
    WAREHOUSE,
    CatalogueColumn,
    CatalogueTable,
    table,
    target_type_for_ses_target,
)

ALL = CATALOGUE_TABLES


def test_there_are_exactly_ten_catalogue_tables():
    assert [each.name for each in ALL] == [
        "SchemaDictionary",
        "FolderDictionary",
        "TableDictionary",
        "ColumnDictionary",
        "IndexDictionary",
        "ForeignKeyDictionary",
        "Dependency",
        "Alias",
        "Installation",
        "Registry",
    ]


def test_reconciliation_order_puts_registry_last():
    """The declared order is the order a build writes them.

    Dictionaries describe, Installation records the binding, Registry certifies.
    Registry last is the invariant the whole failure model rests on, so the
    ordering is asserted here as well as in the planner.
    """

    assert ALL[-1] is REGISTRY
    assert ALL[-2] is INSTALLATION
    assert set(DICTIONARY_TABLES) == set(ALL) - {INSTALLATION, REGISTRY}


def test_every_table_lives_in_the_reserved_schema():
    assert CATALOGUE_SCHEMA == "_"
    for each in ALL:
        assert each.qualified == f"_.{each.name}"


@pytest.mark.parametrize("each", ALL, ids=lambda each: each.name)
def test_every_key_opens_with_the_installation_scope(each: CatalogueTable):
    """Scope is identity, not an extra column beside it.

    This is what makes a partial-target build safe by construction: there is no
    way to name a row without naming the installation it belongs to, so a
    comparison or a delete cannot span both target types by omission.
    """

    assert each.key[:2] == ("repository", "target_type")


@pytest.mark.parametrize("each", ALL, ids=lambda each: each.name)
def test_key_columns_lead_in_key_order(each: CatalogueTable):
    assert each.column_names[: len(each.key)] == each.key


@pytest.mark.parametrize("each", ALL, ids=lambda each: each.name)
def test_key_columns_are_not_null(each: CatalogueTable):
    for name in each.key:
        assert each.column(name).not_null, name


@pytest.mark.parametrize("each", ALL, ids=lambda each: each.name)
def test_every_table_ends_with_a_signature(each: CatalogueTable):
    assert each.column_names[-1] == SIGNATURE
    assert each.column(SIGNATURE).not_null


@pytest.mark.parametrize("each", ALL, ids=lambda each: each.name)
def test_the_audit_columns_are_physical_but_not_declared(each: CatalogueTable):
    """Weaver's audit columns are appended by build, so they are not business columns.

    They still have to be known here, because the catalogue's own DML writes them
    — including the live-row sentinel, since all three are physically not null.
    """

    assert not set(each.column_names) & set(AUDIT_COLUMN_NAMES)
    assert each.physical_columns == each.column_names + AUDIT_COLUMN_NAMES


def test_the_audit_columns_use_the_delta_snake_case_spelling():
    assert AUDIT_COLUMN_NAMES == (
        "row_insert_datetime",
        "row_update_datetime",
        "row_delete_datetime",
    )


@pytest.mark.parametrize("each", ALL, ids=lambda each: each.name)
def test_comparison_columns_are_every_non_key_column(each: CatalogueTable):
    assert each.comparison_columns == tuple(
        name for name in each.column_names if name not in each.key
    )
    # A signature that were not compared would let a changed source file pass as
    # unchanged, which is the one thing the signature exists to prevent.
    assert SIGNATURE in each.comparison_columns


@pytest.mark.parametrize("each", ALL, ids=lambda each: each.name)
def test_every_column_declares_a_type_and_a_description(each: CatalogueTable):
    for column in each.columns:
        assert column.type in ("string", "boolean", "timestamp"), column.name
        assert column.description, f"{each.name}.{column.name}"


# --- the individual shapes ---------------------------------------------------


def test_installation_is_keyed_on_the_scope_alone():
    """One installation per repository per target type — so the key is the scope.

    The bound item's name is an attribute of the installation. Rebinding to a
    different Lakehouse is an update to this row, not a second installation, and
    the key is what guarantees a renderer cannot express the alternative.
    """

    assert INSTALLATION.key == ("repository", "target_type")
    assert INSTALLATION.column_names == (
        "repository",
        "target_type",
        "target_name",
        "weaver_version",
        "signature",
    )
    assert "target_name" in INSTALLATION.comparison_columns


def test_registry_says_what_and_what_for_and_nothing_about_the_source():
    assert REGISTRY.key == ("repository", "target_type", "schema_name", "object_name")
    assert REGISTRY.column_names == (
        "repository",
        "target_type",
        "schema_name",
        "object_name",
        "object_type",
        "object_role",
        "signature",
    )
    # The installed repository is read for load, so the catalogue does not need to
    # repeat where a file was or what language it was in.
    assert not {"source_language", "source_path"} & set(REGISTRY.column_names)


def test_the_column_dictionary_is_purely_descriptive():
    """It describes columns an author wrote about, not the physical shape.

    Ordinals, types and nullability are properties of a built table, and for a
    query-shaped object they are not knowable when the bundle is generated. Keeping
    them out is what lets the whole catalogue be projected at plan time from
    declared SES alone.
    """

    assert COLUMN_DICTIONARY.column_names == (
        "repository",
        "target_type",
        "schema_name",
        "object_name",
        "column_name",
        "description",
        "description_reference",
        "is_identity",
        "signature",
    )
    assert not {
        "column_ordinal",
        "data_type",
        "is_nullable",
        "is_audit",
        "is_primary_key",
        "primary_key_ordinal",
    } & set(COLUMN_DICTIONARY.column_names)


def test_a_logical_key_is_identified_by_its_columns_not_a_name():
    """No index name, because nothing physical is built and names would be invented.

    The key's own columns identify it, so both belong to the row's identity — which
    also means a table may declare several alternate keys without collision.
    """

    assert INDEX_DICTIONARY.key == (
        "repository",
        "target_type",
        "schema_name",
        "object_name",
        "index_type",
        "column_set",
    )
    assert "index_name" not in INDEX_DICTIONARY.column_names
    assert "is_unique" not in INDEX_DICTIONARY.column_names


def test_a_relationship_row_is_the_whole_edge():
    """Every column is key, because a relationship has no name to be keyed by.

    Two objects may be related several times over and an object may reference
    itself, so nothing narrower than the whole edge identifies a row.
    """

    assert FOREIGN_KEY_DICTIONARY.key == FOREIGN_KEY_DICTIONARY.column_names[:-1]
    assert FOREIGN_KEY_DICTIONARY.comparison_columns == (SIGNATURE,)
    assert "foreign_key_name" not in FOREIGN_KEY_DICTIONARY.column_names


def test_a_dependency_reference_is_three_logical_parts_and_no_more():
    """The reference stays logical and inherits the owner's target type.

    Recording a target type or a physical name on the reference would let a
    Warehouse object appear to depend directly on a Lakehouse object. Crossing
    engines is an alias, and aliases are a separate table.
    """

    assert DEPENDENCY.key == (
        "repository",
        "target_type",
        "schema_name",
        "object_name",
        "dependency_repository",
        "dependency_schema_name",
        "dependency_object_name",
    )
    assert not {
        "dependency_target_type",
        "dependency_target_name",
        "dependency_type",
    } & set(DEPENDENCY.column_names)
    assert DEPENDENCY.column("is_within_repository").type == "boolean"


def test_an_alias_is_keyed_by_the_target_type_it_publishes_into():
    """There will be more kinds of alias than two, so the kind is part of identity."""

    assert ALIAS.key == (
        "repository",
        "target_type",
        "schema_name",
        "object_name",
        "alias_target_type",
    )
    assert ALIAS.column_names[-4:-1] == (
        "alias_target_type",
        "alias_schema_name",
        "alias_object_name",
    )


def test_the_table_dictionary_holds_tables_and_views_together():
    assert TABLE_DICTIONARY.column("object_type").not_null
    for expected in (
        "primary_key",
        "not_null_columns",
        "identity_column",
        "comparison_columns",
        "is_incremental",
        "is_static",
        "prohibit_rebuild",
    ):
        assert expected in TABLE_DICTIONARY.column_names


def test_a_folder_keeps_its_two_part_identity_and_its_file_key():
    assert FOLDER_DICTIONARY.key == (
        "repository",
        "target_type",
        "schema_name",
        "object_name",
    )
    assert "file_key" in FOLDER_DICTIONARY.column_names
    assert "path" not in FOLDER_DICTIONARY.column_names


def test_the_schema_dictionary_describes_a_schema_within_one_installation():
    assert SCHEMA_DICTIONARY.key == ("repository", "target_type", "schema_name")


# --- lookups and vocabulary --------------------------------------------------


def test_a_table_is_reachable_by_name():
    assert table("Registry") is REGISTRY


def test_an_unknown_table_name_lists_the_real_ones():
    with pytest.raises(KeyError, match="Registry"):
        table("Nonexistent")


def test_three_ses_target_kinds_collapse_to_two_installation_types():
    assert target_type_for_ses_target("folder") == LAKEHOUSE
    assert target_type_for_ses_target("delta") == LAKEHOUSE
    assert target_type_for_ses_target("sql") == WAREHOUSE


def test_an_unrecognised_ses_target_kind_is_refused():
    with pytest.raises(KeyError, match="unrecognised SES target kind"):
        target_type_for_ses_target("eventhouse")


def test_the_built_in_repository_is_named():
    assert CATALOGUE_REPOSITORY == "_weaver"


# --- the definition guards itself -------------------------------------------


def test_a_table_whose_key_omits_the_scope_cannot_be_declared():
    with pytest.raises(ValueError, match="installation scope"):
        CatalogueTable(
            name="Bad",
            description="x",
            key=("schema_name", "repository"),
            columns=(
                CatalogueColumn("schema_name", not_null=True, description="x"),
                CatalogueColumn("repository", not_null=True, description="x"),
                CatalogueColumn("signature", not_null=True, description="x"),
            ),
        )


def test_a_table_whose_key_does_not_lead_cannot_be_declared():
    with pytest.raises(ValueError, match="key columns must lead"):
        CatalogueTable(
            name="Bad",
            description="x",
            key=("repository", "target_type"),
            columns=(
                CatalogueColumn("target_type", not_null=True, description="x"),
                CatalogueColumn("repository", not_null=True, description="x"),
                CatalogueColumn("signature", not_null=True, description="x"),
            ),
        )


def test_a_table_without_a_trailing_signature_cannot_be_declared():
    with pytest.raises(ValueError, match="signature must be the last"):
        CatalogueTable(
            name="Bad",
            description="x",
            key=("repository", "target_type"),
            columns=(
                CatalogueColumn("repository", not_null=True, description="x"),
                CatalogueColumn("target_type", not_null=True, description="x"),
            ),
        )


def test_a_nullable_key_column_cannot_be_declared():
    with pytest.raises(ValueError, match="must be not null"):
        CatalogueTable(
            name="Bad",
            description="x",
            key=("repository", "target_type"),
            columns=(
                CatalogueColumn("repository", not_null=True, description="x"),
                CatalogueColumn("target_type", description="x"),
                CatalogueColumn("signature", not_null=True, description="x"),
            ),
        )


def test_a_signature_in_the_key_cannot_be_declared():
    """It must compare, because that is what makes a changed source file a changed row.

    In the key it would leave the table with nothing to compare and the merge's
    MATCHED guard empty.
    """

    with pytest.raises(ValueError, match="never part of the key"):
        CatalogueTable(
            name="Bad",
            description="x",
            key=("repository", "target_type", "signature"),
            columns=(
                CatalogueColumn("repository", not_null=True, description="x"),
                CatalogueColumn("target_type", not_null=True, description="x"),
                CatalogueColumn("signature", not_null=True, description="x"),
            ),
        )
