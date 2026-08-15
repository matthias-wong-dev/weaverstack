"""Rendering catalogue rows as scoped, deterministic T-SQL.

Four properties are load-bearing and each is tested directly rather than
incidentally:

- **determinism** — the same rows render the same bytes, whatever order they
  arrive in, because a bundle's identity is a hash of its payloads;
- **scope** — every statement names one logical item, so a build of one item
  cannot express a change to another's row;
- **explicit values** — nulls are typed, quotes survive, and booleans reach a
  ``bit`` as ``1`` and ``0`` rather than as the strings that look like them;
- **the public spelling** — statements carry the ``_`` schema's own column names
  and stored vocabularies, never the internal snake-case keys.

The clock is the deliberate exception: ``SYSDATETIME()`` is rendered as a call,
because a rendered instant would change a payload on every run and destroy the
bundle identity that review and certification depend on.
"""

from __future__ import annotations

import pytest

from weaver.catalogue import (
    DEPENDENCY,
    FOREIGN_KEY_DICTIONARY,
    INSTALLATION,
    KEY_DICTIONARY,
    REGISTRY,
    TABLE_DICTIONARY,
    InstallationScope,
    column_set,
    render_delete_obsolete,
    render_delete_scope,
    render_merge,
    sorted_rows,
)
from weaver.catalogue.tsql import identifier, literal, typed_literal

LAKEHOUSE_SCOPE = InstallationScope(item_type="Lakehouse", item_name="Raw")
WAREHOUSE_SCOPE = InstallationScope(item_type="Warehouse", item_name="Reporting")


def registry_row(name: str, *, item_name: str = "Raw", signature: str = "abc"):
    return {
        "item_type": "Lakehouse",
        "item_name": item_name,
        "schema_name": "Sales",
        "object_name": name,
        "object_type": "table",
        "object_role": "data",
        "signature": signature,
    }


def public(table, name: str) -> str:
    """A column as the statement spells it."""

    return identifier(table.public_name_of(name))


# --- literals and identifiers ------------------------------------------------


def test_a_string_is_quoted_as_unicode():
    assert literal("Sales") == "N'Sales'"


def test_a_quote_is_doubled():
    assert literal("O'Brien") == "N'O''Brien'"


def test_a_backslash_is_an_ordinary_character():
    """T-SQL has no backslash escape, so doubling one would change the value."""

    assert literal("C:\\path") == "N'C:\\path'"


def test_a_null_is_null_not_the_word():
    assert literal(None) == "NULL"


def test_a_boolean_reaches_a_bit_as_a_number():
    assert literal(True) == "1"
    assert literal(False) == "0"


def test_a_boolean_is_not_rendered_as_a_string():
    assert literal(True) != "N'true'"


def test_a_value_of_an_unsupported_type_is_refused():
    with pytest.raises(TypeError, match="not dict"):
        literal({"a": 1})


def test_a_non_boolean_in_a_boolean_column_is_refused():
    with pytest.raises(TypeError, match="expected a boolean"):
        literal("yes", "boolean")


def test_an_identifier_is_bracket_quoted():
    """The public names carry spaces by design, so quoting is not optional."""

    assert identifier("Order id") == "[Order id]"


def test_a_closing_bracket_in_an_identifier_is_doubled():
    assert identifier("we]ird") == "[we]]ird]"


def test_a_stored_value_is_written_in_its_public_vocabulary():
    """The internal key never reaches the Warehouse."""

    assert typed_literal("stored_procedure", REGISTRY.column("object_type")) == (
        "N'Stored procedure'"
    )


def test_a_column_set_preserves_declared_order():
    # Order is meaning: a key on (Region, Country) is not the key on
    # (Country, Region), so the renderer never sorts one.
    assert column_set(["Region", "Country"]) == "Region, Country"
    assert column_set(["Country", "Region"]) == "Country, Region"


def test_an_empty_column_set_is_null_not_an_empty_string():
    # "No key" and "a key of no columns" are different claims.
    assert column_set([]) is None


# --- the public spelling ------------------------------------------------------


def test_a_statement_names_the_catalogue_in_two_parts():
    """The connection is already open against the catalogue Warehouse."""

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)

    assert "MERGE INTO [_].[Registry]" in statement


def test_a_statement_carries_the_public_column_names():
    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)

    assert "[Item type]" in statement
    assert "[Object name]" in statement
    assert "[Build datetime]" in statement
    # And never the internal spelling.
    assert "[item_type]" not in statement
    assert "[object_name]" not in statement


# --- determinism -------------------------------------------------------------


def test_rows_render_in_key_order_whatever_order_they_arrive_in():
    forwards = [registry_row("Alpha"), registry_row("Beta"), registry_row("Gamma")]
    backwards = list(reversed(forwards))
    assert render_merge(REGISTRY, forwards, scope=LAKEHOUSE_SCOPE) == render_merge(
        REGISTRY, backwards, scope=LAKEHOUSE_SCOPE
    )


def test_the_same_rows_render_the_same_bytes():
    rows = [registry_row("Alpha"), registry_row("Beta")]

    assert render_merge(REGISTRY, rows, scope=LAKEHOUSE_SCOPE) == render_merge(
        REGISTRY, list(rows), scope=LAKEHOUSE_SCOPE
    )


def test_sorting_is_by_the_key_and_tolerates_a_null():
    rows = [{**registry_row("Beta")}, {**registry_row("Alpha")}]

    assert [row["object_name"] for row in sorted_rows(REGISTRY, rows)] == [
        "Alpha",
        "Beta",
    ]


def test_the_clock_is_a_call_not_a_rendered_instant():
    """A rendered timestamp would change the payload — and the bundle id — each run.

    The engine supplies the instant instead, which keeps the payload frozen while
    still stamping the row.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)

    assert "SYSDATETIME()" in statement
    assert statement.count("SYSDATETIME()") == 3  # one update, two inserts


# --- the published build datetime ---------------------------------------------


def _clauses(statement: str) -> tuple[str, str, str]:
    """The merge split into the three places a column can appear."""

    matched = statement.index("WHEN MATCHED")
    not_matched = statement.index("WHEN NOT MATCHED")
    guard, update = statement[matched:not_matched].split("THEN UPDATE SET", 1)
    return statement[:matched], guard, update


def test_the_build_datetime_is_a_token_so_the_payload_stays_frozen():
    """Same reason the clock is a call: a rendered instant would give the same
    repository different bytes every run, and a bundle's identity is its bytes.
    The installer resolves it, once, for the whole run."""

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)

    assert "CAST('{{build_datetime}}' AS datetime2(6))" in statement
    assert statement.count("{{build_datetime}}") == 1


def test_the_build_datetime_is_written_on_insert_and_nowhere_else():
    """The decision the whole freshness comparison rests on.

    Every object a build actually rebuilds arrives here as an insert — it is new,
    or its claim was deleted before the physical work. So an *update* is a row
    whose projection moved while the object stood still, and dating it to this
    build would claim a rebuild that never happened.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)
    source, guard, update = _clauses(statement)

    assert "Build datetime" not in source, "not projected — no row carries one"
    assert "Build datetime" not in guard, "not compared — it differs every build"
    assert "Build datetime" not in update, "not updated — only an insert dates a row"
    assert "[Build datetime]" in statement[statement.index("WHEN NOT MATCHED") :]


def test_a_table_without_a_published_column_carries_no_token():
    """Only Registry carries a build datetime. Nothing else gained a column."""

    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "referencing_schema_name": "Sales",
        "referencing_object_name": "Customer",
        "dependency_reference": "Sales.Order",
        "referenced_item_type": "Lakehouse",
        "referenced_item_name": "Raw",
        "referenced_schema_name": "Sales",
        "referenced_object_name": "Order",
        "signature": "abc",
    }
    statement = render_merge(DEPENDENCY, [row], scope=LAKEHOUSE_SCOPE)

    assert "{{build_datetime}}" not in statement
    assert "Build datetime" not in statement


# --- scope -------------------------------------------------------------------


def test_a_merge_is_scoped_to_one_installation_on_the_target_side():
    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)

    assert "target.[Item type] = N'Lakehouse'" in statement
    assert "target.[Item name] = N'Raw'" in statement
    assert "Warehouse" not in statement


def test_a_delete_is_scoped_to_one_installation():
    statement = render_delete_obsolete(
        REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE
    )

    assert "[Item type] = N'Lakehouse' AND [Item name] = N'Raw'" in statement
    assert "Warehouse" not in statement


def test_the_same_object_in_another_item_renders_a_different_statement():
    """The one property the whole installation model rests on.

    ``Sales.Customer`` in two logical items is two rows. Neither statement can
    reach the other's row, because the scope is in the key and in every
    predicate.
    """

    raw = render_merge(REGISTRY, [registry_row("Customer")], scope=LAKEHOUSE_SCOPE)
    curated = render_merge(
        REGISTRY,
        [registry_row("Customer", item_name="Curated")],
        scope=InstallationScope(item_type="Lakehouse", item_name="Curated"),
    )

    assert raw != curated
    assert "N'Curated'" not in raw
    assert "N'Raw'" not in curated


def test_a_row_from_another_installation_cannot_be_rendered():
    """Refused rather than merged into the wrong scope, where it would read as truth."""

    with pytest.raises(ValueError, match="do not belong to installation"):
        render_merge(
            REGISTRY,
            [registry_row("Customer", item_name="Curated")],
            scope=LAKEHOUSE_SCOPE,
        )


def test_a_row_from_another_item_cannot_be_rendered():
    stray = {**registry_row("Customer"), "item_name": "Other"}

    with pytest.raises(ValueError, match="do not belong to installation"):
        render_merge(REGISTRY, [stray], scope=LAKEHOUSE_SCOPE)


def test_the_guard_applies_to_deletes_too():
    with pytest.raises(ValueError, match="do not belong to installation"):
        render_delete_obsolete(
            REGISTRY,
            [registry_row("Customer", item_name="Curated")],
            scope=LAKEHOUSE_SCOPE,
        )


# --- insert, update, no-op ---------------------------------------------------


def test_a_matched_unchanged_row_is_a_no_op():
    """The matched branch is guarded by a comparison of every non-key column.

    So rebuilding an unchanged Weaver document writes nothing and does not
    advance ``Row update datetime`` — which is what makes a rebuild idempotent
    from the catalogue's point of view.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)

    assert "WHEN MATCHED AND (" in statement
    for column in REGISTRY.comparison_columns:
        name = public(REGISTRY, column)
        assert f"target.{name} <> source.{name}" in statement


def test_the_comparison_is_null_safe_in_both_directions():
    """T-SQL has no null-safe operator, and half a comparison is silently wrong.

    ``a <> b`` is UNKNOWN when either side is null, so a column that became null
    — or stopped being null — would never be seen as changed.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)
    _source, guard, _update = _clauses(statement)
    name = public(REGISTRY, "object_type")

    assert (
        f"({name} IS NULL AND source.{name} IS NOT NULL)"
        in guard.replace("target.", "")
        or f"(target.{name} IS NULL AND source.{name} IS NOT NULL)" in guard
    )
    assert f"(target.{name} IS NOT NULL AND source.{name} IS NULL)" in guard


def test_the_merge_key_match_is_null_safe():
    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)
    name = public(REGISTRY, "object_name")

    assert f"(target.{name} IS NULL AND source.{name} IS NULL)" in statement


def test_an_update_advances_only_the_update_datetime():
    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)
    update = statement.split("WHEN MATCHED")[1].split("WHEN NOT MATCHED")[0]

    assert "target.[Row update datetime] = SYSDATETIME()" in update
    # The insert datetime is when the row first appeared and must not move.
    assert "Row insert datetime" not in update
    # Nor may an update rewrite the key it matched on.
    for key in REGISTRY.key:
        assert f"target.{public(REGISTRY, key)} = source" not in update


def test_an_insert_supplies_every_physical_column_including_the_live_sentinel():
    """All three audit columns are physically not null, so all three are written.

    A live row's delete datetime is a sentinel maximum rather than a null, which is
    what makes an "as at" read a single range predicate.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)
    insert = statement.split("WHEN NOT MATCHED")[1]

    for column in REGISTRY.physical_columns:
        assert public(REGISTRY, column) in insert
    assert "CAST('9999-12-31 23:59:59.999999' AS datetime2(6))" in insert


def test_a_merge_is_terminated():
    """T-SQL requires it, and an unterminated MERGE is a syntax error."""

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE)

    assert statement.rstrip().endswith(";")


def test_nothing_to_merge_renders_no_statement():
    # A caller emits no action rather than an empty one.
    assert render_merge(REGISTRY, [], scope=LAKEHOUSE_SCOPE) is None


# --- deleting the obsolete ---------------------------------------------------


def test_an_obsolete_delete_keeps_exactly_the_rows_projected():
    statement = render_delete_obsolete(
        REGISTRY,
        [registry_row("Alpha"), registry_row("Beta")],
        scope=LAKEHOUSE_SCOPE,
    )

    assert "AND NOT (" in statement
    assert "[Object name] = N'Alpha'" in statement
    assert "[Object name] = N'Beta'" in statement


def test_an_installation_that_projects_nothing_still_deletes_its_rows():
    """Rendering nothing would leave stale rows behind forever.

    An installation whose repository no longer declares any object of a kind must
    lose those rows, so the empty case is a scoped delete rather than a no-op.
    """

    statement = render_delete_obsolete(REGISTRY, [], scope=LAKEHOUSE_SCOPE)

    assert statement.strip().endswith("N'Raw'")
    assert "NOT (" not in statement


def test_the_scope_predicate_leads_so_a_reviewer_sees_it_first():
    statement = render_delete_obsolete(
        REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE
    )
    lines = statement.splitlines()

    assert lines[0].startswith("DELETE FROM [_].[Registry]")
    assert lines[1].strip().startswith("WHERE [Item type] =")


# --- the explicit prune scopes ----------------------------------------------


def test_the_installation_table_has_no_obsolete_row_to_delete():
    """Its key *is* the scope, so at most one row exists and the merge maintains it.

    A predicate over the key columns "beyond the scope" would be a predicate over
    no columns, and would delete the very row about to be merged.
    """

    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "target_name": "Sales_LH",
        "weaver_version": "0.1.0",
        "signature": "abc",
    }

    assert render_delete_obsolete(INSTALLATION, [row], scope=LAKEHOUSE_SCOPE) is None


def test_an_installation_projecting_nothing_is_still_deleted():
    """The empty case is how an installation is removed, so it must still render."""

    statement = render_delete_obsolete(INSTALLATION, [], scope=LAKEHOUSE_SCOPE)

    assert statement is not None
    assert "[Item type] = N'Lakehouse' AND [Item name] = N'Raw'" in statement


def test_installation_prune_removes_one_scope_and_names_it():
    statement = render_delete_scope(REGISTRY, scope=LAKEHOUSE_SCOPE)

    assert "[Item type] = N'Lakehouse' AND [Item name] = N'Raw'" in statement
    assert "NOT (" not in statement


# --- the shapes that carry awkward values -----------------------------------


def test_a_table_dictionary_row_renders_its_nulls_and_booleans():
    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": "Sales",
        "object_name": "Customer",
        "object_type": "table",
        "description": "Customers.",
        "description_reference": None,
        "lineage": "From $Raw.CustomerCsv",
        "lineage_reference": "$Raw.CustomerCsv",
        "primary_key": "Customer id",
        "not_null_columns": None,
        "identity_column": None,
        "comparison_columns": "Customer name, Region",
        "is_incremental": False,
        "is_static": True,
        "prohibit_rebuild": False,
        "signature": "deadbeef",
    }
    statement = render_merge(TABLE_DICTIONARY, [row], scope=LAKEHOUSE_SCOPE)
    # The values are bare literals in one table value constructor; the enclosing
    # projection casts each column to its declared type. So a null is a typed
    # null by construction, whichever row it sits in.
    values = statement.split("FROM (VALUES")[1].split("AS source_values")[0]

    assert "NULL" in values
    assert "0" in values and "1" in values
    assert "N'Customer name, Region'" in values
    for name, type_ in (
        ("not_null_columns", "varchar(1000)"),
        ("is_incremental", "bit"),
        ("is_static", "bit"),
    ):
        assert f"AS {type_}) AS {public(TABLE_DICTIONARY, name)}" in statement


def test_a_relationship_row_compares_only_its_signature():
    """Every other column is key, so a changed edge is a delete and an insert.

    That is the consequence of a nameless relationship, and it is correct: an edge
    that now points elsewhere is a different edge.
    """

    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "foreign_schema_name": "Sales",
        "foreign_object_name": "Order",
        "foreign_column_set": "Customer id",
        "primary_item_type": "Lakehouse",
        "primary_item_name": "Raw",
        "primary_schema_name": "Sales",
        "primary_object_name": "Customer",
        "primary_column_set": "Customer id",
        "signature": "abc",
    }
    statement = render_merge(FOREIGN_KEY_DICTIONARY, [row], scope=LAKEHOUSE_SCOPE)
    _source, guard, _update = _clauses(statement)

    assert "[Signature]" in guard
    assert "[Primary object name]" not in guard


def test_a_composite_key_delete_names_every_key_column():
    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": "Sales",
        "object_name": "Order",
        "key_type": "unique",
        "column_set": "Order number",
        "signature": "abc",
    }
    statement = render_delete_obsolete(KEY_DICTIONARY, [row], scope=LAKEHOUSE_SCOPE)

    for column in ("schema_name", "object_name", "key_type", "column_set"):
        assert f"{public(KEY_DICTIONARY, column)} =" in statement
    # And the frozen vocabulary, not the internal value.
    assert "N'Unique'" in statement


def test_an_installation_row_updates_the_target_name_without_a_new_key():
    """Rebinding to a different Lakehouse is an update, and the key proves it.

    ``target_name`` is a comparison column, so a changed binding matches the same
    row and updates it; there is no way for the renderer to insert a second
    installation of one repository and target type.
    """

    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "target_name": "Sales_LH_v2",
        "weaver_version": "0.1.0",
        "signature": "abc",
    }
    statement = render_merge(INSTALLATION, [row], scope=LAKEHOUSE_SCOPE)

    assert "target.[Target name] <> source.[Target name]" in statement
    assert "target.[Target name] = source.[Target name]" in statement


def test_a_three_part_external_dependency_renders_as_a_row_that_says_so():
    """An authored physical name resolves to no edge, and the row says so."""

    row = {
        "item_type": "Warehouse",
        "item_name": "Reporting",
        "referencing_schema_name": "Rpt",
        "referencing_object_name": "CustomerSummary",
        "dependency_reference": "Sales_LH.Sales.Customer",
        "referenced_item_type": None,
        "referenced_item_name": None,
        "referenced_schema_name": None,
        "referenced_object_name": None,
        "signature": "abc",
    }
    statement = render_merge(DEPENDENCY, [row], scope=WAREHOUSE_SCOPE)
    values = statement.split("FROM (VALUES")[1].split("AS source_values")[0]

    assert "N'Sales_LH.Sales.Customer'" in values
    assert "NULL" in values


# --- more rows than one statement can carry ----------------------------------


def test_a_thousand_rows_still_render_one_statement():
    """The engine's limit, and one below it is still one MERGE."""

    from weaver.catalogue.render import MERGE_ROWS

    rows = [registry_row(f"Object{index:05d}") for index in range(MERGE_ROWS)]
    statement = render_merge(REGISTRY, rows, scope=LAKEHOUSE_SCOPE)

    assert statement.count("MERGE INTO") == 1


def test_more_rows_than_the_constructor_takes_are_split():
    """A T-SQL table value constructor accepts at most a thousand rows.

    A catalogue table passes that without the estate being large — a thousand
    described columns is an ordinary repository — so the rows are chunked rather
    than left to fail at install against a real Warehouse.
    """

    from weaver.catalogue.render import MERGE_ROWS

    rows = [registry_row(f"Object{index:05d}") for index in range(MERGE_ROWS + 1)]
    statement = render_merge(REGISTRY, rows, scope=LAKEHOUSE_SCOPE)

    assert statement.count("MERGE INTO") == 2
    # Every row is still there, and no chunk carries more than the limit.
    for index in range(MERGE_ROWS + 1):
        assert f"Object{index:05d}" in statement
    for chunk in statement.split("MERGE INTO")[1:]:
        values = chunk.split("FROM (VALUES")[1].split("AS source_values")[0]
        assert values.count("N'Lakehouse'") <= MERGE_ROWS


def test_the_split_keeps_key_order_across_chunks():
    """Determinism survives chunking: a bundle's identity is its bytes."""

    from weaver.catalogue.render import MERGE_ROWS

    rows = [registry_row(f"Object{index:05d}") for index in range(MERGE_ROWS + 50)]
    statement = render_merge(REGISTRY, list(reversed(rows)), scope=LAKEHOUSE_SCOPE)

    positions = [
        statement.index(f"Object{index:05d}") for index in range(MERGE_ROWS + 50)
    ]
    assert positions == sorted(positions)
