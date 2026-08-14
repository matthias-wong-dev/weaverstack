"""Rendering catalogue rows as scoped, deterministic Spark SQL.

Three properties are load-bearing and each is tested directly rather than
incidentally:

- **determinism** — the same rows render the same bytes, whatever order they
  arrive in, because a bundle's identity is a hash of its payloads;
- **scope** — every statement names one repository and one target type, so a
  Lakehouse build cannot express a change to a Warehouse row;
- **explicit values** — nulls are typed, quotes and backslashes survive, and
  booleans are booleans rather than the strings that look like them.

The clock is the deliberate exception: ``current_timestamp()`` is rendered as a
call, because a rendered instant would change a payload on every run and destroy
the bundle identity that review and certification depend on.
"""

from __future__ import annotations

import pytest

from weaver.catalogue import (
    DEPENDENCY,
    FOREIGN_KEY_DICTIONARY,
    INDEX_DICTIONARY,
    INSTALLATION,
    REGISTRY,
    TABLE_DICTIONARY,
    InstallationScope,
    column_set,
    identifier,
    literal,
    render_delete_obsolete,
    render_delete_scope,
    render_merge,
    sorted_rows,
    typed_literal,
)
from weaver.spark import FabricSparkTarget

#: The Weaver Lakehouse every catalogue statement is addressed to.
WEAVER = FabricSparkTarget(workspace="Demo", lakehouse="Weaver")


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


# --- literals and identifiers ------------------------------------------------


def test_a_string_is_quoted():
    assert literal("Sales") == "'Sales'"


def test_a_quote_is_escaped():
    assert literal("O'Brien") == "'O\\'Brien'"


def test_a_backslash_is_escaped_because_spark_treats_it_as_one():
    # Spark's default parser escapes with a backslash, so an unescaped one would
    # consume the character after it — silently changing a stored value.
    assert literal("C:\\path") == "'C:\\\\path'"


def test_a_null_is_null_not_the_word():
    assert literal(None) == "NULL"


def test_a_boolean_is_a_boolean():
    assert literal(True) == "true"
    assert literal(False) == "false"


def test_a_boolean_is_not_rendered_as_a_string():
    assert literal(True) != "'true'"


def test_a_value_of_an_unsupported_type_is_refused():
    with pytest.raises(TypeError, match="strings, booleans or null"):
        literal({"a": 1})


def test_every_value_is_cast_to_its_declared_type():
    assert typed_literal("Sales", "string") == "CAST('Sales' AS STRING)"
    assert typed_literal(None, "string") == "CAST(NULL AS STRING)"
    assert typed_literal(True, "boolean") == "CAST(true AS BOOLEAN)"


def test_a_non_boolean_in_a_boolean_column_is_refused():
    with pytest.raises(TypeError, match="expected a boolean"):
        typed_literal("yes", "boolean")


def test_an_identifier_is_back_tick_quoted():
    assert identifier("Order id") == "`Order id`"


def test_a_back_tick_in_an_identifier_is_doubled():
    assert identifier("we`ird") == "`we``ird`"


def test_a_column_set_preserves_declared_order():
    # Order is meaning: a key on (Region, Country) is not the key on
    # (Country, Region), so the renderer never sorts one.
    assert column_set(["Region", "Country"]) == "Region, Country"
    assert column_set(["Country", "Region"]) == "Country, Region"


def test_an_empty_column_set_is_null_not_an_empty_string():
    # "No key" and "a key of no columns" are different claims.
    assert column_set([]) is None


# --- determinism -------------------------------------------------------------


def test_rows_render_in_key_order_whatever_order_they_arrive_in():
    forwards = [registry_row("Alpha"), registry_row("Beta"), registry_row("Gamma")]
    backwards = list(reversed(forwards))
    assert render_merge(REGISTRY, forwards, scope=LAKEHOUSE_SCOPE, destination=WEAVER) == render_merge(
        REGISTRY, backwards, scope=LAKEHOUSE_SCOPE
    ,
        destination=WEAVER,)


def test_the_same_rows_render_the_same_bytes():
    rows = [registry_row("Alpha"), registry_row("Beta")]
    first = render_merge(REGISTRY, rows, scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    second = render_merge(REGISTRY, list(rows), scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert first == second


def test_sorting_is_by_the_key_and_tolerates_a_null():
    rows = [
        {**registry_row("Beta")},
        {**registry_row("Alpha")},
    ]
    assert [row["object_name"] for row in sorted_rows(REGISTRY, rows)] == [
        "Alpha",
        "Beta",
    ]


def test_the_clock_is_a_call_not_a_rendered_instant():
    """A rendered timestamp would change the payload — and the bundle id — each run.

    The engine supplies the instant instead, which keeps the payload frozen while
    still stamping the row.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert "current_timestamp()" in statement
    assert statement.count("current_timestamp()") == 3  # one update, two inserts


# --- the published build epoch ------------------------------------------------


def _clauses(statement: str) -> tuple[str, str, str]:
    """The merge split into the three places a column can appear."""

    matched = statement.index("WHEN MATCHED")
    not_matched = statement.index("WHEN NOT MATCHED")
    guard, update = statement[matched:not_matched].split("THEN UPDATE SET", 1)
    return statement[:matched], guard, update


def test_the_epoch_is_a_token_so_the_payload_stays_frozen():
    """Same reason the clock is a call: a rendered instant would give the same
    repository different bytes every run, and a bundle's identity is its bytes.
    The installer resolves it, once, for the whole run."""

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE, destination=WEAVER)

    assert "CAST('{{epoch}}' AS TIMESTAMP)" in statement
    assert statement.count("{{epoch}}") == 1


def test_the_epoch_is_written_on_insert_and_nowhere_else():
    """The decision the whole freshness comparison rests on.

    Every object a build actually rebuilds arrives here as an insert — it is new,
    or its claim was deleted before the physical work. So an *update* is a row
    whose projection moved while the object stood still, and dating it to this
    build would claim a rebuild that never happened.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    source, guard, update = _clauses(statement)

    assert "build_epoch" not in source, "not projected — no row carries one"
    assert "build_epoch" not in guard, "not compared — it differs every build"
    assert "build_epoch" not in update, "not updated — that is the whole point"
    assert "`build_epoch`" in statement[statement.index("WHEN NOT MATCHED") :]


def test_a_table_without_a_published_column_is_rendered_exactly_as_before():
    """Only Registry carries an epoch. Nothing else gained a column."""

    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": "Sales",
        "object_name": "Customer",
        "dependency_name": "Sales.Order",
        "is_within_item": True,
        "signature": "abc",
    }
    statement = render_merge(DEPENDENCY, [row], scope=LAKEHOUSE_SCOPE, destination=WEAVER)

    assert "{{epoch}}" not in statement
    assert "build_epoch" not in statement


# --- scope -------------------------------------------------------------------


def test_a_merge_is_scoped_to_one_installation_on_the_target_side():
    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert "target.`item_type` = 'Lakehouse'" in statement
    assert "target.`item_name` = 'Raw'" in statement
    assert "warehouse" not in statement


def test_a_delete_is_scoped_to_one_installation():
    statement = render_delete_obsolete(
        REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE
    ,
        destination=WEAVER,)
    assert "`item_type` = 'Lakehouse' AND `item_name` = 'Raw'" in statement
    assert "warehouse" not in statement


def test_the_same_object_in_another_item_renders_a_different_statement():
    """The one property the whole installation model rests on.

    ``Sales.Customer`` in two logical items is two rows. Neither statement can
    reach the other's row, because the scope is in the key and in every
    predicate.
    """

    raw = render_merge(REGISTRY, [registry_row("Customer")], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    curated = render_merge(
        REGISTRY,
        [registry_row("Customer", item_name="Curated")],
        scope=InstallationScope(item_type="Lakehouse", item_name="Curated"),
    destination=WEAVER,)
    assert raw != curated
    assert "'Curated'" not in raw
    assert "'Raw'" not in curated


def test_a_row_from_another_installation_cannot_be_rendered():
    """Refused rather than merged into the wrong scope, where it would read as truth."""

    with pytest.raises(ValueError, match="do not belong to installation"):
        render_merge(
            REGISTRY,
            [registry_row("Customer", item_name="Curated")],
            scope=LAKEHOUSE_SCOPE,
        destination=WEAVER,)


def test_a_row_from_another_item_cannot_be_rendered():
    stray = {**registry_row("Customer"), "item_name": "Other"}
    with pytest.raises(ValueError, match="do not belong to installation"):
        render_merge(REGISTRY, [stray], scope=LAKEHOUSE_SCOPE, destination=WEAVER)


def test_the_guard_applies_to_deletes_too():
    with pytest.raises(ValueError, match="do not belong to installation"):
        render_delete_obsolete(
            REGISTRY,
            [registry_row("Customer", item_name="Curated")],
            scope=LAKEHOUSE_SCOPE,
        destination=WEAVER,)


# --- insert, update, no-op ---------------------------------------------------


def test_a_matched_unchanged_row_is_a_no_op():
    """The matched branch is guarded by a comparison of every non-key column.

    So rebuilding unchanged Weaver document writes nothing and does not advance
    ``row_update_datetime`` — which is what makes a rebuild idempotent from the
    catalogue's point of view.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert "WHEN MATCHED AND (" in statement
    for column in REGISTRY.comparison_columns:
        assert f"NOT (target.`{column}` <=> source.`{column}`)" in statement


def test_an_update_advances_only_the_update_datetime():
    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    update = statement.split("WHEN MATCHED")[1].split("WHEN NOT MATCHED")[0]
    assert "target.`row_update_datetime` = current_timestamp()" in update
    # The insert datetime is when the row first appeared and must not move.
    assert "row_insert_datetime" not in update
    # Nor may an update rewrite the key it matched on.
    for key in REGISTRY.key:
        assert f"target.`{key}` = source" not in update


def test_an_insert_supplies_every_physical_column_including_the_live_sentinel():
    """All three audit columns are physically not null, so all three are written.

    A live row's delete datetime is a sentinel maximum rather than a null, which is
    what makes an "as at" read a single range predicate.
    """

    statement = render_merge(REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    insert = statement.split("WHEN NOT MATCHED")[1]
    for column in REGISTRY.physical_columns:
        assert f"`{column}`" in insert
    assert "CAST('9999-12-31 23:59:59.999999' AS TIMESTAMP)" in insert


def test_nothing_to_merge_renders_no_statement():
    # A caller emits no action rather than an empty one.
    assert render_merge(REGISTRY, [], scope=LAKEHOUSE_SCOPE, destination=WEAVER) is None


# --- deleting the obsolete ---------------------------------------------------


def test_an_obsolete_delete_keeps_exactly_the_rows_projected():
    statement = render_delete_obsolete(
        REGISTRY, [registry_row("Alpha"), registry_row("Beta")], scope=LAKEHOUSE_SCOPE
    ,
        destination=WEAVER,)
    assert "AND NOT (" in statement
    assert "`object_name` <=> CAST('Alpha' AS STRING)" in statement
    assert "`object_name` <=> CAST('Beta' AS STRING)" in statement


def test_an_installation_that_projects_nothing_still_deletes_its_rows():
    """Rendering nothing would leave stale rows behind forever.

    An installation whose repository no longer declares any object of a kind must
    lose those rows, so the empty case is a scoped delete rather than a no-op.
    """

    statement = render_delete_obsolete(REGISTRY, [], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert statement.strip().endswith("'Raw'")
    assert "NOT (" not in statement


def test_the_scope_predicate_leads_so_a_reviewer_sees_it_first():
    statement = render_delete_obsolete(
        REGISTRY, [registry_row("Alpha")], scope=LAKEHOUSE_SCOPE
    ,
        destination=WEAVER,)
    lines = statement.splitlines()
    assert lines[0].startswith("DELETE FROM `Demo`.`Weaver`.`_`.`Registry`")
    assert lines[1].strip().startswith("WHERE `item_type` =")


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
    assert render_delete_obsolete(INSTALLATION, [row], scope=LAKEHOUSE_SCOPE, destination=WEAVER) is None


def test_an_installation_projecting_nothing_is_still_deleted():
    """The empty case is how an installation is removed, so it must still render."""

    statement = render_delete_obsolete(INSTALLATION, [], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert statement is not None
    assert "`item_type` = 'Lakehouse' AND `item_name` = 'Raw'" in statement


def test_installation_prune_removes_one_scope_and_names_it():
    statement = render_delete_scope(REGISTRY, scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert "`item_type` = 'Lakehouse' AND `item_name` = 'Raw'" in statement
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
    statement = render_merge(TABLE_DICTIONARY, [row], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    # The values are bare literals in one VALUES relation; the enclosing projection
    # casts each column to its declared type. So a null is a typed null by
    # construction, whichever row it sits in.
    values = statement.split("FROM VALUES")[1].split("AS source_values")[0]
    assert "NULL" in values
    assert "false" in values and "true" in values
    assert "'Customer name, Region'" in values
    for name, type_ in (
        ("not_null_columns", "STRING"),
        ("is_incremental", "BOOLEAN"),
        ("is_static", "BOOLEAN"),
    ):
        assert f"AS {type_}) AS `{name}`" in statement


def test_a_relationship_row_compares_only_its_signature():
    """Every other column is key, so a changed edge is a delete and an insert.

    That is the consequence of a nameless relationship, and it is correct: an edge
    that now points elsewhere is a different edge.
    """

    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": "Sales",
        "object_name": "Order",
        "column_set": "Customer id",
        "reference_item_type": "Lakehouse",
        "reference_item_name": "Raw",
        "reference_schema_name": "Sales",
        "reference_object_name": "Customer",
        "reference_column_set": "Customer id",
        "signature": "abc",
    }
    statement = render_merge(FOREIGN_KEY_DICTIONARY, [row], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert "WHEN MATCHED AND (NOT (target.`signature` <=> source.`signature`))" in statement


def test_a_composite_key_delete_names_every_key_column():
    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": "Sales",
        "object_name": "Order",
        "index_type": "unique",
        "column_set": "Order number",
        "signature": "abc",
    }
    statement = render_delete_obsolete(INDEX_DICTIONARY, [row], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    for column in ("schema_name", "object_name", "index_type", "column_set"):
        assert f"`{column}` <=>" in statement


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
    statement = render_merge(INSTALLATION, [row], scope=LAKEHOUSE_SCOPE, destination=WEAVER)
    assert "NOT (target.`target_name` <=> source.`target_name`)" in statement
    assert "target.`target_name` = source.`target_name`" in statement


def test_a_three_part_external_dependency_renders_as_a_row_that_says_so():
    row = {
        "item_type": "Warehouse",
        "item_name": "Reporting",
        "schema_name": "Rpt",
        "object_name": "CustomerSummary",
        "dependency_name": "Sales_LH.Sales.Customer",
        "is_within_item": False,
        "signature": "abc",
    }
    statement = render_merge(DEPENDENCY, [row], scope=WAREHOUSE_SCOPE, destination=WEAVER)
    assert "AS BOOLEAN) AS `is_within_item`" in statement
    values = statement.split("FROM VALUES")[1].split("AS source_values")[0]
    assert "'Sales_LH.Sales.Customer'" in values
    assert "false" in values
