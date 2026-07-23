"""Dependency extraction: exactly what a file refers to, nothing inferred.

Extraction only. Whether a name resolves to an object, a shortcut or a typo
needs the external-dependency configuration and is settled at build.
"""

from __future__ import annotations

import textwrap

import pytest

from weaver.ses import extract_python_references, extract_sql_references


def refs(sql: str) -> set[str]:
    return {str(reference) for reference in extract_sql_references(textwrap.dedent(sql))}


def parts(sql: str) -> tuple[tuple[str, ...], ...]:
    return tuple(reference.parts for reference in extract_sql_references(textwrap.dedent(sql)))


# --- Python ------------------------------------------------------------------


def test_one_double_underscore_is_an_object_reference():
    assert [str(r) for r in extract_python_references(("Sales__Order",))] == ["Sales.Order"]


def test_the_weaver_import_is_not_a_reference():
    assert extract_python_references(("weaver",)) == ()


def test_a_helper_package_is_not_a_reference():
    assert extract_python_references(("_helpers", "pandas", "datetime")) == ()


def test_an_underscore_prefixed_module_is_not_a_reference():
    assert extract_python_references(("_Sales__Order",)) == ()


@pytest.mark.parametrize("name", ["Sales", "Sales__Order__Extra", "Sales__", "__Order"])
def test_names_that_are_not_two_parts_are_ignored(name):
    assert extract_python_references((name,)) == ()


def test_lowercase_house_style_still_extracts():
    """A developer may write sales__order; matching it is a build concern."""
    assert [str(r) for r in extract_python_references(("sales__order",))] == ["sales.order"]


def test_references_are_deduplicated_in_order():
    got = extract_python_references(("Sales__Order", "Sales__Customer", "Sales__Order"))
    assert [str(r) for r in got] == ["Sales.Order", "Sales.Customer"]


# --- SQL: the shapes ---------------------------------------------------------


def test_a_two_part_name_is_an_object_reference():
    reference = extract_sql_references("select * from Sales.Order")[0]
    assert reference.object_id.qualified == "Sales.Order"
    assert not reference.is_qualified


def test_a_three_part_name_is_captured_but_physical():
    reference = extract_sql_references("select * from Lakehouse.Sales.Order")[0]
    assert reference.parts == ("Lakehouse", "Sales", "Order")
    assert reference.object_id is None
    assert reference.is_qualified


def test_a_four_part_name_is_captured():
    assert parts("select * from Server.Db.Sales.Order") == (
        ("Server", "Db", "Sales", "Order"),
    )


def test_delimiters_are_stripped():
    assert refs("select * from [Sales].[Order]") == {"Sales.Order"}
    assert refs('select * from "Sales"."Order"') == {"Sales.Order"}
    assert refs("select * from `Sales`.`Order`") == {"Sales.Order"}


def test_names_with_spaces_survive_delimiters():
    assert refs("select * from [Sales Team].[Open Order]") == {"Sales Team.Open Order"}


def test_an_escaped_delimiter_is_kept():
    assert refs('select * from "Odd""Name".Thing') == {'Odd"Name.Thing'}


# --- SQL: single-part names are never relations ------------------------------


def test_a_cte_is_not_a_reference():
    assert refs("""
        with recent as (select * from Sales.Order)
        select * from recent
    """) == {"Sales.Order"}


def test_a_temp_view_is_not_a_reference():
    assert refs("""
        create or replace temp view recent as select * from Sales.Order;
        select * from recent
    """) == {"Sales.Order"}


def test_a_temp_table_is_not_a_reference():
    assert refs("""
        select * into #recent from Sales.Order;
        select * from #recent
    """) == {"Sales.Order"}


def test_an_alias_is_not_a_reference():
    assert refs("""
        select o.Amount
        from Sales.Order as o
        join Sales.Customer c on c.Id = o.CustomerId
    """) == {"Sales.Order", "Sales.Customer"}


def test_a_derived_table_contributes_only_its_inner_relations():
    assert refs("""
        select *
        from (select * from Sales.Order) as recent
        join Sales.Customer c on c.Id = recent.CustomerId
    """) == {"Sales.Order", "Sales.Customer"}


# --- SQL: relation positions -------------------------------------------------


def test_joins_apply_and_comma_lists():
    assert refs("""
        select *
        from Sales.Order as o
        join Sales.Customer as c on c.Id = o.CustomerId
        left join Reference.Fx as fx on fx.Date = o.Date
        cross apply Sales.Lines(o.Id) as l
        , Sales.Extra e
    """) == {
        "Sales.Order", "Sales.Customer", "Reference.Fx", "Sales.Lines", "Sales.Extra",
    }


def test_spark_join_flavours():
    assert refs("""
        select *
        from Sales.Order o
        left semi join Sales.Customer c on c.Id = o.CustomerId
        anti join Sales.Cancelled x on x.Id = o.Id
    """) == {"Sales.Order", "Sales.Customer", "Sales.Cancelled"}


def test_set_operators_keep_both_sides():
    assert refs("""
        select Id from Sales.Order
        union all
        select Id from Sales.Archive
    """) == {"Sales.Order", "Sales.Archive"}


def test_a_subquery_in_where_is_still_a_relation():
    assert refs("""
        select * from Sales.Order
        where CustomerId in (select Id from Sales.Customer)
    """) == {"Sales.Order", "Sales.Customer"}


def test_insert_select_captures_the_source():
    assert refs("insert into #t select * from Sales.Order") == {"Sales.Order"}


# --- SQL: things that look like relations but are not ------------------------


def test_a_dotted_column_in_where_is_not_a_relation():
    assert refs("select * from Sales.Order where Some.Column = 1") == {"Sales.Order"}


def test_trim_from_is_not_a_relation_position():
    assert refs("select trim(both ' ' from Name) from Sales.Order") == {"Sales.Order"}


def test_extract_from_is_not_a_relation_position():
    assert refs("select extract(year from OrderDate) from Sales.Order") == {"Sales.Order"}


def test_a_comment_mentioning_from_is_ignored():
    assert refs("""
        -- historically read from Legacy.Order
        select * from Sales.Order
    """) == {"Sales.Order"}


def test_a_block_comment_is_ignored():
    assert refs("""
        /* from Legacy.Order */
        select * from Sales.Order
    """) == {"Sales.Order"}


def test_a_string_literal_mentioning_from_is_ignored():
    assert refs("select * from Sales.Order where Note = 'from Legacy.Order'") == {
        "Sales.Order"
    }


@pytest.mark.parametrize("prefix", ["delta", "parquet", "csv", "json"])
def test_a_spark_path_read_is_not_an_object_reference(prefix):
    """`delta.`abfss://…`` is a format and a path, not schema and object."""
    assert refs(f"select * from {prefix}.`abfss://ws@host/lh/Files/x`") == set()


def test_a_variable_is_not_a_relation():
    assert refs("select * from @table") == set()


# --- SQL: ordering and shape -------------------------------------------------


def test_references_are_ordered_and_deduplicated():
    assert parts("""
        select * from Sales.Order o
        join Sales.Customer c on c.Id = o.CustomerId
        join Sales.Order o2 on o2.Id = o.ParentId
    """) == (("Sales", "Order"), ("Sales", "Customer"))


def test_an_empty_body_yields_nothing():
    assert extract_sql_references("") == ()


def test_unparseable_sql_still_yields_what_it_can():
    assert "Sales.Order" in refs("select * from Sales.Order where ((((")


# --- against the example repository -----------------------------------------


def test_the_example_repository_extracts_across_languages():
    from pathlib import Path

    from weaver import Location
    from weaver.ses import read_repository

    repo = read_repository(Location(str(Path(__file__).parent / "fixtures" / "sales-etl")))
    found = {
        document.qualified: {str(r) for r in document.discovered_references}
        for document in repo
    }
    assert found["Sales.Order"] == {"Sales.OrderExport"}          # python import
    assert found["Sales.OrderSummary"] == {"Sales.Order"}         # spark sql
    assert found["Reporting.OrderReport"] == {"Sales.Order"}      # t-sql
    assert found["Reporting.OrderView"] == {"Reporting.OrderReport"}
    assert found["Sales.OrderExport"] == set()


def test_declared_dependencies_are_kept_beside_what_was_discovered():
    """Declaration overrides at build; both are recorded so a lint can compare."""
    from pathlib import Path

    from weaver import Location
    from weaver.ses import read_repository

    repo = read_repository(Location(str(Path(__file__).parent / "fixtures" / "sales-etl")))
    summary = repo["Sales.OrderSummary"]
    assert [str(d) for d in summary.declared_dependencies] == ["Sales.Order"]
    assert [str(r) for r in summary.discovered_references] == ["Sales.Order"]


def test_a_bare_apply_is_a_relation_position():
    assert refs("select * from Sales.Order o outer apply Sales.Lines(o.Id) l") == {
        "Sales.Order", "Sales.Lines",
    }


def test_apply_without_cross_or_outer_is_not_assumed():
    """`apply` is not a sqlparse keyword, so only the join forms are trusted."""
    assert refs("select apply from Sales.Order") == {"Sales.Order"}


def test_merge_using_is_a_relation_position():
    assert refs("merge Sales.Target t using Sales.Source s on s.Id = t.Id") == {
        "Sales.Target", "Sales.Source",
    }
