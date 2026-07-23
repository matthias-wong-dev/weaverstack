"""Spark SQL dependency extraction over realistic complete statements.

Weaver does not restrict what an author writes — intermediate statements,
custom deletion, unusual patterns are all permitted. The requirement is that
extraction stays accurate and invents nothing.
"""

from __future__ import annotations

import textwrap

import pytest

from weaver.ses import extract_sql_references


def refs(sql: str) -> set[str]:
    return {str(reference) for reference in extract_sql_references(textwrap.dedent(sql))}


# --- backtick-delimited relations -------------------------------------------


def test_backticked_schema_and_relation():
    assert refs("""
        select `Order id`, `Order date`
          from `Sales Domain`.`Open Order`
    """) == {"Sales Domain.Open Order"}


def test_backticked_three_part_name():
    assert refs("select * from `Bronze LH`.`Sales`.`Order`") == {"Bronze LH.Sales.Order"}


def test_mixed_backticked_and_bare_parts():
    assert refs("select * from Sales.`Open Order`") == {"Sales.Open Order"}


# --- temporary views and multiple statements --------------------------------


def test_temp_view_then_final_query():
    assert refs("""
        create or replace temporary view recent as
        select * from Sales.Order where `Order date` >= current_date() - 30;

        select * from recent
    """) == {"Sales.Order"}


def test_several_intermediate_statements():
    assert refs("""
        create or replace temp view a as select * from Sales.Order;
        create or replace temp view b as select * from Sales.Customer;
        create or replace temp view c as
            select * from a join b on b.Id = a.CustomerId;

        select * from c join Sales.Region r on r.Id = c.RegionId
    """) == {"Sales.Order", "Sales.Customer", "Sales.Region"}


def test_a_temp_view_reading_another_temp_view_adds_nothing():
    assert refs("""
        create or replace temp view a as select * from Sales.Order;
        create or replace temp view b as select * from a;
        select * from b
    """) == {"Sales.Order"}


# --- join flavours -----------------------------------------------------------


def test_left_semi_join():
    assert refs("""
        select o.*
          from Sales.Order o
          left semi join Sales.Customer c on c.Id = o.CustomerId
    """) == {"Sales.Order", "Sales.Customer"}


def test_left_anti_join():
    assert refs("""
        select o.*
          from Sales.Order o
          left anti join Sales.Cancelled x on x.OrderId = o.Id
    """) == {"Sales.Order", "Sales.Cancelled"}


def test_semi_and_anti_together_with_a_temp_view():
    assert refs("""
        create or replace temp view active as select * from Sales.Order;

        select a.*
          from active a
          left semi join Sales.Customer c on c.Id = a.CustomerId
          left anti join Sales.Cancelled x on x.OrderId = a.Id
    """) == {"Sales.Order", "Sales.Customer", "Sales.Cancelled"}


# --- CTEs and nesting --------------------------------------------------------


def test_cte_chain():
    assert refs("""
        with recent as (
            select * from Sales.Order where `Order date` > '2026-01-01'
        ),
        enriched as (
            select r.*, c.`Customer name`
              from recent r
              join Sales.Customer c on c.Id = r.CustomerId
        )
        select * from enriched
    """) == {"Sales.Order", "Sales.Customer"}


def test_nested_subqueries():
    assert refs("""
        select *
          from (
            select * from (
                select * from Sales.Order
            ) inner_most
            join Sales.Customer c on c.Id = inner_most.CustomerId
          ) outer_most
         where RegionId in (select Id from Sales.Region)
    """) == {"Sales.Order", "Sales.Customer", "Sales.Region"}


def test_lateral_view_explode_is_not_a_relation():
    assert refs("""
        select o.Id, line
          from Sales.Order o
          lateral view explode(o.Lines) exploded as line
    """) == {"Sales.Order"}


# --- path reads --------------------------------------------------------------


@pytest.mark.parametrize("fmt", ["delta", "parquet", "csv", "json", "orc", "avro"])
def test_a_path_read_is_a_format_and_a_path_not_an_object(fmt):
    assert refs(f"select * from {fmt}.`abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/x`") == set()


def test_a_path_read_beside_a_real_relation():
    assert refs("""
        select *
          from delta.`abfss://ws@host/lh/Tables/raw`
          join Sales.Customer c on c.Id = raw.CustomerId
    """) == {"Sales.Customer"}


def test_a_relative_path_read_is_also_excluded():
    assert refs("select * from csv.`/Files/landing/orders.csv`") == set()


# --- DML ---------------------------------------------------------------------


def test_delete_from_extracts_the_target():
    assert refs("delete from Sales.Order where `Order date` < '2020-01-01'") == {
        "Sales.Order"
    }


def test_delete_with_a_qualified_target():
    assert refs("delete from `Bronze LH`.Sales.Order where Id = 1") == {
        "Bronze LH.Sales.Order"
    }


def test_delete_with_a_subquery_extracts_both():
    assert refs("""
        delete from Sales.Order
         where CustomerId in (select Id from Sales.Cancelled)
    """) == {"Sales.Order", "Sales.Cancelled"}


def test_merge_into_extracts_target_and_source():
    assert refs("""
        merge into Sales.Order as t
        using Sales.OrderStaging as s
           on s.Id = t.Id
         when matched then update set t.Amount = s.Amount
         when not matched then insert (Id, Amount) values (s.Id, s.Amount)
    """) == {"Sales.Order", "Sales.OrderStaging"}


def test_merge_from_a_temp_view_extracts_only_the_target():
    assert refs("""
        create or replace temp view staged as select * from Sales.OrderStaging;

        merge into Sales.Order as t
        using staged as s on s.Id = t.Id
         when matched then update set t.Amount = s.Amount
    """) == {"Sales.Order", "Sales.OrderStaging"}


def test_insert_into_extracts_target_and_source():
    assert refs("insert into Sales.Archive select * from Sales.Order") == {
        "Sales.Archive", "Sales.Order",
    }


# --- what must be ignored ----------------------------------------------------


def test_a_reference_in_a_line_comment_is_ignored():
    assert refs("""
        -- previously joined Legacy.Order
        select * from Sales.Order
    """) == {"Sales.Order"}


def test_a_reference_in_a_block_comment_is_ignored():
    assert refs("""
        /* migrated away from Legacy.Order and Legacy.Customer */
        select * from Sales.Order
    """) == {"Sales.Order"}


def test_a_reference_in_a_string_literal_is_ignored():
    assert refs("""
        select *, 'read from Legacy.Order' as Note
          from Sales.Order
         where Source = 'Legacy.Customer'
    """) == {"Sales.Order"}


def test_dotted_column_expressions_are_ignored():
    assert refs("""
        select o.Amount, c.`Customer name`
          from Sales.Order o
          join Sales.Customer c on c.Id = o.CustomerId
         where o.Status = 1 and c.Region = 'AU'
    """) == {"Sales.Order", "Sales.Customer"}


def test_a_struct_field_access_is_not_a_relation():
    assert refs("""
        select o.Address.Suburb, o.Address.Postcode
          from Sales.Order o
    """) == {"Sales.Order"}


def test_a_function_call_is_not_a_relation():
    assert refs("""
        select date_format(o.`Order date`, 'yyyy-MM') as Period
          from Sales.Order o
    """) == {"Sales.Order"}


# --- a complete, realistic file ----------------------------------------------


REALISTIC = """
-- Order summary. Reads the raw drop and the managed order table, and excludes
-- anything cancelled. Historically this read from Legacy.OrderSummary.

create or replace temporary view raw_orders as
select *
  from delta.`abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/orders`;

create or replace temp view enriched as
select r.*
     , c.`Customer name`
     , 'sourced from Legacy.Customer' as Provenance
  from raw_orders r
  join `Sales Domain`.`Customer` c on c.Id = r.CustomerId
  left anti join Sales.Cancelled x on x.OrderId = r.Id;

with regional as (
    select e.*, g.Region
      from enriched e
      join Reference.Geography g on g.Postcode = e.Postcode
)
select `Customer name`
     , Region
     , count(*) as `Order count`
  from regional
 group by `Customer name`, Region
"""


def test_a_realistic_spark_file_extracts_exactly_its_relations():
    assert refs(REALISTIC) == {
        "Sales Domain.Customer",
        "Sales.Cancelled",
        "Reference.Geography",
    }


def test_the_realistic_file_invents_nothing():
    """No temp views, no CTE, no alias, no path, nothing from comments or strings."""
    found = refs(REALISTIC)
    for invented in ("raw_orders", "enriched", "regional", "Legacy.OrderSummary",
                     "Legacy.Customer", "delta"):
        assert not any(invented in name for name in found)
