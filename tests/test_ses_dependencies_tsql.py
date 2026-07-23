"""T-SQL dependency extraction over realistic complete statements.

Weaver does not restrict what an author writes — intermediate statements,
temporary tables, custom deletion against the current table are all permitted.
The requirement is that extraction stays accurate and invents nothing.
"""

from __future__ import annotations

import textwrap

import pytest

from weaver.ses import extract_sql_references


def refs(sql: str) -> set[str]:
    return {str(reference) for reference in extract_sql_references(textwrap.dedent(sql))}


# --- bracket-delimited relations ---------------------------------------------


def test_bracketed_schema_and_relation():
    assert refs("select * from [Sales].[Open Order]") == {"Sales.Open Order"}


def test_bracketed_three_part_name():
    assert refs("select * from [Warehouse].[Sales].[Order]") == {
        "Warehouse.Sales.Order"
    }


def test_mixed_bracketed_and_bare_parts():
    assert refs("select * from Sales.[Open Order]") == {"Sales.Open Order"}


def test_a_bracketed_name_containing_a_dot_stays_two_parts():
    """The joined string is ambiguous; the parts are not, and they are what count."""
    reference = extract_sql_references("select * from [Sales].[Order.Archive]")[0]
    assert reference.parts == ("Sales", "Order.Archive")
    assert reference.object_id is not None


# --- temporary tables --------------------------------------------------------


def test_select_into_temp_then_read_it():
    assert refs("""
        select o.[Order id], o.[Amount]
          into #recent
          from Sales.[Order] as o
         where o.[Order date] >= dateadd(day, -30, getdate());

        select * from #recent
    """) == {"Sales.Order"}


def test_a_temp_table_joined_to_a_real_relation():
    assert refs("""
        select * into #recent from Sales.[Order];

        select r.*, c.[Customer name]
          from #recent r
          join Sales.Customer c on c.Id = r.CustomerId
    """) == {"Sales.Order", "Sales.Customer"}


def test_modifying_a_temp_table_adds_nothing():
    assert refs("""
        select * into #recent from Sales.[Order];
        update #recent set [Amount] = 0 where [Amount] is null;
        delete from #recent where [Order id] is null;

        select * from #recent
    """) == {"Sales.Order"}


def test_an_explicitly_created_temp_table():
    assert refs("""
        create table #recent ([Order id] int, [Amount] decimal(18,2));
        insert into #recent select [Order id], [Amount] from Sales.[Order];

        select * from #recent
    """) == {"Sales.Order"}


def test_a_table_variable_is_not_a_relation():
    assert refs("""
        declare @recent table ([Order id] int);
        insert into @recent select [Order id] from Sales.[Order];

        select * from @recent
    """) == {"Sales.Order"}


# --- APPLY -------------------------------------------------------------------


def test_cross_apply():
    assert refs("""
        select o.[Order id], l.[Line number]
          from Sales.[Order] o
         cross apply Sales.OrderLines(o.[Order id]) as l
    """) == {"Sales.Order", "Sales.OrderLines"}


def test_outer_apply():
    assert refs("""
        select o.[Order id], l.[Line number]
          from Sales.[Order] o
         outer apply Sales.OrderLines(o.[Order id]) as l
    """) == {"Sales.Order", "Sales.OrderLines"}


def test_apply_over_a_derived_table_adds_only_its_relations():
    assert refs("""
        select *
          from Sales.[Order] o
         cross apply (select top 1 * from Sales.Payment p
                       where p.OrderId = o.[Order id]
                       order by p.PaidOn desc) as latest
    """) == {"Sales.Order", "Sales.Payment"}


# --- CTEs, derived tables, nesting -------------------------------------------


def test_cte_chain():
    assert refs("""
        with recent as (
            select * from Sales.[Order] where [Order date] > '2026-01-01'
        ),
        enriched as (
            select r.*, c.[Customer name]
              from recent r
              join Sales.Customer c on c.Id = r.CustomerId
        )
        select * from enriched
    """) == {"Sales.Order", "Sales.Customer"}


def test_derived_tables_and_nested_subqueries():
    assert refs("""
        select *
          from (
            select * from (select * from Sales.[Order]) as innermost
            join Sales.Customer c on c.Id = innermost.CustomerId
          ) as outermost
         where RegionId in (select Id from Reference.Region)
           and exists (select 1 from Sales.Cancelled x where x.OrderId = outermost.Id)
    """) == {"Sales.Order", "Sales.Customer", "Reference.Region", "Sales.Cancelled"}


# --- qualified physical names ------------------------------------------------


def test_a_three_part_name_is_captured_unresolved():
    reference = extract_sql_references("select * from Lakehouse.Sales.[Order]")[0]
    assert reference.parts == ("Lakehouse", "Sales", "Order")
    assert reference.object_id is None


def test_a_four_part_linked_server_name():
    reference = extract_sql_references("select * from [Srv].[Db].[Sales].[Order]")[0]
    assert reference.parts == ("Srv", "Db", "Sales", "Order")
    assert reference.is_qualified


def test_two_and_three_part_names_side_by_side():
    assert refs("""
        select *
          from Sales.[Order] o
          join Warehouse.Reference.Fx fx on fx.[Date] = o.[Order date]
    """) == {"Sales.Order", "Warehouse.Reference.Fx"}


# --- DML ---------------------------------------------------------------------


def test_delete_against_the_current_table_is_permitted_and_extracted():
    assert refs("delete from Sales.[Order] where [Order date] < '2020-01-01'") == {
        "Sales.Order"
    }


def test_delete_with_a_join_extracts_both():
    assert refs("""
        delete o
          from Sales.[Order] o
          join Sales.Cancelled x on x.OrderId = o.[Order id]
    """) == {"Sales.Order", "Sales.Cancelled"}


def test_update_extracts_target_and_source():
    assert refs("""
        update Sales.[Order]
           set [Amount] = s.[Amount]
          from Sales.OrderStaging s
         where s.Id = Sales.[Order].[Order id]
    """) == {"Sales.Order", "Sales.OrderStaging"}


def test_update_through_an_alias_extracts_the_real_relation():
    assert refs("""
        update o
           set o.[Amount] = 0
          from Sales.[Order] o
         where o.[Amount] is null
    """) == {"Sales.Order"}


def test_insert_extracts_target_and_source():
    assert refs("""
        insert into Sales.Archive ([Order id], [Amount])
        select [Order id], [Amount] from Sales.[Order]
    """) == {"Sales.Archive", "Sales.Order"}


def test_merge_extracts_target_and_source():
    assert refs("""
        merge into Sales.[Order] as t
        using Sales.OrderStaging as s
           on s.Id = t.[Order id]
         when matched then update set t.[Amount] = s.[Amount]
         when not matched by target then insert ([Order id]) values (s.Id)
         when not matched by source then delete;
    """) == {"Sales.Order", "Sales.OrderStaging"}


# --- what must be ignored ----------------------------------------------------


def test_references_in_comments_are_ignored():
    assert refs("""
        -- was Legacy.Order until 2025
        /* and briefly Legacy.Archive */
        select * from Sales.[Order]
    """) == {"Sales.Order"}


def test_references_in_string_literals_are_ignored():
    assert refs("""
        select *, 'from Legacy.Order' as [Note]
          from Sales.[Order]
         where [Source] = 'Legacy.Customer'
    """) == {"Sales.Order"}


def test_variables_are_not_relations():
    assert refs("""
        declare @cutoff date = '2026-01-01';
        select * from Sales.[Order] where [Order date] >= @cutoff
    """) == {"Sales.Order"}


def test_dotted_column_expressions_are_ignored():
    assert refs("""
        select o.[Amount], c.[Customer name]
          from Sales.[Order] o
          join Sales.Customer c on c.Id = o.CustomerId
         where o.[Status] = 1
    """) == {"Sales.Order", "Sales.Customer"}


def test_string_functions_taking_from_are_not_relation_positions():
    assert refs("""
        select trim(both ' ' from o.[Note]) as [Note]
             , substring(o.[Code] from 1 for 3) as [Prefix]
          from Sales.[Order] o
    """) == {"Sales.Order"}


# --- a complete, realistic file ----------------------------------------------


REALISTIC = """
-- Order report. Historically sourced from Legacy.OrderReport; now built from
-- the managed order table with a temp table for the recent window.

declare @cutoff date = dateadd(month, -12, getdate());

select o.[Order id]
     , o.[Customer id]
     , o.[Amount]
  into #recent
  from Sales.[Order] as o
 where o.[Order date] >= @cutoff;

delete from #recent where [Amount] is null;

with enriched as (
    select r.*
         , c.[Customer name]
         , 'sourced from Legacy.Customer' as [Provenance]
      from #recent r
      join [Sales].[Customer] c on c.[Customer id] = r.[Customer id]
      left join Warehouse.Reference.Fx fx on fx.[Date] = r.[Order date]
)
select e.[Order id]
     , e.[Customer name]
     , l.[Line count]
  from enriched e
 cross apply Sales.OrderLineCount(e.[Order id]) as l
 where not exists (select 1 from Sales.Cancelled x where x.OrderId = e.[Order id])
"""


def test_a_realistic_tsql_file_extracts_exactly_its_relations():
    assert refs(REALISTIC) == {
        "Sales.Order",
        "Sales.Customer",
        "Warehouse.Reference.Fx",
        "Sales.OrderLineCount",
        "Sales.Cancelled",
    }


def test_the_realistic_file_invents_nothing():
    """No temp table, CTE, alias, variable, comment or string contributes."""
    found = refs(REALISTIC)
    for invented in ("#recent", "enriched", "Legacy.", "@cutoff"):
        assert not any(invented in name for name in found)
