-- The loadable columns, read from the table the load will write to.
--
-- Read from sys.columns rather than from the declaration, because the physical
-- table is what the procedure has to name. That is also what excludes the
-- identity column for free: the Warehouse generates it, so `is_identity = 0`
-- keeps it out of every insert list without the generator having to know which
-- column it was.
;with source_columns as (
    select
        c.name
      , c.column_id
      , row_number() over (order by c.column_id) as row_ordinal
    from sys.columns as c
    where c.[object_id] = object_id($target_table_literal)
        and $source_column_filter
)
select
    @weaver_source_columns = string_agg(
        case when row_ordinal = 1 then quotename(name) else char(10) + N'      , ' + quotename(name) end,
        N''
    ) within group (order by column_id)
  , @weaver_staging_select_columns = string_agg(
        case when row_ordinal = 1 then N's.' + quotename(name) else char(10) + N'      , s.' + quotename(name) end,
        N''
    ) within group (order by column_id)
  , @weaver_staging_except_columns = string_agg(
        case when row_ordinal = 1 then N's.' + quotename(name) else char(10) + N'          , s.' + quotename(name) end,
        N''
    ) within group (order by column_id)
  , @weaver_target_except_columns = string_agg(
        case when row_ordinal = 1 then N't.' + quotename(name) else char(10) + N'          , t.' + quotename(name) end,
        N''
    ) within group (order by column_id)
  , @weaver_upsert_select_columns = string_agg(
        case when row_ordinal = 1 then N'u.' + quotename(name) else char(10) + N'      , u.' + quotename(name) end,
        N''
    ) within group (order by column_id)
from source_columns;

$update_select
