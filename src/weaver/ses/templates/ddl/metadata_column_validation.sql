declare @weaver_missing_metadata_column nvarchar(2048);

;with described as (
    select
        coalesce(nullif(c.name, ''), concat('Column', c.column_id)) as column_name
    from tempdb.sys.columns as c
    where c.[object_id] = object_id($temp_object_literal)$identity_available_sql
),
metadata_columns as (
$metadata_columns_cte
)
select top (1)
    @weaver_missing_metadata_column =
        concat(m.metadata_kind, N' ', m.column_name, N' does not exist')
from metadata_columns as m
where not exists (
    select 1
    from described as d
    -- Column names are exact, case-sensitive Weaver contracts, so compare under
    -- a binary collation rather than the database's (often case-insensitive) one.
    where d.column_name = m.column_name collate Latin1_General_BIN2
)
order by
    m.metadata_kind
  , m.column_name;

if @weaver_missing_metadata_column is not null
begin
    throw 51004, @weaver_missing_metadata_column, 1;
end;
