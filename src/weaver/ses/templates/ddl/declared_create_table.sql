declare @weaver_column_error nvarchar(2048);

if not exists (
    select 1
    from tempdb.sys.columns as c
    where c.[object_id] = object_id($temp_object_literal)
)
begin
    throw 51001, 'weaver found no temp table columns to create.', 1;
end;

$metadata_validation_sql

;with described as (
    select
        coalesce(nullif(c.name, ''), concat('Column', c.column_id)) as column_name
    from tempdb.sys.columns as c
    where c.[object_id] = object_id($temp_object_literal)
),
declared_columns as (
$declared_columns_cte
)
select top (1)
    @weaver_column_error = mismatch.message
from (
    -- A declared column the query does not return, under the same case.
    select
        1 as ordinal,
        dc.column_name,
        concat(N'declared column ', dc.column_name, N' is not returned by the query') as message
    from declared_columns as dc
    where not exists (
        select 1
        from described as d
        where d.column_name = dc.column_name collate Latin1_General_BIN2
    )

    union all

    -- A query column not present in the declared schema, under the same case.
    select
        2 as ordinal,
        d.column_name,
        concat(N'query column ', d.column_name, N' is not in the declared schema') as message
    from described as d
    where not exists (
        select 1
        from declared_columns as dc
        where dc.column_name = d.column_name collate Latin1_General_BIN2
    )
) as mismatch
order by
    mismatch.ordinal
  , mismatch.column_name;

if @weaver_column_error is not null
begin
    throw 51005, @weaver_column_error, 1;
end;

if object_id($target_table_literal, N'U') is null
begin
    create table $target_table (
$declared_column_definitions
    );
end;
$pk_alter_sql
