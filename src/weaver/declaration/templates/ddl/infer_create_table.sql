declare @weaver_create_sql nvarchar(max);
declare @weaver_pk_sql nvarchar(max);

if not exists (
    select 1
    from tempdb.sys.columns as c
    where c.[object_id] = object_id($temp_object_literal)
)
begin
    throw 51001, 'weaver found no temp table columns to create.', 1;
end;

$metadata_validation_sql

$identity_guard_sql

;with primary_key_columns as (
$primary_key_columns_cte
),
not_null_columns as (
$not_null_columns_cte
),
described as (
    select
        c.column_id as column_ordinal,
        coalesce(nullif(c.name, ''), concat('Column', c.column_id)) as column_name,
        t.name as system_type_name,
        c.max_length,
        c.precision,
        c.scale
    from tempdb.sys.columns as c
    inner join tempdb.sys.types as t on t.user_type_id = c.user_type_id
    where c.[object_id] = object_id($temp_object_literal)
),
mapped as (
    select
        d.column_ordinal,
        quotename(d.column_name) as quoted_column_name,
        $type_case as warehouse_type,
        -- Nullability is a Weaver contract from the Weaver document header (primary key or
        -- Not null), not the query's own nullability, so both engines agree.
        case
            when nn.column_name is not null then N' not null'
            else N' null'
        end as nullability
    from described as d
    left join not_null_columns as nn
        on nn.column_name = d.column_name collate Latin1_General_BIN2
    cross apply (
        select lower(d.system_type_name) as base_type
    ) as bt
),
all_columns as (
$identity_column_sql    select
        column_ordinal,
        quoted_column_name + N' ' + warehouse_type + nullability as column_definition
    from mapped

    union all

    select 1000001, N'[Row insert datetime] datetime2(6) not null'
    union all
    select 1000002, N'[Row update datetime] datetime2(6) not null'
    union all
    select 1000003, N'[Row delete datetime] datetime2(6) not null'
$signature_column_sql)
select
    @weaver_create_sql = (
        select
            N'create table $target_table (' + char(10)
            + string_agg(
                case
                    when column_ordinal = $first_ordinal then N'    ' + column_definition
                    else N'  , ' + column_definition
                end,
                char(10)
            ) within group (order by column_ordinal)
            + char(10) + N');'
        from all_columns
    ),
    @weaver_pk_sql = (
        select
            N'alter table $target_table add constraint $pk_constraint '
            + N'primary key nonclustered ('
            + string_agg(quotename(column_name), N', ') within group (order by column_ordinal)
            + N') not enforced;'
        from primary_key_columns
    );

print @weaver_create_sql;
exec sys.sp_executesql @weaver_create_sql;

if @weaver_pk_sql is not null
begin
    print @weaver_pk_sql;
    exec sys.sp_executesql @weaver_pk_sql;
end;
