declare @weaver_delete_shape_error nvarchar(2048);

;with declared as (
$primary_key_columns_cte
),
produced as (
    select
        coalesce(nullif(c.name, ''), concat('Column', c.column_id)) as column_name
    from tempdb.sys.columns as c
    where c.[object_id] = object_id($temp_object_literal)
)
-- Both directions, and the message names which. A delete query missing part of
-- the key would retire rows it never identified; one carrying anything else is
-- naming something a deletion cannot use. Column names are exact, case-sensitive
-- Weaver contracts, so the comparison is under a binary collation rather than
-- the database's (often case-insensitive) one.
select top (1)
    @weaver_delete_shape_error =
        concat(
            N'the delete query must produce exactly the primary key: '
          , kind
          , N' '
          , column_name
        )
from (
    select N'it does not produce' as kind, d.column_name as column_name
    from declared as d
    where not exists (
        select 1
        from produced as p
        where p.column_name = d.column_name collate Latin1_General_BIN2
    )

    union all

    select N'it also produces' as kind, p.column_name as column_name
    from produced as p
    where not exists (
        select 1
        from declared as d
        where d.column_name = p.column_name collate Latin1_General_BIN2
    )
) as wrong
order by
    kind
  , column_name;

if @weaver_delete_shape_error is not null
begin
    throw 51007, @weaver_delete_shape_error, 1;
end;
