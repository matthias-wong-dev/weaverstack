"""Pure generation of the proven Fabric Warehouse wipe batch."""

from __future__ import annotations


def generate_warehouse_wipe_sql() -> str:
    """Return deterministic SQL that removes supported user-created objects.

    This is ported from the legacy Weaver Warehouse implementation:
    ``src/weaver_runtime/dbrep/sql/templates/admin/wipe.sql`` for object
    enumeration and ordering, plus ``_drop_user_schemas`` in
    ``src/weaver_runtime/dbrep/sql/backend.py`` for schema cleanup.
    """

    return _WAREHOUSE_WIPE_SQL


_WAREHOUSE_WIPE_SQL = """\
set nocount on;

declare @weaver_sql nvarchar(max);

/* Foreign keys first so dependent tables can be dropped. */
select
    @weaver_sql = string_agg(
        convert(
            nvarchar(max),
            N'alter table '
            + quotename(object_schema_name(fk.parent_object_id))
            + N'.'
            + quotename(object_name(fk.parent_object_id))
            + N' drop constraint '
            + quotename(fk.name)
            + N';'
        ),
        char(10)
    ) within group (order by fk.object_id)
from sys.foreign_keys as fk
where lower(schema_name(schema_id)) not in
    (N'guest', N'information_schema', N'sys', N'queryinsights');

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;

/* Views before the objects and tables they can depend on. */
set @weaver_sql = null;
select
    @weaver_sql = string_agg(
        convert(
            nvarchar(max),
            N'drop view '
            + quotename(schema_name(schema_id))
            + N'.'
            + quotename(name)
            + N';'
        ),
        char(10)
    ) within group (order by object_id)
from sys.views
where lower(schema_name(schema_id)) not in
    (N'guest', N'information_schema', N'sys', N'queryinsights');

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;

/* Stored procedures. */
set @weaver_sql = null;
select
    @weaver_sql = string_agg(
        convert(
            nvarchar(max),
            N'drop procedure '
            + quotename(schema_name(schema_id))
            + N'.'
            + quotename(name)
            + N';'
        ),
        char(10)
    ) within group (order by object_id)
from sys.procedures
where lower(schema_name(schema_id)) not in
    (N'guest', N'information_schema', N'sys', N'queryinsights');

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;

/* Functions (scalar, inline table-valued, multi-statement table-valued). */
set @weaver_sql = null;
select
    @weaver_sql = string_agg(
        convert(
            nvarchar(max),
            N'drop function '
            + quotename(schema_name(schema_id))
            + N'.'
            + quotename(name)
            + N';'
        ),
        char(10)
    ) within group (order by object_id)
from sys.objects
where type in (N'FN', N'IF', N'TF', N'FS', N'FT')
    and lower(schema_name(schema_id)) not in
        (N'guest', N'information_schema', N'sys', N'queryinsights');

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;

/* Tables. */
set @weaver_sql = null;
select
    @weaver_sql = string_agg(
        convert(
            nvarchar(max),
            N'drop table '
            + quotename(schema_name(schema_id))
            + N'.'
            + quotename(name)
            + N';'
        ),
        char(10)
    ) within group (order by object_id)
from sys.tables
where lower(schema_name(schema_id)) not in
    (N'guest', N'information_schema', N'sys', N'queryinsights');

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;

/* User schemas last; the built-in and Fabric-owned schemas survive. */
set @weaver_sql = null;
select
    @weaver_sql = string_agg(
        convert(nvarchar(max), N'drop schema ' + quotename(name) + N';'),
        char(10)
    ) within group (order by schema_id)
from sys.schemas
where lower(name) not in
    (N'dbo', N'guest', N'information_schema', N'sys', N'queryinsights', N'_rsc')
    and schema_id < 16384;

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;
"""
