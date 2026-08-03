/* weaver generated load procedure installer. */
set nocount on;

declare @weaver_proc_sql nvarchar(max);
declare @weaver_source_columns nvarchar(max);
declare @weaver_staging_select_columns nvarchar(max);
declare @weaver_staging_except_columns nvarchar(max);
declare @weaver_target_except_columns nvarchar(max);
declare @weaver_upsert_select_columns nvarchar(max);
declare @weaver_update_set_columns nvarchar(max);

$column_metadata_sql

if @weaver_source_columns is null
begin
    throw 51010, 'weaver: the load target has no loadable columns. Build the table before installing its load.', 1;
end;

set @weaver_proc_sql = $procedure_template_sql_literal;
set @weaver_proc_sql = replace(@weaver_proc_sql, N'__SOURCE_COLUMNS__', @weaver_source_columns);
set @weaver_proc_sql = replace(@weaver_proc_sql, N'__STAGING_SELECT_COLUMNS__', @weaver_staging_select_columns);
set @weaver_proc_sql = replace(@weaver_proc_sql, N'__STAGING_EXCEPT_COLUMNS__', @weaver_staging_except_columns);
set @weaver_proc_sql = replace(@weaver_proc_sql, N'__TARGET_EXCEPT_COLUMNS__', @weaver_target_except_columns);
set @weaver_proc_sql = replace(@weaver_proc_sql, N'__UPSERT_SELECT_COLUMNS__', @weaver_upsert_select_columns);
set @weaver_proc_sql = replace(@weaver_proc_sql, N'__UPDATE_SET_COLUMNS__', @weaver_update_set_columns);

exec sys.sp_executesql @weaver_proc_sql;
