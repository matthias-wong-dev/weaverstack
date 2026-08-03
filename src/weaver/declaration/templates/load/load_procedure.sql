create or alter procedure $load_procedure
    @fault_tolerant bit = 0
  , @ignore_stability_threshold bit = 0
as
begin
    set nocount on;

    declare @weaver_load_datetime datetime2(6) = sysutcdatetime();
    declare @weaver_live_datetime datetime2(6) = convert(datetime2(6), '$live_delete_datetime');
    declare @weaver_rows_read bigint = 0;
    declare @weaver_rows_inserted bigint = 0;
    declare @weaver_rows_updated bigint = 0;
    declare @weaver_rows_deleted bigint = 0;
    declare @weaver_rows_rejected bigint = 0;
    declare @weaver_error varchar(4000) = null;
    declare @weaver_target_rows bigint = 0;
    declare @weaver_prospective_deletes bigint = 0;
    declare @weaver_prospective_updates bigint = 0;

$preprocessing_banner
$start_artifact_cleanup

$staging_sql

    select @weaver_rows_read = count(*) from $staging_table;

$postprocessing_banner
$load_body

$end_artifact_cleanup

    select
        cast(case when @weaver_error is null then 1 else 0 end as bit) as succeeded
      , @weaver_rows_read as rows_read
      , @weaver_rows_inserted as rows_inserted
      , @weaver_rows_updated as rows_updated
      , @weaver_rows_deleted as rows_deleted
      , @weaver_rows_rejected as rows_rejected
      , @weaver_error as error_message;
end;
