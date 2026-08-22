-- The load's result is in the output parameters, not in a result set.
create or alter procedure $load_procedure
    @fault_tolerant bit = 0
  , @ignore_stability_threshold bit = 0
  -- Whether this procedure maintains its own bookmark. An orchestrated run
  -- passes 0 and writes it with the run's record; running the procedure by hand
  -- leaves it at 1, so the object's own history stays correct either way.
  , @update_catalogue bit = 1
$result_parameters
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
    declare @weaver_duplicate_keys bigint = 0;
    declare @weaver_merge_conflicts bigint = 0;
    declare @weaver_error varchar(4000) = null;
    declare @weaver_target_rows bigint = 0;
    declare @weaver_prospective_deletes bigint = 0;
    declare @weaver_prospective_updates bigint = 0;
    declare @weaver_target_before bigint = 0;
    declare @weaver_bookmark datetime2(6) = null;

$bookmark_key

$static_gate

$preprocessing_banner
$start_artifact_cleanup

$staging_sql

    select @weaver_rows_read = count(*) from $staging_table;

$postprocessing_banner
$load_body

$end_artifact_cleanup

    -- What the target actually lost, from its own cardinality rather than from
    -- what the load intended to remove.
    select @weaver_rows_deleted =
        @weaver_target_before + @weaver_rows_inserted - count(*)
    from $target_table;

$bookmark_update

$result_assignment
end;
