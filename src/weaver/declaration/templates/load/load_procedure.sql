-- The load's result is in the signature, not in a result set. Authored setup
-- may run EXEC or sp_executesql that returns rows of its own, and a caller
-- reading "the result set this procedure produced" would then be reading
-- somebody else's. Optional outputs cannot be confused with anything.
create or alter procedure $load_procedure
    @fault_tolerant bit = 0
  , @ignore_stability_threshold bit = 0
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

$static_gate

$preprocessing_banner
$start_artifact_cleanup

$staging_sql

    select @weaver_rows_read = count(*) from $staging_table;

$postprocessing_banner
$load_body

$end_artifact_cleanup

    -- What the target actually lost, from its own cardinality. The delete
    -- driver says what the load intended; this says what happened, and the two
    -- differ whenever a key named for deletion was not there to begin with.
    select @weaver_rows_deleted =
        @weaver_target_before + @weaver_rows_inserted - count(*)
    from $target_table;

$result_assignment
end;
