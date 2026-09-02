-- One object's load, and nothing else. Its result is in the output parameters
-- rather than in a result set, and it records nothing: an orchestrated run
-- records what settled, and `exec _.[Load] @object_name = '...'` is what runs
-- this by hand and records the outcome.
create or alter procedure $load_procedure
    @fault_tolerant bit = 0
  , @ignore_stability_threshold bit = 0
  , @reload bit = 0
$result_parameters
as
begin
    set nocount on;

    declare @weaver_load_datetime datetime2(6) = sysutcdatetime();
    declare @weaver_live_datetime datetime2(6) = convert(datetime2(6), '$live_delete_datetime');
    set @weaver_rows_read = 0;
    set @weaver_rows_inserted = 0;
    set @weaver_rows_updated = 0;
    set @weaver_rows_deleted = 0;
    set @weaver_rows_rejected = 0;
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

    -- A reload reconstructs this table from zero. Before the staging query,
    -- because an incremental body may join the target to find what it has still
    -- to produce. The caller has already put the bookmark back to the sentinel.
    if @reload = 1
    begin
        delete from $target_table;
    end;

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

$result_assignment
end;
