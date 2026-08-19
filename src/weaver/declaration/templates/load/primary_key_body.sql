/*-- What this load will refuse, established before anything is changed --*/

-- Shaped from staging, so it holds the refused row itself.
create table $reject_table as
select
    __STAGING_SELECT_COLUMNS__
  , cast(N'' as varchar($reason_width)) as [$rejection_reason]
from $staging_table as s
where 1 = 0;

$reject_discovery

select @weaver_rows_rejected = count(*) from $reject_table;
$duplicate_key_count
-- Nothing is written yet, so the target is left as it was.
if @weaver_rows_rejected > 0 and @fault_tolerant = 0
    throw 51020, '$intolerant_message', 1;

/*-- Staging becomes the accepted incoming state --*/

$staging_purge

/*-- The rows this load will remove --*/

$delete_derivation

/*-- The rows this load will write: new and changed, and nothing else --*/

create table $upsert_table as
select
    __QUERY_SELECT_COLUMNS__
  , q.[$signature_column]
  , case when $target_missing_predicate then cast(1 as int) else cast(0 as int) end as [$is_new_column]
from (
    select
        __STAGING_SELECT_COLUMNS__
      , $signature_expression as [$signature_column]
    from $staging_table as s
) as q
left join $target_table as t on $query_target_join
where
    $target_missing_predicate
    or q.[$signature_column] <> t.[$signature_column];

$merge_uniqueness
-- Counted before anything is written.
select @weaver_target_rows = count(*) from $target_table;
set @weaver_target_before = @weaver_target_rows;
select @weaver_prospective_updates = count(*) from $upsert_table where [$is_new_column] = 0;
$prospective_deletes

if @ignore_stability_threshold = 0 and @weaver_target_rows > 0
    and @weaver_target_rows >= $stability_rows
begin
    if @weaver_prospective_deletes * 100.0 / @weaver_target_rows > $delete_threshold
        set @weaver_error = 'delete of ' + cast(@weaver_prospective_deletes as varchar(20))
            + ' rows is over the $delete_threshold% threshold of '
            + cast(@weaver_target_rows as varchar(20));
    else if @weaver_prospective_updates * 100.0 / @weaver_target_rows > $update_threshold
        set @weaver_error = 'update of ' + cast(@weaver_prospective_updates as varchar(20))
            + ' rows is over the $update_threshold% threshold of '
            + cast(@weaver_target_rows as varchar(20));

    -- A breach never writes. @ignore_stability_threshold is how to permit one.
    if @weaver_error is not null
    begin
        set @weaver_error = @weaver_error + '; the target was not modified';
        if @fault_tolerant = 0
            throw 51021, @weaver_error, 1;
$breach_result_assignment
        return;
    end;
end;

/*-- The target, once every gate has passed --*/

$missing_reconciliation

update c
set
    __UPDATE_SET_COLUMNS__
from $target_table as c
inner join $upsert_table as u on $target_upsert_join
where u.[$is_new_column] = 0;

set @weaver_rows_updated = @@rowcount;

insert into $target_table (
    __SOURCE_COLUMNS__
  , [$signature_column]
  , [Row insert datetime]
  , [Row update datetime]
  , [Row delete datetime]
)
select
    __UPSERT_SELECT_COLUMNS__
  , u.[$signature_column]
  , @weaver_load_datetime
  , @weaver_load_datetime
  , @weaver_live_datetime
from $upsert_table as u
where u.[$is_new_column] = 1;

set @weaver_rows_inserted = @@rowcount;

if @weaver_rows_rejected > 0 and @weaver_error is null
    set @weaver_error = cast(@weaver_rows_rejected as varchar(20))
        + ' $tolerated_message';
