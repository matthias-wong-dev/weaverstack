create table $reject_table as
select
    __STAGING_SELECT_COLUMNS__
  , case
        when $staging_blank_predicate then cast('$blank_reason' as varchar(100))
        else cast('$duplicate_reason' as varchar(100))
    end as [$rejection_reason]
from $staging_table as s
where $staging_blank_predicate
   or s.[$rank_column] > 1;

select @weaver_rows_rejected = count(*) from $reject_table;

delete s
from $staging_table as s
where $staging_blank_predicate
   or s.[$rank_column] > 1;

-- Intolerant of rejects: the target is left exactly as it was. Nothing has been
-- written yet, so refusing here is a decision not to start rather than an
-- unwind — and the reject table survives as the evidence.
--
-- Raised rather than returned, so `exec [_].[Load S.N]` fails the same way
-- `.load()` does. A primitive that returned a quiet row where its sibling
-- raised would make every caller special-case which one it was talking to.
if @weaver_rows_rejected > 0 and @fault_tolerant = 0
    throw 51020, '$intolerant_message', 1;

create table $upsert_table as
select
    __STAGING_SELECT_COLUMNS__
  , case when $target_missing_predicate then cast(1 as int) else cast(0 as int) end as [_Is new row]
from $staging_table as s
left join $target_table as t on $staging_target_join
where
    $target_missing_predicate
    or exists (
        select __STAGING_EXCEPT_COLUMNS__
        except
        select __TARGET_EXCEPT_COLUMNS__
    );

-- Everything this load is about to do, counted before it does any of it. A
-- breach with @fault_tolerant = 0 leaves the target exactly as it was, so
-- refusing is a decision not to start rather than an unwind.
select @weaver_target_rows = count(*) from $target_table;
set @weaver_target_before = @weaver_target_rows;
select @weaver_prospective_updates = count(*) from $upsert_table where [_Is new row] = 0;
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

    -- A breach never writes. Tolerating exactly the change the threshold was
    -- declared to prevent would defeat the guard, so @fault_tolerant decides
    -- only whether the refusal is raised or returned. Permitting it is what
    -- @ignore_stability_threshold is for.
    if @weaver_error is not null
    begin
        set @weaver_error = @weaver_error + '; the target was not modified';
        if @fault_tolerant = 0
            throw 51021, @weaver_error, 1;
        select
            cast(0 as bit) as succeeded
          , @weaver_rows_read as rows_read
          , cast(0 as bigint) as rows_inserted
          , cast(0 as bigint) as rows_updated
          , cast(0 as bigint) as rows_deleted
          , @weaver_rows_rejected as rows_rejected
          , @weaver_error as error_message;
        return;
    end;
end;

insert into $target_table (
    __SOURCE_COLUMNS__
  , [Row insert datetime]
  , [Row update datetime]
  , [Row delete datetime]
)
select
    __UPSERT_SELECT_COLUMNS__
  , @weaver_load_datetime
  , @weaver_load_datetime
  , @weaver_live_datetime
from $upsert_table as u
where u.[_Is new row] = 1;

set @weaver_rows_inserted = @@rowcount;

update c
set
    __UPDATE_SET_COLUMNS__
from $target_table as c
inner join $upsert_table as u on $target_upsert_join
where u.[_Is new row] = 0;

set @weaver_rows_updated = @@rowcount;

$missing_reconciliation

if @weaver_rows_rejected > 0 and @weaver_error is null
    set @weaver_error = cast(@weaver_rows_rejected as varchar(20))
        + ' $tolerated_message';
