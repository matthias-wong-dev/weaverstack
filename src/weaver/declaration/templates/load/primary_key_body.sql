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
if @weaver_rows_rejected > 0 and @fault_tolerant = 0
begin
    select
        cast(0 as bit) as succeeded
      , @weaver_rows_read as rows_read
      , cast(0 as bigint) as rows_inserted
      , cast(0 as bigint) as rows_updated
      , cast(0 as bigint) as rows_deleted
      , @weaver_rows_rejected as rows_rejected
      , cast('$intolerant_message' as varchar(4000)) as error_message;
    return;
end;

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

if @weaver_rows_rejected > 0
    set @weaver_error = cast(@weaver_rows_rejected as varchar(20))
        + ' $tolerated_message';
