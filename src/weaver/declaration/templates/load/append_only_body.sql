/*-- What this append window will refuse, established before writing --*/

create table $reject_table as
select
    __STAGING_SELECT_COLUMNS__
  , cast(N'' as varchar($reason_width)) as [$rejection_reason]
from $staging_table as s
where 1 = 0;

$reject_discovery

select @weaver_rows_rejected = count(*) from $reject_table;
if @weaver_rows_rejected > 0 and @fault_tolerant = 0
begin
$reject_refusal_assignment
    if @return_refusal = 0
        throw 51020, '$intolerant_message', 1;
    return;
end;

/*-- The accepted rows in this append window --*/

create table $upsert_table as
select __STAGING_SELECT_COLUMNS__
from $staging_table as s
where 1 = 0;

$survivor_materialisation

/*-- Append only: no target match, update or absence-based delete --*/

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
from $upsert_table as u;

set @weaver_rows_inserted = @@rowcount;

if @weaver_rows_rejected > 0
    set @weaver_error = cast(@weaver_rows_rejected as varchar(20))
        + ' $tolerated_message';
