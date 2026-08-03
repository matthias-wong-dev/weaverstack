-- No primary key, so no row can be matched to a target row: there is no such
-- thing as an update here, and nothing to reject. The target's contents become
-- the source's.
delete from $target_table;

set @weaver_rows_deleted = @@rowcount;

insert into $target_table (
    __SOURCE_COLUMNS__
  , [Row insert datetime]
  , [Row update datetime]
  , [Row delete datetime]
)
select
    __STAGING_SELECT_COLUMNS__
  , @weaver_load_datetime
  , @weaver_load_datetime
  , @weaver_live_datetime
from $staging_table as s;

set @weaver_rows_inserted = @@rowcount;
