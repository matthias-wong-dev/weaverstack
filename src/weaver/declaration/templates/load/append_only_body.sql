-- An incremental table with no primary key treats every staged row as new.
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
