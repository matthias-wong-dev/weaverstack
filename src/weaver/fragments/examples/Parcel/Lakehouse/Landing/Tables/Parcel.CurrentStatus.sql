/*
Table ID: Parcel.CurrentStatus

Description: The latest recorded status for each parcel.

Lineage: Parcel status events ordered by event sequence.

Dependencies:
  - Parcel.Event

Primary key: Parcel ID

Schema:
  Parcel ID: string
  Status: string
  Depot: string
*/
with ranked as (
    select `Parcel ID`
         , `Status`
         , `Depot`
         , row_number() over (
               partition by `Parcel ID`
               order by `Event sequence` desc
           ) as `Event rank`
    from Parcel.Event
)
select `Parcel ID`, `Status`, `Depot`
from ranked
where `Event rank` = 1;
