/*
View ID: Parcel.CurrentStatus

Description: The latest recorded status for each parcel.

Lineage: Parcel status events ordered by event sequence.
*/
with ranked as (
    select [Parcel ID]
         , [Status]
         , [Depot]
         , row_number() over (
               partition by [Parcel ID]
               order by [Event sequence] desc
           ) as [Event rank]
    from [Parcel].[StatusEvent]
)
select [Parcel ID]
     , [Status]
     , [Depot]
from ranked
where [Event rank] = 1;
