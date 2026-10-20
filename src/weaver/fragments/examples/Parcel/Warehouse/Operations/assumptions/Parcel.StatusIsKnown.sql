/*
Assumption ID: Parcel.StatusIsKnown

Description: Every recorded parcel event has a recognised status.
*/
select [Parcel ID]
     , [Event sequence]
     , [Status]
from [Parcel].[StatusEvent]
where [Status] is null
   or [Status] not in ('Accepted', 'In transit', 'Delivered');
