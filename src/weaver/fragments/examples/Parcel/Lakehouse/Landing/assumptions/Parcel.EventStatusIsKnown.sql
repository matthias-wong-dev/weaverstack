/*
Assumption ID: Parcel.EventStatusIsKnown

Description: Every parcel event has a recognised status.
*/
select `Parcel ID`, `Event sequence`, `Status`
from Parcel.Event
where `Status` is null
   or `Status` not in ('Accepted', 'In transit', 'Delivered');
