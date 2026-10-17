/*
Test ID: Parcel.CurrentStatusMatches

Description: Current statuses match the carrier export's expected latest events.

Primary key: Parcel ID
*/
select *
from values
    ('P-1001', 'In transit', 'Central'),
    ('P-1002', 'Delivered', 'South')
as expected(`Parcel ID`, `Status`, `Depot`);

select `Parcel ID`, `Status`, `Depot`
from Parcel.CurrentStatus;
