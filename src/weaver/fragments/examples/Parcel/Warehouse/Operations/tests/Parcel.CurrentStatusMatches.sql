/*
Test ID: Parcel.CurrentStatusMatches

Description: Latest parcel status matches the independently expected rows.

Primary key: Parcel ID
*/
select v.[Parcel ID]
     , v.[Status]
     , v.[Depot]
from (values
    ('P-1001', 'In transit', 'Central'),
    ('P-1002', 'Delivered', 'South')
) as v ([Parcel ID], [Status], [Depot]);

select [Parcel ID]
     , [Status]
     , [Depot]
from [Parcel].[CurrentStatus];
