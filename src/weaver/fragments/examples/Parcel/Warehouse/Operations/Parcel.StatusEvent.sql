/*
Table ID: Parcel.StatusEvent

Description: One row per parcel status event.

Lineage: A deterministic carrier feed represented by this example.

Primary key: Parcel ID, Event sequence

Schema:
  Parcel ID: varchar(20)
  Event sequence: int
  Status: varchar(30)
  Depot: varchar(50)
*/
select v.[Parcel ID]
     , v.[Event sequence]
     , v.[Status]
     , v.[Depot]
from (values
    ('P-1001', 1, 'Accepted', 'North'),
    ('P-1001', 2, 'In transit', 'Central'),
    ('P-1002', 1, 'Delivered', 'South')
) as v ([Parcel ID], [Event sequence], [Status], [Depot]);
