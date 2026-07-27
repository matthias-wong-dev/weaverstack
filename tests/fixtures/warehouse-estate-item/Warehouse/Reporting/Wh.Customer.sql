/*
Table ID: Wh.Customer

Description: Base customers, seeded from literal values.

Lineage: A deterministic VALUES seed.

Primary key: CustomerId
*/
select v.CustomerId, v.CustomerName
from (values
    (1, 'Ada Lovelace'),
    (2, 'Bo Diddley'),
    (3, 'Cy Young')
) as v (CustomerId, CustomerName)
