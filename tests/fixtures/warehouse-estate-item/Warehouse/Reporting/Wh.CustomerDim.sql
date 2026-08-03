/*
Table ID: Wh.CustomerDim

Description: Customer dimension with an engine-generated surrogate key.

Lineage: $Wh.Customer

Primary key: CustomerId

Identity: CustomerKey
*/
select c.CustomerId, c.CustomerName
from [Wh].[Customer] as c
