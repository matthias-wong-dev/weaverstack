/*
Table ID: Wh.CustomerDim

Description: Customer dimension with a Weaver-managed surrogate key.

Lineage: $Wh.Customer

Primary key: CustomerKey

Identity: CustomerKey
*/
select c.CustomerId, c.CustomerName
from [Wh].[Customer] as c
