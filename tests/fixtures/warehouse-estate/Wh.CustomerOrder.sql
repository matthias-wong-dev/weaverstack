/*
Table ID: Wh.CustomerOrder

Description: Customer orders derived from the base customer table.

Lineage: $Wh.Customer

Primary key: CustomerId
*/
select c.CustomerId, c.CustomerName, cast(c.CustomerId * 100 as int) as OrderTotal
from [Wh].[Customer] as c
