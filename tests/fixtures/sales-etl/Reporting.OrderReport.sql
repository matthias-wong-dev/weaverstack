/*
Table ID: Reporting.OrderReport

Description: Orders enriched for reporting.

Lineage: $Sales.Order

Primary key: Order id

Column notes:
  Order id: Matches the source order identifier.

Revision notes:
  - 2026-07-23 Created.
*/

select o.[Order id]
     , o.[Customer id]
     , o.[Amount]
  from Sales.Order as o
