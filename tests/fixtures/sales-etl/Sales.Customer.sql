/*
Table ID: Sales.Customer

Description: Customers, materialised into the Warehouse for reporting joins.

Lineage: $Sales.Customer

Notes: |
  Deliberately shares its ID with the Delta table of the same name. They are
  different physical objects in different targets, and both are legitimate —
  which is why the source is named in three parts rather than two: a bare
  Sales.Customer here would bind to this very object.

Revision notes:
  - 2026-07-23 Created.
*/

select c.[Customer id]
     , c.[Customer name]
  from [Sales_LH].[Sales].[Customer] as c
