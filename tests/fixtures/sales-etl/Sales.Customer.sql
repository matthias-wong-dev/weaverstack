/*
Table ID: Sales.Customer

Description: Customers, materialised into the Warehouse for reporting joins.

Lineage: $Sales.Customer

Notes: |
  Deliberately shares its ID with the Delta table of the same name. They are
  different physical objects in different targets, and both are legitimate.

Revision notes:
  - 2026-07-23 Created.
*/

select c.[Customer id]
     , c.[Customer name]
  from Sales.Customer as c
