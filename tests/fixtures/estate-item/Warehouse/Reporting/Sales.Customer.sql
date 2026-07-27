/*
Table ID: Sales.Customer

Description: Customers, materialised into the Warehouse for reporting joins.

Lineage: $Lakehouse/Sales/Sales.Customer

Notes: |
  Deliberately shares its ID with the Delta table of the same name. They are
  different logical items, and both are legitimate — which is why the
  lineage names the producing item in full: a bare Sales.Customer here
  would mean this very object.

Revision notes:
  - 2026-07-23 Created.
*/

select c.[Customer id]
     , c.[Customer name]
  from [Sales_LH].[Sales].[Customer] as c
