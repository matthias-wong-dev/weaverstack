/*
Table ID: Sales.Customer

Description: Customers, materialised into the Warehouse for reporting joins.

Lineage: $Sales.Customer

Notes: |
  Deliberately shares its ID with the Delta table of the same name. They are
  different physical objects in different installations, and both are
  legitimate — which is why the source below names three parts.

Primary key: Customer id

Lakehouse alias: Sales.CustomerWarehouse
*/
select c.[Customer id]
     , c.[Customer name]
  from [Sales_LH].[Sales].[Customer] as c
