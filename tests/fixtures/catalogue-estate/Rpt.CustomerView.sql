/*
View ID: Rpt.CustomerView

Description: Customers shaped for reporting.

Lineage: $Sales.Customer

Primary key: Customer id

Unique keys:
  - Customer name

Foreign keys:
  - Customer id: Sales.Customer[Customer id]
*/
select c.[Customer id]
     , c.[Customer name]
  from [Sales].[Customer] as c
