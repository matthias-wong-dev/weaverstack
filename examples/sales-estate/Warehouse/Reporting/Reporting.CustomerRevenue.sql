/*
Table ID: Reporting.CustomerRevenue

Description: Revenue per customer, with a surrogate key for the BI model.

Lineage: $Reporting.OrderSummary

Primary key: Customer key

Identity: Customer key

Comparison columns: Customer name, Order count, Total amount

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
  Order count: bigint
  Total amount: decimal(18,2)

Notes: |
  The Identity column is the Warehouse's own: build declares it `bigint
  identity` and the engine assigns it, so a load never inserts into it. The
  values are Fabric's to choose — they are neither consecutive nor ordered, so
  nothing may read sequence into them.

Revision notes:
  - 2026-08-03 Created.
*/

select s.[Customer id]
     , s.[Customer name]
     , s.[Order count]
     , s.[Total amount]
  from [Reporting].[OrderSummary] s
