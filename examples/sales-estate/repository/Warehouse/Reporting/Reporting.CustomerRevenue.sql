/*
Table ID: Reporting.CustomerRevenue

Description: Revenue per customer, with a surrogate key for the BI model.

Lineage: $Reporting.OrderSummary

Primary key: Customer id

Identity: Customer key

Comparison columns: Customer name, Order count, Total amount

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
  Order count: bigint
  Total amount: decimal(18,2)

Notes: |
  The primary key is the *business* key, and the identity column is a separate
  surrogate for the BI model. That separation is not a style choice: the engine
  assigns the identity on insert, so the query never produces it and a load
  matching on it could never find an existing row. The business key is what
  identifies a row across loads; the surrogate is what a star schema joins on.

  Build declares the identity `bigint identity` and the engine assigns it, so a
  load never inserts into it. The values are Fabric's to choose — neither
  consecutive nor ordered — so nothing may read sequence into them.

Revision notes:
  - 2026-08-03 Created.
*/

select s.[Customer id]
     , s.[Customer name]
     , s.[Order count]
     , s.[Total amount]
  from [Reporting].[OrderSummary] s
