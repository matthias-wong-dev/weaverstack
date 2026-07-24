/*
Table ID: Sales.Ledger

Description: Ledger entries as a Delta table, shaped from the customer table.

Lineage: $Sales.Customer

Dependencies:
  - Sales.Customer

Primary key: Entry id

Schema:
  Entry id: string
  Customer id: string
  Amount: decimal(18,2)

Notes: |
  A Delta table that shares the name Sales.Ledger with a Folder and a Warehouse
  table. It carries no alias, so nothing publishes this name across engines.

Revision notes:
  - 2026-07-24 Created.
*/

select cast(null as string) as `Entry id`
     , c.`Customer id`
     , cast(0 as decimal(18,2)) as `Amount`
  from Sales.Customer as c
