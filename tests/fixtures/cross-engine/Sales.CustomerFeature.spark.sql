/*
Table ID: Sales.CustomerFeature

Description: A feature derived back in the Lakehouse from the Warehouse summary.

Lineage: $Reporting.CustomerSummary

Dependencies:
  - Reporting.CustomerSummary

Primary key: Customer id

Schema:
  Customer id: string
  Weighted orders: bigint

Notes: |
  Reads Reporting.CustomerSummary as a two-part name — it resolves through the
  Lakehouse alias the Warehouse table publishes, closing the loop back into the
  Lakehouse. The dependency is declared because a Spark query may read by path.

Revision notes:
  - 2026-07-24 Created.
*/

select s.`Customer id`
     , s.`Order count` * 2 as `Weighted orders`
  from Reporting.CustomerSummary as s
