/*
Table ID: Reporting.CustomerSummary

Description: Customer totals, shaped in the Warehouse.

Lineage: $Sales.Customer

Lakehouse alias: Reporting.CustomerSummary

Primary key: Customer id

Column notes:
  Customer id: Matches the source customer identifier.

Notes: |
  Reads Sales.Customer as a two-part name — it resolves through the Warehouse
  alias the Delta table publishes, so no Lakehouse is named here. Publishes
  itself back into the Lakehouse as Reporting.CustomerSummary.

Revision notes:
  - 2026-07-24 Created.
*/

select c.[Customer id]
     , count(*) as [Order count]
  from Sales.Customer as c
 group by c.[Customer id]
