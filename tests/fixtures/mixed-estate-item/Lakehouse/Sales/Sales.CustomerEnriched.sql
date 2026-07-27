/*
Table ID: Sales.CustomerEnriched

Description: Enriched customers, shape inferred from the query.

Lineage: $Sales.Customer

Primary key: CustomerId

Dependencies:
  - Sales.Customer
*/
select CustomerId, CustomerName from Sales.Customer
