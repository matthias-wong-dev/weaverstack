/*
View ID: Sales.ActiveCustomer

Description: A Spark view over the enriched customers.

Lineage: $Sales.CustomerEnriched

Dependencies:
  - Sales.CustomerEnriched
*/
select CustomerId, CustomerName from Sales.CustomerEnriched
