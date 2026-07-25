/*
Table ID: Sales.InferredCustomer

Description: Customers, with the physical shape inferred from the query.

Lineage: $Sales.Customer

Primary key: CustomerId

Dependencies:
  - Sales.Customer
*/
select
    CustomerId,
    CustomerName
from Sales.Customer
