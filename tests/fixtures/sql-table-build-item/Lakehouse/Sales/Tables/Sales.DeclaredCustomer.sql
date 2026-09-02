/*
Table ID: Sales.DeclaredCustomer

Description: Customers, with a declared schema wider than the query infers.

Lineage: $Sales.Customer

Primary key: CustomerId

Dependencies:
  - Sales.Customer

Schema:
  CustomerId: bigint
  CustomerName: string
*/
select
    CustomerId,
    CustomerName
from Sales.Customer;
