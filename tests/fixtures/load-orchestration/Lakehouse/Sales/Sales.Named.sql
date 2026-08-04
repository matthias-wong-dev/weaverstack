/*
Table ID: Sales.Named

Description: Customers whose name is known, downstream of the base table.

Lineage: $Sales.Customer

Primary key: Customer id

Dependencies:
  - Sales.Customer
*/
select `Customer id`, upper(`Customer name`) as `Customer name`
  from Sales.Customer
 where `Customer name` is not null
