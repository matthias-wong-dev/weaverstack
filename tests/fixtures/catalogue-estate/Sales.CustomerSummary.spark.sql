/*
Table ID: Sales.CustomerSummary

Description: $Sales.Customer

Lineage: $Sales.Customer

Dependencies:
  - Sales.Customer

Primary key: Region code

Column notes:
  Customer count: How many customers the region holds.
*/
select c.`Region code`
     , count(*) as `Customer count`
  from Sales.Customer as c
 group by c.`Region code`
