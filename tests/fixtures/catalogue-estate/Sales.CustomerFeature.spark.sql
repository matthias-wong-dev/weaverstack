/*
Table ID: Sales.CustomerFeature

Description: Customer features read back from the Warehouse publication.

Lineage: $Sales.CustomerWarehouse

Dependencies:
  - Sales.CustomerWarehouse
*/
select c.`Customer id`
     , upper(c.`Customer name`) as `Customer name upper`
  from Sales.CustomerWarehouse as c
