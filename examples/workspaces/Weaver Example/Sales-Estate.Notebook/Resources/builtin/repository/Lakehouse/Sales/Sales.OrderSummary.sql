/*
Table ID: Sales.OrderSummary

Description: Order totals by customer, for reporting.

Lineage: $Sales.Order

Primary key: Customer id

Dependencies:
  - Sales.Order
  - Sales.Customer

Comparison columns: Order count, Total amount

Schema:
  Customer id: string
  Customer name: string
  Order count: bigint
  Total amount: decimal(18,2)

Revision notes:
  - 2026-08-03 Created.
*/

select o.`Customer id`
     , c.`Customer name`
     , count(*)                                as `Order count`
     , cast(sum(o.`Amount`) as decimal(18,2))  as `Total amount`
  from Sales.Order o
  join Sales.Customer c on c.`Customer id` = o.`Customer id`
 group by o.`Customer id`, c.`Customer name`;
