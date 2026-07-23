/*
Table ID: Sales.OrderSummary

Description: Order totals by customer.

Lineage: Aggregated from the order table.

Primary key: Customer id

Dependencies:
  - Sales.Order

Schema:
  Customer id: string
  Order count: bigint
  Total amount: decimal(18,2)

Revision notes:
  - 2026-07-23 Created.
*/

create or replace temp view recent as
select *
  from Sales.Order
 where `Order date` >= add_months(current_date(), -12);

select `Customer id`
     , count(*)     as `Order count`
     , sum(`Amount`) as `Total amount`
  from recent
 group by `Customer id`
