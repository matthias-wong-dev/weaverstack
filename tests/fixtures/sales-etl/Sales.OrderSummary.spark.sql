/*
Table ID: Sales.OrderSummary

Description: Order totals by customer, excluding anything cancelled.

Lineage: $Sales.Order

Primary key: Customer id

Dependencies:
  - Sales.Order

Schema:
  Customer id: string
  Order count: bigint
  Total amount: decimal(18,2)

Notes: |
  Uses a temporary view for the recent window, and a left anti join to drop
  cancelled orders rather than a not-exists subquery.

Revision notes:
  - 2026-07-23 Created.
*/

-- Historically this read from Legacy.OrderSummary.
create or replace temporary view recent as
select *
  from `Sales`.`Order`
 where `Order date` >= add_months(current_date(), -12);

select r.`Customer id`
     , count(*)          as `Order count`
     , sum(r.`Amount`)   as `Total amount`
  from recent r
  left anti join Sales.Cancelled x on x.`Order id` = r.`Order id`
 group by r.`Customer id`
