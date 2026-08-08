/*
Test ID: Sales.OrderSummaryReconciliation

Description: The materialised summary matches an aggregation done independently.

Primary key: Customer id

Notes: |
  The point of a Test is that the two sides are derived *separately*. The
  expected side aggregates the orders here, in this file; the actual side reads
  the summary table the load built. If the two agree, the summary load is
  behaving; if they disagree, the rows say which customers and by how much.

  A Spark SQL Test is exactly the file you would have written by hand to check
  this — two queries, expected then actual — and Weaver supplies the comparison
  so that "the Sales tests pass" means the same thing here as it does for a Test
  somebody wrote in Python.

Revision notes:
  - 2026-08-08 Created.
*/

-- Expected: the summary, derived again from the orders.
select o.`Customer id`
     , c.`Customer name`
     , count(*)                                as `Order count`
     , cast(sum(o.`Amount`) as decimal(18,2))  as `Total amount`
  from Sales.Order o
  join Sales.Customer c on c.`Customer id` = o.`Customer id`
 group by o.`Customer id`, c.`Customer name`;

-- Actual: what the load actually wrote.
select `Customer id`
     , `Customer name`
     , `Order count`
     , `Total amount`
  from Sales.OrderSummary;
