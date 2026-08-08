/*
Assumption ID: DWG.OrderHasCustomer

Description: Every order names a customer that exists.

Revision notes:
  - 2026-08-08 Created.
*/

-- A Spark SQL Assumption, so the journey installs a *compiled* validation
-- module as well as a deployed one and proves both reach the estate.
--
-- One result-producing query, and it returns the rows that contradict the
-- assumption: an order whose customer is not there. Empty is what holding looks
-- like.
select
    o.OrderId
  , o.CustomerId
from DWG.Order as o
left join DWG.Customer as c
    on c.CustomerId = o.CustomerId
where c.CustomerId is null;
