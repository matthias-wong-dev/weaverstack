/*
Assumption ID: CUR.TransactionHasCustomer

Description: Every transaction names a customer that exists.

Revision notes:
  - 2026-08-24 Created.
*/

-- The rows that contradict the assumption. Empty is what holding looks like.
select
    t.TransactionId
  , t.CustomerId
from CUR.Transaction as t
left join CUR.Customer as c
    on c.CustomerId = t.CustomerId
where c.CustomerId is null;
