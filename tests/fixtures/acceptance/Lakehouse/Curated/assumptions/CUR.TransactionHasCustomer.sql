/*
Assumption ID: CUR.TransactionHasCustomer

Description: Every transaction names an active or retired customer.

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
left join CUR.RetiredCustomer as r
    on r.CustomerId = t.CustomerId
where c.CustomerId is null
  and r.CustomerId is null;
