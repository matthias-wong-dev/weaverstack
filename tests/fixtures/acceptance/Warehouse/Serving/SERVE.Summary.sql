/*
Table ID: SERVE.Summary

Description: What each customer transacted, aggregated in the Warehouse.

Lineage: The Warehouse's own customers and transactions.

Dependencies:
  - SERVE.Customer
  - SERVE.Transaction

Primary key: CustomerId
*/
select
    c.CustomerId
  , c.CustomerName
  , count(t.TransactionId) as TransactionCount
  , coalesce(sum(t.Amount), 0) as TotalAmount
from [SERVE].[Customer] as c
left join [SERVE].[Transaction] as t
    on t.CustomerId = c.CustomerId
group by c.CustomerId, c.CustomerName;
