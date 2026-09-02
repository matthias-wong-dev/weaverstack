/*
Table ID: CUR.CustomerSummary

Description: One row per customer, with what they transacted.

Lineage: The current customers and their transactions.

Dependencies:
  - CUR.CustomerCurrent
  - CUR.Transaction

Primary key: CustomerId

Notes: |
  Two parents, and one of them is a view. A table consuming a view is the shape
  that catches a build ordering a view after something that reads it.

Schema:
  CustomerId: integer
  CustomerName: string
  TransactionCount: integer
  TotalAmount: decimal(18, 2)

Revision notes:
  - 2026-08-24 Created.
*/
select
    c.CustomerId
  , c.CustomerName
  , cast(count(t.TransactionId) as int) as TransactionCount
  , cast(coalesce(sum(t.Amount), 0) as decimal(18, 2)) as TotalAmount
from CUR.CustomerCurrent as c
left join CUR.Transaction as t
    on t.CustomerId = c.CustomerId
group by c.CustomerId, c.CustomerName;
