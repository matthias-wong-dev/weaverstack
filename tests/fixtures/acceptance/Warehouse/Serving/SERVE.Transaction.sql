/*
Table ID: SERVE.Transaction

Description: Transactions, copied whole on every load.

Lineage: $SRC.Transaction

Primary key: TransactionId

Notes: |
  Non-incremental, so the source is the whole truth and absence retires a row.
*/
select TransactionId, CustomerId, Amount
from [SRC].[Transaction];
