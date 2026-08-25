/*
Table ID: CUR.Transaction

Description: Transactions, authored in Spark SQL rather than Python.

Lineage: $SRC.Transaction

Primary key: TransactionId

Dependencies:
  - SRC.Transaction

Notes: |
  A Spark SQL table beside the Python ones, so the estate carries both authoring
  languages for a Delta target. It compiles to a SparkSqlTable and loads through
  the ordinary Table.load().

Schema:
  TransactionId: integer
  CustomerId: integer
  Amount: decimal(18, 2)

Revision notes:
  - 2026-08-24 Created.
*/
select
    cast(TransactionId as int) as TransactionId
  , cast(CustomerId as int) as CustomerId
  , cast(Amount as decimal(18, 2)) as Amount
from SRC.Transaction;
