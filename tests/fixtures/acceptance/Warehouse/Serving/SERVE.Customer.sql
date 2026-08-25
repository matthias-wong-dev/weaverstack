/*
Table ID: SERVE.Customer

Description: Customers, materialised in the Warehouse from the Lakehouse.

Lineage: $SRC.Customer

Primary key: CustomerId

Incremental: true

Notes: |
  Incremental, so absence from the staging query retires nothing and the second
  statement claims what to retire. The claim reads the retire feed Curated
  materialised: a T-SQL load cannot read its own target to work out what went.
*/
select CustomerId, CustomerName
from [SRC].[Customer];

select CustomerId
from [SRC].[RetiredCustomer];
