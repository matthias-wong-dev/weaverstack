/*
Table ID: SERVE.Reporting

Description: The reporting table, materialised from the view over the summary.

Lineage: $SERVE.vSummary

Dependencies:
  - SERVE.vSummary

Primary key: CustomerId

Notes: |
  A table over a view, so the build has to order the view before the table that
  reads it.
*/
select CustomerId, CustomerName, TransactionCount, TotalAmount
from [SERVE].[vSummary];
