/*
View ID: SERVE.vSummary

Description: The summary a consumer reads.

Lineage: $SERVE.Summary

Dependencies:
  - SERVE.Summary
*/
select CustomerId, CustomerName, TransactionCount, TotalAmount
from [SERVE].[Summary];
