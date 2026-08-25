/*
Test ID: SERVE.ReportReconciles

Description: The Warehouse report holds exactly the customers Curated produced.

Primary key: CustomerId

Revision notes:
  - 2026-08-24 Created.
*/

-- Expected: what Curated holds, read through this item's shortcut.
select CustomerId, CustomerName from [SRC].[Customer];

-- Actual: what this Warehouse materialised and reported.
select CustomerId, CustomerName from [SERVE].[Reporting];
