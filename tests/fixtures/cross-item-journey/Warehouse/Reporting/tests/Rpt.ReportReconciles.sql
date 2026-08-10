/*
Test ID: Rpt.ReportReconciles

Description: >-
  The Warehouse report holds exactly the customers the Lakehouse produced. The
  claim the whole crossing exists to make — and the one that cannot be made on
  either side alone, because each side is self-consistent when the shortcut is
  stale.

Primary key: CustomerId

Revision notes:
  - 2026-08-10 Created.
*/

-- Expected: what the producing Lakehouse holds, read through this item's alias.
select CustomerId, CustomerName from [Rpt].[PortableCustomer];

-- Actual: what this Warehouse materialised from it.
select CustomerId, CustomerName from [Rpt].[CustomerReport];
