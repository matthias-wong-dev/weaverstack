/*
Test ID: CUR.CustomerMatches

Description: The curated customers are the ones Landing produced.

Primary key: CustomerId

Revision notes:
  - 2026-08-24 Created.
*/

-- Expected: what Landing holds, read through this item's shortcut.
select CustomerId, CustomerName from SRC.Customer;

-- Actual: what the incremental load left behind.
select CustomerId, CustomerName from CUR.Customer;
