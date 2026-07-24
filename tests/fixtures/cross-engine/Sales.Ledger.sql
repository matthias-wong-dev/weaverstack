/*
Table ID: Sales.Ledger

Description: Ledger entries as a Warehouse table.

Lineage: A constant seed, for the fixture.

Notes: |
  A Warehouse table that shares the name Sales.Ledger with a Delta table of the
  same name. Different engine, different object, one owner of the name here.

Revision notes:
  - 2026-07-24 Created.
*/

select cast(null as varchar(64)) as [Entry id]
     , cast(0 as decimal(18, 2)) as [Amount]
