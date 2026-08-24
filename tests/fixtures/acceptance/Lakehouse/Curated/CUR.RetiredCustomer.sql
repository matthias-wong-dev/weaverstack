/*
Table ID: CUR.RetiredCustomer

Description: The customers a retirement event has withdrawn.

Lineage: $CUR.Event

Dependencies:
  - CUR.Event

Primary key: CustomerId

Notes: |
  The retire feed the Warehouse reads. A T-SQL incremental load claims what to
  retire from a source rather than from its own target, so the claim has to be
  materialised somewhere it can read.

Schema:
  CustomerId: integer

Revision notes:
  - 2026-08-24 Created.
*/
select distinct cast(CustomerId as int) as CustomerId
from CUR.Event
where Kind = 'retired';
