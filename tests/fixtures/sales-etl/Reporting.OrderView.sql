/*
View ID: Reporting.OrderView

Description: The reporting order table, without deleted rows.

Lineage: $Reporting.OrderReport

Prohibit rebuild: true

Notes: |
  Row-level security is applied to this view by the platform team. Rebuilding
  it drops those grants, hence Prohibit rebuild.

Revision notes:
  - 2026-07-23 Created.
*/

select *
  from Reporting.OrderReport
