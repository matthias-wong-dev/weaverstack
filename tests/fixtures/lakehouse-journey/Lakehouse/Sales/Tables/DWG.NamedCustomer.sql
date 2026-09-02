/*
Table ID: DWG.NamedCustomer

Description: One row per customer, name only, authored in Spark SQL.

Lineage: $DWG.Customer

Dependencies:
  - DWG.Customer

Primary key: CustomerId

Notes: |
  A SQL-authored table beside the Python-authored ones, so the journey crosses
  both. It installs as DWG__NamedCustomer.py — a SparkSqlTable carrying this
  query — and loads through the ordinary Table.load(), which is the whole point
  of compiling rather than generating a load program.

Schema:
  CustomerId: integer
  CustomerName: string

Revision notes:
  - 2026-08-07 Created.
*/
create or replace temporary view named as
select CustomerId, CustomerName
  from DWG.Customer
 where CustomerName is not null;

select CustomerId, CustomerName from named;
