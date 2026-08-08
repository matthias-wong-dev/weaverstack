/*
Assumption ID: Reporting.CustomerRevenuePresent

Description: Every customer with revenue carries a name and a surrogate key.

Notes: |
  The Warehouse half of the example, and it shows what a T-SQL validation is:
  one query returning the rows that contradict the statement, compiled into an
  independently runnable procedure —

      exec [_].[Assumption Reporting.CustomerRevenuePresent];

  which anyone with a query tool can run without Weaver being involved at all.

  It checks the surrogate as well as the name because the identity column is the
  engine's to assign: a row that reached the table without one would break every
  star-schema join downstream, silently.

Revision notes:
  - 2026-08-08 Created.
*/

select [Customer id]
     , [Customer name]
     , [Customer key]
  from [Reporting].[CustomerRevenue]
 where [Customer name] is null
    or [Customer key] is null;
