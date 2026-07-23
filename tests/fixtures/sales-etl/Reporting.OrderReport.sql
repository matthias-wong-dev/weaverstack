/*
Table ID: Reporting.OrderReport

Description: Orders enriched with customer and line counts, for reporting.

Lineage: $Sales.Order

Primary key: Order id

Column notes:
  Order id: Matches the source order identifier.
  Line count: Number of order lines at the time of the load.

Notes: |
  Stages the recent window into a temp table so the line-count apply runs
  against a smaller set.

  Reads the order table out of the Lakehouse by its three-part name. That is
  the plain way to do it and needs no configuration; it is not portable to a
  Lakehouse of a different name, which is when shortcuts start to earn their
  keep.

Revision notes:
  - 2026-07-23 Created.
*/

-- Superseded Legacy.OrderReport in 2026.
select o.[Order id]
     , o.[Customer id]
     , o.[Amount]
  into #recent
  from [Sales_LH].[Sales].[Order] as o
 where o.[Order date] >= dateadd(month, -12, getdate());

select r.[Order id]
     , c.[Customer name]
     , l.[Line count]
  from #recent as r
  join [Sales].[Customer] as c on c.[Customer id] = r.[Customer id]
 cross apply Sales.OrderLineCount(r.[Order id]) as l
