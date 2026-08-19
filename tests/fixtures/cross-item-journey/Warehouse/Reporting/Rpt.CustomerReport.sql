/*
Table ID: Rpt.CustomerReport

Description: >-
  Customers, materialised in the Warehouse from the Lakehouse table this item
  shortcuts. A table rather than a view, so it owns a generated load procedure —
  which is what gives the run graph something to dispatch on this side of the
  crossing, and what makes the item order between the two a real constraint.

Lineage: $Rpt.PortableCustomer

Primary key: CustomerId
*/
select CustomerId, CustomerName
from [Rpt].[PortableCustomer]
