/*
Table ID: Rpt.CustomerReport

Description: >-
  Customers, materialised in the Warehouse from the Lakehouse table this item
  aliases. A table rather than a view, so it owns a generated load procedure and
  the graph has something to dispatch on this side of the crossing.

Lineage: $Rpt.PortableCustomer

Primary key: CustomerId
*/
select CustomerId, CustomerName
from [Rpt].[PortableCustomer]
