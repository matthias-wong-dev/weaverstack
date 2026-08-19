/*
View ID: Rpt.CustomerReport

Description: A reporting view over a Lakehouse table, read through this item's shortcut.

Lineage: $Rpt.PortableCustomer
*/
select CustomerId, CustomerName
from [Rpt].[PortableCustomer]
