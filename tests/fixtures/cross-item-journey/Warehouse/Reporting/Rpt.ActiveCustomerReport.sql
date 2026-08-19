/*
View ID: Rpt.ActiveCustomerReport

Description: >-
  The reporting view a consumer actually reads. A view over this item's own
  materialised table rather than over the shortcut, so the Warehouse has a second
  object to prune, order and certify.

Lineage: $Rpt.CustomerReport
*/
select CustomerId, CustomerName
from [Rpt].[CustomerReport]
