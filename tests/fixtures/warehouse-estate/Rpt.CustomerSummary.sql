/*
View ID: Rpt.CustomerSummary

Description: A reporting view over customer orders.

Lineage: $Wh.CustomerOrder
*/
select CustomerId, CustomerName, OrderTotal
from [Wh].[CustomerOrder]
