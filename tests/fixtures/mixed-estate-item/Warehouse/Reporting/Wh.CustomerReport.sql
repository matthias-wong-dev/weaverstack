/*
Table ID: Wh.CustomerReport

Description: A Warehouse report reading the Lakehouse by its physical name.

Lineage: A hard-coded three-part read of the Lakehouse.

Primary key: CustomerId
*/
select CustomerId, CustomerName from [Sales_LH].[Sales].[Customer]
