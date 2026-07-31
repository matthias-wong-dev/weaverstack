/*
View ID: DWG.ActiveCustomer

Description: Customers currently marked active.

Lineage: $DWG.Customer

Dependencies:
  - DWG.Customer
*/
select
    CustomerId,
    CustomerName
from DWG.Customer
where IsActive = true
