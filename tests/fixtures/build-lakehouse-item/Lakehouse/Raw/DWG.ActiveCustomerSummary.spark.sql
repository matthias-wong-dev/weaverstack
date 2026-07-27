/*
View ID: DWG.ActiveCustomerSummary

Description: How many customers are active.

Lineage: $DWG.ActiveCustomer

Dependencies:
  - DWG.ActiveCustomer
*/
select
    count(*) as CustomerCount
from DWG.ActiveCustomer
