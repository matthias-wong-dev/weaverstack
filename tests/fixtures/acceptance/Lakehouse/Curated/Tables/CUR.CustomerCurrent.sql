/*
View ID: CUR.CustomerCurrent

Description: The current customers, named.

Lineage: $CUR.Customer

Dependencies:
  - CUR.Customer
*/
select
    CustomerId
  , CustomerName
from CUR.Customer
