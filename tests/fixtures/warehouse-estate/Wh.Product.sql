/*
Table ID: Wh.Product

Description: Base products, seeded from literal values.

Lineage: A deterministic VALUES seed.

Primary key: ProductId

Schema:
  ProductId: int
  ProductName: varchar(100)
  Price: decimal(10,2)
*/
select v.ProductId, v.ProductName, v.Price
from (values
    (10, 'Widget', 9.99),
    (20, 'Gadget', 19.50)
) as v (ProductId, ProductName, Price)
