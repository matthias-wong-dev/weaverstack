"""
Table ID: LAND.Product

Description: The foreign product table, copied whole through the schema shortcut.

Lineage: The foreign Lakehouse's Reference.Product, through this item's shortcut.

Primary key: ProductId

Schema:
  ProductId: integer
  ProductName: string

Revision notes:
  - 2026-08-24 Created.
"""

from shortcuts import Source__Product

from weaver import Table


class LAND__Product(Table):
    def read(self):
        return Source__Product(self).dataframe()
