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

from shortcuts import Reference

from weaver import Table


class LAND__Product(Table):
    def read(self):
        # Through the schema shortcut, which presents the foreign namespace
        # whole. Its tables are the foreign item's and are not documents here.
        return Reference(self).Product.dataframe()
