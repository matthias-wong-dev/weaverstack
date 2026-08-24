"""
Table ID: LAND.Product

Description: The foreign product table, copied whole through the schema shortcut.

Lineage: The foreign Lakehouse's Reference.Product, through this item's schema shortcut.

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
        # Named rather than by attribute, so the schema shortcut is exercised the
        # way an author reaches a table it did not declare.
        return Reference(self).table("Product").dataframe()
