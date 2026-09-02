"""
Table ID: CUR.Product

Description: Products, copied whole on every load.

Lineage: $SRC.Product

Primary key: ProductId

Notes: |
  Non-incremental on purpose. Nothing mutates the foreign product source, so
  this is the branch a repeat load must leave unchanged.

Schema:
  ProductId: integer
  ProductName: string

Revision notes:
  - 2026-08-24 Created.
"""

from shortcuts import SRC__Product

from weaver import Table


class CUR__Product(Table):
    def read(self):
        return SRC__Product(self).dataframe()
