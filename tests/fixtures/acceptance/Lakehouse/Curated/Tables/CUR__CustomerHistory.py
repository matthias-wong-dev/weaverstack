"""
Table ID: CUR.CustomerHistory

Description: Immutable customer versions with a generated surrogate key.

Lineage: $SRC.Customer

Incremental: true

Identity: ChangeKey

Notes: |
  Each load reads source rows changed after this table's bookmark. With no
  primary key, every row in that window is appended and existing versions remain.

Schema:
  CustomerId: integer
  CustomerName: string
  UpdatedAt: timestamp
"""

from shortcuts import SRC__Customer

from weaver import Table


class CUR__CustomerHistory(Table):
    def read(self):
        source = SRC__Customer(self).dataframe()
        return source.where(source.UpdatedAt > self.bookmark())
