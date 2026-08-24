"""
Table ID: LAND.Transaction

Description: The foreign Warehouse's transactions, copied whole.

Lineage: The foreign Warehouse's Source.Transaction, through this item's shortcut.

Primary key: TransactionId

Schema:
  TransactionId: integer
  CustomerId: integer
  Amount: decimal(18, 2)

Revision notes:
  - 2026-08-24 Created.
"""

from shortcuts import Source__Transaction

from weaver import Table


class LAND__Transaction(Table):
    def read(self):
        return Source__Transaction(self).dataframe()
