"""
Table ID: PUB.Reporting

Description: The Warehouse's reporting table, back in a Lakehouse.

Lineage: The Serving Warehouse's SERVE.Reporting, through this item's shortcut.

Primary key: CustomerId

Notes: |
  The chain closes here. A Fabric Warehouse publishes its tables into OneLake, so
  a Lakehouse can read one, which is what makes Lakehouse to Warehouse to
  Lakehouse a real round trip rather than one-way movement.

Schema:
  CustomerId: integer
  CustomerName: string
  TransactionCount: integer
  TotalAmount: decimal(18, 2)

Revision notes:
  - 2026-08-24 Created.
"""

from shortcuts import WH__Reporting

from weaver import Table


class PUB__Reporting(Table):
    def read(self):
        return WH__Reporting(self).dataframe()
