"""
Table ID: DWG.Order

Description: One row per order, carrying the customer it belongs to.

Lineage: $DWG.Customer

Primary key: OrderId

Schema:
  OrderId: integer
  CustomerId: integer
  Amount: decimal(18, 2)

Revision notes:
  - 2026-07-31 Created.
"""

from weaver import Table

from .DWG__Customer import DWG__Customer


class DWG__Order(Table):
    def read(self):
        # Build creates structure; load moves data. Never called by a build — the
        # import above is what makes this object depend on the one it names.
        return [], []
