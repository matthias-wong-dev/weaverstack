"""
Table ID: DWG.Customer

Description: One row per customer. The producer side of the cross-item alias.

Lineage: A source system.

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string

Revision notes:
  - 2026-07-30 Created.
"""

from weaver import Table


class DWG__Customer(Table):
    def read(self):
        # Build creates structure; load moves data. This is never called by build.
        return [], []
