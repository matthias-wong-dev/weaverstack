"""
Table ID: CUR.Customer

Description: One row per current customer, kept up to date incrementally.

Lineage: $SRC.Customer

Primary key: CustomerId

Incremental: true

Prohibit rebuild: true

Notes: |
  Incremental, so the window is what changed since this object's bookmark and a
  row's absence from that window means nothing. What retires a row is the
  explicit claim returned beside the staging frame.

  Protected from rebuild, and it has dependants, so a declaration change must
  leave the physical table and everything below it in place.

Schema:
  CustomerId: integer
  CustomerName: string
  UpdatedAt: timestamp

Revision notes:
  - 2026-08-24 Created.
"""

from shortcuts import SRC__Customer

from weaver import Table


class CUR__Customer(Table):
    def read(self):
        source = SRC__Customer(self).dataframe()
        changed = source.where(source.UpdatedAt > self.bookmark())
        # The source is the whole customer list, so anything the target holds and
        # the source no longer offers has been retired at the source.
        retired = (
            self.dataframe().select("CustomerId").subtract(source.select("CustomerId"))
        )
        return changed, retired
