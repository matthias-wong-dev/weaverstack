"""
Table ID: LAND.Customer

Description: The foreign customer table, copied whole.

Lineage: The foreign Lakehouse's Source.Customer, through this item's shortcut.

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string
  UpdatedAt: timestamp

Revision notes:
  - 2026-08-24 Created.
"""

from shortcuts import Source__Customer

from weaver import Table


class LAND__Customer(Table):
    def read(self):
        # Non-incremental: the foreign table is the whole truth, so a row's
        # absence from it retires the local copy.
        return (
            Source__Customer(self)
            .dataframe()
            .selectExpr(
                "cast(CustomerId as int) as CustomerId",
                "CustomerName",
                "UpdatedAt",
            )
        )
