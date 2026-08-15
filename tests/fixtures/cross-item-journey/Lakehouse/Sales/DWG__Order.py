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

from DWG__Customer import DWG__Customer

from weaver import Table


class DWG__Order(Table):
    def read(self):
        # Build creates structure; load moves data. Never called by a build — the
        # import above is what makes this object depend on the one it names, and
        # reading that table here is what makes the dependency matter: an order
        # exists only for a customer that has already been loaded.
        customers = DWG__Customer(self).dataframe()
        return (
            customers.selectExpr(
                "cast(CustomerId as int) as OrderId",
                "cast(CustomerId as int) as CustomerId",
                "cast(CustomerId * 10 as decimal(18, 2)) as Amount",
            ),
            None,
        )
