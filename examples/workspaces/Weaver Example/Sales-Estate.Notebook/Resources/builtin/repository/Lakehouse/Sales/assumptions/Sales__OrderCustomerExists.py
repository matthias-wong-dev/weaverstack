"""
Assumption ID: Sales.OrderCustomerExists

Description: Every order names a customer the estate knows about.

Notes: |
  An Assumption rather than a Test, because there is no expected relation to
  compare against — the statement is about the data on its own, and what
  contradicts it is a row. Holding looks like an empty result.

  Both tables come from the same export, so this is not a tautology: the two
  objects read it separately, and an order for a customer the customer load
  filtered out is exactly the kind of thing that survives every individual
  load's own checks and is still wrong.

Revision notes:
  - 2026-08-08 Created.
"""

from Tables.Sales__Customer import Sales__Customer
from Tables.Sales__Order import Sales__Order

from weaver import Assumption


class Sales__OrderCustomerExists(Assumption):
    def read(self):
        # The dependencies are the imports, and each is constructed from `self`
        # so both resolve against the Lakehouse this Assumption was pointed at.
        orders = Sales__Order(self).dataframe()
        customers = Sales__Customer(self).dataframe()
        return orders.join(customers, "Customer id", "left_anti").select(
            "Order id", "Customer id"
        )
