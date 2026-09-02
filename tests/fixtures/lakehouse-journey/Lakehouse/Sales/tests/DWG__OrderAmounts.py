"""
Test ID: DWG.OrderAmounts

Description: Every order carries ten times its customer's id, as the load derives it.

Primary key: OrderId

Revision notes:
  - 2026-08-08 Created.
"""

from Tables.DWG__Customer import DWG__Customer
from Tables.DWG__Order import DWG__Order

from weaver import Test


class DWG__OrderAmounts(Test):
    """The journey's Test, and it is a real one rather than a tautology.

    ``expected`` derives the orders independently from the customers — the same
    arithmetic ``DWG__Order`` performs, stated once more from the other side —
    and ``actual`` reads what the load actually wrote. A load that dropped rows,
    doubled them or miscomputed an amount is caught; a load that worked is not.

    The imports are the dependency, exactly as they are for a table, and both
    objects are constructed from ``self`` so they resolve against the Lakehouse
    this validation was pointed at.
    """

    def expected(self):
        customers = DWG__Customer(self).dataframe()
        return customers.selectExpr(
            "cast(CustomerId as int) as OrderId",
            "cast(CustomerId as int) as CustomerId",
            "cast(CustomerId * 10 as decimal(18, 2)) as Amount",
        )

    def actual(self):
        return (
            DWG__Order(self).dataframe().selectExpr("OrderId", "CustomerId", "Amount")
        )
