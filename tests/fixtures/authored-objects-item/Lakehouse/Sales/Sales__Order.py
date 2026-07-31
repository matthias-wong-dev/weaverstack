"""
Table ID: Sales.Order

Description: One row per confirmed customer order.

Lineage: $Files/Sales.OrderExport

Primary key: Order id

Schema:
  Order id: string
  Customer id: string
  Amount: decimal(18,2)

Revision notes:
  - 2026-07-30 Created.
"""

from .Files.Sales__OrderExport import Sales__OrderExport
from .Sales__Customer import Sales__Customer

from weaver import Table


class Sales__Order(Table):
    """Every access the authoring surface offers, in one read().

    The body is mirrored by ``tests/fabric/test_authored_object_access.py``,
    which runs it against a built target — locally and in Fabric — because
    importing an installed repository is the load executor's job and that does
    not exist yet.
    """

    def read(self):
        source = Sales__OrderExport(self).path()
        customers = Sales__Customer(self).dataframe()
        existing = self.dataframe()
        if existing.count() and not customers.count():
            return self.empty_dataframe(), []
        rows = self.spark.read.csv(source, header=True).join(customers, "Customer id")
        return rows, []
