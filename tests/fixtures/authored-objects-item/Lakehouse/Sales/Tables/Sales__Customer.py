"""
Table ID: Sales.Customer

Description: One row per customer known to the sales system.

Lineage: $Files/Sales.OrderExport

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string

Revision notes:
  - 2026-07-30 Created.
"""

from Files.Sales__OrderExport import Sales__OrderExport

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        source = Sales__OrderExport(self).path()
        rows = self.spark.read.csv(source, header=True).selectExpr(
            "`Customer id` as `Customer id`", "`Customer name` as `Customer name`"
        )
        return rows
