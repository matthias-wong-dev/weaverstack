"""
Table ID: Sales.Customer

Description: One row per customer known to the sales system.

Lineage: $Files/Sales.OrderExport

Primary key: Customer id

Comparison columns: Customer name

Schema:
  Customer id: string
  Customer name: string

Notes: |
  Read from the export folder rather than from a source system directly, so the
  table is rebuildable from files that were kept.

Revision notes:
  - 2026-08-03 Created.
"""

from Files.Sales__OrderExport import Sales__OrderExport

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        exported = Sales__OrderExport(self).path()
        rows = (
            self.spark.read.option("header", True)
            .csv(exported)
            .selectExpr("`Customer id`", "`Customer name`")
            .dropDuplicates(["Customer id"])
        )
        return rows, self.spark.createDataFrame([], "`Customer id` string")
