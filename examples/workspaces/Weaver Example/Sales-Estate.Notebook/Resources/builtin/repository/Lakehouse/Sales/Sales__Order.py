"""
Table ID: Sales.Order

Description: One row per confirmed customer order.

Lineage: $Files/Sales.OrderExport

Primary key: Order id

Incremental: true

Comparison columns: Amount, Order status

Schema:
  Order id: string
  Customer id: string
  Order date: date
  Amount: decimal(18,2)
  Order status: string

Notes: |
  Incremental because the export is a window on recent activity: an order
  missing from tonight's file has not been cancelled, it is simply older than
  the window. Cancellations arrive as an explicit delete instead.

Revision notes:
  - 2026-08-03 Created.
"""

from Files.Sales__OrderExport import Sales__OrderExport

from weaver import Table


class Sales__Order(Table):
    def read(self):
        # spark_path(), because Spark is what reads it: on Fabric that is
        # the abfss:// form, which pathlib cannot express. path() is the
        # mounted Path for ordinary Python — see the folder's own module.
        exported = Sales__OrderExport(self).spark_path()
        rows = self.spark.read.option("header", True).csv(exported).selectExpr(
            "`Order id`",
            "`Customer id`",
            "cast(`Order date` as date) as `Order date`",
            "cast(`Amount` as decimal(18,2)) as `Amount`",
            "`Order status`",
        )
        # A cancelled order is retired explicitly. Absence would not do it: this
        # source is incremental, so absence only means "outside the window".
        cancelled = rows.where("`Order status` = 'cancelled'").select("`Order id`")
        return rows.where("`Order status` <> 'cancelled'"), cancelled
