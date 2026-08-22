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

  The read is incremental too: it opens only the export files that arrived
  since this table last loaded cleanly, rather than every file ever kept.

Revision notes:
  - 2026-08-03 Created.
  - 2026-08-22 Read only the exports that arrived since the last clean load.
"""

from Files.Sales__OrderExport import Sales__OrderExport

from weaver import Table


class Sales__Order(Table):
    def read(self):
        export = Sales__OrderExport(self)
        # `self.bookmark()` is the UTC instant immediately before this table's most
        # recent clean load began, and the folder records when each of its files
        # changed — so the two compose into "what has arrived since". A table
        # that has never loaded cleanly carries the sentinel, and every file is
        # newer than that, so the first load reads the lot.
        arrived = export.files_since(self.bookmark())
        if not arrived:
            # Nothing new. An incremental table's no-op is its own shape with no
            # rows, and no order to retire.
            return self.empty_dataframe(), None

        # spark_path(), because Spark is what reads it: on Fabric that is the
        # abfss:// form, which pathlib cannot express. The keys of files_since()
        # are the mounted paths for ordinary Python, so only the names carry
        # across — see the folder's own module.
        root = export.spark_path()
        rows = (
            self.spark.read.option("header", True)
            .csv([f"{root}/{path.name}" for path in sorted(arrived)])
            .selectExpr(
                "`Order id`",
                "`Customer id`",
                "cast(`Order date` as date) as `Order date`",
                "cast(`Amount` as decimal(18,2)) as `Amount`",
                "`Order status`",
            )
        )
        # A cancelled order is retired explicitly. Absence would not do it: this
        # source is incremental, so absence only means "outside the window".
        cancelled = rows.where("`Order status` = 'cancelled'").select("`Order id`")
        return rows.where("`Order status` <> 'cancelled'"), cancelled
