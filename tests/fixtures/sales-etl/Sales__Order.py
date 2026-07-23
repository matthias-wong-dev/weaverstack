"""
Table ID: Sales.Order

Description: One row per confirmed customer order.

Lineage: $Sales.OrderExport

Primary key: Order id

Not null:
  - Order date

Comparison columns: Last modified

Schema:
  Order id: string
  Customer id: string
  Order date: date
  Amount: decimal(18,2)
  Last modified: timestamp

Column notes:
  Amount: Order total including tax.
  Last modified: Source watermark, drives the upsert comparison.

Revision notes:
  - 2026-07-23 Created.
"""

from Sales__OrderExport import Sales__OrderExport

from weaver import Table

from ._helpers.dates import parse_order_date


class Sales__Order(Table):
    def read(self):
        source = Sales__OrderExport.folder_path()
        rows = parse_order_date(source)
        return rows, []
