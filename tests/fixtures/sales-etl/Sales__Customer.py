"""
Table ID: Sales.Customer

Description: One row per customer known to the sales system.

Lineage: $Sales.OrderExport

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string

Revision notes:
  - 2026-07-23 Created.
"""

from Sales__OrderExport import Sales__OrderExport

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        source = Sales__OrderExport.folder_path()
        return [], []
