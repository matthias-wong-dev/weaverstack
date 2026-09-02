"""
Table ID: Sales.Customer

Description: One row per customer, the Lakehouse base table.

Lineage: $Files/Raw.Orders

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string
"""

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        raise RuntimeError("read() must not run during build")
