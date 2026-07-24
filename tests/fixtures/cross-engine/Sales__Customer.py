"""
Table ID: Sales.Customer

Description: One row per customer, shaped from the raw drop.

Lineage: $Raw.Customer

Warehouse alias: Sales.Customer

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string

Revision notes:
  - 2026-07-24 Created.

Notes: |
  A Lakehouse-native Delta table that publishes itself into the Warehouse under
  the same name, so a Warehouse query can read it as Sales.Customer without
  naming a Lakehouse in three parts.
"""

from Raw__Customer import Raw__Customer

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        source = Raw__Customer.folder_path()
        return [], []
