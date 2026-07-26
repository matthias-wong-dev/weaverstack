"""
Table ID: Sales.Customer

Description: One row per customer known to the sales system.

Lineage: $Sales.CustomerCsv

Primary key: Customer key

Identity: Customer key

Unique keys:
  - Customer id
  - Region code, Customer name

Foreign keys:
  - Region code: Sales.Region[Region code]
  - Parent customer id: Sales.Customer[Customer id]

Not null:
  - Customer id

Comparison columns: Customer name, Region code

Warehouse alias: Rpt.CustomerDelta

Schema:
  Customer id: string
  Customer name: string
  Region code: string
  Parent customer id: string
  Last modified: timestamp

Column notes:
  Customer id: Natural key from the sales system.
  Last modified: $Sales.Region[Region name]

Revision notes:
  - 2026-07-24 Created.
"""

from Sales__CustomerCsv import Sales__CustomerCsv

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        return Sales__CustomerCsv.folder_path(), []
