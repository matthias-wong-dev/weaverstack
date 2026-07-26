"""
Table ID: Sales.Region

Description: One row per sales region.

Lineage: $Sales.CustomerCsv

Primary key: Region code

Static: true

Schema:
  Region code: string
  Region name: string

Column notes:
  Region name: Display name, as the sales system spells it.
"""

from Sales__CustomerCsv import Sales__CustomerCsv

from weaver import Table


class Sales__Region(Table):
    def read(self):
        return Sales__CustomerCsv.folder_path(), []
