"""
Table ID: LAND.Region

Description: The foreign Warehouse's regions, copied whole.

Lineage: The foreign Warehouse's Reference.Region, through this item's shortcut.

Primary key: RegionId

Notes: |
  Nothing mutates the foreign source, so this branch is the one a load must
  leave alone. It is here to be unchanged.

Schema:
  RegionId: integer
  RegionName: string

Revision notes:
  - 2026-08-24 Created.
"""

from shortcuts import Source__Region

from weaver import Table


class LAND__Region(Table):
    def read(self):
        return Source__Region(self).dataframe()
