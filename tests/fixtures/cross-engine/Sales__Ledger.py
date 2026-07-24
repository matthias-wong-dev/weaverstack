"""
Folder ID: Sales.Ledger

Description: Ledger files staged into the Lakehouse.

Lineage: Nightly drop.

File key: "*.jsonl"

Notes: |
  Shares the name Sales.Ledger with a Delta table and a Warehouse table. All
  three are different physical objects in different places, which Fabric allows,
  and no alias publishes across engines, so each name has exactly one owner.

Revision notes:
  - 2026-07-24 Created.
"""

from weaver import Folder


class Sales__Ledger(Folder):
    def read(self):
        staging = self.staging_folder()
        return staging, []
