"""
Folder ID: Sales.OrderExport

Description: Raw order export files as delivered by the sales system.

Lineage: Nightly SFTP drop.

File key: "*.csv"

Notes: |
  Files arrive named order_YYYYMMDD.csv. Retained indefinitely — the sales
  system keeps only 30 days.

Revision notes:
  - 2026-07-23 Created.
"""

from weaver import Folder


class Sales__OrderExport(Folder):
    def read(self):
        with self.staging_folder() as staging:
            # …fetch into staging.path…
            pass

        return staging, []
