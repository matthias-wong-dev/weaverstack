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
        staging = self.staging_folder()
        # …fetch into staging…
        return staging, []
