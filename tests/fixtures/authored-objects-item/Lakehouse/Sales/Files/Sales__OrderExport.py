"""
Folder ID: Sales.OrderExport

Description: Raw order export files as delivered by the sales system.

Lineage: A nightly drop from the sales system.

File key: "*.csv"

Revision notes:
  - 2026-07-30 Created.
"""

from weaver import Folder


class Sales__OrderExport(Folder):
    def read(self):
        # Weaver issues the staging directory and resets it before read() runs,
        # so an object fills what it was given rather than choosing where to
        # write. `staging.path` is an ordinary Path, mounted where the resolved
        # Lakehouse lives.
        return self.staging_folder(), []
