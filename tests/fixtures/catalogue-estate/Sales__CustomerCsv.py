"""
Folder ID: Sales.CustomerCsv

Description: Raw customer export files.

Lineage: Nightly SFTP drop from the sales system.

File key:
  - "customer_*.csv"
  - "region_*.csv"

Revision notes:
  - 2026-07-24 Created.
"""

from weaver import Folder


class Sales__CustomerCsv(Folder):
    def read(self):
        return self.staging_folder(), []
