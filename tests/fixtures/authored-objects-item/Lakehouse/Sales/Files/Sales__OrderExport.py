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
        # The staging directory is addressed through the Lakehouse's own root, so
        # a fetch writes there with Hadoop-compatible access rather than ordinary
        # file calls — the destination Lakehouse is not mounted when a load runs
        # detached against it.
        return self.staging_folder(), []
