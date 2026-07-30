"""
Folder ID: Sales.OrderExport

Description: Raw order export files as delivered by the sales system.

Lineage: A nightly drop from the sales system.

File key: "*.csv"

Revision notes:
  - 2026-07-30 Created.
"""

import shutil
from pathlib import Path

from weaver import Folder


class Sales__OrderExport(Folder):
    def read(self):
        staging = Path(self.staging_folder())
        shutil.copyfile(
            Path(__file__).parent.parent / "lib" / "data" / "orders.csv",
            staging / "orders.csv",
        )
        return staging, []
