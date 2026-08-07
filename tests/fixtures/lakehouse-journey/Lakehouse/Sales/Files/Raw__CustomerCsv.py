"""
Folder ID: Raw.CustomerCsv

Description: Customer records as delivered, a single CSV.

Lineage: A deterministic test drop shipped in the item's lib/.

File key: "*.csv"

Revision notes:
  - 2026-07-24 Created.
"""

import shutil
from pathlib import Path

from weaver import Folder


class Raw__CustomerCsv(Folder):
    def read(self):
        item_root = Path(__file__).parent.parent
        source = item_root / "lib" / "data" / "customers.csv"
        with self.staging_folder() as staging:
            shutil.copyfile(source, staging.path / "customers.csv")

        return staging, []
