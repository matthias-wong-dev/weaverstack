"""
Folder ID: Raw.CustomerCsv

Description: Customer records as delivered, a single CSV.

Lineage: A deterministic test drop shipped beside this object.

File key: "*.csv"

Revision notes:
  - 2026-07-24 Created.
"""

import shutil
from pathlib import Path

from weaver import Folder


class Raw__CustomerCsv(Folder):
    def read(self):
        staging = Path(self.staging_folder())
        source = Path(__file__).parent / "data" / "customers.csv"
        shutil.copyfile(source, staging / "customers.csv")
        return staging, []
