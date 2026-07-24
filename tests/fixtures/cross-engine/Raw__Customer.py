"""
Folder ID: Raw.Customer

Description: Raw customer files as delivered.

Lineage: Nightly drop.

File key: "*.csv"

Revision notes:
  - 2026-07-24 Created.
"""

from weaver import Folder


class Raw__Customer(Folder):
    def read(self):
        staging = self.staging_folder()
        # …fetch into staging…
        return staging, []
