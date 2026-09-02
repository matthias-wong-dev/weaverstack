"""
Folder ID: PUB.Export

Description: The final result, written out as one file per load.

Lineage: $PUB.Reporting

File key: "*.json"

Incremental: false

Revision notes:
  - 2026-08-24 Created.
"""

import json

from Tables.PUB__Reporting import PUB__Reporting

from weaver import Folder


class PUB__Export(Folder):
    def read(self):
        rows = [row.asDict() for row in PUB__Reporting(self).dataframe().collect()]
        with self.staging_folder() as staging:
            (staging.path / "reporting.json").write_text(
                json.dumps(sorted(rows, key=lambda row: row["CustomerId"]), default=str)
                + "\n",
                encoding="utf-8",
            )

        return staging, []
