"""
Folder ID: DWG.Seed

Description: The customer extract a nightly drop delivers.

Lineage: A deterministic test drop.

File key: "*.csv"

Incremental: false
"""

from weaver import Folder


class DWG__Seed(Folder):
    def read(self):
        with self.staging_folder() as staging:
            (staging.path / "customers.csv").write_text(
                "CustomerId,CustomerName\n1,Ada\n2,Grace\n3,Katherine\n",
                encoding="utf-8",
            )

        return staging, []
