"""
Folder ID: DWG.Seed

Description: The customer extract a nightly drop delivers.

Lineage: A deterministic test drop.

File key: "*.csv"

Incremental: false
"""
from pathlib import Path

from weaver import Folder


class DWG__Seed(Folder):
    def read(self):
        staging = Path(self.staging_folder())
        staging.mkdir(parents=True, exist_ok=True)
        (staging / "customers.csv").write_text(
            "CustomerId,CustomerName\n1,Ada\n2,Grace\n3,Katherine\n",
            encoding="utf-8",
        )
        return str(staging), []
