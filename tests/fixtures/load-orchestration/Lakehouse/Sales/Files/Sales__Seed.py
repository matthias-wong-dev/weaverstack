"""
Folder ID: Sales.Seed

Description: The customer extract a nightly drop delivers.

Lineage: A nightly drop from the sales system.

File key: "*.csv"

Incremental: false
"""
from pathlib import Path

from weaver import Folder


class Sales__Seed(Folder):
    def read(self):
        staging = Path(self.staging_folder())
        staging.mkdir(parents=True, exist_ok=True)
        (staging / "customers.csv").write_text(
            "Customer id,Customer name\nC1,Ada\nC2,Grace\nC3,Katherine\n",
            encoding="utf-8",
        )
        return str(staging), []
