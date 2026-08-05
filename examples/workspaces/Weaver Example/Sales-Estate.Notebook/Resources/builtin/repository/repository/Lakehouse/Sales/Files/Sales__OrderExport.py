"""
Folder ID: Sales.OrderExport

Description: Raw order export files as delivered by the sales system.

Lineage: Nightly drop from the sales system.

File key: "*.csv"

Incremental: true

Notes: |
  Files arrive named order_YYYYMMDD.csv and are retained indefinitely, because
  the sales system keeps only thirty days. Incremental for that reason: a load
  adds the night's file and must not retire the ones before it.

Revision notes:
  - 2026-08-03 Created.
"""

from pathlib import Path

from weaver import Folder


class Sales__OrderExport(Folder):
    #: What a real deployment fetches from the source system. Set by the caller
    #: in this example so the estate has data to carry through the chain.
    incoming = {}

    def read(self):
        staging = Path(self.staging_folder())
        for name, text in self.incoming.items():
            (staging / name).write_text(text, encoding="utf-8")
        return str(staging), []
