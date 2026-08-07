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

from weaver import Folder


class Sales__OrderExport(Folder):
    #: What a real deployment fetches from the source system. Set by the caller
    #: in this example so the estate has data to carry through the chain.
    incoming = {}

    def read(self):
        # Weaver issues the staging directory and empties it before read() runs,
        # so an object fills what it was given rather than choosing where to
        # write — and returns that same object, which is what load() publishes.
        with self.staging_folder() as staging:
            for name, text in self.incoming.items():
                (staging.path / name).write_text(text, encoding="utf-8")

        return staging, []

    def most_recent(self):
        """The newest export on disk, read with ordinary Python.

        ``path()`` is a ``pathlib.Path``, so a folder's own files are reachable
        the way any other files are — globbed, opened, read. Compare
        ``spark_path()``, which is what ``Sales.Customer`` and ``Sales.Order``
        hand to Spark.
        """

        exports = sorted(self.path().glob("*.csv"))
        return exports[-1].read_text(encoding="utf-8") if exports else ""
