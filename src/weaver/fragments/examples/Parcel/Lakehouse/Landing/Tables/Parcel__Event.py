"""
Table ID: Parcel.Event

Description: One row per parcel tracking event.

Lineage: The parcel event CSV snapshot managed in this Lakehouse.

Primary key: Parcel ID, Event sequence

Schema:
  Parcel ID: string
  Event sequence: integer
  Status: string
  Depot: string
"""

from Files.Parcel__Events import Parcel__Events
from weaver import Table


class Parcel__Event(Table):
    def read(self):
        source = Parcel__Events(self).spark_path()
        return (
            self.spark.read.option("header", True)
            .csv(source)
            .selectExpr(
                "`Parcel ID`",
                "cast(`Event sequence` as int) as `Event sequence`",
                "Status",
                "Depot",
            )
        )
