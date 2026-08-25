"""
Table ID: CUR.Event

Description: Every event file, shaped into rows.

Lineage: The two event folders this item shortcuts from Landing.

Primary key: EventId

Notes: |
  Folder to Delta, over two folders: one copied from the foreign drop and one
  this estate generates. Non-incremental, so the folders are the whole truth and
  the row count follows the file count.

Schema:
  EventId: integer
  CustomerId: integer
  Kind: string
  Source: string

Revision notes:
  - 2026-08-24 Created.
"""

from shortcuts import SRC__GeneratedEvents, SRC__SourceEvents

from weaver import Table


class CUR__Event(Table):
    def read(self):
        def shaped(folder, source: str):
            return self.spark.read.json(folder.spark_path() + "/*.json").selectExpr(
                "cast(EventId as int) as EventId",
                "cast(CustomerId as int) as CustomerId",
                "Kind",
                f"'{source}' as Source",
            )

        return shaped(SRC__SourceEvents(self), "source").unionByName(
            shaped(SRC__GeneratedEvents(self), "generated")
        )
