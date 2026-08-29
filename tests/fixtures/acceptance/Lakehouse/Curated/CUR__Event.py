"""
Table ID: CUR.Event

Description: Every event file, shaped into rows.

Lineage: The two event folders this item shortcuts from Landing.

Primary key: EventId

Incremental: true

Notes: |
  Folder to Delta, over two logical Folder shortcuts. Incremental, so each load
  reads the files Landing recorded in `_changes` after this table's bookmark and
  merges them on EventId. A load with nothing new on either branch stages
  nothing.

Schema:
  EventId: integer
  CustomerId: integer
  Kind: string
  Source: string

Revision notes:
  - 2026-08-24 Created.
  - 2026-08-29 Incremental, over the logical shortcuts' Folder change history.
"""

from shortcuts import SRC__GeneratedEvents, SRC__SourceEvents

from weaver import Table


class CUR__Event(Table):
    def read(self):
        # This table's own bookmark, so a run it skipped is caught up on the
        # next one. The shortcut's own bookmark would mean "since Landing last
        # loaded", which is a different window.
        bookmark = self.bookmark()

        def shaped(folder, source: str):
            arrived = folder.files_since(bookmark)
            if not arrived:
                return None
            # Spark reads these, so they are addressed as Spark addresses a
            # folder. files_since answers in the mounted spelling, which is the
            # one ordinary Python opens.
            root, spark_root = folder.path(), folder.spark_path()
            files = [
                f"{spark_root}/{path.relative_to(root).as_posix()}"
                for path in sorted(arrived)
            ]
            return self.spark.read.json(files).selectExpr(
                "cast(EventId as int) as EventId",
                "cast(CustomerId as int) as CustomerId",
                "Kind",
                f"'{source}' as Source",
            )

        frames = [
            frame
            for frame in (
                shaped(SRC__SourceEvents(self), "source"),
                shaped(SRC__GeneratedEvents(self), "generated"),
            )
            if frame is not None
        ]
        if not frames:
            return None
        first, *rest = frames
        for frame in rest:
            first = first.unionByName(frame)
        return first
