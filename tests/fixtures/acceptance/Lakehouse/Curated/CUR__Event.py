"""
Table ID: CUR.Event

Description: Every event file, shaped into rows.

Lineage: The two event folders this item shortcuts from Landing.

Primary key: EventId

Incremental: true

Notes: |
  Folder to Delta, over two logical Folder shortcuts. Incremental, so each load
  reads what Landing recorded in `_changes` after this table's bookmark: the
  files that arrived become rows, and the files that went retire the rows they
  brought.

  `SourceFile` is what makes the second half possible. A deleted file is gone by
  the time this reads, so the rows it brought are found by the name recorded
  when they arrived. It carries the branch as well, so two folders delivering
  one file name stay distinct.

Schema:
  EventId: integer
  CustomerId: integer
  Kind: string
  Source: string
  SourceFile: string

Revision notes:
  - 2026-08-24 Created.
  - 2026-08-29 Incremental, over the logical shortcuts' Folder change history.
"""

from shortcuts import SRC__GeneratedEvents, SRC__SourceEvents

from weaver import Table


def _relative(folder, paths) -> list[str]:
    """Each path as a name relative to the folder root, in name order."""

    root = folder.path()
    return sorted(path.relative_to(root).as_posix() for path in paths)


class CUR__Event(Table):
    def read(self):
        # This table's own bookmark, so a run it skipped is caught up on the
        # next one. The shortcut's own bookmark would mean "since Landing last
        # loaded", which is a different window.
        bookmark = self.bookmark()
        branches = (
            (SRC__SourceEvents(self), "source"),
            (SRC__GeneratedEvents(self), "generated"),
        )

        staged = None
        for folder, source in branches:
            # Spark reads these, so they are addressed the way Spark addresses a
            # folder. The change history answers in the mounted spelling, which
            # is the one ordinary Python opens.
            spark_root = folder.spark_path()
            # One read per file, so every row carries the file it came from.
            for relative in _relative(folder, folder.files_since(bookmark)):
                arrived = self.spark.read.json(f"{spark_root}/{relative}").selectExpr(
                    "cast(EventId as int) as EventId",
                    "cast(CustomerId as int) as CustomerId",
                    "Kind",
                    f"'{source}' as Source",
                    f"'{source}/{relative}' as SourceFile",
                )
                staged = arrived if staged is None else staged.unionByName(arrived)

        withdrawn = [
            f"{source}/{relative}"
            for folder, source in branches
            for relative in _relative(folder, folder.deleted_since(bookmark))
        ]
        if not withdrawn:
            return staged, None
        # Reached only once this table holds rows: nothing can have been deleted
        # from a source before this table's first load read it.
        held = self.dataframe()
        return staged, held.where(held.SourceFile.isin(withdrawn)).select("EventId")
