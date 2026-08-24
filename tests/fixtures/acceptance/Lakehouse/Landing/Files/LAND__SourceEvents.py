"""
Folder ID: LAND.SourceEvents

Description: The foreign event drop, copied into this item's own Files area.

Lineage: The foreign Lakehouse's Files/Source/Events, through this item's shortcut.

File key: "*.json"

Incremental: false

Revision notes:
  - 2026-08-24 Created.
"""

import shutil

from shortcuts import Source__Events

from weaver import Folder


class LAND__SourceEvents(Folder):
    def read(self):
        # The whole drop each time: the shortcut is the whole truth, so a file
        # that has gone from it goes from the copy too.
        source = Source__Events(self).path()
        with self.staging_folder() as staging:
            for path in sorted(source.glob("*.json")):
                shutil.copyfile(path, staging.path / path.name)

        return staging, []
