"""
Folder ID: LAND.GeneratedEvents

Description: One deterministic event file appended per load.

Lineage: Generated locally, so a load has a branch that always moves.

File key: "*.json"

Incremental: true

Notes: |
  Incremental, so staging adds to what is there rather than replacing it. The
  name follows the count already present, which keeps the estate predictable
  without a timestamp or a random value in it.

Revision notes:
  - 2026-08-24 Created.
"""

import json

from weaver import Folder


class LAND__GeneratedEvents(Folder):
    def read(self):
        existing = sorted(self.path().glob("generated-*.json"))
        sequence = len(existing) + 1
        with self.staging_folder() as staging:
            (staging.path / f"generated-{sequence:03d}.json").write_text(
                json.dumps(
                    {
                        "EventId": 1000 + sequence,
                        "CustomerId": 1,
                        "Kind": "generated",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

        return staging, []
