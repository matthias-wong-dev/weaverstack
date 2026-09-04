"""Showing which Fabric crossings this installation can make.

One line per check, padded so the outcomes line up, and the reason underneath
where one failed. An ordinary connectivity failure is a sentence and a next
action, not a traceback.
"""

from __future__ import annotations

import sys

#: Wide enough for the longest name a configured project produces, which is a
#: Warehouse reached over TDS.
NAME_WIDTH = 34


def render(report) -> None:
    """Show every check, and what to do about the ones that failed."""

    for check in report.checks:
        print(f"  {check.name.ljust(NAME_WIDTH)}{'OK' if check.passed else 'FAILED'}")

    if report.succeeded:
        print()
        print("Everything checked is reachable.")
        return

    for check in report.failures:
        print(file=sys.stderr)
        print(f"{check.name} failed.", file=sys.stderr)
        if check.detail:
            print(file=sys.stderr)
            print(check.detail, file=sys.stderr)
        if check.remedy:
            print(file=sys.stderr)
            print(check.remedy, file=sys.stderr)
