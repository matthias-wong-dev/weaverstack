"""Render workspace connectivity evidence."""

NAME_WIDTH = 34


def render(report) -> None:
    """Show each status with its diagnostic detail and probe item."""

    width = max([NAME_WIDTH, *(len(check.name) + 2 for check in report.checks)])
    for check in report.checks:
        print(f"  {check.name.ljust(width)}{check.status.upper()}")
        if check.via:
            print(f"    via {check.via}")
        if check.detail:
            print(f"    {check.detail}")
        if check.remedy:
            print(f"    {check.remedy}")
        print()
