"""Date helpers shared by this item's objects.

A plain module, not a Weaver object: it declares nothing and materialises
nothing. It is claimed as a load artefact all the same, so it is deployed
beside the objects that import it and removed when it stops being authored.
"""

from datetime import date, timedelta


def month_start(on: date) -> date:
    """The first day of the month ``on`` falls in."""

    return on.replace(day=1)


def previous_day(on: date) -> date:
    return on - timedelta(days=1)
