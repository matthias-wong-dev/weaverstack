"""The transitions every mirror journey drives, named once.

A mirror is proved by what happens after it: a rerun that converges, an
unchanged build that leaves it standing, and a changed declaration that
materialises one object locally. Those are the expensive claims and the easy
ones to drop, so both Fabric journeys name their steps from here and
``tests/test_mirror_journey_invariant.py`` holds each to driving all of them.
"""

from __future__ import annotations

BUILD_SOURCE = "build the source"
MIRROR = "mirror"
MIRROR_AGAIN = "mirror again"
VALIDATE = "validate the mirror"
UNCHANGED_BUILD = "build with nothing changed"
CHANGE = "change one declaration"
CHANGED_BUILD = "build the changed declaration"

#: What a mirror journey takes one estate through, whatever the item kind.
LIFECYCLE = (
    BUILD_SOURCE,
    MIRROR,
    MIRROR_AGAIN,
    VALIDATE,
    UNCHANGED_BUILD,
    CHANGE,
    CHANGED_BUILD,
)
