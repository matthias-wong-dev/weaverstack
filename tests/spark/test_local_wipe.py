"""The populated-Lakehouse wipe, against the local emulator.

A thin wrapper over `tests/support/wipe_claims.py`, whose Fabric twin is
`tests/fabric/test_shared_wipe.py`. Separate files rather than one parametrised
module so that marker, directory and fixture agree — this one resolves nothing
that would reach for a workspace.
"""

from __future__ import annotations

import pytest
from support.wipe_claims import assert_a_wipe_removes_every_table

pytestmark = pytest.mark.spark


def test_a_wipe_removes_every_table(populated_local_lakehouse):
    assert_a_wipe_removes_every_table(populated_local_lakehouse)
