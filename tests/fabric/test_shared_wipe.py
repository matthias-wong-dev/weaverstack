"""The populated-Lakehouse wipe, against a real Fabric Lakehouse.

A thin wrapper. The claim lives in `tests/support/wipe_claims.py` and its local
twin is `tests/spark/test_local_wipe.py`; what only Fabric answers is that the
same wipe holds where OneLake has a `dbo` schema Weaver never created and must
not remove.

Driven from this checkout. `wipe_delta_target` takes its store as an argument and
removes directories — it never needed the installed package, only a real
OneLake. The session that seeds the fixture runs raw Spark and imports nothing.
"""

from __future__ import annotations

import pytest
from support.wipe_claims import assert_a_wipe_removes_every_table

pytestmark = pytest.mark.fabric


def test_a_wipe_removes_every_table(populated_fabric_lakehouse):
    assert_a_wipe_removes_every_table(populated_fabric_lakehouse)
