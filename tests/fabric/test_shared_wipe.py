"""The populated-Lakehouse wipe, against a real Fabric Lakehouse.

A thin wrapper. The claim lives in `tests/support/wipe_claims.py` and its local
twin is `tests/spark/test_local_wipe.py`; what only Fabric answers is that the
same wipe holds where OneLake has a `dbo` schema Weaver never created and must
not remove.
"""

from __future__ import annotations

import pytest
from support.wipe_claims import assert_a_wipe_removes_every_table

pytestmark = pytest.mark.published_weaver


def test_a_wipe_removes_every_table(populated_fabric_lakehouse):
    assert_a_wipe_removes_every_table(populated_fabric_lakehouse)
