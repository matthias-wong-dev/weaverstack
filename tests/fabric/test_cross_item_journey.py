"""The lifecycle journey over an estate that spans a Lakehouse and a Warehouse.

A thin wrapper: every assertion lives in `tests/support/journey_claims.py`,
because the claims are about a build lifecycle and not about a transport. What
this file supplies is the estate.

A Warehouse, so a real workspace is the only place this can run. The order the
build gives it is asserted
without an engine in `tests/targeted/test_cross_item_composition_representation.py`;
what is left for a real workspace is whether the statements that order produces
are accepted, and whether the two sides reconcile once loaded.
"""

from __future__ import annotations

import pytest
from support.build_envs import CROSS_ITEM_JOURNEY_FIXTURE
from support.journey_claims import drive_across_items

pytestmark = [
    pytest.mark.fabric,
    pytest.mark.hosted,
    pytest.mark.full_integration,
    pytest.mark.parametrize(
        "weaver_repo_fixture", [CROSS_ITEM_JOURNEY_FIXTURE], indirect=True
    ),
]


def test_a_lakehouse_and_the_warehouse_that_reports_on_it(fabric_cross_item_journey):
    drive_across_items(fabric_cross_item_journey)
