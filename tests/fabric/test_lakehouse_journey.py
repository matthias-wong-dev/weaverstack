"""The lifecycle journey, on Fabric.

A thin wrapper: every assertion lives in `tests/support/journey_claims.py`,
because the claims are about a build lifecycle and not about a transport. What
this file supplies is the estate.

Split from its local Spark twin so that marker, directory and fixture agree. A single
parametrised module ran under both `-m fabric` and the other, which meant one of
them was collecting a test it could not honestly claim to be about.
"""

from __future__ import annotations

import pytest
from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE
from support.journey_claims import drive

pytestmark = [
    pytest.mark.full_integration,
    pytest.mark.parametrize(
        "weaver_repo_fixture", [LAKEHOUSE_JOURNEY_FIXTURE], indirect=True
    ),
]


def test_a_lakehouse_estate_through_a_whole_build_lifecycle(fabric_lakehouse_journey):
    drive(fabric_lakehouse_journey)
