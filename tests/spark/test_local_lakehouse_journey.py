"""The lifecycle journey, on local Spark.

A thin wrapper: every assertion lives in `tests/support/journey_claims.py`,
because the claims are about a build lifecycle and not about a transport. What
this file supplies is the estate.

Marked `spark` and not `full_integration`: this drives the emulator, and
`-m full_integration` is reserved for the run that crosses into a workspace.
Split from its Fabric twin so that marker, directory and fixture agree. A single
parametrised module ran under both `-m spark` and the other, which meant one of
them was collecting a test it could not honestly claim to be about.
"""

from __future__ import annotations

import pytest
from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE
from support.journey_claims import drive

pytestmark = [
    pytest.mark.spark,
    pytest.mark.parametrize(
        "weaver_repo_fixture", [LAKEHOUSE_JOURNEY_FIXTURE], indirect=True
    ),
]


def test_a_lakehouse_estate_through_a_whole_build_lifecycle(local_lakehouse_journey):
    drive(local_lakehouse_journey)
