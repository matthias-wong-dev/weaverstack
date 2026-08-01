"""Shared setup for the local Spark suites.

Everything here runs in this process against a `tmp_path`. Nothing reaches for a
workspace, a credential or a session — which is the property that makes
`pytest -m spark` mean what it says, and the reason the local build environment
was moved out of `tests/fabric/conftest.py`.
"""

from __future__ import annotations

import pytest
from factories import bound_target, item_id, registered_document, target_inventory
from support.local_build import _local_build_context

from weaver import ItemRef
from weaver.build_bundle import execute_action, plan_item_build
from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.locations import Location
from weaver.spark import SparkCatalogue


@pytest.fixture
def local_build_env(tmp_path, spark, weaver_repo_fixture):
    """One local build environment per test, over its own `tmp_path`.

    Per-test rather than per-module because a local Lakehouse costs a fraction of
    a millisecond to make — the reason the emulator stays disposable while Fabric
    reuses fixed items and empties them instead.
    """

    with _local_build_context(tmp_path, spark, weaver_repo_fixture) as env:
        yield env


@pytest.fixture
def weaver_catalogue(spark, lakehouses):
    """Catalogue operations against this test's own Weaver Lakehouse.

    The schema is dropped afterwards. That is harness isolation, not product
    behaviour: a real installation has one Weaver Lakehouse for the life of the
    session, while this suite presents a succession of temporary directories under
    the same logical name — so without the drop, the second test's tables would
    land in the first test's directory.
    """

    catalogue = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.weaver)
    )
    catalogue.create_schema("_")
    try:
        yield catalogue
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {catalogue.qualified_schema('_')} CASCADE")


@pytest.fixture(scope="module")
def weaver_repo_fixture(request):
    """Which Weaver document fixture an estate is built from.

    Indirectly parametrised by the modules that need a particular estate; the
    default serves the ones that only need *a* repository.
    """

    from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE

    return getattr(request, "param", LAKEHOUSE_JOURNEY_FIXTURE)


@pytest.fixture(
    scope="module", params=[pytest.param("local", marks=pytest.mark.spark, id="local")]
)
def local_lakehouse_estate(request, weaver_repo_fixture):
    """One Lakehouse estate on local Spark, installed once per module.

    For modules whose subject is transport-independent — what the planner
    decides, what DDL is emitted, how a dependency chain orders. Six assertions
    over one estate cost what one does.
    """

    from support.build_env import _install_estate
    from support.local_build import _local_build_context

    spark = request.getfixturevalue("spark")
    root = request.getfixturevalue("tmp_path_factory").mktemp("estate")
    with _local_build_context(root, spark, weaver_repo_fixture) as env:
        yield _install_estate(env)


@pytest.fixture(scope="module")
def local_lakehouse_journey(request, weaver_repo_fixture):
    """One local estate for a journey to drive.

    The Fabric twin is `fabric_lakehouse_journey`. They are separate fixtures in
    separate conftests rather than one parametrised fixture, so that a run asking
    for local Spark never resolves anything that would reach for a workspace.
    """

    from support.build_env import Journey
    from support.local_build import _local_build_context

    spark = request.getfixturevalue("spark")
    root = request.getfixturevalue("tmp_path_factory").mktemp("journey")
    with _local_build_context(root, spark, weaver_repo_fixture) as env:
        yield Journey(env, "lakehouse")


@pytest.fixture
def populated_local_lakehouse(populated_local_lakehouses):
    """A local Lakehouse holding tables and files, with its own wipe."""

    from weaver import DeltaTarget, wipe_delta_target
    from support.build_env import PopulatedLakehouse

    lakehouses = populated_local_lakehouses
    target = DeltaTarget(lakehouse=lakehouses.target)

    def wipe() -> tuple[str, ...]:
        return tuple(
            wipe_delta_target(target, lakehouses.workspace).removed
        )

    return PopulatedLakehouse(
        workspace=lakehouses.workspace,
        target=lakehouses.target,
        resolver=lakehouses.resolver,
        store=lakehouses.store,
        wipe=wipe,
    )

