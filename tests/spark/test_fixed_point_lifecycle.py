"""The fixed point where it meets a real catalogue: two builds, one estate.

The planner-level proof lives in `tests/targeted/test_build_fixed_point_lifecycle`
and is where this property should be iterated on — it runs in under a second and
every input to it can be constructed. What it cannot answer is whether a *real*
catalogue, written by an installer and read back over Spark, produces the same
description the planner compares against. That round trip is the only reason
this test needs a session.

So the assertions here are the ones a session alone can make: the Delta version
of each catalogue table, and the publication epochs of the rows in it. A build
that emitted an idempotent merge writing no rows would leave the epochs alone and
still commit a new table version — which is exactly the difference between "wrote
nothing" and "did nothing", and exactly what the previous design could not tell
apart.
"""

from __future__ import annotations

import pytest
from support.build_envs import MIXED_ESTATE_FIXTURE

from weaver.catalogue.tables import CATALOGUE_TABLES, REGISTRY

pytestmark = [
    pytest.mark.spark,
    pytest.mark.parametrize(
        "weaver_repo_fixture", [MIXED_ESTATE_FIXTURE], indirect=True
    ),
]


def _catalogue_versions(env) -> dict[str, int]:
    """Each catalogue table's current Delta version.

    The number a write increments whether or not the write changed a row, which
    is what makes it the right question to ask here. Addressed through the
    environment's own object tokens, because the emulator folds the Lakehouse
    into one namespace level where Fabric has a four-part name.
    """

    versions = {}
    for table in CATALOGUE_TABLES:
        rows = env.query(
            f"DESCRIBE HISTORY {{{{object:_.{table.name}}}}} LIMIT 1",
            destination=env.weaver_destination,
        )
        versions[table.name] = rows[0]["version"]
    return versions


def _registry_epochs(env) -> dict[tuple, object]:
    rows = env.query(
        "SELECT item_type, item_name, schema_name, object_name, build_epoch "
        f"FROM {{{{object:_.{REGISTRY.name}}}}}",
        destination=env.weaver_destination,
    )
    return {
        (r["item_type"], r["item_name"], r["schema_name"], r["object_name"]): r[
            "build_epoch"
        ]
        for r in rows
    }


@pytest.fixture
def built(local_build_env):
    """One complete build of the estate, installed."""

    local_build_env.install_repo()
    outcome = local_build_env.install(local_build_env.generate("fixed-point-first"))
    assert outcome.status == "succeeded", outcome
    return local_build_env


def test_the_second_build_plans_no_action_at_all(built):
    """The complete plan against a catalogue a real build actually wrote."""

    second = built.generate("fixed-point-second")

    assert [action for _sequence, _batch, action in second.plan.actions()] == []


def test_no_catalogue_table_is_written_by_the_second_build(built):
    """Delta versions, which move on any write — including one that changes nothing."""

    before = _catalogue_versions(built)

    second = built.generate("fixed-point-versions")
    assert built.install(second).status == "succeeded"

    assert _catalogue_versions(built) == before


def test_no_publication_epoch_moves(built):
    """An unchanged object keeps the epoch of the build that really made it."""

    before = _registry_epochs(built)
    assert before, "the first build must have certified something"

    second = built.generate("fixed-point-epochs")
    assert built.install(second).status == "succeeded"

    assert _registry_epochs(built) == before


def test_the_weaver_endpoint_is_not_refreshed(built):
    """The refresh follows catalogue DML, and there was none to follow."""

    second = built.generate("fixed-point-refresh")

    assert not [
        action
        for _sequence, _batch, action in second.plan.actions()
        if action.kind == "refresh_sql_endpoint"
    ]


def test_a_real_change_still_reaches_the_catalogue(built, weaver_repo_fixture):
    """Guards every assertion above from passing because builds do nothing.

    The estate is edited, rebuilt, and the catalogue must move — otherwise
    "no work" would be indistinguishable from "broken".
    """

    document = next(
        path
        for path in (weaver_repo_fixture.path / "Lakehouse" / "Sales").rglob("*.py")
        if "Files" not in path.parts and path.name != "__init__.py"
    )
    document.write_text(
        document.read_text(encoding="utf-8").replace(
            "Description: ", "Description: revised — ", 1
        ),
        encoding="utf-8",
    )
    built.install_repo()

    before = _catalogue_versions(built)
    changed = built.generate("fixed-point-changed")
    assert built.install(changed).status == "succeeded"

    assert _catalogue_versions(built) != before
