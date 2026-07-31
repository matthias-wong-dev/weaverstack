"""An alias built twice, and left alone the second time.

``Lakehouse/Raw`` produces ``DWG.Customer``; ``Lakehouse/Curated`` aliases it as
``DWG.PortableCustomer`` and builds a view over that name. Both items are bound,
to *different* Lakehouses, because an alias that did not cross would not be one.

An alias used to be remade on every build. This asserts it is not: the second
build over an unchanged repository plans no alias action at all, and the pointer
the first build made is still the same object afterwards. That is the whole
subject, and it needs a real build rather than a planner test, because what is
being claimed is that the *physical* alias survives untouched.

**Local only, deliberately.** The emulator makes a filesystem link where Fabric
makes a OneLake shortcut, and the incremental *decision* — plan an alias action
or do not — is transport-independent: it is made from signatures and Registry
epochs before any pointer is touched. So it is proved here, cheaply, over as many
builds as it takes. Fabric carries the same estate for the things only a
workspace answers (``test_cross_item_alias``: a shortcut is an API call, it is
discovered asynchronously, and a Warehouse alias is a view over a SQL endpoint),
and asserts the no-rebuild once there rather than repeating this.
"""

from __future__ import annotations

import pytest
from build_envs import CROSS_ITEM_ALIAS_FIXTURE

pytestmark = [
    pytest.mark.spark,
    pytest.mark.parametrize(
        "weaver_repo_fixture", [CROSS_ITEM_ALIAS_FIXTURE], indirect=True
    ),
]

PRODUCER = "Lakehouse/Raw"
CONSUMER = "Lakehouse/Curated"


def _alias_actions(bundle):
    return [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "create_alias"
    ]


def _consumer_destination(build_env):
    return build_env.destinations[CONSUMER]


def test_the_first_build_makes_the_alias_and_the_second_leaves_it(local_build_env):
    """Two builds of an unchanged repository. Only the first touches the alias."""

    local_build_env.install_repo()

    first = local_build_env.generate("alias-first")
    assert len(_alias_actions(first)) == 1
    assert local_build_env.install(first).status == "succeeded"

    consumer = _consumer_destination(local_build_env)
    rows = local_build_env.query(
        "SELECT count(*) AS n FROM {{object:DWG.PortableCustomer}}",
        destination=consumer,
    )
    assert next(iter(rows[0].values())) == 0

    second = local_build_env.generate("alias-second")

    assert _alias_actions(second) == [], (
        "an unchanged alias over an unchanged source must not be replaced"
    )
    assert local_build_env.install(second).status == "succeeded"

    # Still readable through the consumer's own name, so leaving it alone left a
    # working alias rather than merely skipping the work.
    rows = local_build_env.query(
        "SELECT count(*) AS n FROM {{object:DWG.PortableCustomer}}",
        destination=consumer,
    )
    assert next(iter(rows[0].values())) == 0


def test_the_second_build_rebuilds_nothing_at_all(local_build_env):
    """The alias is not a special case: an unchanged estate is a no-op estate.

    Were the alias still being replaced, this would show up as physical work in a
    build that has nothing to do — which is exactly the symptom the change was
    made to remove.
    """

    local_build_env.install_repo()
    assert local_build_env.install(local_build_env.generate("noop-first")).status == "succeeded"

    second = local_build_env.generate("noop-second")
    physical = {
        action.kind
        for _sequence, _batch, action in second.plan.actions()
        if action.kind
        not in {
            "publish_catalogue",
            "publish_registry",
            "delete_catalogue_claims",
            "refresh_sql_endpoint",
        }
    }

    assert physical == set()


def test_a_repointed_alias_is_replaced_and_its_consumers_follow(local_build_env):
    """The other half: when the declaration does change, the alias is remade and
    the view written against it is rebuilt with it."""

    local_build_env.install_repo()
    assert local_build_env.install(local_build_env.generate("repoint-first")).status == "succeeded"

    local_build_env.write_repo_file(
        "Lakehouse/Curated/alias.yml",
        "aliases:\n  DWG.PortableCustomer: Lakehouse/Raw/DWG.Second\n",
    )
    local_build_env.write_repo_file(
        "Lakehouse/Raw/DWG__Second.py",
        _SECOND_TABLE,
    )

    second = local_build_env.generate("repoint-second")
    kinds = {
        action.resource_node_id or action.kind
        for _sequence, _batch, action in second.plan.actions()
    }

    assert len(_alias_actions(second)) == 1
    assert "Lakehouse/Curated/DWG.CustomerName" in kinds, (
        "the view over the alias depends on it and must be rebuilt with it"
    )
    assert local_build_env.install(second).status == "succeeded"


_SECOND_TABLE = '''"""
Table ID: DWG.Second

Description: A second producer table, so the alias has somewhere else to point.

Lineage: A source system.

Primary key: CustomerId

Schema:
  CustomerId: integer
  CustomerName: string
"""

from weaver import Table


class DWG__Second(Table):
    def read(self):
        return [], []
'''
