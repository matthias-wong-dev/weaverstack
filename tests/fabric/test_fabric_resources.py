"""Workspace and item resolution against a real Fabric workspace.

Resolution is Weaver's business and stays in the ordinary Fabric suite. Creating
and deleting Lakehouses is **Fabric's**, and is marked ``provision`` so it is
opt-in separately: it changes rarely, and because ``fabric_lakehouses`` is
function-scoped, these few tests alone make and destroy eight Lakehouses — churn
that slows every run and that Fabric's namespace resolver reacts badly to
underneath a long-lived session.

``create_lakehouse`` is still real product surface (``weaver initialise`` makes
the Weaver Lakehouse with it), so this is a statement about *when* to run the
cover, not about it being unnecessary.
"""

from __future__ import annotations

import pytest

from weaver.errors import CommandError
from weaver.fabric import LAKEHOUSE, find_item, list_items

pytestmark = [pytest.mark.fabric, pytest.mark.remote]

def test_the_workspace_resolves_to_an_id(fabric_workspace_item):
    assert fabric_workspace_item.id
    assert fabric_workspace_item.name


def test_an_unknown_workspace_lists_what_there_is(fabric_workspace_item):
    """Takes the fixture it does not read, to inherit its skip.

    Listing what there is needs a reachable tenant, so without one this cannot
    run — and unguarded it failed on the credential lookup instead of skipping,
    which is the one thing the opt-in suite promises not to do.
    """

    from weaver.fabric import find_workspace

    with pytest.raises(CommandError, match="no workspace named"):
        find_workspace("weavertest_no_such_workspace")


@pytest.mark.provision
def test_created_lakehouses_appear_in_the_workspace(fabric_lakehouses, fabric_client):
    names = {
        item.name
        for item in list_items(
            fabric_lakehouses["workspace"], item_type=LAKEHOUSE, client=fabric_client
        )
    }
    assert fabric_lakehouses["weaver"].name in names
    assert fabric_lakehouses["target"].name in names


@pytest.mark.provision
def test_a_lakehouse_is_findable_by_name(fabric_lakehouses, fabric_client):
    found = find_item(
        fabric_lakehouses["workspace"],
        fabric_lakehouses["target"].name,
        item_type=LAKEHOUSE,
        client=fabric_client,
    )
    assert found.id == fabric_lakehouses["target"].id


@pytest.mark.provision
def test_creating_an_existing_lakehouse_returns_it(fabric_lakehouses, fabric_client):
    """Idempotent, so a rerun after an interruption does not fail."""
    from weaver.fabric import create_lakehouse

    again = create_lakehouse(
        fabric_lakehouses["workspace"],
        fabric_lakehouses["target"].name,
        client=fabric_client,
    )
    assert again.id == fabric_lakehouses["target"].id


@pytest.mark.provision
def test_an_unknown_item_says_which_workspace(fabric_lakehouses, fabric_client):
    with pytest.raises(CommandError, match="no Lakehouse named"):
        find_item(
            fabric_lakehouses["workspace"],
            "weavertest_absent",
            item_type=LAKEHOUSE,
            client=fabric_client,
        )
