"""Workspace and item resolution against a real Fabric workspace.

Resolution is Weaver's business and stays in the ordinary Fabric suite. Creating
and deleting Lakehouses is Fabric's, and belongs to
``tests/fabric/provision_estate.py``: the suite finds its items rather than
making them, so item churn no longer happens underneath a long-lived session.

``create_lakehouse`` remains product machinery. The fixed-estate fixtures use it
to fill in an item this tenant has not got yet, so it is covered where it runs.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError


@weaver_test(remote=True)
def test_the_workspace_resolves_to_an_id(fabric_workspace_item):
    assert fabric_workspace_item.id
    assert fabric_workspace_item.name


@weaver_test(remote=True, resources={"rest"})
def test_an_unknown_workspace_lists_what_there_is(
    fabric_workspace_item, session_fabric_client
):
    """Takes the fixture it does not read, to inherit its skip.

    Listing what there is needs a reachable tenant, so without one this cannot
    run — and unguarded it failed on the credential lookup instead of skipping,
    which is the one thing the opt-in suite promises not to do.
    """

    from weaver.fabric import find_workspace

    with pytest.raises(
        CommandError, match="Workspace 'weavertest_no_such_workspace' was not found"
    ):
        find_workspace("weavertest_no_such_workspace", client=session_fabric_client)
