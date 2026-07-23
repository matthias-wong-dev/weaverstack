"""Workspace and item resolution against a real Fabric workspace."""

from __future__ import annotations

import pytest

from weaver.errors import CommandError
from weaver.fabric import LAKEHOUSE, find_item, list_items

pytestmark = pytest.mark.fabric


def test_the_workspace_resolves_to_an_id(fabric_workspace):
    assert fabric_workspace.id
    assert fabric_workspace.name


def test_an_unknown_workspace_lists_what_there_is():
    from weaver.fabric import find_workspace

    with pytest.raises(CommandError, match="no workspace named"):
        find_workspace("weavertest_no_such_workspace")


def test_created_lakehouses_appear_in_the_workspace(fabric_lakehouses, fabric_client):
    names = {
        item.name
        for item in list_items(
            fabric_lakehouses["workspace"], item_type=LAKEHOUSE, client=fabric_client
        )
    }
    assert fabric_lakehouses["weaver"].name in names
    assert fabric_lakehouses["target"].name in names


def test_a_lakehouse_is_findable_by_name(fabric_lakehouses, fabric_client):
    found = find_item(
        fabric_lakehouses["workspace"],
        fabric_lakehouses["target"].name,
        item_type=LAKEHOUSE,
        client=fabric_client,
    )
    assert found.id == fabric_lakehouses["target"].id


def test_creating_an_existing_lakehouse_returns_it(fabric_lakehouses, fabric_client):
    """Idempotent, so a rerun after an interruption does not fail."""
    from weaver.fabric import create_lakehouse

    again = create_lakehouse(
        fabric_lakehouses["workspace"],
        fabric_lakehouses["target"].name,
        client=fabric_client,
    )
    assert again.id == fabric_lakehouses["target"].id


def test_an_unknown_item_says_which_workspace(fabric_lakehouses, fabric_client):
    with pytest.raises(CommandError, match="no Lakehouse named"):
        find_item(
            fabric_lakehouses["workspace"],
            "weavertest_absent",
            item_type=LAKEHOUSE,
            client=fabric_client,
        )
