"""OneLake URL rendering and parsing, without a tenant.

``onelake_url`` builds the abfss URL a store reaches an item through, and
``parse_onelake`` reads one back. Both are string work over a workspace, an item
and a relative path, so the answers are decided in this process.

A named item carries its type suffix because OneLake resolves a name per type,
and a GUID does not because a GUID already names one item. These ran in the
Fabric suite and crossed nothing there.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric import onelake_url, parse_onelake
from weaver.locations import Location


@weaver_test()
def test_a_guid_item_needs_no_type_suffix():
    url = onelake_url("ws-id", "3fa85f64-5717-4562-b3fc-2c963f66afa6", "Files/x")
    assert "3fa85f64-5717-4562-b3fc-2c963f66afa6/Files/x" in url


@weaver_test()
def test_a_named_item_carries_its_type():
    assert onelake_url("MyWorkspace", "Weaver", "Files").endswith(
        "Weaver.Lakehouse/Files"
    )


@weaver_test()
def test_a_onelake_location_splits_back_into_its_parts():
    parsed = parse_onelake(
        Location(onelake_url("ws", "item-id", "Files/weaver_items/x"))
    )
    assert (parsed.workspace, parsed.relative) == ("ws", "Files/weaver_items/x")


@weaver_test()
def test_a_local_path_is_not_a_onelake_location():
    with pytest.raises(CommandError, match="not a OneLake location"):
        parse_onelake(Location("/srv/.local/Sales_LH"))
