"""The same populated-Lakehouse wipe lifecycle on local and Fabric workspaces."""

from __future__ import annotations

import pytest


#: What a wipe leaves behind in the Tables area. A schema-enabled Fabric
#: Lakehouse is created holding ``dbo``; Fabric owns it and nothing recreates it,
#: so a wipe empties it rather than removing it. The local emulator never had one,
#: which is why this is a subset rather than an equality — the same assertion has
#: to hold where the default schema exists and where it does not.
KEPT = {"dbo"}


def assert_target_wiped(populated_lakehouse) -> None:
    """Assert the logical result without exposing backend-specific paths."""

    tables = populated_lakehouse.resolver.tables_root(populated_lakehouse.target)
    assert populated_lakehouse.store.exists(tables)
    remaining = {entry.location.name for entry in populated_lakehouse.store.list(tables)}
    assert remaining <= KEPT, f"a wipe left {sorted(remaining - KEPT)} behind"

    files = populated_lakehouse.resolver.files_root(populated_lakehouse.target)
    assert populated_lakehouse.store.exists(files / "notes.txt")


@pytest.mark.parametrize(
    "populated_lakehouse",
    [
        pytest.param(
            "populated_local_lakehouse",
            id="local",
            marks=pytest.mark.spark,
        ),
        pytest.param(
            "populated_fabric_lakehouse",
            id="fabric",
            marks=pytest.mark.fabric,
        ),
    ],
    indirect=True,
)
def test_a_wipe_removes_every_table(populated_lakehouse):
    """Every schema the target holds goes, and the default schema is not one.

    Removing ``dbo`` would not be clearing the Lakehouse but damaging it: Fabric
    created it, Weaver never manages it, and nothing brings it back — the item is
    simply left unable to resolve a schema it is supposed to have.
    """

    removed = populated_lakehouse.wipe()

    assert set(removed) == {"Sales", "Reporting"}
    assert not KEPT & set(removed)
    assert_target_wiped(populated_lakehouse)
