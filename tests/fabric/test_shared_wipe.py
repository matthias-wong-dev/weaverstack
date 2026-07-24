"""The same populated-Lakehouse wipe lifecycle on local and Fabric hosts."""

from __future__ import annotations

import pytest


def assert_target_wiped(populated_lakehouse) -> None:
    """Assert the logical result without exposing backend-specific paths."""

    tables = populated_lakehouse.resolver.tables_root(populated_lakehouse.target)
    assert populated_lakehouse.store.exists(tables)
    assert populated_lakehouse.store.list(tables) == []

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
    removed = populated_lakehouse.wipe()
    assert set(removed) == {"Sales", "Reporting"}
    assert_target_wiped(populated_lakehouse)
