"""What a wipe must leave behind, wherever the Lakehouse lives.

The claim is about a Lakehouse, not about a transport, so it is written once and
driven from both. What differs is only which populated Lakehouse is handed in.
"""

from __future__ import annotations

#: What a wipe leaves behind in the Tables area. A schema-enabled Fabric
#: Lakehouse is created holding ``dbo``; Fabric owns it and nothing recreates it,
#: so a wipe empties it rather than removing it.
KEPT = {"dbo"}


def assert_target_wiped(populated) -> None:
    """Assert the logical result without exposing backend-specific paths."""

    tables = populated.resolver.tables_root(populated.target)
    assert populated.store.exists(tables)
    remaining = {entry.location.name for entry in populated.store.list(tables)}
    assert remaining <= KEPT, f"a wipe left {sorted(remaining - KEPT)} behind"

    files = populated.resolver.files_root(populated.target)
    assert populated.store.exists(files / "notes.txt")


def assert_a_wipe_removes_every_table(populated) -> None:
    """Every schema the target holds goes, and the default schema is not one.

    Removing ``dbo`` would not be clearing the Lakehouse but damaging it: Fabric
    created it, Weaver never manages it, and nothing brings it back — the item is
    simply left unable to resolve a schema it is supposed to have.
    """

    removed = populated.wipe()

    assert set(removed) == {"Sales", "Reporting"}
    assert not KEPT & set(removed)
    assert_target_wiped(populated)
