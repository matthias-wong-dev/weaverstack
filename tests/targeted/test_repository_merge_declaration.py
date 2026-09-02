"""The three merge rules, and that there is no fourth.

A unique identity is added, a duplicate identity is refused, and identities
differing only by case are refused. No part takes precedence, so a collision is
an error and never a replacement.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import (
    LOGICAL_TARGET,
    TABLE_SHORTCUT,
    ShortcutDeclaration,
    WeaverDocumentId,
    WeaverItemId,
)
from weaver.declaration.programmable import generated_programmable
from weaver.declaration.repository import RepositoryPart, merge_repository
from weaver.errors import DiscoveryError

REPORTING = WeaverItemId.parse("Warehouse/Reporting")
SALES = WeaverItemId.parse("Lakehouse/Sales")


def _part(label: str, schema: str, name: str) -> RepositoryPart:
    """One part contributing one Programmable, named as given."""

    programmable = generated_programmable(
        WeaverDocumentId(
            REPORTING,
            ObjectId(schema=schema, object=name),
            shape="procedure",
        ),
        text=f"create or alter procedure [{schema}].[{name}] as begin end;",
        signature="sig",
        role="load",
    )
    return RepositoryPart(
        label=label,
        items=(REPORTING,),
        programmables={programmable.identity: programmable},
    )


@weaver_test()
def test_unique_declarations_merge():
    """Weaver-owned content and a second contribution compose into one part."""

    owned = RepositoryPart(label="catalogue", items=(BUILTIN_ITEM,))

    merged = merge_repository(owned, _part("generated", "dbo", "Refresh"))

    assert BUILTIN_ITEM in merged.items
    assert REPORTING in merged.items
    assert [str(each) for each in merged.programmables] == [
        "Warehouse/Reporting/procedure:dbo/Refresh"
    ]


@weaver_test()
def test_a_duplicate_identity_is_refused():
    """Two contributions claiming one procedure is a fault, never an override."""

    with pytest.raises(DiscoveryError, match="contributed twice"):
        merge_repository(
            _part("first", "dbo", "Refresh"), _part("second", "dbo", "Refresh")
        )


@weaver_test()
def test_a_case_only_collision_is_refused():
    """SQL folds case, so two spellings of one procedure cannot coexist."""

    with pytest.raises(DiscoveryError, match="differ only by case"):
        merge_repository(
            _part("lower", "dbo", "refresh"), _part("upper", "dbo", "REFRESH")
        )


@weaver_test()
def test_shortcuts_follow_the_same_rules():
    """One rule for every declaration class."""

    shortcut = ShortcutDeclaration(
        owner=SALES,
        name="DWG__Customer",
        shortcut_type=TABLE_SHORTCUT,
        target_type=LOGICAL_TARGET,
        target="Lakehouse/Curated/Tables/DWG.Customer",
    )
    other = ShortcutDeclaration(
        owner=SALES,
        name="DWG__Customer",
        shortcut_type=TABLE_SHORTCUT,
        target_type=LOGICAL_TARGET,
        target="Lakehouse/Other/Tables/DWG.Customer",
    )

    with pytest.raises(DiscoveryError, match="contributed twice"):
        merge_repository(
            RepositoryPart(label="one", shortcuts=(shortcut,)),
            RepositoryPart(label="two", shortcuts=(other,)),
        )


@weaver_test()
def test_the_catalogue_fragment_cannot_be_composed_twice():
    """Composing Weaver-owned content twice is the duplicate case, refused."""

    from weaver.declaration.repository import _catalogue_part

    owned = _catalogue_part()

    with pytest.raises(DiscoveryError, match="contributed twice"):
        merge_repository(owned, owned)


__all__: tuple = ()
