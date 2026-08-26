"""What merge_repository accepts and what it refuses.

The rules are deliberately simple: a unique identity is added, a duplicate
identity is refused, identities differing only by case are refused. No
precedence exists, so these tests also pin that nothing is overridden: a
collision is an error, never a replacement.
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
    WeaverSchemaId,
)
from weaver.declaration.programmable import generated_programmable
from weaver.declaration.repository import RepositoryPart, merge_repository
from weaver.errors import DiscoveryError

REPORTING = WeaverItemId.parse("Warehouse/Reporting")
SALES = WeaverItemId.parse("Lakehouse/Sales")


def _programmable(schema: str, name: str) -> object:
    return generated_programmable(
        WeaverDocumentId(
            REPORTING,
            ObjectId(schema=schema, object=name),
            shape="procedure",
        ),
        text=f"create or alter procedure [{schema}].[{name}] as begin end;",
        signature="sig",
        role="load",
    )


@weaver_test()
def test_unique_declarations_merge():
    """Weaver-owned content and a second contribution compose into one part."""

    owned = RepositoryPart(label="package-owned", items=(BUILTIN_ITEM,))
    extra = RepositoryPart(
        label="generated",
        items=(REPORTING,),
        programmables={REPORTING: (_programmable("dbo", "Refresh"),)},
    )

    merged = merge_repository(owned, extra)

    assert BUILTIN_ITEM in merged.items
    assert REPORTING in merged.items
    assert [
        str(each.identity)
        for each in merged.programmables[REPORTING]
    ] == ["Warehouse/Reporting/procedure:dbo/Refresh"]


@weaver_test()
def test_a_duplicate_identity_is_refused():
    """Two contributions claiming one procedure is a fault, never an override."""

    first = RepositoryPart(
        label="first",
        items=(REPORTING,),
        programmables={REPORTING: (_programmable("dbo", "Refresh"),)},
    )
    second = RepositoryPart(
        label="second",
        items=(REPORTING,),
        programmables={REPORTING: (_programmable("dbo", "Refresh"),)},
    )

    with pytest.raises(DiscoveryError, match="contributed twice"):
        merge_repository(first, second)


@weaver_test()
def test_a_case_only_collision_is_refused():
    """SQL folds case, so two spellings of one procedure cannot coexist."""

    lower = RepositoryPart(
        label="lower",
        items=(REPORTING,),
        programmables={REPORTING: (_programmable("dbo", "refresh"),)},
    )
    upper = RepositoryPart(
        label="upper",
        items=(REPORTING,),
        programmables={REPORTING: (_programmable("dbo", "REFRESH"),)},
    )

    with pytest.raises(DiscoveryError, match="differ only by case"):
        merge_repository(lower, upper)


@weaver_test()
def test_shortcuts_follow_the_same_rules():
    """One rule for every declaration class."""

    shortcut = ShortcutDeclaration(
        owner=SALES,
        name="DWG__Customer",
        shortcut_type=TABLE_SHORTCUT,
        target_type=LOGICAL_TARGET,
        target="Lakehouse/Curated/DWG.Customer",
    )
    other = ShortcutDeclaration(
        owner=SALES,
        name="DWG__Customer",
        shortcut_type=TABLE_SHORTCUT,
        target_type=LOGICAL_TARGET,
        target="Lakehouse/Other/DWG.Customer",
    )

    with pytest.raises(DiscoveryError, match="contributed twice"):
        merge_repository(
            RepositoryPart(label="one", shortcuts=(shortcut,)),
            RepositoryPart(label="two", shortcuts=(other,)),
        )


@weaver_test()
def test_the_package_contribution_cannot_be_composed_twice():
    """Composing Weaver-owned content twice is the duplicate case, refused."""

    from weaver.declaration.repository import weaver_owned_content

    owned = weaver_owned_content()

    with pytest.raises(DiscoveryError, match="contributed twice"):
        merge_repository(owned, owned)


__all__: tuple = ()
