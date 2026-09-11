"""``_estate_targets``: the read whose answer defines what a wipe destroys.

The function is tested directly over a real ``CatalogueConnection`` answering
from recorded Installation rows. Nothing about it is mocked: the same reader
and connection a tenant wipe uses decide the scope here, over rows shaped as
the catalogue stores them.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.connection import CatalogueConnection
from weaver.errors import CommandError
from weaver.operations.wipe import _estate_targets
from weaver.workspaces import Workspace

SCHEMA_READ = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS"


class RecordingCatalogue:
    """A ``_`` schema whose Installation rows the caller supplies.

    The shape read from ``INFORMATION_SCHEMA`` is answered with an
    Installation of the expected columns before any row is asked for.
    """

    def __init__(self, installation_rows=()):
        self.installation_rows = list(installation_rows)
        self.statements: list[str] = []
        self._shape: dict | None = None

    def rows(self, statement: str) -> list[dict]:
        self.statements.append(statement)
        if statement.startswith(SCHEMA_READ):
            return [
                {"TABLE_NAME": "Installation", "COLUMN_NAME": "Item type"},
                {"TABLE_NAME": "Installation", "COLUMN_NAME": "Target name"},
            ]
        return [dict(row) for row in self.installation_rows]


def connection_with(installation_rows=()) -> CatalogueConnection:
    catalogue = RecordingCatalogue(installation_rows)
    return CatalogueConnection(catalogue.rows)


def workspace(catalogue: str = "Warehouse/Control") -> Workspace:
    return Workspace(workspace="Demo", catalogue=catalogue)


@weaver_test()
def test_the_estate_is_the_catalogue_plus_every_installed_target():
    rows = [
        {"item_type": "Lakehouse", "target_name": "Sales"},
        {"item_type": "Warehouse", "target_name": "Reporting"},
        {"item_type": "Lakehouse", "target_name": "Landing"},
    ]
    estate = _estate_targets(workspace(), catalogue=connection_with(rows))
    assert [str(target) for target in estate] == [
        "Warehouse/Control",
        "Lakehouse/Sales",
        "Warehouse/Reporting",
        "Lakehouse/Landing",
    ]


@weaver_test()
def test_two_items_on_one_target_are_wiped_once():
    rows = [
        {"item_type": "Lakehouse", "target_name": "Sales"},
        {"item_type": "Lakehouse", "target_name": "sales"},
        {"item_type": "Warehouse", "target_name": "Sales"},
    ]
    estate = _estate_targets(workspace(), catalogue=connection_with(rows))
    assert [str(target) for target in estate] == [
        "Warehouse/Control",
        "Lakehouse/Sales",
        "Warehouse/Sales",
    ]


@weaver_test()
def test_a_target_named_like_the_catalogue_is_still_one_target():
    rows = [{"item_type": "Lakehouse", "target_name": "Control"}]
    estate = _estate_targets(workspace(), catalogue=connection_with(rows))
    # A Lakehouse/Control is a different Fabric item from Warehouse/Control,
    # so both are in the estate.
    assert [str(target) for target in estate] == [
        "Warehouse/Control",
        "Lakehouse/Control",
    ]


@weaver_test()
def test_a_warehouse_item_on_the_catalogue_target_is_wiped_once():
    rows = [{"item_type": "Warehouse", "target_name": "Control"}]
    estate = _estate_targets(workspace(), catalogue=connection_with(rows))
    assert [str(target) for target in estate] == ["Warehouse/Control"]


@weaver_test()
def test_an_empty_installation_leaves_the_catalogue_alone():
    estate = _estate_targets(workspace(), catalogue=connection_with())
    assert [str(target) for target in estate] == ["Warehouse/Control"]


@weaver_test()
def test_an_installation_of_an_unwipeable_kind_is_refused():
    rows = [{"item_type": "Environment", "target_name": "weaver"}]
    with pytest.raises(CommandError, match="not a wipeable item kind"):
        _estate_targets(workspace(), catalogue=connection_with(rows))


@weaver_test()
def test_a_catalogue_that_names_no_warehouse_is_refused():
    with pytest.raises(CommandError, match="needs a Weaver catalogue"):
        _estate_targets(Workspace(workspace="Demo"), catalogue=connection_with())


@weaver_test()
def test_the_estate_read_is_one_question_about_the_installation():
    rows = [
        {"item_type": "Lakehouse", "target_name": "Sales"},
        {"item_type": "Warehouse", "target_name": "Reporting"},
    ]
    catalogue = RecordingCatalogue(rows)
    _estate_targets(workspace(), catalogue=CatalogueConnection(catalogue.rows))
    installation_reads = [
        statement
        for statement in catalogue.statements
        if "FROM [_].[Installation]" in statement
    ]
    assert len(installation_reads) == 1
