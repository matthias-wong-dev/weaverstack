"""Weaver owns `_` in its catalogue Warehouse, and nothing else there.

The catalogue may live in a Warehouse that already holds a user's schemas:

.. code-block:: text

    Warehouse/Curated
        Sales.*       user-owned
        Finance.*     user-owned
        _.*           Weaver-owned

That only works if the boundary is real, so it is asserted rather than assumed.
Every statement Weaver renders against its own catalogue names an object in
``_``; nothing it emits can reach `Sales.Customer` in the same Warehouse, and
nothing about resetting the catalogue touches the Warehouse containing it.

These are structural claims about what Weaver *renders*, so they hold without a
tenant. What a real shared Warehouse does with them is proved in
``tests/fabric``.
"""

from __future__ import annotations

import re

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue import (
    CATALOGUE_TABLES,
    InstallationScope,
    render_delete_obsolete,
    render_delete_scope,
    render_merge,
)
from weaver.catalogue.reconcile import prune_installation
from weaver.catalogue.render import InstallationScopes
from weaver.catalogue.tables import CATALOGUE_SCHEMA

SCOPE = InstallationScope(item_type="Lakehouse", item_name="Raw")

#: What a user's own schemas in a shared catalogue host might be called.
NEIGHBOURS = ("Sales", "Finance", "dbo", "PBI_Model")

#: ``[schema].[object]`` as the renderer writes it.
_ADDRESSED = re.compile(r"\[([^\]]+)\]\.\[([^\]]+)\]")


def _row(table):
    """One projected row for any catalogue table, keyed correctly for its scope."""

    values = {}
    for column in table.columns:
        if column.name == "item_type":
            values[column.name] = SCOPE.item_type
        elif column.name == "item_name":
            values[column.name] = SCOPE.item_name
        elif column.vocabulary is not None:
            values[column.name] = next(iter(column.vocabulary))
        elif column.type == "boolean":
            values[column.name] = False
        else:
            values[column.name] = f"value-{column.name}"
    return values


def _statements(table) -> list[str]:
    """Every statement a build can render against one catalogue table."""

    rows = [_row(table)]
    scopes = InstallationScopes((SCOPE,))
    found = [
        render_merge(table, rows, scope=SCOPE),
        render_delete_obsolete(table, rows, scope=SCOPE),
        render_delete_obsolete(table, [], scope=SCOPE),
        render_delete_scope(table, scope=SCOPE),
        render_merge(table, rows, scope=scopes),
    ]
    return [statement for statement in found if statement]


@pytest.mark.parametrize("table", CATALOGUE_TABLES, ids=lambda table: table.name)
@weaver_test()
def test_every_rendered_statement_addresses_only_the_reserved_schema(table):
    """The whole of the shared-host guarantee, per table."""

    for statement in _statements(table):
        addressed = {schema for schema, _object in _ADDRESSED.findall(statement)}
        assert addressed == {CATALOGUE_SCHEMA}, statement


@pytest.mark.parametrize("neighbour", NEIGHBOURS)
@weaver_test()
def test_no_rendered_statement_names_a_neighbouring_schema(neighbour):
    """A user's own schemas in the same Warehouse are not Weaver's to touch."""

    for table in CATALOGUE_TABLES:
        for statement in _statements(table):
            assert f"[{neighbour}]." not in statement


@weaver_test()
def test_installation_prune_stays_inside_the_reserved_schema():
    """Decommissioning removes Weaver's rows, never a neighbour's table."""

    statements = prune_installation(InstallationScopes((SCOPE,)))

    assert statements
    for statement in statements:
        addressed = {schema for schema, _object in _ADDRESSED.findall(statement)}
        assert addressed == {CATALOGUE_SCHEMA}, statement
        assert "DROP" not in statement.upper(), (
            "removing an installation deletes rows; it never drops an object"
        )


@weaver_test()
def test_nothing_the_catalogue_renders_drops_a_schema_or_a_table():
    """Resetting the catalogue must not reach the Warehouse containing it.

    Weaver's rows go; the Warehouse, its other schemas and its other tables are
    not Weaver's to remove, and no statement it renders can express doing so.
    """

    rendered = [
        statement for table in CATALOGUE_TABLES for statement in _statements(table)
    ] + list(prune_installation(InstallationScopes((SCOPE,))))

    for statement in rendered:
        upper = statement.upper()
        assert "DROP TABLE" not in upper
        assert "DROP SCHEMA" not in upper
        assert "DROP DATABASE" not in upper
        assert "TRUNCATE" not in upper


@weaver_test()
def test_the_reserved_schema_is_the_one_weaver_claims():
    """Stated once, so the rest of this module has something to compare against."""

    assert CATALOGUE_SCHEMA == "_"
    assert all(table.qualified.startswith("_.") for table in CATALOGUE_TABLES)


# --- what the catalogue item's inventory may see ------------------------------


class _Warehouse:
    """A Warehouse holding Weaver's `_` and a user's own schemas beside it."""

    OBJECTS = [
        {"schema_name": "_", "object_name": "Registry", "object_type": "U "},
        {"schema_name": "_", "object_name": "Log", "object_type": "U "},
        {"schema_name": "Finance", "object_name": "Ledger", "object_type": "U "},
        {"schema_name": "Finance", "object_name": "OpenLedger", "object_type": "V "},
        {"schema_name": "Sales", "object_name": "LoadCustomer", "object_type": "P "},
    ]
    SCHEMAS = [{"name": "_"}, {"name": "Finance"}, {"name": "Sales"}]

    def query(self, statement: str):
        return self.SCHEMAS if "sys.schemas" in statement else self.OBJECTS


def _inventory(*, logical_item_name: str):
    from weaver.build_bundle.prune import read_warehouse_inventory
    from weaver.build_bundle.targets import BoundTarget

    return read_warehouse_inventory(
        BoundTarget(
            id="warehouse-Curated",
            kind="warehouse",
            item_id="Curated",
            item_name="Curated",
            logical_item_type="Warehouse",
            logical_item_name=logical_item_name,
        ),
        sql=_Warehouse(),
    )


@weaver_test()
def test_the_catalogue_items_inventory_sees_only_the_reserved_schema():
    """The whole of the shared-host guarantee, at the point it is decided.

    Prune offers an item everything its inventory can see. So an inventory for
    the catalogue item that could see `Finance` would offer a user's tables to
    an item that never declared them — and prune would remove them, correctly,
    having been told they were the item's to manage.

    Caught against a real Warehouse, which had a neighbour's schema in it and
    did not afterwards.
    """

    seen = _inventory(logical_item_name="_weaver")

    assert seen.schemas == ("_",)
    assert seen.tables == ("_.Log", "_.Registry")
    assert not any("Finance" in name for name in seen.tables + seen.views)
    assert not seen.procedures


@weaver_test()
def test_an_ordinary_item_still_sees_the_whole_warehouse():
    """The restriction is the catalogue item's alone.

    An ordinary item bound to a Warehouse must go on seeing everything it could
    be managing, or prune would stop reconciling the estate.
    """

    seen = _inventory(logical_item_name="Reporting")

    assert set(seen.schemas) == {"_", "Finance", "Sales"}
    assert "Finance.Ledger" in seen.tables
    assert "Finance.OpenLedger" in seen.views
    assert "Sales.LoadCustomer" in seen.procedures
