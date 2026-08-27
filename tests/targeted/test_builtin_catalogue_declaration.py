"""The checked-in Weaver-owned catalogue against the catalogue's own contract.

A table that gains or loses a column, a key or its protection without its
document changing is refused here rather than discovered by a build.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.tables import (
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    RUNTIME_TABLES,
)
from weaver.declaration.model import WAREHOUSE
from weaver.declaration.source import read_source_document
from weaver.fragments import CATALOGUE, fragment_files

#: How each runtime table's lineage announces itself. The projected tables
#: share one lineage; each runtime one carries its own.
_RUNTIME_LINEAGE_OPENINGS = {
    "Log": "Appended",
    "Bookmark": "Maintained",
    "LoadStatus": "Maintained",
    "LoadStatistic": "Appended",
    "TestStatus": "Maintained",
}

_PROJECTED_LINEAGE_OPENING = "Projected"


def _documents() -> dict[str, tuple[str, object]]:
    """Every checked-in catalogue document, keyed by ``_.Name``."""

    found: dict[str, tuple[str, object]] = {}
    for relative, data in fragment_files(CATALOGUE).items():
        if not relative.endswith(".sql"):
            continue
        source = read_source_document(relative, data, WAREHOUSE)
        qualified = f"{CATALOGUE_SCHEMA}.{source.object_id.object}"
        found[qualified] = (relative, source.document)
    return found


@weaver_test()
def test_every_catalogue_table_is_declared_and_nothing_else_is():
    documents = _documents()
    assert set(documents) == {table.qualified for table in CATALOGUE_TABLES}


@weaver_test()
def test_the_catalogue_schema_is_declared():
    assert f"schemas/{CATALOGUE_SCHEMA}.yml" in fragment_files(CATALOGUE)


@pytest.mark.parametrize("table", CATALOGUE_TABLES, ids=lambda table: table.name)
@weaver_test()
def test_the_declaration_matches_the_table_contract(table):
    _relative, document = _documents()[table.qualified]

    declared = {column.name: column for column in document.schema}
    expected = {column.public_name: column for column in table.columns}
    assert set(declared) == set(expected), table.name
    for name, column in expected.items():
        assert declared[name].type == column.warehouse_type, (table.name, name)

    key = tuple(table.public_name_of(name) for name in table.key)
    assert document.primary_key == key, table.name

    # The catalogue describes itself through these headers: Static keeps it out
    # of every load, Prohibit rebuild out of every drop, and no load procedure
    # means the catalogue's own DML is the only writer.
    assert document.static is True, table.name
    assert document.prohibit_rebuild is True, table.name
    assert document.has_load_procedure is False, table.name

    expected_not_null = {
        public
        for public_name, column in (
            (table.public_name_of(column.name), column) for column in table.columns
        )
        if column.not_null and public_name not in key
        for public in (public_name,)
    }
    assert set(document.declared_not_null) == expected_not_null, table.name


@pytest.mark.parametrize("table", CATALOGUE_TABLES, ids=lambda table: table.name)
@weaver_test()
def test_a_table_declares_where_its_rows_come_from(table):
    text = fragment_files(CATALOGUE)[f"{table.qualified}.sql"].decode("utf-8")
    opening = (
        _RUNTIME_LINEAGE_OPENINGS[table.name]
        if table in RUNTIME_TABLES
        else _PROJECTED_LINEAGE_OPENING
    )
    lineage = text.split("Lineage: >-", 1)[1]
    assert lineage.lstrip().startswith(opening), table.name


@weaver_test()
def test_no_document_declares_dependencies():
    """Catalogue rows come from Weaver's own projection, never from a load."""

    for _relative, document in _documents().values():
        assert document.dependencies == (), document.object_id
