"""What a catalogue fork copies, what it leaves, and how it says so in T-SQL.

The rules the statements encode, held here so a table added to the catalogue
cannot quietly fall out of a fork.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.fork import (
    FORKED_TABLES,
    copy_statement,
    fork_statements,
    forked_table_names,
    source_relation,
    uncopied_table_names,
)
from weaver.catalogue.tables import (
    BOOKMARK,
    CATALOGUE_TABLES,
    HISTORY_TABLES,
    INSTALLATION,
    LOAD_STATISTIC,
    LOG,
    REGISTRY,
)

# --- what moves ---------------------------------------------------------------


@weaver_test()
def test_every_catalogue_table_either_moves_or_stays():
    """A new catalogue table has to be classified, and this is what asks.

    Without it a table added later would be built into the destination and left
    empty, and nothing would say so.
    """

    accounted = {table.name for table in FORKED_TABLES} | {
        table.name for table in HISTORY_TABLES
    }

    assert accounted == {table.name for table in CATALOGUE_TABLES}


@weaver_test()
def test_history_stays_with_the_estate_whose_runs_produced_it():
    left = uncopied_table_names()

    assert set(left) == {LOG.name, LOAD_STATISTIC.name}
    assert set(left).isdisjoint(forked_table_names())


@weaver_test()
def test_installed_state_and_how_far_it_has_run_both_move():
    """A fork inherits what is installed and where each object got to."""

    copied = set(forked_table_names())

    assert {INSTALLATION.name, REGISTRY.name, BOOKMARK.name} <= copied


# --- how the statements are spelled -------------------------------------------


@weaver_test()
def test_the_source_is_named_three_part():
    """Three-part is how a Fabric Warehouse reaches another in its workspace."""

    assert source_relation("Weaver", REGISTRY) == "[Weaver].[_].[Registry]"


@weaver_test()
def test_a_copy_names_its_columns_rather_than_starring_them():
    """The statement says which value lands where.

    A ``select *`` would carry whatever order the source declared its columns
    in, and land them positionally in a destination built from this Weaver's
    declaration.
    """

    statement = copy_statement(REGISTRY, source_catalogue="Weaver")

    assert "select *" not in statement
    for column in REGISTRY.public_columns:
        assert f"[{column}]" in statement
    assert "insert into [_].[Registry] (" in statement
    assert "from [Weaver].[_].[Registry]" in statement


@weaver_test()
@pytest.mark.parametrize("table", FORKED_TABLES, ids=lambda table: table.name)
def test_no_copy_carries_the_destinations_own_catalogue_rows(table):
    """``Warehouse/_weaver`` rows belong to the build that made this catalogue.

    They name the Warehouse the ``_`` schema is in. Copying the source's would
    replace that with the address of a catalogue this one is not.
    """

    statement = copy_statement(table, source_catalogue="Weaver")

    assert "where not ([Item type] = N'Warehouse' and [Item name] = N'_weaver')" in (
        statement
    )


@weaver_test()
def test_one_statement_per_copied_table():
    """Server-side, so no row passes through this process."""

    statements = fork_statements(source_catalogue="Weaver")

    assert len(statements) == len(FORKED_TABLES)
    assert all(statement.count("insert into") == 1 for statement in statements)


@weaver_test()
def test_no_statement_touches_a_history_table():
    body = "\n".join(fork_statements(source_catalogue="Weaver"))

    for table in HISTORY_TABLES:
        assert f"[{table.name}]" not in body
