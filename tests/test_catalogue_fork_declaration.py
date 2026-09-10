"""What a catalogue fork copies, what it leaves, and how it says so in T-SQL.

The rules the statements encode. A table added to the catalogue has to be
classified as one a fork copies or one it leaves, and these hold it to that.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.fork import (
    FORKED_TABLES,
    copied_tables,
    copy_statement,
    create_statement,
    fork_statements,
    forked_table_names,
    source_relation,
    uncopied_table_names,
)
from weaver.catalogue.tables import (
    BOOKMARK,
    BUILD_DATETIME,
    CATALOGUE_TABLES,
    HISTORY_TABLES,
    INSTALLATION,
    LOAD_STATISTIC,
    LOG,
    MIRROR,
    REGISTRY,
)

#: The instant a fork installs the destination at.
FORKED_AT = datetime(2026, 3, 4, 5, 6, 7, 8)

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

    statement = copy_statement(REGISTRY, source_catalogue="Weaver", published=FORKED_AT)

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

    statement = copy_statement(table, source_catalogue="Weaver", published=FORKED_AT)

    assert "where not ([Item type] = N'Warehouse' and [Item name] = N'_weaver')" in (
        statement
    )


@weaver_test()
def test_a_copied_row_is_dated_to_the_fork_and_not_to_the_source_build():
    """The destination's clock, because freshness compares its own rows.

    ``Warehouse/_weaver`` is the build this fork has just run and its rows are
    the only ones a fork does not copy. One instant on every copied row is what
    puts both sides of a ``_`` surface chain on one clock, which
    :func:`weaver.build_bundle.incremental.stale_through_shortcuts` reads.
    """

    published = REGISTRY.public_name_of(BUILD_DATETIME)
    statement = copy_statement(REGISTRY, source_catalogue="Weaver", published=FORKED_AT)
    selected = statement.split("select ", 1)[1].split("\n", 1)[0]

    assert f"({published}" not in selected
    assert f"[{published}]" not in selected
    assert "CAST('2026-03-04T05:06:07.000008' AS datetime2(6))" in selected
    # Every other column still carries the source's value.
    for column in REGISTRY.public_columns:
        if column != published:
            assert f"[{column}]" in selected


@weaver_test()
@pytest.mark.parametrize("table", copied_tables(borrowed=True), ids=lambda t: t.name)
def test_no_copy_carries_a_build_datetime_from_the_source(table):
    """One fork, one publication instant, on every table that records one."""

    statement = copy_statement(table, source_catalogue="Weaver", published=FORKED_AT)
    selected = statement.split("select ", 1)[1].split("\n", 1)[0]
    carries = BUILD_DATETIME in table.physical_columns

    assert ("CAST('2026-03-04T05:06:07.000008' AS datetime2(6))" in selected) is carries


@weaver_test()
def test_one_statement_per_copied_table():
    """Server-side, so no row passes through this process."""

    statements = fork_statements(source_catalogue="Weaver", published=FORKED_AT)

    assert len(statements) == len(FORKED_TABLES)
    assert all(statement.count("insert into") == 1 for statement in statements)


@weaver_test()
def test_no_statement_touches_a_history_table():
    body = "\n".join(fork_statements(source_catalogue="Weaver", published=FORKED_AT))

    for table in HISTORY_TABLES:
        assert f"[{table.name}]" not in body


# --- the one table no document declares ---------------------------------------


@weaver_test()
def test_a_fork_carries_nothing_borrowed_unless_the_source_has_some():
    """``_.Mirror`` exists only where something mirrored into that catalogue.

    A source only ever reached by ``weaver build`` has no such table, so a fork
    of one neither creates nor reads it.
    """

    assert MIRROR not in copied_tables()
    assert MIRROR not in FORKED_TABLES

    body = "\n".join(fork_statements(source_catalogue="Weaver", published=FORKED_AT))

    assert "[Mirror]" not in body


@weaver_test()
def test_a_borrowed_source_gives_the_destination_the_table_and_its_rows():
    """Which objects are borrowed is installed state, so a fork inherits it."""

    statements = fork_statements(source_catalogue="Weaver", published=FORKED_AT, borrowed=True)

    assert copied_tables(borrowed=True)[-1] is MIRROR
    # Created before the rows land: no build makes this one.
    created = next(i for i, each in enumerate(statements) if "create table" in each)
    copied = next(
        i for i, each in enumerate(statements) if "insert into [_].[Mirror]" in each
    )
    assert created < copied


@weaver_test()
def test_the_borrowed_table_is_created_only_where_there_is_none():
    """A fork reruns, and a second one stands on the table the first made."""

    statement = create_statement(MIRROR)

    assert statement.startswith("if object_id(N'_.Mirror', N'U') is null")
    for column in MIRROR.public_columns:
        assert f"[{column}]" in statement
    # Every column of it is written on every row.
    assert "null," not in statement.replace("not null,", "")
