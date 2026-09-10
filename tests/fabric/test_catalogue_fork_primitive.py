"""Forking one Fabric catalogue into another, against a real workspace.

The claim: ``weaver mirror --no-item`` empties the destination Warehouse,
rebuilds the ``_`` schema there, and copies the source catalogue's installed
state into it, server-side.

It needs a tenant because the copy is Fabric's work. Three-part cross-Warehouse
``insert ... select`` is the mechanism, and whether it carries a signature
string and a null through unchanged, and lands the fork's own
``datetime2(6)``, is Fabric's answer. What the fork decides is settled without
a tenant, in
``tests/test_catalogue_fork_declaration.py`` and ``tests/test_mirror_boundary.py``.

The destination is ``PYTEST_WEAVER_FORK``, which nothing else in the suite
reads, because a fork empties what it writes into.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from support.build_envs import WAREHOUSE_ESTATE_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.fork import FORKED_TABLES, forked_table_names
from weaver.catalogue.tables import (
    CATALOGUE_SCHEMA,
    HISTORY_TABLES,
    INSTALLATION,
    REGISTRY,
)
from weaver.catalogue.tsql import identifier, literal
from weaver.targets import ItemRef, WarehouseTarget

#: Rows the destination catalogue writes about itself. Its own build published
#: them, naming the Warehouse its ``_`` schema is in, so they are held apart
#: from the rows that came across.
NOT_THE_CATALOGUES_OWN = "not ([Item type] = N'Warehouse' and [Item name] = N'_weaver')"


@dataclass(frozen=True)
class Forked:
    """One fork, and a way to read either side of it."""

    result: Any
    source_sql: Any
    sql: Any
    source_name: str
    destination_name: str


@pytest.fixture(scope="module")
def source_estate(
    fabric_workspace, session_disposable_warehouse, warehouse_session, tmp_path_factory
):
    """One built Warehouse item, so the source catalogue has rows to copy.

    A fork of an empty catalogue copies nothing, and every claim about the copy
    would hold trivially. This builds the ordinary Warehouse estate fixture, so
    the source catalogue certifies real objects and carries the dictionary rows
    that describe them.
    """

    register_session(warehouse_session)
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("fork-source"))
    target = f"Warehouse/Reporting=Warehouse/{session_disposable_warehouse.item.name}"
    built = weaver.build(str(estate.path), items=[target], session=warehouse_session)
    assert built.succeeded, [failure.describe() for failure in built.errors]
    return built


@pytest.fixture(scope="module")
def forked(
    fabric_workspace,
    fabric_catalogue,
    fabric_fork_catalogue,
    warehouse_session,
    source_estate,
):
    """One fork, performed once for every question about it.

    A fork is a whole-Warehouse reconstruction, and each claim below is about
    the same one. Running it per test would ask about several estates and cost
    a reconstruction each.
    """

    result = weaver.mirror(
        no_item=True,
        session=warehouse_session,
        workspace=fabric_workspace.workspace,
        catalogue=f"Warehouse/{fabric_fork_catalogue.name}",
        mirror=f"Warehouse/{fabric_catalogue.name}",
    )
    return Forked(
        result=result,
        source_sql=_sql(warehouse_session, fabric_workspace, fabric_catalogue.name),
        sql=_sql(warehouse_session, fabric_workspace, fabric_fork_catalogue.name),
        source_name=fabric_catalogue.name,
        destination_name=fabric_fork_catalogue.name,
    )


def _sql(session, workspace, name: str):
    return session.sql_executor(WarehouseTarget(ItemRef(name)), workspace=workspace)


def _counts(sql, tables, *, where: str = "1 = 1") -> dict[str, int]:
    """How many rows each table holds, in one crossing.

    One statement, because the subject is one estate at one moment. A query per
    table would ask about several.
    """

    statement = "\nunion all\n".join(
        f"select {literal(table.name)} as [Table], count(*) as [Rows] "
        f"from {identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)} "
        f"where {where}"
        for table in tables
    )
    return {str(row["Table"]): int(row["Rows"]) for row in sql.query(statement)}


@weaver_test(remote=True)
def test_the_fork_reports_what_it_copied(forked):
    """Every table a fork copies is reported, and history is named as left."""

    assert set(forked.result.copied) == set(forked_table_names())
    assert set(forked.result.uncopied) == {table.name for table in HISTORY_TABLES}
    assert forked.result.source_catalogue.endswith(forked.source_name)
    assert forked.result.destination_catalogue.endswith(forked.destination_name)
    assert forked.result.wiped == (f"Warehouse/{forked.destination_name}",)


@weaver_test(remote=True, resources={"tds"})
def test_the_destination_holds_the_sources_rows(forked):
    """The copy is exact, table by table.

    Both sides are counted without the catalogue's own scope: the source's
    ``_weaver`` rows stay behind, and the destination's are its own build's.
    """

    source = _counts(forked.source_sql, FORKED_TABLES, where=NOT_THE_CATALOGUES_OWN)
    destination = _counts(forked.sql, FORKED_TABLES, where=NOT_THE_CATALOGUES_OWN)

    assert destination == source
    assert source[REGISTRY.name] > 0, (
        "the source catalogue certifies nothing, so an exact copy proves nothing"
    )


@weaver_test(remote=True, resources={"tds"})
def test_history_is_left_where_it_happened(forked):
    """A fork inherits an estate, not the record of what was done to it.

    The tables are there, because a load writes into them from the first run.
    """

    counts = _counts(forked.sql, HISTORY_TABLES)

    assert set(counts) == {table.name for table in HISTORY_TABLES}
    assert all(rows == 0 for rows in counts.values()), counts


@weaver_test(remote=True, resources={"tds"})
def test_the_forked_catalogue_still_names_the_sources_physical_targets(forked):
    """What makes the fork a fork: every item begins where it already is.

    Reading this workspace's own ``targets:`` here would move items nobody
    asked to move.
    """

    assert _installations(forked.sql) == _installations(forked.source_sql)


@weaver_test(remote=True, resources={"tds"})
def test_the_catalogue_owns_its_own_installation_row(forked):
    """``Warehouse/_weaver`` names the Warehouse its ``_`` schema is in.

    Copied across, it would name the source's Warehouse, and this catalogue
    would assert its own tables live in a catalogue it is not.
    """

    rows = forked.sql.query(
        f"select [Target name] from {identifier(CATALOGUE_SCHEMA)}."
        f"{identifier(INSTALLATION.name)} "
        "where [Item type] = N'Warehouse' and [Item name] = N'_weaver'"
    )

    assert [str(row["Target name"]) for row in rows] == [forked.destination_name]


@weaver_test(remote=True, resources={"tds"})
def test_a_signature_survives_the_copy_and_the_instant_is_the_forks(forked):
    """What incremental selection compares, as the destination holds it.

    A signature is what says an object is unchanged, so it comes across
    unaltered. The build datetime is one catalogue's own publication clock, and
    the copy writes one instant of its own over every row: see
    :func:`weaver.catalogue.fork.copy_statement`.
    """

    identity = "[Item type], [Item name], [Schema name], [Object name]"
    registry = f"{identifier(CATALOGUE_SCHEMA)}.{identifier(REGISTRY.name)}"
    query = (
        f"select {identity}, [Signature], [Build datetime] from {registry} "
        f"where {NOT_THE_CATALOGUES_OWN} order by {identity}"
    )

    source = [tuple(row.values()) for row in forked.source_sql.query(query)]
    destination = [tuple(row.values()) for row in forked.sql.query(query)]

    assert [row[:-1] for row in destination] == [row[:-1] for row in source]
    instants = {row[-1] for row in destination}
    assert len(instants) == 1
    assert None not in instants
    # The source estate's own builds dated its rows, and one of them cannot be
    # the fork's instant, so the fork's clock is what the destination carries.
    assert instants.isdisjoint({row[-1] for row in source})


@weaver_test(remote=True, resources={"tds"})
def test_running_the_fork_again_converges(forked, warehouse_session, fabric_workspace):
    """A fork is reconstruction, so a second one leaves the same estate.

    That is what makes a half-finished fork recoverable: nothing to repair, the
    same work to do again.
    """

    before = _counts(forked.sql, FORKED_TABLES, where=NOT_THE_CATALOGUES_OWN)

    weaver.mirror(
        no_item=True,
        session=warehouse_session,
        workspace=fabric_workspace.workspace,
        catalogue=f"Warehouse/{forked.destination_name}",
        mirror=f"Warehouse/{forked.source_name}",
    )

    assert _counts(forked.sql, FORKED_TABLES, where=NOT_THE_CATALOGUES_OWN) == before


@weaver_test(remote=True, resources={"tds", "rest"})
def test_a_source_that_is_not_there_fails_before_anything_is_emptied(
    forked, warehouse_session, fabric_workspace
):
    """A misspelled source must not cost the destination.

    Run after the fork, so what it proves is that the destination survived: the
    rows counted afterwards are the ones the fork left.
    """

    from weaver.errors import CommandError

    before = _counts(forked.sql, FORKED_TABLES, where=NOT_THE_CATALOGUES_OWN)

    with pytest.raises(CommandError, match="could not read"):
        weaver.mirror(
            no_item=True,
            session=warehouse_session,
            workspace=fabric_workspace.workspace,
            catalogue=f"Warehouse/{forked.destination_name}",
            mirror=f"Warehouse/{forked.source_name}_no_such_catalogue",
        )

    assert _counts(forked.sql, FORKED_TABLES, where=NOT_THE_CATALOGUES_OWN) == before


def _installations(sql) -> set[tuple[str, str, str]]:
    rows = sql.query(
        "select [Item type], [Item name], [Target name] from "
        f"{identifier(CATALOGUE_SCHEMA)}.{identifier(INSTALLATION.name)} "
        f"where {NOT_THE_CATALOGUES_OWN}"
    )
    return {
        (str(row["Item type"]), str(row["Item name"]), str(row["Target name"]))
        for row in rows
    }
