"""The platform's ceiling on how wide a Warehouse table can be.

A declared schema is counted while the project is still a project, because the
count is knowable without a workspace and a build that started would have
already done work. A query-derived schema is not knowable offline at all, so
its count is frozen into the generated script and made where the engine has
just settled the shape.

The total is the physical one. Weaver adds an identity column, three audit
columns and, for a keyed table with a load, a signature, so the number an
author declares is not the number the engine is asked for.
"""

from __future__ import annotations

from pathlib import Path
from shutil import copytree

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.capacity import WAREHOUSE_MAX_COLUMNS
from weaver.declaration.model import WAREHOUSE, WeaverItemId
from weaver.declaration.source import read_source_document
from weaver.declaration.tsql_ddl import (
    generate_tsql_table_script,
    managed_column_count,
)
from weaver.errors import DiscoveryError
from weaver.operations.check import check

ITEM = WeaverItemId("Warehouse", "Reporting")


def _columns(count: int) -> str:
    return "".join(f"  Column{index:05d}: varchar(10)\n" for index in range(count))


def _source(*, business: int, keyed: bool = True, identity: bool = True) -> bytes:
    """One Warehouse table of a declared width."""

    key = "\nPrimary key: Column00000\n" if keyed else ""
    surrogate = "\nIdentity: Customer key\n" if identity else ""
    return (
        "/*\nTable ID: Sales.Customer\n\nDescription: Customers.\n\n"
        f"Lineage: The sales system.\n{key}{surrogate}"
        f"\nSchema:\n{_columns(business)}*/\nselect 1;\n"
    ).encode("utf-8")


def _document(**shape):
    return read_source_document("Sales.Customer.sql", _source(**shape), WAREHOUSE)


# --- what the physical total actually is ---------------------------------------


@weaver_test()
def test_the_ceiling_is_one_constant_with_its_source_beside_it():
    assert WAREHOUSE_MAX_COLUMNS == 1024


@weaver_test()
@pytest.mark.parametrize(
    ("keyed", "identity", "expected"),
    [
        # Three audit columns, plus an identity, plus a signature where a keyed
        # table has a load to compute one.
        (True, True, 5),
        (True, False, 4),
        (False, True, 4),
        (False, False, 3),
    ],
)
def test_weaver_adds_what_the_declaration_implies(keyed, identity, expected):
    document = _document(business=3, keyed=keyed, identity=identity).document

    assert managed_column_count(document) == expected
    assert len(document.effective_schema) == 3 + expected


# --- just below, exactly at, and just above ------------------------------------


def _project(root: Path, *, business: int, keyed: bool, identity: bool) -> Path:
    fixture = Path(__file__).parent / "fixtures" / "build-lakehouse-item"
    copytree(fixture, root)
    warehouse = root / "Warehouse" / "Reporting" / "Sales.Customer.sql"
    warehouse.parent.mkdir(parents=True)
    warehouse.write_bytes(_source(business=business, keyed=keyed, identity=identity))
    return root


@weaver_test()
@pytest.mark.parametrize(
    ("keyed", "identity", "managed"),
    [(True, True, 5), (True, False, 4), (False, True, 4), (False, False, 3)],
)
def test_the_total_measured_is_the_physical_one(keyed, identity, managed):
    """Not the authored count, which is the same in all four of these."""

    document = _document(
        business=WAREHOUSE_MAX_COLUMNS - managed, keyed=keyed, identity=identity
    ).document

    assert len(document.schema) == WAREHOUSE_MAX_COLUMNS - managed
    assert len(document.effective_schema) == WAREHOUSE_MAX_COLUMNS


@weaver_test()
def test_a_table_exactly_at_the_ceiling_is_accepted(tmp_path):
    root = _project(
        tmp_path / "at",
        business=WAREHOUSE_MAX_COLUMNS - 5,
        keyed=True,
        identity=True,
    )
    document = read_source_document(
        "Sales.Customer.sql",
        (root / "Warehouse" / "Reporting" / "Sales.Customer.sql").read_bytes(),
        WAREHOUSE,
    ).document

    assert len(document.effective_schema) == WAREHOUSE_MAX_COLUMNS
    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_a_table_one_column_below_the_ceiling_is_accepted(tmp_path):
    root = _project(
        tmp_path / "below",
        business=WAREHOUSE_MAX_COLUMNS - 6,
        keyed=True,
        identity=True,
    )

    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_a_table_one_column_over_the_ceiling_is_refused(tmp_path):
    root = _project(
        tmp_path / "over",
        business=WAREHOUSE_MAX_COLUMNS - 4,
        keyed=True,
        identity=True,
    )

    with pytest.raises(DiscoveryError) as refused:
        check(root)

    said = str(refused.value)
    assert "Sales.Customer.sql" in said
    assert f"would have {WAREHOUSE_MAX_COLUMNS + 1} columns" in said
    assert f"holds {WAREHOUSE_MAX_COLUMNS}" in said
    # The decomposition, so an author knows which part to change.
    assert f"{WAREHOUSE_MAX_COLUMNS - 4} declared" in said
    assert "plus 5 Weaver adds" in said


@weaver_test()
def test_an_unkeyed_table_is_measured_by_its_own_additions():
    """No key means no signature, so the same declared count fits."""

    keyed = _document(business=100, keyed=True, identity=True).document
    unkeyed = _document(business=100, keyed=False, identity=True).document

    assert len(unkeyed.effective_schema) == len(keyed.effective_schema) - 1


@weaver_test()
def test_a_wide_table_is_refused_by_the_ceiling_and_by_nothing_else(tmp_path):
    """The only limit a wide table meets here is the platform's.

    A table this wide leaves its comparison set to default, and the default is
    every non-key column. The catalogue records what an author named, so a
    default it does not store cannot be what refuses the project.
    """

    root = _project(tmp_path / "derived", business=200, keyed=True, identity=True)

    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_a_lakehouse_table_is_not_refused_by_a_warehouse_rule(tmp_path):
    """The ceiling is a Warehouse storage limit and belongs to Warehouse items."""

    fixture = Path(__file__).parent / "fixtures" / "build-lakehouse-item"
    root = tmp_path / "lakehouse"
    copytree(fixture, root)
    wide = root / "Lakehouse" / "Raw" / "Tables" / "DWG__Wide.py"
    wide.write_text(
        '"""\nTable ID: DWG.Wide\n\nDescription: Wide.\n\n'
        "Lineage: The sales system.\n\nPrimary key: Column00000\n"
        f"\nSchema:\n{_columns(WAREHOUSE_MAX_COLUMNS + 50)}"
        '"""\n\nfrom weaver import Table\n\n\n'
        "class DWG__Wide(Table):\n    def read(self):\n        return None\n",
        encoding="utf-8",
    )

    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_a_generated_working_table_is_never_wider_than_its_target():
    """So the target's own count is the one that decides.

    Staging carries the business columns, reject adds a reason and upsert adds a
    signature and a new-row flag; the target adds three audit columns to the
    same business set, and more where it has an identity or a signature.
    """

    document = _document(business=10).document
    business = len(document.schema)

    assert len(document.effective_schema) >= business + 3
    # Reject is business + 1, upsert is business + 2.
    assert len(document.effective_schema) > business + 2


# --- a shape only the engine can count -----------------------------------------


def _inferred_script(object: str = "Customer") -> str:
    """The same declaration with its shape left to the query."""

    return generate_tsql_table_script(
        read_source_document(
            f"Sales.{object}.sql",
            (
                f"/*\nTable ID: Sales.{object}\n\nDescription: Customers.\n\n"
                "Lineage: The sales system.\n\nPrimary key: Column00000\n"
                "\nIdentity: Customer key\n*/\nselect * from [Raw].[Customer];\n"
            ).encode("utf-8"),
            WAREHOUSE,
        ).document,
        "select * from [Raw].[Customer];",
    )


@weaver_test()
def test_an_inferred_shape_is_counted_where_the_engine_settles_it():
    script = _inferred_script()

    assert "@weaver_columns" in script
    assert f"> {WAREHOUSE_MAX_COLUMNS}" in script
    assert "throw 51002" in script


@weaver_test()
def test_the_guard_runs_after_the_shape_and_before_the_persistent_create():
    """A count before the shape exists has nothing to count; one after the create
    has nothing left to prevent."""

    script = _inferred_script()
    shape = script.index("#weaver_shape")
    guard = script.index("@weaver_columns = count(*)")
    created = script.index("N'create table ")

    assert shape < guard < created


@weaver_test()
def test_the_guard_counts_the_managed_columns_the_declaration_implies():
    script = _inferred_script()

    # Identity, three audit columns and the signature of a keyed table.
    assert "@weaver_columns + 5" in script


@weaver_test()
def test_the_guard_says_what_the_total_was_made_of():
    script = _inferred_script()

    assert "from the query, plus 5 Weaver adds" in script
    assert "Sales.Customer would have" in script


@weaver_test()
def test_a_declared_schema_needs_no_generated_guard():
    """It was counted while the project was still a project."""

    document = _document(business=3).document
    script = generate_tsql_table_script(document, "select 1;")

    assert "@weaver_columns" not in script


@weaver_test()
def test_an_object_name_holding_an_apostrophe_survives_every_literal():
    """An inferred table builds itself from strings, and a name is legal in one.

    The shape is only known to the engine, so the create statement, its primary
    key and the width diagnostic are all assembled as text and executed. A name
    written straight into any of them closes the literal and leaves the whole
    batch unparseable, which is a table that cannot build rather than a bad
    message.
    """

    script = _inferred_script("O'Brien")

    assert "N'weaver: Sales.O''Brien would have '" in script
    assert "N'create table [Sales].[O''Brien] ('" in script
    assert "N'alter table [Sales].[O''Brien] add constraint [PK_O''Brien] '" in script
    # Nothing else reopened a literal: the guard still ends where it should.
    assert "throw 51002, @weaver_width_error, 1;" in script


@weaver_test()
def test_an_ordinary_name_reads_as_it_always_did():
    assert "N'weaver: Sales.Customer would have '" in _inferred_script()
