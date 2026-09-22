"""The storage facts the offline capacity checks depend on.

Both are the engine's answers, not Weaver's. A ``varchar(n)`` column's budget
is what it is, and a Warehouse table's column ceiling is where it is. What the
offline checks do is refuse a project before the engine has to give either
answer, so the answers themselves are established here.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.capacity import WAREHOUSE_MAX_COLUMNS, stored_size
from weaver.catalogue.tables import TABLE_DICTIONARY
from weaver.catalogue.tsql import literal
from weaver.declaration.model import WAREHOUSE
from weaver.declaration.source import read_source_document
from weaver.declaration.tsql_ddl import generate_tsql_table_script

SCHEMA = "Capacity"

#: The width the catalogue gives authored prose, read from the declaration.
PROSE = 4000


def _prepare(executor, table: str, definition: str) -> None:
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
    )
    executor.execute_script(
        f"if object_id(N'{SCHEMA}.{table}', N'U') is not null "
        f"drop table [{SCHEMA}].[{table}];"
    )
    executor.execute_script(f"create table [{SCHEMA}].[{table}] ({definition});")


def _row(comparison: str) -> dict:
    """One TableDictionary row, as a build's projection shapes it."""

    return {
        "item_type": "Warehouse",
        "item_name": "Probe",
        "schema_name": "Sales",
        "object_name": "Customer",
        "object_type": "table",
        "description": "Customers.",
        "description_reference": None,
        "lineage": "The sales system.",
        "lineage_reference": None,
        "primary_key": "CustomerId",
        "not_null_columns": None,
        "identity_column": None,
        "comparison_columns": comparison,
        "is_incremental": False,
        "is_static": False,
        "prohibit_rebuild": False,
        "signature": "sig",
    }


@weaver_test(remote=True, resources={"tds"})
def test_a_varchar_column_budget_is_counted_in_bytes(clean_disposable_warehouse):
    """A multibyte character costs more than one character of the budget.

    This is why a capacity check measures the UTF-8 encoding rather than the
    Python string, and why a value that fits in characters can still overflow.
    """

    executor = clean_disposable_warehouse.executor
    _prepare(executor, "Prose", f"[Description] varchar({PROSE}) null")

    # Every character three bytes, so a third of the column's characters fill it.
    fitting = "日" * (PROSE // 3)
    assert stored_size(fitting) == PROSE - 1

    executor.execute_script(
        f"insert into [{SCHEMA}].[Prose] ([Description]) values ({literal(fitting)});"
    )
    stored = executor.query(f"select [Description] as d from [{SCHEMA}].[Prose];")[0][
        "d"
    ]

    assert stored == fitting

    from weaver.sql.errors import SqlError

    # One character more is three bytes more, which is two bytes past the end.
    with pytest.raises(SqlError) as refused:
        executor.execute_script(
            f"insert into [{SCHEMA}].[Prose] ([Description]) values "
            f"({literal(fitting + '日')});"
        )

    assert "truncat" in str(refused.value).casefold()


@weaver_test(remote=True, resources={"tds"})
def test_an_exactly_fitting_catalogue_value_round_trips(clean_disposable_warehouse):
    """The boundary is usable, not merely unrefused.

    Written through the same literal rendering a catalogue statement uses, into
    a column of the width the catalogue declares, and read back unchanged.
    """

    executor = clean_disposable_warehouse.executor
    column = TABLE_DICTIONARY.column("description")
    _prepare(executor, "Exact", f"[Description] {column.warehouse_type} null")

    # Apostrophes and newlines: what a literal has to escape, and what the
    # column counts, are different things.
    value = "it's a\nlong note. " + "d" * (PROSE - len("it's a\nlong note. "))
    assert stored_size(value) == PROSE

    executor.execute_script(
        f"insert into [{SCHEMA}].[Exact] ([Description]) values ({literal(value)});"
    )
    stored = executor.query(f"select [Description] as d from [{SCHEMA}].[Exact];")[0][
        "d"
    ]

    assert stored == value


@weaver_test(remote=True, resources={"tds"})
def test_a_query_shape_over_the_column_ceiling_is_refused_before_the_create(
    emptied_disposable_warehouse,
):
    """The shape only the engine can count, counted where it has just counted it.

    The generated guard runs against the temporary shape table, so the
    persistent table is never created and the refusal names the totals.
    """

    from weaver.sql.errors import SqlError

    executor = emptied_disposable_warehouse.executor
    # Five columns are Weaver's, so 1,020 from the query is the last that fits.
    business = WAREHOUSE_MAX_COLUMNS - 4
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
    )
    executor.execute_script(
        f"if object_id(N'{SCHEMA}.Wide', N'U') is not null "
        f"drop table [{SCHEMA}].[Wide];"
    )
    executor.execute_script(
        f"if object_id(N'{SCHEMA}.Source', N'U') is not null "
        f"drop table [{SCHEMA}].[Source];"
    )
    columns = ", ".join(
        f"[Column{index:05d}] varchar(10) null" for index in range(business)
    )
    executor.execute_script(f"create table [{SCHEMA}].[Source] ({columns});")

    body = f"select * from [{SCHEMA}].[Source];"
    document = read_source_document(
        f"{SCHEMA}.Wide.sql",
        (
            f"/*\nTable ID: {SCHEMA}.Wide\n\nDescription: Wide.\n\n"
            "Lineage: The sales system.\n\nPrimary key: Column00000\n"
            "\nIdentity: Wide key\n*/\n" + body + "\n"
        ).encode("utf-8"),
        WAREHOUSE,
    ).document

    with pytest.raises(SqlError) as refused:
        executor.execute_script(generate_tsql_table_script(document, body))

    said = str(refused.value)
    assert f"would have {WAREHOUSE_MAX_COLUMNS + 1} columns" in said
    assert f"holds {WAREHOUSE_MAX_COLUMNS}" in said
    assert f"{business} from the query, plus 5 Weaver adds" in said

    existing = executor.query(
        f"select count(*) as n from sys.tables "
        f"where schema_id = schema_id(N'{SCHEMA}') and name = N'Wide';"
    )
    assert existing[0]["n"] == 0


@weaver_test(remote=True, resources={"tds"})
def test_an_unbounded_comparison_set_survives_the_catalogues_own_merge(
    clean_disposable_warehouse,
):
    """`varchar(max)` is only usable if the catalogue's own statements accept it.

    A named comparison set has no bound, because narrowing one to fit storage
    would change what a load treats as a change. The column it lands in is
    therefore ``varchar(max)``, and the catalogue maintains its own rows with a
    MERGE that compares every non-key column. Create, write, compare, update
    and read, with a value past every bounded width, through the production
    statements rather than hand-written ones.
    """

    from weaver.catalogue import InstallationScope, render_merge
    from weaver.catalogue.capacity import capacity_of
    from weaver.catalogue.fork import create_statement

    executor = clean_disposable_warehouse.executor
    column = TABLE_DICTIONARY.column("comparison_columns")
    assert capacity_of(column) is None

    named = ", ".join(f"Column{index:05d}" for index in range(1200))
    assert stored_size(named) > 8000

    scope = InstallationScope(item_type="Warehouse", item_name="Probe")
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
    )
    executor.execute_script(
        f"if object_id(N'{SCHEMA}.TableDictionary', N'U') is not null "
        f"drop table [{SCHEMA}].[TableDictionary];"
    )
    here = create_statement(TABLE_DICTIONARY).replace("[_].", f"[{SCHEMA}].")
    executor.execute_script(here)
    assert f"[{column.public_name}] varchar(max)" in here

    def merged(comparison: str) -> None:
        statement = render_merge(TABLE_DICTIONARY, [_row(comparison)], scope=scope)
        executor.execute_script(statement.replace("[_].", f"[{SCHEMA}]."))

    def recorded() -> dict:
        rows = executor.query(
            f"select [{column.public_name}] as c, [Row update datetime] as u "
            f"from [{SCHEMA}].[TableDictionary];"
        )
        return dict(rows[0])

    merged(named)
    inserted = recorded()
    # The same rows again: the MERGE compares this column and must find no change.
    merged(named)
    unchanged = recorded()
    merged(named + ", ColumnExtra")
    changed = recorded()

    assert inserted["c"] == named
    assert unchanged["u"] == inserted["u"]
    assert changed["u"] != inserted["u"]
    assert changed["c"] == named + ", ColumnExtra"


@weaver_test(remote=True, resources={"tds"})
def test_an_object_name_holding_an_apostrophe_builds(emptied_disposable_warehouse):
    """The width guard renders the object's name into its diagnostic.

    A name written straight into that message would close the literal and leave
    the whole batch unparseable, so this table would not build at all.
    """

    executor = emptied_disposable_warehouse.executor
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
    )
    for name in ("O'Brien", "Narrow"):
        executor.execute_script(
            f"if object_id(N'{SCHEMA}.{name.replace(chr(39), chr(39) * 2)}', N'U') "
            f"is not null drop table [{SCHEMA}].[{name}];"
        )
    executor.execute_script(
        f"create table [{SCHEMA}].[Narrow] "
        "([Column00000] varchar(10) null, [Note] varchar(10) null);"
    )

    body = f"select * from [{SCHEMA}].[Narrow];"
    document = read_source_document(
        f"{SCHEMA}.O'Brien.sql",
        (
            f"/*\nTable ID: {SCHEMA}.O'Brien\n\nDescription: Named for a person.\n\n"
            "Lineage: The sales system.\n\nPrimary key: Column00000\n"
            "\nIdentity: Brien key\n*/\n" + body + "\n"
        ).encode("utf-8"),
        WAREHOUSE,
    ).document

    executor.execute_script(generate_tsql_table_script(document, body))

    built = executor.query(
        f"select count(*) as n from sys.tables "
        f"where schema_id = schema_id(N'{SCHEMA}') and name = N'O''Brien';"
    )
    assert built[0]["n"] == 1
