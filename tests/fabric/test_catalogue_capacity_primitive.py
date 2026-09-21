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
