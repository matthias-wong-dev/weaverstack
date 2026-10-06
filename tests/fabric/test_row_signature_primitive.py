"""Row signatures over the adversarial corpus, signed by each real engine.

The fast suite pins how a signature is written (``tests/targeted/
test_row_signature_representation.py``) and cannot say what an engine makes of
it. Here each engine signs every case in ``support.signature_corpus``: two
cases must sign equally exactly when their comparison values are equal.

Spark signs through ``row_signature`` with the target's types over source-typed
columns, as ``load_table`` does. The Warehouse signs through its generated load
procedure, installed by hand and executed twice: once to sign the corpus and
once more, unchanged, to show a signature is repeatable.

``tests/fabric/test_row_signature_journey.py`` proves a load acts on them.
"""

from __future__ import annotations

import pytest
from sql_support import (
    PROCEDURE_ITEM,
    drop_load_script,
    forget_installations,
    prepare_hand_installed,
)
from support import signature_corpus as corpus
from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE, WeaverItemId
from weaver.declaration.tsql_load import (
    PROCEDURE_RESULT_PARAMETERS,
    logical_result_row,
)
from weaver.runtime import LoadResult
from weaver.runtime.delta_sql import row_signature

SCHEMA = "DWG"
OBJECT = "SignatureCorpus"
RAW = f"{OBJECT}Raw"
ITEM = WeaverItemId(*PROCEDURE_ITEM)


# --- Spark -------------------------------------------------------------------


@weaver_test(remote=True)
def test_spark_signs_distinct_rows_apart_and_equal_rows_alike(livy_session):
    engine = corpus.LAKEHOUSE
    tracked = corpus.columns(engine)
    signature = row_signature(
        "s",
        tuple(column.name for column in tracked),
        {column.name: column.target(engine) for column in tracked},
    )
    relation = corpus.spark_rows(corpus.cases(engine))
    body = (
        f"spark.sql('''{relation}''').createOrReplaceTempView('signature_corpus')\n"
        "def signed():\n"
        f"    rows = spark.sql('''SELECT `{corpus.KEY}`, {signature} AS signature "
        "FROM signature_corpus AS s''').collect()\n"
        "    return {row[0]: row[1] for row in rows}\n"
        "emit({'first': signed(), 'second': signed()})\n"
    )

    signed = livy_session.run(body).payload

    assert signed["first"] == signed["second"]
    assert set(signed["first"]) == {case.id for case in corpus.cases(engine)}
    assert corpus.collisions(signed["first"], engine) == []
    assert corpus.partition(signed["first"]) == corpus.expected_partition(engine)


# --- Warehouse ---------------------------------------------------------------


def _source() -> str:
    schema = "\n".join(
        f"  {column.name}: {column.tsql}" for column in corpus.columns(WAREHOUSE)
    )
    return f"""/*
Table ID: {SCHEMA}.{OBJECT}

Description: Rows built to break a row signature.

Lineage: The signature corpus.

Primary key: {corpus.KEY}

Schema:
  {corpus.KEY}: varchar(64)
{schema}
*/
select {corpus.tsql_staged_select()} from [{SCHEMA}].[{RAW}]
"""


@pytest.fixture(scope="module")
def warehouse_signatures(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    """The corpus loaded twice by its generated procedure, and what each left."""

    executor = clean_disposable_warehouse.executor
    fabric_initialise_catalogue()
    document = read_source_document(
        f"{SCHEMA}.{OBJECT}.sql", _source().encode("utf-8"), WAREHOUSE
    )
    prepare_hand_installed(executor, SCHEMA, fabric_workspace.catalogue_item.name)
    executor.execute_script(drop_load_script(SCHEMA, OBJECT, also=("Raw",)))
    executor.execute_script(
        f"create table [{SCHEMA}].[{RAW}] ({corpus.tsql_source_columns()});\n"
        f"insert into [{SCHEMA}].[{RAW}] values\n"
        f"{corpus.tsql_rows(corpus.cases(WAREHOUSE))};"
    )
    executor.execute_script(document.create_ddl().content)
    executor.execute_script(document.create_load(item=ITEM).payload.decode("utf-8"))

    def load() -> LoadResult:
        return LoadResult.from_row(
            logical_result_row(
                executor.call_procedure(
                    f"[_].[Load {SCHEMA}.{OBJECT}]",
                    inputs=(("fault_tolerant", 0),),
                    outputs=PROCEDURE_RESULT_PARAMETERS,
                )
            )
        )

    def signatures() -> dict:
        rows = executor.query(
            f"select [{corpus.KEY}] as id, [Row signature] as signature "
            f"from [{SCHEMA}].[{OBJECT}]"
        )
        return {row["id"]: bytes(row["signature"]) for row in rows}

    try:
        first = load()
        first_signed = signatures()
        second = load()
        yield {
            "first": first,
            "first_signed": first_signed,
            "second": second,
            "second_signed": signatures(),
        }
    finally:
        executor.execute_script(drop_load_script(SCHEMA, OBJECT, also=("Raw",)))
        forget_installations(executor)


@weaver_test(remote=True)
def test_a_warehouse_signs_distinct_rows_apart_and_equal_rows_alike(
    warehouse_signatures,
):
    signed = warehouse_signatures["first_signed"]

    assert warehouse_signatures["first"].rows_inserted == len(signed)
    assert set(signed) == {case.id for case in corpus.cases(WAREHOUSE)}
    assert corpus.collisions(signed, WAREHOUSE) == []
    assert corpus.partition(signed) == corpus.expected_partition(WAREHOUSE)


@weaver_test(remote=True)
def test_a_warehouse_signs_an_unchanged_row_the_same_again(warehouse_signatures):
    second = warehouse_signatures["second"]

    assert (second.rows_inserted, second.rows_updated, second.rows_deleted) == (
        0,
        0,
        0,
    )
    assert warehouse_signatures["second_signed"] == warehouse_signatures["first_signed"]
