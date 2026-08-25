"""Compiling a ``.sql`` table into the module a build deploys.

Three things have to survive the compilation exactly, and each is a defect
waiting to happen if it does not: the authored metadata header, because it is
the contract the primitive reads at run time; the authored SQL, because it is
what the primitive executes; and the class name, because it is what the
orchestrator imports by.

The rest of these assert what the module is. That it is importable Python,
that it announces itself as generated, and that it carries object tokens rather
than resolved names. Nothing here needs Spark: the module is text until someone
runs it.
"""

from __future__ import annotations

import ast
import itertools

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.metadata import extract_sql_metadata_and_body
from weaver.declaration.model import LAKEHOUSE
from weaver.declaration.spark_sql_module import (
    GENERATED_MODULE_MARKER,
    class_name,
    deployed_module_name,
    python_string,
)
from weaver.spark import FabricSparkTarget

SALES = FabricSparkTarget(workspace="Demo", lakehouse="Sales_LH")

SOURCE = """/*
Table ID: Sales.OrderSummary

Description: Order totals by customer.

Lineage: The sales system.

Dependencies:
  - Sales.Order

Primary key: Customer id

Schema:
  Customer id: string
  Total amount: decimal(18,2)
*/
create or replace temporary view recent as
select * from Sales.Order where `Order date` > current_date() - 30;

select `Customer id`, sum(`Amount`) as `Total amount`
  from recent group by `Customer id`;
"""


def _document(source: str = SOURCE):
    return read_source_document(
        "Sales.OrderSummary.sql", source.encode("utf-8"), LAKEHOUSE
    )


def _module(source: str = SOURCE) -> str:
    return _document(source).create_load(destination=SALES).payload.decode("utf-8")


def _namespace(source: str = SOURCE) -> dict:
    """Execute the generated module the way an import would."""

    namespace: dict = {}
    exec(compile(_module(source), "Sales__OrderSummary.py", "exec"), namespace)
    return namespace


# --- what the module is -------------------------------------------------------


@weaver_test()
def test_the_generated_module_is_importable_python():
    ast.parse(_module())


@weaver_test()
def test_the_module_says_it_is_generated_on_its_first_line():
    assert _module().splitlines()[0].startswith(GENERATED_MODULE_MARKER)


@weaver_test()
def test_the_marker_is_a_comment_so_the_docstring_is_still_the_docstring():
    module = ast.parse(_module())

    assert ast.get_docstring(module) is not None


@weaver_test()
def test_the_module_defines_the_class_the_orchestrator_will_import():
    namespace = _namespace()

    assert "Sales__OrderSummary" in namespace


@weaver_test()
def test_the_class_derives_from_the_public_spark_sql_base():
    from weaver import SparkSqlTable

    assert issubclass(_namespace()["Sales__OrderSummary"], SparkSqlTable)


@weaver_test()
def test_the_class_and_file_names_are_the_deployed_module_convention():
    object_id = _document().object_id

    assert class_name(object_id) == "Sales__OrderSummary"
    assert deployed_module_name(object_id) == "Sales__OrderSummary.py"


# --- what survives compilation ------------------------------------------------


@weaver_test()
def test_the_authored_header_becomes_the_docstring_exactly():
    header, _body = extract_sql_metadata_and_body(SOURCE)

    assert ast.get_docstring(ast.parse(_module()), clean=False) == header


@weaver_test()
def test_the_docstring_still_parses_as_the_contract_it_was():
    from weaver.declaration.metadata import SPARK_SQL, parse_document

    document = parse_document(
        ast.get_docstring(ast.parse(_module())), language=SPARK_SQL
    )

    assert document.qualified == "Sales.OrderSummary"
    assert document.primary_key == ("Customer id",)


@weaver_test()
def test_the_authored_sql_survives_byte_for_byte_but_addressed():
    from weaver.declaration.spark_sql_module import addressed

    _header, body = extract_sql_metadata_and_body(SOURCE)

    assert _namespace()["Sales__OrderSummary"].sql == addressed(body.strip(), SALES)


@weaver_test()
def test_the_module_names_the_lakehouse_it_reads():
    """Addressed when the bundle is generated, like every other payload.

    A deployed module is opened by whoever is debugging a load, and what it
    reads and writes should be readable there rather than resolved later.
    """

    sql = _namespace()["Sales__OrderSummary"].sql

    assert "`Sales_LH`.`Sales`.`Order`" in sql
    assert "from Sales.Order where" not in sql


@weaver_test()
def test_the_temporary_view_is_not_addressed_as_a_managed_object():
    sql = _namespace()["Sales__OrderSummary"].sql

    assert "from recent group by" in sql


@weaver_test()
def test_generation_is_deterministic():
    assert _module() == _module()


# --- the one encoder ----------------------------------------------------------


HOSTILE = [
    "",
    "select 1;",
    'select "quoted";',
    "select 'quoted';",
    'a """ b',
    'ends with a quote"',
    'ends with two""',
    'ends with three"""',
    "back\\slash",
    "trailing backslash\\",
    'four """" quotes',
    "\n\n",
    "no newline at end",
]


@pytest.mark.parametrize("text", HOSTILE)
@weaver_test()
def test_the_encoder_round_trips_text_designed_to_break_it(text):
    assert eval(python_string(text)) == text  # noqa: S307 - the value under test


@weaver_test()
def test_the_encoder_round_trips_every_short_string_over_a_hostile_alphabet():
    """Exhaustive rather than representative.

    The failure this guards against is a specific character sequence closing
    the literal early, and a handful of examples is exactly the wrong shape of
    evidence for that. Every string up to four characters over the three
    characters that can do it is cheap and settles the question.
    """

    for length in range(5):
        for characters in itertools.product('"\\\nq', repeat=length):
            text = "".join(characters)
            assert eval(python_string(text)) == text  # noqa: S307


@weaver_test()
def test_a_body_containing_a_triple_quote_still_produces_a_valid_module():
    source = SOURCE.replace(
        "select * from Sales.Order where `Order date` > current_date() - 30;",
        'select * from Sales.Order where `Note` <> \'"""\';',
    )

    assert '"""' in _namespace(source)["Sales__OrderSummary"].sql
