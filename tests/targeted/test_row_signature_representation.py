"""The row signature: what goes into it, and how a value is spelled.

A keyed load decides whether a row changed by comparing one stored digest with
one computed digest, so everything about correctness here is about the payload
the digest is taken over. Two properties matter and neither is obvious from
reading a hash call:

What is hashed must be the comparison columns and only those, or a load reports
a change the declaration excluded, or misses one it did not.

How each value is written must be unambiguous. A concatenation cannot
distinguish a null from an empty string, or two values from one value containing
whatever separator was chosen, and both mistakes are silent: the row never
updates.

The two engines are asserted side by side because they are required to agree on
those properties and required not to agree on the bytes. A Warehouse hashes to
``varbinary(32)``; Spark's ``sha2`` returns hex text. A signature is only ever
compared with another signature from the same table.

That the digests behave. That a changed row really does produce a different one,
is proved by running a load (``tests/fabric/test_warehouse_load_primitive.py``
and ``tests/fabric/test_delta_table_load_primitive.py``).
"""

from __future__ import annotations

from support.generated_load import procedure
from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.metadata import PYTHON, SPARK_SQL, parse_document
from weaver.declaration.model import WAREHOUSE, WeaverItemId
from weaver.runtime.delta_sql import NULL_MARKER, row_signature
from weaver.runtime.load_contract import LoadContract

HEADER = """Table ID: Sales.Customer

Description: Customers.

Lineage: The sales system.

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string
  Amount: decimal(18,2)
  Opened: date
  Seen: timestamp
  Active: boolean
  Token: binary
"""

TYPES = {
    "Customer name": "string",
    "Amount": "decimal(18,2)",
    "Opened": "date",
    "Seen": "timestamp",
    "Active": "boolean",
    "Token": "binary",
}


def _contract(header: str = HEADER) -> LoadContract:
    return LoadContract.from_document(parse_document(header, language=PYTHON))


def _delta(header: str = HEADER) -> str:
    contract = _contract(header)
    return row_signature("s", contract.comparison_columns, TYPES)


WAREHOUSE_TABLE = (
    f"""/*
{HEADER}*/
select 1
""".replace("Customer id: string", "Customer id: varchar(50)")
    .replace("Customer name: string", "Customer name: varchar(200)")
    .replace("boolean", "bit")
    .replace("binary", "varbinary(32)")
    .replace("timestamp", "datetime2(6)")
)


def _warehouse_installer(source: str = WAREHOUSE_TABLE) -> str:
    """The installer, which is where a Warehouse payload is assembled.

    The generator leaves a placeholder: the payload has to name each column's
    physical type, and an inferred table's types are settled by the build.
    """

    document = read_source_document(
        "Sales.Customer.sql", source.encode("utf-8"), WAREHOUSE
    )
    return document.create_load(
        item=WeaverItemId("Warehouse", "Reporting")
    ).payload.decode()


# --- what is hashed ----------------------------------------------------------


@weaver_test()
def test_the_signature_covers_the_comparison_columns(monkeypatch=None):
    """Which, undeclared, is every business column that is not the key."""

    contract = _contract()

    assert contract.comparison_columns == (
        "Customer name",
        "Amount",
        "Opened",
        "Seen",
        "Active",
        "Token",
    )
    delta = _delta()
    for column in contract.comparison_columns:
        assert f"`{column}`" in delta


@weaver_test()
def test_the_key_is_not_hashed():
    """A matched row has equal keys by definition, so it says nothing."""

    assert "`Customer id`" not in _delta()
    assert "and lower(c.name) not in (N'customer id')" in _warehouse_installer()


@weaver_test()
def test_weavers_own_columns_are_not_hashed():
    """The audit columns move on every write, and the signature is the answer."""

    delta = _delta()
    installer = _warehouse_installer()

    for column in ("row_insert_datetime", "row_update_datetime", "row_signature"):
        assert f"`{column}`" not in delta
    for column in ("Row insert datetime", "Row update datetime", "Row signature"):
        assert f"N'{column}'" in installer  # named in the exclusion list


@weaver_test()
def test_a_declared_comparison_subset_is_what_gets_hashed():
    header = HEADER.replace(
        "Primary key: Customer id",
        "Primary key: Customer id\n\nComparison columns: Amount, Opened",
    )
    delta = _delta(header)

    assert "`Amount`" in delta
    assert "`Opened`" in delta
    assert "`Customer name`" not in delta
    assert "`Active`" not in delta


@weaver_test()
def test_an_inferred_table_takes_its_comparison_set_from_the_target():
    """A Spark SQL table may infer its schema, and then the contract has no list.

    Left at that, every row would sign identically and no change would ever be
    detected. The runtime falls back to the target's own business columns, which is
    the rule the Warehouse installer applies against ``sys.columns``.
    """

    from weaver.runtime.table_load import _comparison_columns

    inferred = LoadContract.from_document(
        parse_document(
            "Table ID: Sales.Customer\n\nDescription: x\n\nLineage: y\n\n"
            "Dependencies: []\n\nPrimary key: Customer id\n",
            language=SPARK_SQL,
        )
    )
    physical = ("Customer id", "Customer name", "Amount")

    assert inferred.comparison_columns == ()
    assert _comparison_columns(inferred, physical) == ("Customer name", "Amount")


@weaver_test()
def test_a_declared_comparison_set_is_not_overridden_by_the_target():
    from weaver.runtime.table_load import _comparison_columns

    contract = _contract(
        HEADER.replace(
            "Primary key: Customer id",
            "Primary key: Customer id\n\nComparison columns: Amount",
        )
    )

    assert _comparison_columns(
        contract, ("Customer id", "Customer name", "Amount")
    ) == ("Amount",)


@weaver_test()
def test_a_table_with_nothing_to_compare_still_signs():
    """Every row then signs identically, which is what "nothing to compare" means.

    An expression that collapsed to nothing would be a syntax error, and one that
    produced null would make every row look unchanged for good.
    """

    delta = row_signature("s", (), {})

    assert delta == "sha2('', 256)"


# --- how a value is written --------------------------------------------------


@weaver_test()
def test_a_null_is_written_as_a_marker_no_present_value_can_produce():
    """A present value begins with its length, so it always begins with a digit."""

    delta = _delta()
    installer = _warehouse_installer()

    assert f"IS NULL THEN '{NULL_MARKER}'" in delta
    assert f"is null then N''{NULL_MARKER}''" in installer


@weaver_test()
def test_a_present_value_carries_its_own_length():
    """Which is what makes the payload readable back as the values that made it.

    Without it, ``('a|b', 'c')`` and ``('a', 'b|c')`` are the same payload, and
    one row's change into the other would never be noticed.
    """

    delta = _delta()
    installer = _warehouse_installer()

    assert "CAST(length(" in delta
    assert "':'" in delta
    assert "datalength(" in installer
    assert "N'':''" in installer


@weaver_test()
def test_the_columns_are_written_in_a_settled_order():
    """The payload is ordered, so two columns cannot swap contributions."""

    delta = _delta()
    positions = [
        delta.index(f"`{column}`") for column in _contract().comparison_columns
    ]

    assert positions == sorted(positions)
    assert "within group (order by column_id)" in _warehouse_installer()


@weaver_test()
def test_a_type_whose_default_text_is_not_stable_is_spelled_explicitly():
    """A timestamp's text moves with the session time zone; a signature must not.

    Same for a boolean and a binary, where the text depends on the cast the engine
    happens to choose rather than on the value.
    """

    delta = _delta()

    assert "unix_micros(s.`Seen`)" in delta
    assert "CAST(CAST(s.`Active` AS INT) AS STRING)" in delta
    assert "hex(s.`Token`)" in delta


@weaver_test()
def test_the_warehouse_names_a_style_for_every_ambiguous_type():
    """Assembled at install time against ``sys.types``, so an inferred table too."""

    installer = _warehouse_installer()

    assert "when 'date' then N'convert(varchar(10), __COLUMN__, 23)'" in installer
    assert "when 'datetime2' then N'convert(varchar(27), __COLUMN__, 126)'" in installer
    assert "when 'bit' then N'cast(cast(__COLUMN__ as int) as varchar(1))'" in installer
    assert "when 'varbinary' then N'convert(varchar(max), __COLUMN__, 2)'" in installer


@weaver_test()
def test_the_warehouse_hash_is_narrowed_to_the_column_it_is_stored_in():
    """``varbinary(32)`` holds it, so the digest is converted to it explicitly.

    Left to Fabric's inference the expression is a broad ``varbinary``, and what
    reaches the column then depends on the conversion rather than on the digest.
    """

    assert "convert(varbinary(32), hashbytes('SHA2_256'" in procedure(
        _warehouse_installer()
    )


@weaver_test()
def test_the_two_engines_agree_on_the_payload_and_not_on_the_bytes():
    """Required to differ: Spark's sha2 returns hex text, hashbytes returns bytes.

    Within one table a signature is only ever compared with another from the same
    table, so what has to match across engines is which columns are covered and
    how each value is written, not the digest.
    """

    delta = _delta()
    warehouse = procedure(_warehouse_installer())

    assert delta.startswith("sha2(")
    assert "hashbytes('SHA2_256'" in warehouse
    assert "sha2" not in warehouse.lower().replace("sha2_256", "")
    assert "hashbytes" not in delta


@weaver_test()
def test_the_expression_is_the_same_every_time_it_is_rendered():
    assert _delta() == _delta()
    assert _warehouse_installer() == _warehouse_installer()
