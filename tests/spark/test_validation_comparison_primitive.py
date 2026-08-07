"""What a Test means, proved against a real Spark session.

The comparison is set-based and its counting is physical: a changed entity is
two discrepancy rows, one from each side. The primary key correlates those two
rows and changes nothing about how many there are — which is the property that
lets the same Test be authored in Python, compiled from Spark SQL or rendered as
a Warehouse procedure and still mean one thing.
"""

from __future__ import annotations

import pytest

from weaver.errors import ValidationError
from weaver.runtime.test_compare import ACTUAL, EXPECTED, compare

pytestmark = pytest.mark.spark

COLUMNS = "OrderId int, Amount int"


def frame(spark, rows, columns: str = COLUMNS):
    return spark.createDataFrame(rows, columns)


def diagnostics(result) -> list[tuple]:
    """The diagnostic rows, ordered so a comparison reads the same every run."""

    return sorted(
        (row["_weaver_side"], row["_weaver_sk"], row["OrderId"], row["Amount"])
        for row in result.collect()
    )


# --- the symmetric difference ----------------------------------------------


def test_identical_sides_produce_no_rows(spark):
    rows = [(1, 100), (2, 200)]
    result = compare(frame(spark, rows), frame(spark, rows), primary_key=("OrderId",))

    assert result.count() == 0
    assert result.columns == ["_weaver_side", "_weaver_sk", "OrderId", "Amount"]


def test_a_row_only_expected_is_missing(spark):
    result = compare(
        frame(spark, [(1, 100), (2, 200)]),
        frame(spark, [(1, 100)]),
        primary_key=("OrderId",),
    )

    assert [(side, order) for side, _sk, order, _amount in diagnostics(result)] == [
        (EXPECTED, 2)
    ]


def test_a_row_only_actual_is_unexpected(spark):
    result = compare(
        frame(spark, [(1, 100)]),
        frame(spark, [(1, 100), (3, 300)]),
        primary_key=("OrderId",),
    )

    assert [(side, order) for side, _sk, order, _amount in diagnostics(result)] == [
        (ACTUAL, 3)
    ]


def test_a_changed_row_is_two_rows_sharing_one_key(spark):
    """One changed entity, two discrepancy rows — deliberately, and paired."""

    result = compare(
        frame(spark, [(1, 100)]),
        frame(spark, [(1, 110)]),
        primary_key=("OrderId",),
    )

    rows = diagnostics(result)
    assert [(side, amount) for side, _sk, _order, amount in rows] == [
        (ACTUAL, 110),
        (EXPECTED, 100),
    ]
    assert len({sk for _side, sk, _order, _amount in rows}) == 1


def test_the_failure_count_is_the_raw_row_count(spark):
    """missing + unexpected, with no logical row model laid over the top."""

    result = compare(
        frame(spark, [(1, 100), (2, 200)]),
        frame(spark, [(1, 110), (3, 300)]),
        primary_key=("OrderId",),
    )

    assert result.count() == 4
    assert result.where("_weaver_side = 'expected'").count() == 2
    assert result.where("_weaver_side = 'actual'").count() == 2


def test_the_comparison_is_set_semantics(spark):
    """A repeated row is the same set, so there is nothing to report."""

    result = compare(
        frame(spark, [(1, 100), (1, 100)]),
        frame(spark, [(1, 100)]),
    )

    assert result.count() == 0


# --- correlation ------------------------------------------------------------


def test_missing_and_unexpected_rows_get_keys_of_their_own(spark):
    result = compare(
        frame(spark, [(1, 100), (2, 200)]),
        frame(spark, [(1, 110), (3, 300)]),
        primary_key=("OrderId",),
    )

    by_order = {order: sk for _side, sk, order, _amount in diagnostics(result)}
    assert len(set(by_order.values())) == 3
    paired = [sk for _side, sk, order, _amount in diagnostics(result) if order == 1]
    assert len(paired) == 2 and len(set(paired)) == 1


def test_a_composite_key_pairs_on_every_column(spark):
    columns = "OrderId int, LineNo int, Amount int"
    result = compare(
        frame(spark, [(1, 1, 100), (1, 2, 200)], columns),
        frame(spark, [(1, 1, 111), (1, 2, 200)], columns),
        primary_key=("OrderId", "LineNo"),
    )

    rows = [(row["_weaver_side"], row["_weaver_sk"], row["LineNo"]) for row in result.collect()]
    assert len(rows) == 2
    assert {line for _side, _sk, line in rows} == {1}
    assert len({sk for _side, sk, _line in rows}) == 1


def test_without_a_key_no_two_rows_are_paired(spark):
    """Nothing to pair by, so a reader can never read two rows as one entity."""

    result = compare(
        frame(spark, [(1, 100), (2, 200)]),
        frame(spark, [(1, 110), (2, 200)]),
    )

    keys = [row["_weaver_sk"] for row in result.collect()]
    assert len(keys) == 2
    assert len(set(keys)) == 2


def test_declaring_a_key_does_not_change_what_is_counted(spark):
    expected, actual = [(1, 100), (2, 200)], [(1, 110), (3, 300)]
    keyed = compare(frame(spark, expected), frame(spark, actual), primary_key=("OrderId",))
    unkeyed = compare(frame(spark, expected), frame(spark, actual))

    assert keyed.count() == unkeyed.count()


# --- execution failures, which are not evidence -----------------------------


def test_a_duplicate_key_is_a_contract_failure(spark):
    with pytest.raises(ValidationError, match="repeats on the expected side"):
        compare(
            frame(spark, [(1, 100), (1, 200)]),
            frame(spark, [(1, 100)]),
            primary_key=("OrderId",),
        )


def test_a_duplicate_key_on_the_actual_side_is_named_as_such(spark):
    with pytest.raises(ValidationError, match="repeats on the actual side"):
        compare(
            frame(spark, [(1, 100)]),
            frame(spark, [(1, 100), (1, 200)]),
            primary_key=("OrderId",),
        )


def test_a_null_key_cannot_identify_a_row(spark):
    with pytest.raises(ValidationError, match="null or blank"):
        compare(
            frame(spark, [(None, 100)]),
            frame(spark, [(1, 100)]),
            primary_key=("OrderId",),
        )


def test_a_blank_key_is_refused_with_null(spark):
    columns = "OrderId string, Amount int"
    with pytest.raises(ValidationError, match="null or blank"):
        compare(
            frame(spark, [("  ", 100)], columns),
            frame(spark, [("1", 100)], columns),
            primary_key=("OrderId",),
        )


def test_different_column_counts_are_refused_before_the_engine_sees_them(spark):
    with pytest.raises(ValidationError, match="same shape"):
        compare(
            frame(spark, [(1, 100)]),
            frame(spark, [(1,)], "OrderId int"),
        )


def test_the_same_columns_in_a_different_order_are_refused(spark):
    """Comparing them positionally would report every row as a discrepancy."""

    with pytest.raises(ValidationError, match="different order"):
        compare(
            frame(spark, [(1, 100)]),
            frame(spark, [(100, 1)], "Amount int, OrderId int"),
        )


def test_a_key_absent_from_a_side_is_refused(spark):
    with pytest.raises(ValidationError, match="Primary key names CustomerId"):
        compare(
            frame(spark, [(1, 100)]),
            frame(spark, [(1, 100)]),
            primary_key=("CustomerId",),
        )


def test_a_reserved_column_name_is_refused(spark):
    with pytest.raises(ValidationError, match="reserved"):
        compare(
            frame(spark, [(1, "expected")], "OrderId int, _weaver_side string"),
            frame(spark, [(1, "expected")], "OrderId int, _weaver_side string"),
        )


def test_the_name_of_the_test_is_in_the_message(spark):
    with pytest.raises(ValidationError, match="Sales__OrdersReconcile"):
        compare(
            frame(spark, [(1, 100)]),
            frame(spark, [(1,)], "OrderId int"),
            what="Sales__OrdersReconcile",
        )
