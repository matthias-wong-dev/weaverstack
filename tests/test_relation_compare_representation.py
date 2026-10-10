"""Row comparison follows the Test contract that Spark comparison enforces."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from support.weaver_test import weaver_test

from weaver.errors import ValidationError
from weaver.runtime.relation_compare import Relation, compare_rows
from weaver.semantic_validation import dax_column, dax_relation

COLUMNS = ("Month", "Revenue")
EXPECTED = Relation(COLUMNS, ((1, Decimal("10.00")), (2, Decimal("20.00"))))


def sides(rows):
    return sorted((r["_weaver_side"], r["_weaver_sk"], r["Month"]) for r in rows)


@pytest.mark.parametrize(
    "actual, key, found",
    [
        (((1, 10.0), (2, 20)), (), []),
        (((1, 10.0),), ("Month",), [("expected", 1, 2)]),
        (
            ((1, 10.0), (2, 20), (3, 30)),
            ("Month",),
            [("actual", 1, 3)],
        ),
        (
            ((1, 10.0), (2, 21)),
            ("Month",),
            [("actual", 1, 2), ("expected", 1, 2)],
        ),
        (
            ((1, 10.0), (2, 21)),
            (),
            [("actual", 2, 2), ("expected", 1, 2)],
        ),
        # Sets, not bags: a repeated row is one row.
        (((1, 10.0), (1, 10.0), (2, 20)), (), []),
    ],
)
@weaver_test()
def test_symmetric_difference_with_key_pairing(actual, key, found):
    assert (
        sides(compare_rows(EXPECTED, Relation(COLUMNS, actual), primary_key=key))
        == found
    )


@pytest.mark.parametrize(
    "actual, key, message",
    [
        (Relation(("Month",), ((1,),)), (), "expected has 2 column"),
        (
            Relation(("Revenue", "Month"), ((10, 1),)),
            (),
            "same columns in a different order",
        ),
        (Relation(COLUMNS, ((None, 10),)), ("Month",), "null or blank on the actual"),
        (Relation(COLUMNS, ((" ", 10),)), ("Month",), "null or blank on the actual"),
        (
            Relation(COLUMNS, ((1, 10), (1, 11))),
            ("Month",),
            "repeats on the actual side",
        ),
        (Relation(COLUMNS, ((1, 10),)), ("Year",), "which expected does not return"),
        (
            Relation(("_weaver_side", "Revenue"), ((1, 10),)),
            (),
            "reserved for diagnostics",
        ),
    ],
)
@weaver_test()
def test_incomparable_shapes_and_keys_are_refused(actual, key, message):
    with pytest.raises(ValidationError, match=message):
        compare_rows(EXPECTED, actual, primary_key=key)


@pytest.mark.parametrize("reserved", ["_weaver_side", "_weaver_sk"])
@weaver_test()
def test_reserved_columns_are_refused_with_both_row_sets_empty(reserved):
    with pytest.raises(ValidationError, match="reserved for diagnostics"):
        compare_rows(Relation((reserved,), ()), Relation((reserved,), ()))


@weaver_test()
def test_transport_forms_of_one_value_are_equal():
    expected = Relation(
        ("Day", "At", "Flag", "Amount"),
        ((date(2026, 7, 1), datetime(2026, 7, 1, 9, 30), True, Decimal("0.10")),),
    )
    actual = Relation(
        ("Day", "At", "Flag", "Amount"),
        (("2026-07-01T00:00:00", "2026-07-01T09:30:00Z", True, 0.1),),
    )
    assert compare_rows(expected, actual) == []
    aware = Relation(
        expected.columns,
        (
            (
                date(2026, 7, 1),
                datetime(2026, 7, 1, 9, 30, tzinfo=timezone.utc),
                True,
                0.1,
            ),
        ),
    )
    assert compare_rows(expected, aware) == []
    text = Relation(expected.columns, (("2026-07-01", "x", True, 0.1),))
    assert len(compare_rows(expected, text)) == 2


@pytest.mark.parametrize("key", [(), ("Month",)])
@pytest.mark.parametrize("unknown_side", ["expected", "actual", "both"])
@weaver_test()
def test_empty_transport_results_without_schema_are_refused(key, unknown_side):
    unknown = Relation((), ())
    expected = unknown if unknown_side in {"expected", "both"} else EXPECTED
    actual = unknown if unknown_side in {"actual", "both"} else EXPECTED
    with pytest.raises(ValidationError, match="no column metadata"):
        compare_rows(expected, actual, primary_key=key, what="RevenueReconciles")


@pytest.mark.parametrize("key", [(), ("Month",)])
@weaver_test()
def test_known_compatible_empty_relations_compare_equal(key):
    empty = Relation(COLUMNS, ())
    assert compare_rows(empty, empty, primary_key=key) == []


@pytest.mark.parametrize("populated_side", ["expected", "actual"])
@weaver_test()
def test_one_known_empty_relation_keeps_discrepancy_pairing(populated_side):
    empty = Relation(COLUMNS, ())
    expected, actual = (
        (EXPECTED, empty) if populated_side == "expected" else (empty, EXPECTED)
    )
    assert sides(compare_rows(expected, actual, primary_key=("Month",))) == [
        (populated_side, 1, 1),
        (populated_side, 2, 2),
    ]


@pytest.mark.parametrize("populated_side", ["expected", "actual", "neither"])
@pytest.mark.parametrize(
    "actual_columns, key, message",
    [
        (("Month",), (), "expected has 2 column"),
        (("Revenue", "Month"), (), "same columns in a different order"),
        (COLUMNS, ("Year",), "which expected does not return"),
        (("Year", "Revenue"), ("Month",), "which actual does not return"),
    ],
)
@weaver_test()
def test_empty_relations_obey_shared_shape_and_key_guards(
    populated_side, actual_columns, key, message
):
    expected = Relation(COLUMNS, ((1, 10),) if populated_side == "expected" else ())
    actual = Relation(
        actual_columns,
        (tuple(range(len(actual_columns))),) if populated_side == "actual" else (),
    )
    with pytest.raises(ValidationError, match=message):
        compare_rows(expected, actual, primary_key=key)


@pytest.mark.parametrize(
    "label, column",
    [
        ("Sales[Month]", "Month"),
        ("'Sales Fact'[Month]", "Month"),
        ("'It''s'[A]]B]", "A]B"),
        ("[Revenue]", "Revenue"),
        ("Revenue", "Revenue"),
    ],
)
@weaver_test()
def test_dax_labels_name_their_column(label, column):
    assert dax_column(label) == column


@weaver_test()
def test_dax_columns_that_collide_are_refused():
    with pytest.raises(ValidationError, match="more than one column named Month"):
        dax_relation([{"Sales[Month]": 1, "Date[Month]": 1}], what="Sales.Test")
