"""The load result primitive: one meaning, three transports.

A Warehouse procedure returns a T-SQL result set, a Spark SQL program a
``DataFrame``, a Python object an instance. The claim worth testing is that the
meaning survives all three: the column names are one list, so a generator
cannot spell a column its reader will not find, and a result read back off a row
says what the load did.

The counts are the subtle part. They describe the target, not the source, so
they do not have to add up to ``rows_read``, and a failed load keeps them,
because "failed having written nothing" and "failed having written four hundred
rows" are different things to recover from.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

from weaver.runtime import RESULT_COLUMNS, LoadResult


@weaver_test()
def test_the_result_columns_are_the_dataclass_fields():
    """The generators build their result row from this list.

    If the two could drift, a generated program would project a column the
    reader does not look for, and the mismatch would surface as a load whose
    counts were silently zero rather than as a generation error.

    Every field is a transport column. Caller policy, such as the mode a load was
    asked to run in, belongs to whoever asked and is written by the recorder;
    a field here that no engine reports would weaken exactly this check.
    """

    assert RESULT_COLUMNS == tuple(LoadResult.__dataclass_fields__)


@weaver_test()
def test_a_result_round_trips_through_a_transport_row():
    result = LoadResult(
        succeeded=True,
        rows_read=10,
        rows_inserted=3,
        rows_updated=2,
        rows_deleted=1,
        rows_rejected=0,
    )

    assert LoadResult.from_row(result.as_row()) == result


@weaver_test()
def test_a_row_is_read_back_whatever_the_transport_called_its_types():
    """T-SQL hands back a bit and ints; the reader must not care.

    A `bit` arrives as 0 or 1, counts may arrive as `Decimal`, and an absent
    message as None. Coercion belongs here rather than in three call sites.
    """

    row = {
        "succeeded": 1,
        "rows_read": "10",
        "rows_inserted": 3,
        "rows_updated": 0,
        "rows_deleted": 0,
        "rows_rejected": 2,
        "error_message": None,
        "bookmark_datetime": None,
        "is_static_skip": 0,
    }

    result = LoadResult.from_row(row)

    assert result.succeeded is True
    assert result.rows_read == 10
    assert result.rows_rejected == 2


@weaver_test()
def test_the_reported_bookmark_instant_is_always_aware_utc():
    """Neither transport carries a zone, and both took the instant in UTC.

    A Warehouse procedure reports a ``datetime2`` from ``sysutcdatetime()`` and a
    Python primitive reports ISO text through a Livy payload. A naive value read
    as local time would move a bookmark by hours.
    """

    from datetime import datetime, timezone

    def row(value):
        return {
            "succeeded": 1,
            "rows_read": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "rows_rejected": 0,
            "error_message": None,
            "bookmark_datetime": value,
            "is_static_skip": 0,
        }

    expected = datetime(2026, 8, 22, 4, 5, 6, tzinfo=timezone.utc)

    assert LoadResult.from_row(
        row(datetime(2026, 8, 22, 4, 5, 6))
    ).bookmark_datetime == (expected)
    assert LoadResult.from_row(row("2026-08-22T04:05:06")).bookmark_datetime == expected
    assert (
        LoadResult.from_row(row("2026-08-22T04:05:06+00:00")).bookmark_datetime
        == expected
    )
    assert LoadResult.from_row(row(None)).bookmark_datetime is None


@weaver_test()
def test_a_failure_keeps_what_the_load_managed_before_it_failed():
    result = LoadResult.failure("write failed", rows_read=100, rows_inserted=40)

    assert result.succeeded is False
    assert result.error_message == "write failed"
    assert (result.rows_read, result.rows_inserted) == (100, 40)


@weaver_test()
def test_a_load_that_rejected_rows_did_not_succeed():
    """Tolerating rejects changes what is written, never what is reported.

    A caller that only checked for an exception would call a fault-tolerant load
    with rejects a clean load. The rows did not arrive, so it is not one.
    """

    tolerated = LoadResult(
        succeeded=True, rows_read=10, rows_inserted=8, rows_rejected=2
    )

    reported = tolerated.rejected("2 rows rejected")

    assert reported.succeeded is False
    assert reported.rows_inserted == 8
    assert reported.rows_rejected == 2
