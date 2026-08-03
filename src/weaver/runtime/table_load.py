"""Loading a Python-defined Delta table — the mechanics behind ``Table.load()``.

The authored class proposes rows; this owns everything that happens to them.
``read()`` returns ``(upserts, deletes)`` and never touches the target, which is
the invariant the whole runtime rests on: an object that wrote to its own table
would make Weaver's accounting a guess.

The semantics are the ones the generated Spark SQL program implements, and the
rules are *imported* from it rather than restated — the null-and-blank key test,
the null-safe change comparison, the key join. Two copies of "what counts as a
change" would eventually disagree, and the disagreement would show up as rows
that a SQL-defined table updates and a Python-defined one does not.

What differs is only where the rows come from. A Spark SQL table stages a query;
here the authored ``read()`` hands back a ``DataFrame``, which is registered as
a temporary view and staged the same way. From that point the two are the same
load.

``deletes`` is the one thing the SQL form has no equivalent for: a Python table
may name rows to retire explicitly, which is how an incremental source reports a
deletion it saw. Those are removed whatever the incremental policy says, because
they were *stated* rather than inferred from absence.

Nothing here imports PySpark. The session and the frames arrive from the caller
and are used through their ordinary API, and every Delta operation is issued as
SQL text — the same way :mod:`weaver.catalogue.render` reaches Delta.
"""

from __future__ import annotations

from ..declaration.spark_load import (
    COLUMN_MAPPING,
    REJECTION_REASON,
    blank_key_predicate,
    changed_predicate,
    delta_audit_names,
    key_join,
    live_delete_literal,
)
from ..errors import LoadError
from .load_contract import LoadContract
from .load_result import LoadResult

#: The rank a duplicate key gets while staging, matching the generated program's.
RANK_COLUMN = "__weaver_pk_row_number"

INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"


def load_table(
    spark,
    *,
    contract: LoadContract,
    target: str,
    upserts,
    deletes=None,
    fault_tolerant: bool = False,
) -> LoadResult:
    """Load one Delta table from the rows its object proposed.

    ``target`` is the qualified physical name the session must use — resolved by
    the caller, never inferred here, because a load that guessed its destination
    from the attached Lakehouse would write to the control plane.
    """

    columns = _business_columns(spark, target)
    staged = _stage(spark, upserts, contract, columns)
    rows_read = staged.count()

    if contract.replaces_wholesale:
        return _full_replace(spark, target, staged, columns, rows_read)

    rejects = staged.filter(_rejected_expression(contract))
    rows_rejected = rejects.count()
    valid = staged.filter(f"NOT ({_rejected_expression(contract)})")

    if rows_rejected and not fault_tolerant:
        # Nothing has been written, so refusing is a decision not to start. The
        # rejected rows are materialised anyway: without them the caller is told
        # a count and left to work out which rows it meant.
        _write_rejects(spark, target, rejects, contract)
        return LoadResult.failure(INTOLERANT_MESSAGE, rows_read=rows_read,
                                  rows_rejected=rows_rejected)
    if rows_rejected:
        _write_rejects(spark, target, rejects, contract)

    view = _register(spark, valid, target, "valid")
    counts = _measure(spark, target, view, contract)
    _merge(spark, target, view, contract, columns)

    rows_deleted = _apply_deletes(spark, target, view, contract, deletes)

    result = LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=counts["inserted"],
        rows_updated=counts["updated"],
        rows_deleted=rows_deleted,
        rows_rejected=rows_rejected,
    )
    if rows_rejected:
        return result.rejected(f"{rows_rejected} {TOLERATED_MESSAGE}")
    return result


# --- staging -----------------------------------------------------------------


def _stage(spark, upserts, contract: LoadContract, columns):
    """The proposed rows, validated and ranked.

    Ranking here rather than later is what makes a duplicate key identifiable at
    all: the second row of a key is only "the second" relative to an ordering,
    so the ordering has to be established once and carried.
    """

    if upserts is None:
        raise LoadError(
            "read() returned no upserts frame — return (upserts, deletes), using "
            "self.empty_dataframe() when there is nothing to load"
        )
    _require_columns(upserts, columns, contract)
    if not contract.primary_key:
        return upserts
    partition = ", ".join(f"`{c}`" for c in contract.primary_key)
    from_view = _register(spark, upserts, "weaver_staged", "raw")
    return spark.sql(
        f"SELECT s.*, row_number() OVER ("
        f" PARTITION BY {partition} ORDER BY (SELECT NULL)) AS `{RANK_COLUMN}`"
        f" FROM {from_view} AS s"
    )


def _require_columns(frame, columns, contract: LoadContract) -> None:
    """Every column the target needs must be present, by exact name.

    Checked before anything is written, and by name rather than by position: a
    frame whose columns happen to line up today would silently load the wrong
    values the day an author reorders a select.
    """

    produced = set(frame.columns)
    missing = [name for name in columns if name not in produced]
    if missing:
        raise LoadError(
            f"{contract.qualified}: read() did not produce "
            f"{', '.join(repr(name) for name in missing)} — the upserts frame "
            "must carry every column the table declares, by exact name"
        )


def _business_columns(spark, target: str) -> tuple[str, ...]:
    """The target's own columns, less the audit ones the load supplies itself.

    Read from the table rather than from the declaration, for the reason the
    Warehouse installer reads sys.columns: the physical table is what is being
    written to, and a declaration that had drifted from it would produce a merge
    naming a column that is not there.
    """

    audit = set(delta_audit_names())
    return tuple(
        field.name
        for field in spark.table(target).schema.fields
        if field.name not in audit
    )


def _rejected_expression(contract: LoadContract) -> str:
    """Unqualified, because it filters a frame rather than joining relations."""

    blank = blank_key_predicate(contract.primary_key, alias="")
    return f"({blank} OR `{RANK_COLUMN}` > 1)"


def _register(spark, frame, target: str, role: str) -> str:
    name = "weaver_" + role + "_" + target.replace(".", "_").replace("`", "").replace(" ", "_")
    frame.createOrReplaceTempView(name)
    return name


# --- the writes --------------------------------------------------------------


def _full_replace(spark, target: str, staged, columns, rows_read: int) -> LoadResult:
    """No key, so no match and no update: the target's contents become these.

    The delete is counted before the insert because afterwards there is nothing
    left to count — the rows it removed are gone.
    """

    rows_deleted = spark.table(target).count()
    view = _register(spark, staged, target, "valid")
    audit = delta_audit_names()
    named = ", ".join(f"`{c}`" for c in columns)
    spark.sql(f"DELETE FROM {target}")
    spark.sql(
        f"INSERT INTO {target} ({named}, {', '.join(f'`{a}`' for a in audit)})\n"
        f"SELECT {named}, current_timestamp(), current_timestamp(), "
        f"{live_delete_literal()} FROM {view}"
    )
    return LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=rows_read,
        rows_deleted=rows_deleted,
    )


def _measure(spark, target: str, view: str, contract: LoadContract) -> dict:
    """What the merge is about to do, counted before it does it.

    A Delta merge reports its own metrics, but only through the table history —
    reading them back would be a second round trip against state a concurrent
    write could already have moved on.
    """

    join = key_join("v", "t", contract.primary_key)
    changed = changed_predicate("v", "t", contract)
    inserted = spark.sql(
        f"SELECT count(*) AS n FROM {view} AS v "
        f"WHERE NOT EXISTS (SELECT 1 FROM {target} AS t WHERE {join})"
    ).collect()[0]["n"]
    updated = spark.sql(
        f"SELECT count(*) AS n FROM {view} AS v "
        f"JOIN {target} AS t ON {join} WHERE {changed}"
    ).collect()[0]["n"]
    return {"inserted": int(inserted), "updated": int(updated)}


def _merge(spark, target: str, view: str, contract: LoadContract, columns) -> None:
    """Insert new rows, update only those that actually differ.

    Updating every matched row would be simpler and wrong: it would rewrite the
    update timestamp of rows nothing changed, so "when did this row last change"
    would come to mean "when was this table last loaded".
    """

    audit = delta_audit_names()
    join = key_join("s", "t", contract.primary_key)
    changed = changed_predicate("s", "t", contract)
    sets = [
        f"t.`{c}` = s.`{c}`" for c in columns if c not in contract.primary_key
    ] + [
        f"t.`{audit[1]}` = current_timestamp()",
        f"t.`{audit[2]}` = {live_delete_literal()}",
    ]
    named = ", ".join(f"`{c}`" for c in columns)
    values = ", ".join(f"s.`{c}`" for c in columns)
    spark.sql(
        f"MERGE INTO {target} AS t USING {view} AS s ON {join}\n"
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {', '.join(sets)}\n"
        f"WHEN NOT MATCHED THEN INSERT "
        f"({named}, {', '.join(f'`{a}`' for a in audit)})\n"
        f"VALUES ({values}, current_timestamp(), current_timestamp(), "
        f"{live_delete_literal()})"
    )


def _apply_deletes(spark, target: str, view: str, contract, deletes) -> int:
    """Rows the object named, and — unless incremental — rows it stopped naming.

    The two are different claims. An explicit delete is the object *stating* that
    a row is gone, and is honoured whatever the policy; absence from the source
    only means retirement when the source is the whole truth, which is exactly
    what a non-incremental declaration says it is.
    """

    removed = 0
    if deletes is not None and _has_rows(deletes):
        removed += _delete_by_key(spark, target, deletes, contract, "explicit")
    if contract.deletes_absent_rows:
        join = key_join("v", "t", contract.primary_key)
        keys = ", ".join(f"t.`{c}`" for c in contract.primary_key)
        absent = spark.sql(
            f"SELECT {keys} FROM {target} AS t "
            f"WHERE NOT EXISTS (SELECT 1 FROM {view} AS v WHERE {join})"
        )
        removed += _delete_by_key(spark, target, absent, contract, "absent")
    return removed


def _delete_by_key(spark, target: str, frame, contract, role: str) -> int:
    """Delete the rows these keys name, through a merge rather than a DELETE.

    Delta refuses a subquery in ``DELETE``, so matching against another relation
    has to be a merge. Counting first is deliberate: afterwards the rows are not
    there to count.
    """

    keys = _register(spark, frame, target, role)
    count = int(spark.sql(f"SELECT count(*) AS n FROM {keys}").collect()[0]["n"])
    if not count:
        return 0
    join = key_join("d", "t", contract.primary_key)
    spark.sql(
        f"MERGE INTO {target} AS t USING {keys} AS d ON {join} "
        f"WHEN MATCHED THEN DELETE"
    )
    return count


def _write_rejects(spark, target: str, rejects, contract: LoadContract) -> None:
    """Keep the refused rows, with the reason, beside the table they were for.

    A count alone tells a caller that something went wrong and nothing about
    what, which is the state this exists to prevent — so the reason travels with
    the row rather than being left to be inferred from the key.
    """

    blank = blank_key_predicate(contract.primary_key, alias="")
    reject_table = _reject_table(target)
    view = _register(spark, rejects, target, "reject")
    spark.sql(f"DROP TABLE IF EXISTS {reject_table}")
    spark.sql(
        f"CREATE TABLE {reject_table} USING delta {COLUMN_MAPPING} AS "
        f"SELECT *, CASE WHEN {blank} THEN 'null primary key' "
        f"ELSE 'duplicate primary key' END AS `{REJECTION_REASON}` "
        f"FROM {view}"
    )


def _reject_table(target: str) -> str:
    """``sales.`Customer``` -> ``sales.`Customer_Reject```."""

    head, _, tail = target.rpartition(".")
    name = tail.strip("`")
    return f"{head}.`{name}{REJECT_SUFFIX}`" if head else f"`{name}{REJECT_SUFFIX}`"


REJECT_SUFFIX = "_Reject"


def _has_rows(frame) -> bool:
    return bool(frame.take(1))


__all__ = ["INTOLERANT_MESSAGE", "TOLERATED_MESSAGE", "load_table"]
