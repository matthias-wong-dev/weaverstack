"""Loading a Python-defined Delta table — the mechanics behind ``Table.load()``.

The authored class proposes rows; this owns everything that happens to them.
``read()`` returns ``(upserts, deletes)`` and never touches the target, which is
the invariant the whole runtime rests on: an object that wrote to its own table
would make Weaver's accounting a guess.

Ported from ``weaver_runtime.dbrep.runtime.delta_table_load``, whose shape is
worth stating because it is not the obvious one:

**One validation pass, not several.** The reject reason becomes a *column* —
blank key, duplicate key, or null for accepted — and a single ``GROUP BY`` over
it yields the input, accepted and rejected counts together. Counting each
condition separately would walk the staged rows three times and, worse, walk
them at three different moments.

**One merge, not three statements.** Upserts and explicit deletes travel in the
same source relation, distinguished by an operation column, so inserts, updates
and both kinds of delete are one atomic Delta operation. An upsert wins when the
same key is also explicitly deleted.

**The counts come from the merge itself.** Delta reports
``numTargetRowsInserted``/``Updated``/``Deleted`` in its operation metrics, read
back through ``DESCRIBE HISTORY``. Counting by querying before the write would
be a second pass over state a concurrent writer could already have moved.

Two departures from the reference, both forced and both narrow:

*It is written in SQL, not the DataFrame API.* The reference imports ``pyspark``
and ``delta.tables``; ``tests/test_core_boundary.py`` forbids both anywhere in
``weaver``, so the core stays importable without a JVM. Every operation here is
the same operation issued as text — including the metrics, which
``DESCRIBE HISTORY`` exposes without ``DeltaTable``.

*``fault_tolerant`` is Weaver's addition.* The reference always proceeds and
reports rejects afterwards. Here an intolerant run returns before the merge, so
the target is untouched — which is also why this module may safely use
``WHEN NOT MATCHED BY SOURCE`` where the generated Spark program cannot: it
never reaches the merge with an empty source.
"""

from __future__ import annotations

from ..declaration.spark_load import (
    COLUMN_MAPPING,
    blank_key_predicate,
    changed_predicate,
    delta_audit_names,
    key_join,
    live_delete_literal,
)
from ..errors import LoadError
from .load_contract import (
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
    LoadContract,
)
from .load_result import LoadResult

INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"

REJECT_SUFFIX = "_Reject"


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
    _require_columns(upserts, contract, columns)

    if contract.replaces_wholesale:
        return _full_replace(spark, target, upserts, columns)

    validated, reason = _validate(spark, upserts, contract, columns)
    try:
        counts = _reason_counts(spark, validated, reason)
        rows_read = sum(counts.values())
        rows_rejected = rows_read - counts.get(None, 0)

        if rows_rejected:
            _write_rejects(spark, target, validated, reason, columns)
        if rows_rejected and not fault_tolerant:
            # Nothing has been written, so refusing is a decision not to start
            # rather than an unwind — and the reject table is the evidence.
            return LoadResult.failure(
                INTOLERANT_MESSAGE, rows_read=rows_read, rows_rejected=rows_rejected
            )

        accepted = _accepted_view(spark, validated, reason, columns, target)
        written = _merge(spark, target, accepted, contract, columns, deletes)
    finally:
        # The reference unpersists its validation state, and a session that
        # loads many objects is exactly where not doing so is felt: each load
        # would leave its staged rows cached for the life of the session.
        spark.sql(f"UNCACHE TABLE IF EXISTS {validated}")

    result = LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=written["inserted"],
        rows_updated=written["updated"],
        rows_deleted=written["deleted"],
        rows_rejected=rows_rejected,
    )
    if rows_rejected:
        return result.rejected(f"{rows_rejected} {TOLERATED_MESSAGE}")
    return result


# --- validation ---------------------------------------------------------------


def _validate(spark, upserts, contract: LoadContract, columns) -> tuple[str, str]:
    """Stage the proposed rows with a reject reason attached to each.

    The reason is a column rather than a filter so that accepting, rejecting and
    counting are all reads of one materialised relation — the reference's shape,
    and the reason the rows are only walked once.
    """

    staged = _register(spark, upserts, target="staged")
    reason = _temporary_column(columns, REJECTION_REASON)
    rank = _temporary_column((*columns, reason), "_weaver_primary_key_rank")
    blank = blank_key_predicate(contract.primary_key, alias="s")
    partition = ", ".join(f"s.`{c}`" for c in contract.primary_key)
    named = ", ".join(f"s.`{c}`" for c in columns)

    view = f"weaver_validated_{_clean(staged)}"
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {view} AS\n"
        f"SELECT {named}, `{rank}`,\n"
        f"  CASE WHEN {blank} THEN '{REASON_BLANK_PK}'\n"
        f"       WHEN `{rank}` > 1 THEN '{REASON_DUPLICATE_PK}'\n"
        f"       ELSE CAST(NULL AS STRING) END AS `{reason}`\n"
        f"FROM (\n"
        f"  SELECT s.*, row_number() OVER ("
        f" PARTITION BY {partition} ORDER BY (SELECT NULL)) AS `{rank}`\n"
        f"  FROM {staged} AS s\n"
        f") AS s"
    )
    # Materialised, because every count and both branches below read it again.
    spark.sql(f"CACHE TABLE {view}")
    return view, reason


def _reason_counts(spark, view: str, reason: str) -> dict:
    """Input, accepted and rejected counts in one pass over the staged rows."""

    rows = spark.sql(
        f"SELECT `{reason}` AS reason, count(*) AS n FROM {view} GROUP BY `{reason}`"
    ).collect()
    return {row["reason"]: int(row["n"]) for row in rows}


def _accepted_view(spark, validated: str, reason: str, columns, target: str) -> str:
    view = f"weaver_accepted_{_clean(target)}"
    named = ", ".join(f"`{c}`" for c in columns)
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {view} AS "
        f"SELECT {named} FROM {validated} WHERE `{reason}` IS NULL"
    )
    return view


def _require_columns(frame, contract: LoadContract, columns) -> None:
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
    key_missing = [name for name in contract.primary_key if name not in produced]
    if key_missing:
        raise LoadError(
            f"{contract.qualified}: the primary key columns "
            f"{', '.join(repr(name) for name in key_missing)} are not in the "
            "staged frame, so no row can be matched"
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


# --- the writes ----------------------------------------------------------------


def _full_replace(spark, target: str, staged, columns) -> LoadResult:
    """No key, so no match and no update: the target's contents become these."""

    rows_deleted = int(
        spark.sql(f"SELECT count(*) AS n FROM {target}").collect()[0]["n"]
    )
    view = _register(spark, staged, target="replace")
    rows_read = int(spark.sql(f"SELECT count(*) AS n FROM {view}").collect()[0]["n"])
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


def _merge(spark, target: str, accepted: str, contract, columns, deletes) -> dict:
    """One Delta operation carrying every change this load makes.

    Upserts and explicit deletes share a source relation, told apart by an
    operation column. An upsert wins when the same key is also explicitly
    deleted — a row both proposed and retired is a source that contradicts
    itself, and keeping it is the recoverable reading.
    """

    operation = _temporary_column(columns, "_weaver_operation")
    source = _merge_source(spark, accepted, contract, columns, deletes, operation)

    join = key_join("s", "t", contract.primary_key)
    changed = changed_predicate("s", "t", contract)
    audit = delta_audit_names()
    sets = [
        f"t.`{c}` = s.`{c}`" for c in columns if c not in contract.primary_key
    ] + [
        f"t.`{audit[1]}` = current_timestamp()",
        f"t.`{audit[2]}` = {live_delete_literal()}",
    ]
    named = ", ".join(f"`{c}`" for c in columns)
    values = ", ".join(f"s.`{c}`" for c in columns)

    clauses = []
    if deletes is not None:
        clauses.append(f"WHEN MATCHED AND s.`{operation}` = 'delete' THEN DELETE")
    clauses.append(
        f"WHEN MATCHED AND s.`{operation}` = 'upsert' AND ({changed}) "
        f"THEN UPDATE SET {', '.join(sets)}"
    )
    clauses.append(
        f"WHEN NOT MATCHED AND s.`{operation}` = 'upsert' THEN INSERT "
        f"({named}, {', '.join(f'`{a}`' for a in audit)}) "
        f"VALUES ({values}, current_timestamp(), current_timestamp(), "
        f"{live_delete_literal()})"
    )
    if contract.deletes_absent_rows:
        # Safe here, unlike in the generated program: an intolerant run has
        # already returned, so the source is never empty for want of tolerance.
        clauses.append("WHEN NOT MATCHED BY SOURCE THEN DELETE")

    before = _version(spark, target)
    spark.sql(
        f"MERGE INTO {target} AS t USING {source} AS s ON {join}\n"
        + "\n".join(clauses)
    )
    return _written(spark, target, after=before)


def _merge_source(spark, accepted: str, contract, columns, deletes, operation: str) -> str:
    """The merge's source: proposed rows, plus any keys explicitly retired."""

    view = f"weaver_source_{_clean(accepted)}"
    named = ", ".join(f"`{c}`" for c in columns)
    upserts = f"SELECT {named}, 'upsert' AS `{operation}` FROM {accepted}"
    if deletes is None:
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS {upserts}")
        return view

    keys = _register(spark, deletes, target="deletes")
    # Non-key columns are null on a delete row: the merge reads only the key.
    filled = ", ".join(
        f"d.`{c}`" if c in contract.primary_key else f"CAST(NULL AS STRING) AS `{c}`"
        for c in columns
    )
    anti = key_join("a", "d", contract.primary_key)
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {view} AS\n{upserts}\n"
        f"UNION ALL\n"
        f"SELECT {filled}, 'delete' AS `{operation}`\n"
        f"FROM (SELECT DISTINCT {', '.join(f'`{c}`' for c in contract.primary_key)} "
        f"FROM {keys}) AS d\n"
        f"WHERE NOT EXISTS (SELECT 1 FROM {accepted} AS a WHERE {anti})"
    )
    return view


def _write_rejects(spark, target: str, validated: str, reason: str, columns) -> None:
    """Keep the refused rows, with the reason, beside the table they were for.

    A count alone tells a caller that something went wrong and nothing about
    what, which is the state this exists to prevent.
    """

    reject_table = _reject_table(target)
    named = ", ".join(f"`{c}`" for c in columns)
    spark.sql(f"DROP TABLE IF EXISTS {reject_table}")
    spark.sql(
        f"CREATE TABLE {reject_table} USING delta {COLUMN_MAPPING} AS "
        f"SELECT {named}, `{reason}` FROM {validated} WHERE `{reason}` IS NOT NULL"
    )


# --- Delta's own accounting ----------------------------------------------------


def _version(spark, target: str) -> int:
    return int(
        spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").collect()[0]["version"]
    )


def _written(spark, target: str, *, after: int) -> dict:
    """What the merge reported doing, from Delta's operation metrics.

    Guarded on the version: a merge that changed nothing writes no commit, so
    the newest history entry would otherwise be the previous operation's and its
    counts would be attributed to this load.
    """

    row = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").collect()[0]
    if int(row["version"]) <= after:
        return {"inserted": 0, "updated": 0, "deleted": 0}
    metrics = dict(row["operationMetrics"] or {})

    def metric(*names) -> int:
        for name in names:
            if name in metrics:
                return int(metrics[name])
        return 0

    return {
        "inserted": metric("numTargetRowsInserted", "numInsertedRows"),
        "updated": metric("numTargetRowsUpdated", "numUpdatedRows"),
        "deleted": metric("numTargetRowsDeleted", "numDeletedRows"),
    }


# --- names ---------------------------------------------------------------------


def _temporary_column(columns, preferred: str) -> str:
    """A column name of Weaver's that cannot collide with the author's.

    The reference's rule: keep appending an underscore until the name is free.
    A fixed name would eventually meet an author who chose the same one, and the
    failure would be a wrong load rather than an error.
    """

    name = preferred
    existing = set(columns)
    while name in existing:
        name += "_"
    return name


def _register(spark, frame, *, target: str) -> str:
    name = f"weaver_{_clean(target)}"
    frame.createOrReplaceTempView(name)
    return name


def _clean(name: str) -> str:
    return (
        name.replace(".", "_").replace("`", "").replace(" ", "_").replace("-", "_")
    )


def _reject_table(target: str) -> str:
    """``sales.`Customer``` -> ``sales.`Customer_Reject```."""

    head, _, tail = target.rpartition(".")
    name = tail.strip("`")
    return f"{head}.`{name}{REJECT_SUFFIX}`" if head else f"`{name}{REJECT_SUFFIX}`"


__all__ = ["INTOLERANT_MESSAGE", "TOLERATED_MESSAGE", "load_table"]
