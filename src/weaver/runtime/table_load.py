"""Loading a Python-defined Delta table — the mechanics behind ``Table.load()``.

The authored class proposes rows; this owns everything that happens to them.
``read()`` returns ``(staging, deletes)`` and never touches the target, which is
the invariant the whole runtime rests on: an object that wrote to its own table
would make Weaver's accounting a guess.

**The first value is staging, not an upsert set.** It has not been validated,
nothing has been rejected from it, and no row in it has yet been classified as
new or changed. It goes through the same phases the Warehouse procedure runs::

    staging
      → validate keys
      → reject invalid rows
      → valid staging
      → compare against target
      → derive the upsert set
      → insert new rows
      → update changed rows
      → apply explicit deletes, separately, by key
      → delete absent rows, for non-incremental loads only

**The intermediate tables are real**, as they are in the Warehouse and in the
generated Spark SQL program: ``<Schema>.<Object>_Staging``, ``_Upsert`` and
``_Reject``. That is what makes a load inspectable — what the source produced,
what was refused, and what Weaver decided to change are all still there
afterwards, and a run that failed can be understood without re-running the
authored code that produced it. Temporary views cannot do that: they vanish with
the session that made them, which is exactly when someone wants to look.

**Explicit deletes are not part of staging reconciliation.** They are applied
afterwards, by key, so a key that was both staged and deleted ends up deleted —
no conflict rule is needed, because the later statement simply wins. That is a
deliberate departure from ``delta_table_load.py``, which filtered deletes
against the accepted keys so an upsert would survive; running deletes last makes
the same decision without a rule to remember.

Two departures from the reference remain, both forced. It is written in SQL
rather than the DataFrame API, because ``tests/test_core_boundary.py`` forbids
importing ``pyspark`` or ``delta`` anywhere in ``weaver``. And ``fault_tolerant``
is Weaver's own addition: an intolerant run returns before any target mutation.
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

#: The suffixes of the three artefacts, matching the Warehouse and the generated
#: Spark program so one vocabulary describes a load whichever engine ran it.
STAGING_SUFFIX = "_Staging"
UPSERT_SUFFIX = "_Upsert"
REJECT_SUFFIX = "_Reject"

#: What ranks duplicate keys, and what marks a row of the upsert set as new.
#: Both are Weaver's, and both sit in a table beside the author's own columns.
RANK_COLUMN = "__weaver_pk_row_number"
IS_NEW_COLUMN = "_Is new row"

INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"

#: What a run reports when the change is larger than the object allows. Governed
#: by ``fault_tolerant`` exactly as row rejection is: refused outright at 0, gone
#: ahead with but still reported as a failure at 1.
BREACH_REFUSED = "{reason}, and fault_tolerant = 0, so the target was not modified"
BREACH_TOLERATED = "{reason}"


def load_table(
    spark,
    *,
    contract: LoadContract,
    lakehouse,
    staging_frame,
    deletes=None,
    fault_tolerant: bool = False,
    ignore_stability_threshold: bool = False,
) -> LoadResult:
    """Load one Delta table from the rows its object staged.

    The destination is resolved by the caller, never inferred here: a load that
    guessed it from the attached Lakehouse would write to the control plane.
    """

    schema, name = contract.object_id.schema, contract.object_id.object
    names = {
        "target": lakehouse.qualify(schema, name),
        "staging": lakehouse.qualify(schema, name + STAGING_SUFFIX),
        "upsert": lakehouse.qualify(schema, name + UPSERT_SUFFIX),
        "reject": lakehouse.qualify(schema, name + REJECT_SUFFIX),
    }
    columns = _business_columns(spark, names["target"])
    _require_columns(staging_frame, contract, columns)

    # Every run starts from nothing a previous run left. Otherwise a clean load
    # leaves the last run's reject table standing, and it reads as evidence about
    # the run that just succeeded.
    _clear(spark, names)

    _materialise_staging(spark, names, staging_frame, contract, columns)
    rows_read = _count(spark, names["staging"])

    if contract.replaces_wholesale:
        return _full_replace(spark, names, columns, rows_read)

    rows_rejected = _reject_invalid_keys(spark, names, contract, columns)
    if rows_rejected and not fault_tolerant:
        # Nothing has been written, so refusing is a decision not to start
        # rather than an unwind — and the reject table is the evidence.
        return LoadResult.failure(
            INTOLERANT_MESSAGE, rows_read=rows_read, rows_rejected=rows_rejected
        )

    _derive_upserts(spark, names, contract, columns)

    # Everything the load is about to do, counted before it does any of it —
    # which is the whole reason the change set is a table. A breach at
    # fault_tolerant = 0 leaves the target exactly as it was.
    breach = None
    if not ignore_stability_threshold:
        breach = contract.breaches(
            target_rows=_count(spark, names["target"]),
            deleting=_prospective_deletes(spark, names, contract, deletes),
            updating=_count(spark, names["upsert"], where=f"`{IS_NEW_COLUMN}` = 0"),
        )
    if breach and not fault_tolerant:
        return LoadResult.failure(
            BREACH_REFUSED.format(reason=breach),
            rows_read=rows_read,
            rows_rejected=rows_rejected,
        )

    inserted, updated = _apply_upserts(spark, names, contract, columns)
    deleted = _apply_deletes(spark, names, contract, columns, deletes)

    result = LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=inserted,
        rows_updated=updated,
        rows_deleted=deleted,
        rows_rejected=rows_rejected,
    )
    if breach:
        return result.rejected(BREACH_TOLERATED.format(reason=breach))
    if rows_rejected:
        # The artefacts stay: a run that refused rows is one someone will want
        # to look at, and the reject table alone does not explain itself.
        return result.rejected(f"{rows_rejected} {TOLERATED_MESSAGE}")
    _clear(spark, names)
    return result


# --- phases ------------------------------------------------------------------


def _materialise_staging(spark, names, frame, contract: LoadContract, columns) -> None:
    """Put what ``read()`` produced into a real table, ranked for duplicates.

    Materialised before anything else happens, and that ordering is the point:
    the authored query runs exactly once, and a source that fails does so before
    the target has been touched.
    """

    view = _register(spark, frame, names["target"], "staged")
    named = ", ".join(f"s.`{c}`" for c in columns)
    if not contract.primary_key:
        selected = named
    else:
        partition = ", ".join(f"s.`{c}`" for c in contract.primary_key)
        selected = (
            f"{named}, row_number() OVER ("
            f" PARTITION BY {partition} ORDER BY (SELECT NULL)) AS `{RANK_COLUMN}`"
        )
    spark.sql(
        f"CREATE TABLE {names['staging']} USING delta {COLUMN_MAPPING} AS "
        f"SELECT {selected} FROM {view} AS s"
    )


def _reject_invalid_keys(spark, names, contract: LoadContract, columns) -> int:
    """Move rows Weaver will not load into the reject table, with the reason.

    A count alone says something went wrong and nothing about what, so the rows
    are kept. They are then removed from staging, which from here on is the
    *valid* staging every later phase reads.
    """

    blank = blank_key_predicate(contract.primary_key, alias="s")
    rejected = f"({blank} OR s.`{RANK_COLUMN}` > 1)"
    named = ", ".join(f"s.`{c}`" for c in columns)
    spark.sql(
        f"CREATE TABLE {names['reject']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT {named},\n"
        f"  CASE WHEN {blank} THEN '{REASON_BLANK_PK}'\n"
        f"       ELSE '{REASON_DUPLICATE_PK}' END AS `{REJECTION_REASON}`\n"
        f"FROM {names['staging']} AS s WHERE {rejected}"
    )
    count = _count(spark, names["reject"])
    if count:
        unqualified = rejected.replace("s.`", "`")
        spark.sql(f"DELETE FROM {names['staging']} WHERE {unqualified}")
    else:
        # Nothing was refused, so there is no evidence to keep and an empty table
        # standing next to the object would only invite the wrong conclusion.
        spark.sql(f"DROP TABLE IF EXISTS {names['reject']}")
    return count


def _derive_upserts(spark, names, contract: LoadContract, columns) -> None:
    """Record what this load has decided to change, before it changes anything.

    A table rather than a subquery, so what Weaver decided is inspectable
    afterwards — and so the stability check can read the size of the change
    before a single row has moved.
    """

    join = key_join("s", "t", contract.primary_key)
    changed = changed_predicate("s", "t", contract)
    missing = f"t.`{contract.primary_key[0]}` IS NULL"
    named = ", ".join(f"s.`{c}`" for c in columns)
    spark.sql(
        f"CREATE TABLE {names['upsert']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT {named},\n"
        f"  CASE WHEN {missing} THEN 1 ELSE 0 END AS `{IS_NEW_COLUMN}`\n"
        f"FROM {names['staging']} AS s\n"
        f"LEFT JOIN {names['target']} AS t ON {join}\n"
        f"WHERE {missing} OR ({changed})"
    )


def _apply_upserts(spark, names, contract: LoadContract, columns) -> tuple[int, int]:
    """Insert the new rows, then update the changed ones.

    Two statements over the one materialised set, as the Warehouse does, so both
    counts describe exactly the rows the writes touched.
    """

    audit = delta_audit_names()
    insert_columns = ", ".join(f"`{c}`" for c in columns)
    audit_columns = ", ".join(f"`{a}`" for a in audit)
    inserted = _count(spark, names["upsert"], where=f"`{IS_NEW_COLUMN}` = 1")
    if inserted:
        spark.sql(
            f"INSERT INTO {names['target']} ({insert_columns}, {audit_columns})\n"
            f"SELECT {insert_columns}, current_timestamp(), current_timestamp(), "
            f"{live_delete_literal()}\n"
            f"FROM {names['upsert']} WHERE `{IS_NEW_COLUMN}` = 1"
        )

    updated = _count(spark, names["upsert"], where=f"`{IS_NEW_COLUMN}` = 0")
    if updated:
        sets = [
            f"t.`{c}` = u.`{c}`" for c in columns if c not in contract.primary_key
        ] + [
            f"t.`{audit[1]}` = current_timestamp()",
            f"t.`{audit[2]}` = {live_delete_literal()}",
        ]
        # A merge rather than an UPDATE ... FROM, which Delta does not have. The
        # rows were already chosen when the upsert set was built, so this only
        # applies the change it recorded.
        spark.sql(
            f"MERGE INTO {names['target']} AS t\n"
            f"USING (SELECT * FROM {names['upsert']} WHERE `{IS_NEW_COLUMN}` = 0) AS u\n"
            f"   ON {key_join('u', 't', contract.primary_key)}\n"
            f"WHEN MATCHED THEN UPDATE SET {', '.join(sets)}"
        )
    return inserted, updated


def _prospective_deletes(spark, names, contract, deletes) -> int:
    """How many target rows this load is about to remove, before it removes any.

    Counted rather than measured after the fact, because the point of the guard
    is to decide *not* to delete — a number obtained by deleting would be a
    report rather than a check.
    """

    total = 0
    if deletes is not None:
        keys = _register(spark, deletes, names["target"], "delete_probe")
        named = ", ".join(f"`{c}`" for c in contract.primary_key)
        join = key_join("d", "t", contract.primary_key)
        total += _count(
            spark,
            f"{names['target']} AS t JOIN (SELECT DISTINCT {named} FROM {keys}) AS d "
            f"ON {join}",
        )
    if contract.deletes_absent_rows:
        join = key_join("s", "t", contract.primary_key)
        total += _count(
            spark,
            f"{names['target']} AS t WHERE NOT EXISTS "
            f"(SELECT 1 FROM {names['staging']} AS s WHERE {join})",
        )
    return total


def _apply_deletes(spark, names, contract, columns, deletes) -> int:
    """Rows the object named, then — unless incremental — rows it stopped naming.

    Two different claims. An explicit delete is the object *stating* that a row
    is gone; absence from the source only means retirement when the source is
    the whole truth, which is what a non-incremental declaration says it is.

    Explicit deletes run after the upserts, so a key that was both staged and
    deleted ends up deleted. No conflict rule is needed: the later statement
    wins, and over-deleting is the recoverable direction.
    """

    removed = 0
    if deletes is not None:
        keys = _register(spark, deletes, names["target"], "deletes")
        named = ", ".join(f"`{c}`" for c in contract.primary_key)
        wanted = _count(spark, f"(SELECT DISTINCT {named} FROM {keys})")
        if wanted:
            removed += _delete_matching(
                spark, names["target"], f"(SELECT DISTINCT {named} FROM {keys})", contract
            )

    if contract.deletes_absent_rows:
        # Safe here, unlike in the generated program: an intolerant run has
        # already returned, so valid staging is never empty for want of
        # tolerance and this cannot match the whole target by accident.
        before = _count(spark, names["target"])
        spark.sql(
            f"MERGE INTO {names['target']} AS t\n"
            f"USING {names['staging']} AS s\n"
            f"   ON {key_join('s', 't', contract.primary_key)}\n"
            f"WHEN NOT MATCHED BY SOURCE THEN DELETE"
        )
        removed += before - _count(spark, names["target"])
    return removed


def _delete_matching(spark, target: str, source: str, contract) -> int:
    """Remove the rows these keys name, counting what was actually there."""

    join = key_join("d", "t", contract.primary_key)
    before = _count(spark, target)
    spark.sql(
        f"MERGE INTO {target} AS t USING {source} AS d ON {join} "
        f"WHEN MATCHED THEN DELETE"
    )
    return before - _count(spark, target)


def _full_replace(spark, names, columns, rows_read: int) -> LoadResult:
    """No key, so no row can be matched: the target's contents become these.

    Staging is materialised first and the target emptied only afterwards, which
    is the whole reason staging is a table. Clearing the target and *then*
    evaluating the authored source would leave nothing behind if the source
    failed.
    """

    audit = delta_audit_names()
    named = ", ".join(f"`{c}`" for c in columns)
    audit_columns = ", ".join(f"`{a}`" for a in audit)
    rows_deleted = _count(spark, names["target"])
    spark.sql(f"DELETE FROM {names['target']}")
    spark.sql(
        f"INSERT INTO {names['target']} ({named}, {audit_columns})\n"
        f"SELECT {named}, current_timestamp(), current_timestamp(), "
        f"{live_delete_literal()} FROM {names['staging']}"
    )
    _clear(spark, names)
    return LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=rows_read,
        rows_deleted=rows_deleted,
    )


# --- helpers -----------------------------------------------------------------


def _clear(spark, names) -> None:
    """Drop the three artefacts, newest dependency first."""

    for key in ("upsert", "reject", "staging"):
        spark.sql(f"DROP TABLE IF EXISTS {names[key]}")


def _count(spark, relation: str, *, where: str | None = None) -> int:
    """How many rows. Unfiltered, Delta answers this from its transaction log.

    Which is why the target's own count is affordable: it is the ``sys.partitions``
    equivalent rather than a scan. A filtered count does read, so the filtered
    ones here are over the upsert set, never over the target.
    """

    clause = f" WHERE {where}" if where else ""
    return int(
        spark.sql(f"SELECT count(*) AS n FROM {relation}{clause}").collect()[0]["n"]
    )


def _business_columns(spark, target: str) -> tuple[str, ...]:
    """The target's own columns, less the audit ones the load supplies itself.

    Read from the table rather than from the declaration, for the reason the
    Warehouse installer reads sys.columns: the physical table is what is being
    written to, and a declaration that had drifted from it would produce a
    statement naming a column that is not there.
    """

    audit = set(delta_audit_names())
    return tuple(
        field.name
        for field in spark.table(target).schema.fields
        if field.name not in audit
    )


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
            f"{', '.join(repr(name) for name in missing)} — the staged frame "
            "must carry every column the table declares, by exact name"
        )
    key_missing = [name for name in contract.primary_key if name not in produced]
    if key_missing:
        raise LoadError(
            f"{contract.qualified}: the primary key columns "
            f"{', '.join(repr(name) for name in key_missing)} are not in the "
            "staged frame, so no row can be matched"
        )


def _register(spark, frame, target: str, role: str) -> str:
    """A temporary view over one frame, so SQL can name it.

    The one legitimate use of a temp view here: it names an in-flight
    ``DataFrame`` for a single statement. It is never where a phase's result
    lives — those are tables, because they have to outlive the session.
    """

    name = "weaver_" + role + "_" + _clean(target)
    frame.createOrReplaceTempView(name)
    return name


def _clean(name: str) -> str:
    return name.replace(".", "_").replace("`", "").replace(" ", "_").replace("-", "_")


__all__ = ["INTOLERANT_MESSAGE", "TOLERATED_MESSAGE", "load_table"]
