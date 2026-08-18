"""Loading a Delta table — the mechanics behind ``Table.load()``.

The authored class proposes rows; this owns everything that happens to them.
``read()`` returns staging, or ``(staging, deletes)``, and never touches the
target. A generated ``SparkSqlTable`` enters here too, so both authoring
languages reconcile through one implementation.

The first value is *staging*: unvalidated, with nothing yet rejected or
classified. It goes through the same phases the Warehouse procedure runs — raw
staging, every refusal discovered into ``_Reject``, the rejection gate, the purge
that makes staging the clean incoming state, ``_Delete``, an ``_Upsert`` of new
and changed rows only, merge uniqueness, the stability gate, then the target.
What each phase means, and where the two engines differ physically, is
``design/keyed-load.md``.

The intermediate tables are real — ``<Schema>.<Object>_Staging``, ``_Reject``,
``_Delete`` and ``_Upsert`` — so a failed run can be inspected afterwards.
Temporary views vanish with the session that made them.

``Incremental`` chooses the delete driver, and there is only ever one. An
incremental source is a window on the truth, so absence proves nothing and the
object states what went (``read()[1]``); a non-incremental source is the whole
truth, so absence is the statement and an explicit list is refused. One driver
serves both the stability count and the deletion, so the guard cannot protect
against a number the load never intended to delete.

Written in SQL rather than the DataFrame API because
``tests/test_core_boundary.py`` forbids importing ``pyspark`` or ``delta``
anywhere in ``weaver``. ``fault_tolerant`` is Weaver's own addition: an
intolerant run returns before any target mutation.
"""

from __future__ import annotations

from ..errors import LoadError
from .delta_sql import (
    COLUMN_MAPPING,
    blank_key_predicate,
    delta_audit_names,
    delta_signature_name,
    key_join,
    live_delete_literal,
    moves_off,
    participates,
    qualified,
    row_signature,
    violation_predicate,
)
from .load_contract import (
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
    LoadContract,
    duplicate_unique_reason,
    null_column_reason,
)
from .load_result import LoadResult

#: The suffixes of the four artefacts, matching the Warehouse so one vocabulary
#: describes a load whichever engine ran it.
STAGING_SUFFIX = "_Staging"
UPSERT_SUFFIX = "_Upsert"
REJECT_SUFFIX = "_Reject"

#: The keys a load will remove, settled before it removes any.
DELETE_SUFFIX = "_Delete"

#: Where the purge assembles the clean incoming state before it replaces staging
#: with it. Not an artefact: it exists inside one statement pair and is dropped.
KEEP_SUFFIX = "_StagingKeep"

#: Columns a working relation carries. Weaver's, and named so: they sit beside
#: the author's own columns in a real table.
RANK_COLUMN = "__weaver_rank"
WORKING_SIGNATURE_COLUMN = "__weaver_signature"
SURVIVOR_COLUMN = "__weaver_survivor"

#: What marks a row of the upsert set as one the target does not yet hold.
#: Membership already means new or changed, so this only says which.
IS_NEW_COLUMN = "_Is new row"

INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"

#: What a run reports when the change is larger than the object allows. Governed
#: by ``fault_tolerant`` exactly as row rejection is: refused outright at 0, gone
#: ahead with but still reported as a failure at 1.
#: A breach never mutates. ``fault_tolerant`` decides only how the refusal is
#: surfaced, raised or returned; tolerating a change this large is what
#: ``ignore_stability_threshold`` is for.
BREACH_MESSAGE = "{reason}; the target was not modified"

#: What a run reports when its proposed changes do not describe a valid target.
#: Not a row-level reject and not governed by ``fault_tolerant``: the incoming
#: rows are individually fine, and it is the state they would leave that is not.
MERGE_CONFLICT_MESSAGE = (
    "the proposed changes would leave a declared unique key held by two rows, "
    "so the target was not modified"
)


def table_is_populated(spark, *, contract: LoadContract, lakehouse) -> bool:
    """Whether the target already holds a row.

    A static object's load asks whether it has been seeded, so the query stops
    at one row rather than counting. An empty table is not populated: a build
    guarantees the table exists, and a static load puts the first rows in it.
    """

    target = lakehouse.qualify(contract.object_id.schema, contract.object_id.object)
    return bool(spark.sql(f"SELECT 1 FROM {target} LIMIT 1").take(1))


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
    guessed it from the attached Lakehouse would write wherever the session
    happened to live rather than where the object belongs.
    """

    schema, name = contract.object_id.schema, contract.object_id.object
    names = {
        key: lakehouse.qualify(schema, name + suffix)
        for key, suffix in (
            ("target", ""),
            ("staging", STAGING_SUFFIX),
            ("upsert", UPSERT_SUFFIX),
            ("reject", REJECT_SUFFIX),
            ("delete", DELETE_SUFFIX),
            ("keep", KEEP_SUFFIX),
        )
    }
    columns, types = _business_columns(spark, names["target"])
    _require_columns(staging_frame, contract, columns)
    deletes = _delete_driver(contract, deletes)

    # Every run starts from nothing a previous run left. Otherwise a clean load
    # leaves the last run's reject table standing, and it reads as evidence about
    # the run that just succeeded.
    _clear(spark, names)

    _materialise_staging(spark, names, staging_frame, columns)
    rows_read = _count(spark, names["staging"])

    if contract.replaces_wholesale:
        return _full_replace(spark, names, columns, rows_read)

    signature = row_signature("s", contract.comparison_columns, types)
    rows_rejected = _discover_rejects(spark, names, contract, columns, signature)
    if rows_rejected and not fault_tolerant:
        # Nothing has been written, so refusing is a decision not to start
        # rather than an unwind — and the reject table is the evidence.
        raise LoadError(
            f"{contract.qualified}: {INTOLERANT_MESSAGE}",
            result=LoadResult.failure(
                INTOLERANT_MESSAGE, rows_read=rows_read, rows_rejected=rows_rejected
            ),
        )
    if rows_rejected:
        _purge_staging(spark, names, contract, columns, signature)

    _derive_deletes(spark, names, contract, deletes)
    _derive_upserts(spark, names, contract, columns, signature)
    if contract.checks_merge_uniqueness:
        _require_merge_uniqueness(spark, names, contract, deletes is not None)

    # Everything the load is about to do, counted before it does any of it —
    # which is the whole reason the change set is a table.
    target_before = _count(spark, names["target"])
    breach = None
    if not ignore_stability_threshold:
        breach = contract.breaches(
            target_rows=target_before,
            deleting=_count(spark, names["delete"]),
            updating=_count(spark, names["upsert"], where=f"`{IS_NEW_COLUMN}` = 0"),
        )
    if breach:
        # A breach never writes. Tolerating one would be tolerating exactly the
        # change the threshold was declared to prevent, so what fault_tolerant
        # decides here is only whether the refusal is raised or returned.
        refused = LoadResult.failure(
            BREACH_MESSAGE.format(reason=breach),
            rows_read=rows_read,
            rows_rejected=rows_rejected,
        )
        if not fault_tolerant:
            raise LoadError(f"{contract.qualified}: {breach}", result=refused)
        return refused

    _apply_deletes(spark, names, contract)
    inserted, updated = _apply_upserts(spark, names, contract, columns)

    # What the target actually lost, from its own cardinality. The delete driver
    # says what the load *intended*; this says what happened, and the two differ
    # whenever a key named for deletion was not there to begin with.
    deleted = target_before + inserted - _count(spark, names["target"])

    result = LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=inserted,
        rows_updated=updated,
        rows_deleted=deleted,
        rows_rejected=rows_rejected,
    )
    if rows_rejected:
        # The artefacts stay: a run that refused rows is one someone will want
        # to look at, and the reject table alone does not explain itself.
        return result.rejected(f"{rows_rejected} {TOLERATED_MESSAGE}")
    _clear(spark, names)
    return result


# --- phases ------------------------------------------------------------------


def _materialise_staging(spark, names, frame, columns) -> None:
    """Put what ``read()`` produced into a real table, exactly as it produced it.

    Business columns and nothing else. Materialised first, so the authored query
    runs exactly once and a source that fails does so before the target is
    touched.
    """

    view = _register(spark, frame, names["target"], "staged")
    spark.sql(
        f"CREATE TABLE {names['staging']} USING delta {COLUMN_MAPPING} AS "
        f"SELECT {qualified('s', columns)} FROM {view} AS s"
    )


def _discover_rejects(
    spark, names, contract: LoadContract, columns, signature: str
) -> int:
    """Everything this load refuses, in one statement.

    One statement because the stages are sequential and the chain says so
    directly: each unique key reads the rows that survived the ones before it, so
    a row already refused never becomes the arbitrary survivor of a later group.

    Every scan is narrow. A duplicate is found by grouping, and the only window is
    over rows already known to sit in a duplicate primary key group.
    """

    chain, rejects = _validation_chain(names, contract, columns, signature)
    union = "\nUNION ALL\n".join(f"SELECT * FROM {name}" for name in rejects)
    spark.sql(
        f"CREATE TABLE {names['reject']} USING delta {COLUMN_MAPPING} AS\n"
        f"WITH {chain}\n{union}"
    )
    count = _count(spark, names["reject"])
    if not count:
        # Nothing was refused, so there is no evidence to keep and an empty table
        # standing next to the object would only invite the wrong conclusion.
        spark.sql(f"DROP TABLE IF EXISTS {names['reject']}")
    return count


def _purge_staging(
    spark, names, contract: LoadContract, columns, signature: str
) -> None:
    """Replace staging with the rows the refusals left.

    One overwrite from the same chain discovery ran, rather than a delete per
    stage, because Delta cannot delete through a ranked expression — and rows
    sharing a primary key may be identical in every column, so no predicate can
    tell one of them from the other. Assembling the survivors and overwriting is
    also what makes the purge agree with discovery by construction.

    Only ever reached when something was refused, so an ordinary load performs no
    staging write at all.
    """

    chain, _rejects = _validation_chain(names, contract, columns, signature)
    spark.sql(f"DROP TABLE IF EXISTS {names['keep']}")
    spark.sql(
        f"CREATE TABLE {names['keep']} USING delta {COLUMN_MAPPING} AS\n"
        f"WITH {chain}\n"
        f"SELECT {qualified('s', columns)} FROM {_surviving_relation(contract)} AS s"
    )
    # Through a table rather than straight from staging: overwriting a table from
    # a read of itself is not something Spark guarantees.
    spark.sql(
        f"INSERT OVERWRITE TABLE {names['staging']} "
        f"SELECT {qualified('k', columns)} FROM {names['keep']} AS k"
    )
    spark.sql(f"DROP TABLE IF EXISTS {names['keep']}")


def _validation_chain(
    names, contract: LoadContract, columns, signature: str
) -> tuple[str, list[str]]:
    """The common table expressions both discovery and the purge read.

    One definition, so what is refused and what survives cannot disagree — which
    matters most where the choice is arbitrary, as it is inside a duplicate
    primary key group.
    """

    named = qualified("s", columns)
    violation = violation_predicate(contract)
    ctes = [
        (
            "weaver_null_reject",
            f"SELECT {named}, {_violation_reason(contract)} AS `{REJECTION_REASON}`\n"
            f"FROM {names['staging']} AS s WHERE {violation}",
        ),
        (
            "weaver_valid",
            f"SELECT {named}, {signature} AS `{WORKING_SIGNATURE_COLUMN}`\n"
            f"FROM {names['staging']} AS s WHERE NOT ({violation})",
        ),
        (
            "weaver_duplicate_key",
            f"SELECT {qualified('', contract.primary_key)} FROM weaver_valid\n"
            f"GROUP BY {qualified('', contract.primary_key)} HAVING count(*) > 1",
        ),
        (
            "weaver_ranked_key",
            f"SELECT {named}, row_number() OVER (\n"
            f"    PARTITION BY {qualified('s', contract.primary_key)}\n"
            f"    ORDER BY s.`{WORKING_SIGNATURE_COLUMN}`) AS `{RANK_COLUMN}`\n"
            f"FROM weaver_valid AS s JOIN weaver_duplicate_key AS d\n"
            f"    ON {key_join('d', 's', contract.primary_key)}",
        ),
        (
            "weaver_key_reject",
            f"SELECT {named}, '{REASON_DUPLICATE_PK}' AS `{REJECTION_REASON}`\n"
            f"FROM weaver_ranked_key AS s WHERE s.`{RANK_COLUMN}` > 1",
        ),
        (
            "weaver_unique_key",
            # One row per surviving primary key. From here on the key identifies a
            # row, which is what lets a unique key name its losers by key.
            f"SELECT {named} FROM weaver_valid AS s\n"
            f"WHERE NOT EXISTS (SELECT 1 FROM weaver_duplicate_key AS d\n"
            f"    WHERE {key_join('d', 's', contract.primary_key)})\n"
            f"UNION ALL\n"
            f"SELECT {named} FROM weaver_ranked_key AS s "
            f"WHERE s.`{RANK_COLUMN}` = 1",
        ),
    ]
    rejects = ["weaver_null_reject", "weaver_key_reject"]

    source = "weaver_unique_key"
    for index, unique_key in enumerate(contract.unique_keys, start=1):
        ctes.extend(_unique_key_ctes(contract, unique_key, index, source, columns))
        rejects.append(f"weaver_unique_{index}_reject")
        source = f"weaver_unique_{index}_survivor"

    chain = ",\n".join(f"{name} AS (\n{sql}\n)" for name, sql in ctes)
    return chain, rejects


def _unique_key_ctes(
    contract: LoadContract, unique_key, index: int, source: str, columns
) -> list[tuple[str, str]]:
    """One unique key's duplicate groups, its losers, and what survives it.

    Which row survives a group is arbitrary and settled cheaply. A single-column
    primary key gives an aggregate to settle it with; a composite one has none, so
    those groups are ranked — over the duplicate groups alone, never over the whole
    population.
    """

    named = qualified("s", columns)
    reason = duplicate_unique_reason(unique_key)
    keys = qualified("", unique_key)
    ctes = []

    if len(contract.primary_key) == 1:
        key = f"`{contract.primary_key[0]}`"
        ctes.append(
            (
                f"weaver_unique_{index}_duplicate",
                f"SELECT {keys}, min({key}) AS `{SURVIVOR_COLUMN}`\n"
                f"FROM {source} WHERE {participates(unique_key, '')}\n"
                f"GROUP BY {keys} HAVING count(*) > 1",
            )
        )
        ctes.append(
            (
                f"weaver_unique_{index}_reject",
                f"SELECT {named}, '{reason}' AS `{REJECTION_REASON}`\n"
                f"FROM {source} AS s JOIN weaver_unique_{index}_duplicate AS d\n"
                f"    ON {key_join('d', 's', unique_key)}\n"
                f"WHERE s.{key} <> d.`{SURVIVOR_COLUMN}`",
            )
        )
    else:
        ctes.append(
            (
                f"weaver_unique_{index}_duplicate",
                f"SELECT {keys} FROM {source} "
                f"WHERE {participates(unique_key, '')}\n"
                f"GROUP BY {keys} HAVING count(*) > 1",
            )
        )
        ctes.append(
            (
                f"weaver_unique_{index}_ranked",
                f"SELECT {named}, row_number() OVER (\n"
                f"    PARTITION BY {qualified('s', unique_key)}\n"
                f"    ORDER BY {qualified('s', contract.primary_key)}) "
                f"AS `{RANK_COLUMN}`\n"
                f"FROM {source} AS s JOIN weaver_unique_{index}_duplicate AS d\n"
                f"    ON {key_join('d', 's', unique_key)}\n"
                f"WHERE {participates(unique_key)}",
            )
        )
        ctes.append(
            (
                f"weaver_unique_{index}_reject",
                f"SELECT {named}, '{reason}' AS `{REJECTION_REASON}`\n"
                f"FROM weaver_unique_{index}_ranked AS s "
                f"WHERE s.`{RANK_COLUMN}` > 1",
            )
        )

    ctes.append(
        (
            f"weaver_unique_{index}_survivor",
            f"SELECT {named} FROM {source} AS s\n"
            f"WHERE NOT EXISTS (SELECT 1 FROM weaver_unique_{index}_reject AS r\n"
            f"    WHERE {key_join('r', 's', contract.primary_key)})",
        )
    )
    return ctes


def _surviving_relation(contract: LoadContract) -> str:
    """Which chain relation holds the rows every refusal left."""

    if not contract.unique_keys:
        return "weaver_unique_key"
    return f"weaver_unique_{len(contract.unique_keys)}_survivor"


def _violation_reason(contract: LoadContract, alias: str = "s") -> str:
    """Which refusal a row met, taking the first that applies.

    One reason per refused row. A row that is wrong twice over is still one row
    the load will not take, and counting it twice would let it weigh twice
    against the rejection threshold.
    """

    if not contract.not_null_columns:
        return f"'{REASON_BLANK_PK}'"
    branches = [
        f"WHEN {blank_key_predicate(contract.primary_key, alias)} "
        f"THEN '{REASON_BLANK_PK}'"
    ]
    branches += [
        f"WHEN {alias}.`{column}` IS NULL THEN '{null_column_reason(column)}'"
        for column in contract.not_null_columns
    ]
    return "CASE " + " ".join(branches) + " END"


def _derive_upserts(
    spark, names, contract: LoadContract, columns, signature: str
) -> None:
    """Record what this load has decided to change, before it changes anything.

    New rows, and rows whose stored signature no longer matches what staging
    proposes. A table rather than a subquery, so the decision is inspectable
    afterwards and the stability check can read the size of the change before a
    row moves.
    """

    stored = delta_signature_name()
    missing = f"t.`{contract.primary_key[0]}` IS NULL"
    spark.sql(
        f"CREATE TABLE {names['upsert']} USING delta {COLUMN_MAPPING} AS\n"
        f"WITH weaver_proposed AS (\n"
        f"    SELECT {qualified('s', columns)}, {signature} AS `{stored}`\n"
        f"    FROM {names['staging']} AS s\n"
        f")\n"
        f"SELECT {qualified('q', columns)}, q.`{stored}`,\n"
        f"  CASE WHEN {missing} THEN 1 ELSE 0 END AS `{IS_NEW_COLUMN}`\n"
        f"FROM weaver_proposed AS q\n"
        f"LEFT JOIN {names['target']} AS t "
        f"ON {key_join('q', 't', contract.primary_key)}\n"
        f"WHERE {missing} OR q.`{stored}` <> t.`{stored}`"
    )


def _require_merge_uniqueness(spark, names, contract: LoadContract, has_claim) -> None:
    """The one question an incremental load with unique keys has to ask.

    If every surviving delete and upsert were applied, would a declared unique key
    still be held by another target row? A non-incremental load never asks: it
    leaves the target equal to clean staging, and staging has already been made
    unique.

    A holder gives its value up in exactly two ways — the load deletes it, or the
    load moves it off that value. So a swap, and a cycle whose proposed state is
    unique, both pass, and a claim against an untouched holder does not.

    Any collision that remains stops the load. There is no partial application and
    no closure to compute: the proposed target state is either valid under the
    declared keys or it is not.
    """

    branches = [
        _conflict_branch(names, contract, unique_key, has_claim)
        for unique_key in contract.unique_keys
    ]
    union = "\nUNION ALL\n".join(branches)
    conflicts = int(
        spark.sql(
            f"SELECT count(*) AS n FROM (\n{union}\n) AS weaver_merge_conflict"
        ).collect()[0]["n"]
    )
    if conflicts:
        # Fatal whatever fault_tolerant says: that governs recoverable problems
        # with incoming rows, and this is the target's own validity.
        raise LoadError(
            f"{contract.qualified}: {MERGE_CONFLICT_MESSAGE}",
            result=LoadResult.failure(MERGE_CONFLICT_MESSAGE),
        )


def _conflict_branch(names, contract: LoadContract, unique_key, has_claim: bool) -> str:
    differs = " OR ".join(f"holder.`{c}` <> u.`{c}`" for c in contract.primary_key)
    vacated = [
        f"AND NOT EXISTS (SELECT 1 FROM {names['upsert']} AS moving\n"
        f"    WHERE {key_join('moving', 'holder', contract.primary_key)}\n"
        f"      AND ({moves_off(unique_key)}))"
    ]
    if has_claim:
        vacated.insert(
            0,
            f"AND NOT EXISTS (SELECT 1 FROM {names['delete']} AS d\n"
            f"    WHERE {key_join('d', 'holder', contract.primary_key)})",
        )
    return (
        f"SELECT {qualified('u', contract.primary_key)}\n"
        f"FROM {names['upsert']} AS u\n"
        f"JOIN {names['target']} AS holder\n"
        f"    ON {key_join('holder', 'u', unique_key)} AND ({differs})\n"
        f"WHERE {participates(unique_key, 'u')}\n" + "\n".join(vacated)
    )


def _apply_upserts(spark, names, contract: LoadContract, columns) -> tuple[int, int]:
    """Insert the new rows, then update the changed ones.

    Two statements over the one materialised set, as the Warehouse does, so both
    counts describe exactly the rows the writes touched. Both carry the signature
    the upsert set already computed.
    """

    audit = delta_audit_names()
    stored = delta_signature_name()
    written = [*columns, stored]
    insert_columns = qualified("", written)
    audit_columns = qualified("", audit)
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
            f"t.`{c}` = u.`{c}`" for c in written if c not in contract.primary_key
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


def _delete_driver(contract: LoadContract, deletes):
    """Which delete claim this object makes, refusing the one it may not.

    ``Incremental`` decides exclusively. A non-incremental table returning
    explicit deletes states the same thing twice, and the second would be
    applied on top of a reconciliation that already accounted for it, so it is
    refused rather than ignored.
    """

    if contract.incremental:
        return deletes
    if deletes is not None and bool(deletes.take(1)):
        raise LoadError(
            f"{contract.qualified}: a non-incremental table cannot name explicit "
            "deletes — the source is the whole truth, so a row's absence from it "
            "is what retires it. Return an empty frame, or declare "
            "Incremental: true."
        )
    return None


def _derive_deletes(spark, names, contract: LoadContract, deletes) -> None:
    """Materialise the keys this load will remove, before it removes any.

    One relation from one driver, settled before anything is removed: a number
    obtained by deleting would be a report rather than a check, and the guard
    exists to decide not to delete.

    A non-incremental load reads staging *after* the purge, so a target row whose
    only staged proposal was refused is retired by the same rule as any other
    absence, and no later repair pass is needed.
    """

    keys = qualified("", contract.primary_key)
    target_keys = qualified("t", contract.primary_key)
    spark.sql(f"DROP TABLE IF EXISTS {names['delete']}")
    if contract.incremental:
        source = (
            f"SELECT DISTINCT {keys} FROM "
            f"{_register(spark, deletes, names['target'], 'delete_keys')}"
            if deletes is not None
            else f"SELECT {keys} FROM {names['target']} WHERE false"
        )
        # Only keys the target actually holds: a delete for a row that was never
        # there is not a deletion, and counting it would make the guard protect
        # against work the load was never going to do.
        spark.sql(
            f"CREATE TABLE {names['delete']} USING delta {COLUMN_MAPPING} AS\n"
            f"SELECT {target_keys}\n"
            f"FROM {names['target']} AS t JOIN ({source}) AS d "
            f"ON {key_join('d', 't', contract.primary_key)}"
        )
        return

    spark.sql(
        f"CREATE TABLE {names['delete']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT {target_keys}\n"
        f"FROM {names['target']} AS t\n"
        f"WHERE NOT EXISTS (SELECT 1 FROM {names['staging']} AS s "
        f"WHERE {key_join('s', 't', contract.primary_key)})"
    )


def _apply_deletes(spark, names, contract) -> None:
    """Remove exactly the keys the driver settled on."""

    spark.sql(
        f"MERGE INTO {names['target']} AS t USING {names['delete']} AS d "
        f"ON {key_join('d', 't', contract.primary_key)} WHEN MATCHED THEN DELETE"
    )


def _full_replace(spark, names, columns, rows_read: int) -> LoadResult:
    """No key, so no row can be matched: the target's contents become these.

    Staging is materialised first and the target emptied afterwards, which is
    why staging is a table: clearing the target and then evaluating the source
    would leave nothing behind if it failed.
    """

    audit = delta_audit_names()
    named = qualified("", columns)
    audit_columns = qualified("", audit)
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
    """Drop the working tables, newest dependency first."""

    for key in ("upsert", "reject", "delete", "keep", "staging"):
        spark.sql(f"DROP TABLE IF EXISTS {names[key]}")


def _count(spark, relation: str, *, where: str | None = None) -> int:
    """How many rows. Unfiltered, Delta answers this from its transaction log.

    So the target's own count is affordable — the ``sys.partitions`` equivalent
    rather than a scan. A filtered count does read, so the filtered ones here
    are over the upsert set, never the target.
    """

    clause = f" WHERE {where}" if where else ""
    return int(
        spark.sql(f"SELECT count(*) AS n FROM {relation}{clause}").collect()[0]["n"]
    )


def _business_columns(spark, target: str) -> tuple[tuple[str, ...], dict[str, str]]:
    """The target's own columns and their types, less the ones Weaver supplies.

    Read from the table rather than the declaration, as the Warehouse installer
    reads ``sys.columns``: a declaration that had drifted would name a column
    that is not there. The types come with them because a row signature spells a
    timestamp and a boolean differently from the rest.
    """

    reserved = {*delta_audit_names(), delta_signature_name()}
    fields = [
        field
        for field in spark.table(target).schema.fields
        if field.name not in reserved
    ]
    names = tuple(field.name for field in fields)
    types = {field.name: field.dataType.simpleString() for field in fields}
    return names, types


def _require_columns(frame, contract: LoadContract, columns) -> None:
    """Every column the target needs must be present, by exact name.

    Checked before anything is written, and by name rather than position: a
    frame whose columns line up today would load the wrong values the day an
    author reorders a select.
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

    A temp view names an in-flight ``DataFrame`` for a single statement. No
    phase's result lives in one; those are tables, because they outlive the
    session.
    """

    name = "weaver_" + role + "_" + _clean(target)
    frame.createOrReplaceTempView(name)
    return name


def _clean(name: str) -> str:
    return name.replace(".", "_").replace("`", "").replace(" ", "_").replace("-", "_")


__all__ = [
    "INTOLERANT_MESSAGE",
    "IS_NEW_COLUMN",
    "MERGE_CONFLICT_MESSAGE",
    "TOLERATED_MESSAGE",
    "load_table",
]
