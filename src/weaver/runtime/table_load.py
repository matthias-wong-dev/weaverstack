"""Reconcile staged rows into a Delta table.

Each phase is a persisted temporary relation until every gate passes. Durable
Delta tables are reserved for failure evidence.
"""

from __future__ import annotations

from contextlib import contextmanager

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

#: Durable evidence suffixes shared with Warehouse loads.
STAGING_SUFFIX = "_Staging"
REJECT_SUFFIX = "_Reject"
DELETE_SUFFIX = "_Delete"

_CASE_SENSITIVE = "spark.sql.caseSensitive"


@contextmanager
def _exact_case(spark):
    """Make one physical create honour its Weaver identifier spelling."""

    previous = spark.conf.get(_CASE_SENSITIVE)
    restore = str(previous).lower() != "true"
    if restore:
        spark.conf.set(_CASE_SENSITIVE, "true")
    try:
        yield
    finally:
        if restore:
            spark.conf.set(_CASE_SENSITIVE, previous)


#: Weaver-owned columns carried only by working relations.
RANK_COLUMN = "__weaver_rank"
WORKING_SIGNATURE_COLUMN = "__weaver_signature"
SURVIVOR_COLUMN = "__weaver_survivor"

#: Operation recorded on each row in the settled change relation.
OPERATION_COLUMN = "__weaver_operation"
INSERT_OP = "I"
UPDATE_OP = "U"
DELETE_OP = "D"

INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"

#: A stability breach never mutates the target; fault tolerance changes only
#: whether the refusal is raised or returned.
BREACH_MESSAGE = "{reason}; the target was not modified"

#: Merge conflicts are target-state failures, not fault-tolerant row rejections.
MERGE_CONFLICT_MESSAGE = (
    "the proposed changes would leave a declared unique key held by two rows, "
    "so the target was not modified"
)


def clear_table(spark, *, contract: LoadContract, lakehouse) -> None:
    """Delete rows while preserving the installed table and Weaver columns."""

    schema, name = contract.object_id.schema, contract.object_id.object
    spark.sql(f"DELETE FROM {lakehouse.qualify(schema, name)}")


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
    """Load the caller-resolved Delta table from staged rows."""

    schema, name = contract.object_id.schema, contract.object_id.object
    names = {
        key: lakehouse.qualify(schema, name + suffix)
        for key, suffix in (
            ("target", ""),
            ("staging", STAGING_SUFFIX),
            ("reject", REJECT_SUFFIX),
            ("delete", DELETE_SUFFIX),
        )
    }
    columns, types = _business_columns(spark, names["target"])
    _require_columns(staging_frame, contract, columns)
    deletes = _delete_driver(contract, deletes)

    # Evidence an earlier faulted run left, dropped before this run can write any
    # of its own. Otherwise the last failure's reject table stands beside a load
    # that has just succeeded and reads as evidence about it.
    _drop_evidence(spark, names)

    held: list = []
    # What the load has settled so far, and which of it has already been written
    # out. A failure with no outcome of its own reads them from here rather than
    # working the state machine out a second time.
    kept: set = set()
    evidence: dict = {}
    try:
        return _reconcile(
            spark,
            held,
            kept=kept,
            evidence=evidence,
            names=names,
            contract=contract,
            columns=columns,
            types=types,
            staging_frame=staging_frame,
            deletes=deletes,
            fault_tolerant=fault_tolerant,
            ignore_stability_threshold=ignore_stability_threshold,
        )
    except Exception:
        # An outcome Weaver did not classify, so the relations it had settled are
        # all there is to read afterwards. Written here, and the original failure
        # goes out unchanged.
        _keep_unclassified_evidence(spark, names, kept, evidence)
        raise
    finally:
        # Every exit: a clean load, a refusal at any gate, an unexpected failure.
        _release(spark, held)


def _reconcile(
    spark,
    held,
    *,
    kept: set,
    evidence: dict,
    names,
    contract: LoadContract,
    columns,
    types,
    staging_frame,
    deletes,
    fault_tolerant: bool,
    ignore_stability_threshold: bool,
) -> LoadResult:
    """Settle every relation and gate before mutating the target."""

    def keep(**relations) -> None:
        _keep_evidence(spark, names, kept, **relations)

    source = _register(spark, held, staging_frame, names["target"], "source")
    staging, staging_view = _hold(
        spark,
        held,
        f"SELECT {qualified('s', columns)} FROM {source} AS s",
        names["target"],
        "staging",
    )
    # Both the metric and the force that materialises staging for the phases after
    # it, so the authored source is evaluated exactly once.
    rows_read = staging.count()
    # Settled, so a failure from here on has something to leave behind. Recorded
    # as the raw proposal rather than as whatever supersedes it, because what
    # ``_Staging`` answers is what the source proposed.
    evidence["staging"] = staging_view

    if contract.replaces_wholesale:
        return _full_replace(spark, names, staging_view, columns, rows_read)

    signature = row_signature("s", _comparison_columns(contract, columns), types)
    rejects, reject_view = _discover_rejects(
        spark, held, names["target"], staging_view, contract, columns, signature
    )
    rows_rejected = rejects.count()
    if rows_rejected:
        evidence["reject"] = reject_view
        # However this ends, it owes an explanation: it either stops here or loads
        # the survivors and reports what it left out. Written before the purge,
        # which supersedes the relation that says what the source proposed.
        keep(staging=staging_view, reject=reject_view)
        if not fault_tolerant:
            # Nothing has been written, so refusing is a decision not to start
            # rather than an unwind.
            raise LoadError(
                f"{contract.qualified}: {INTOLERANT_MESSAGE}",
                result=LoadResult.failure(
                    INTOLERANT_MESSAGE,
                    rows_read=rows_read,
                    rows_rejected=rows_rejected,
                ),
            )
        staging_view = _purge_staging(
            spark, held, names["target"], staging_view, contract, columns, signature
        )

    change_view = _settled_changes(
        spark, held, names, staging_view, contract, columns, signature
    )
    inserted, updated, deleted = _change_counts(spark, change_view)

    # An incremental load's deletions are a claim rather than an absence, so they
    # are settled as their own relation. Derived only where a claim was returned:
    # with none there is nothing to remove, and an empty relation built to say so
    # is a Spark job for a question already answered.
    delete_view = None
    if contract.incremental:
        if deletes is not None:
            claimed, delete_view = _claimed_deletes(
                spark, held, names, staging_view, contract, deletes
            )
            deleted = claimed.count()
    elif deleted:
        delete_view = _delete_keys(spark, held, names, change_view, contract)
    deleting = delete_view if deleted else None
    if deleting is not None:
        evidence["delete"] = deleting

    if contract.checks_merge_uniqueness and _merge_conflicts(
        spark, names, change_view, delete_view, contract, has_claim=deletes is not None
    ):
        keep(staging=staging_view, delete=deleting)
        # Fatal whatever fault_tolerant says: that governs recoverable problems
        # with incoming rows, and this is the target's own validity.
        raise LoadError(
            f"{contract.qualified}: {MERGE_CONFLICT_MESSAGE}",
            result=LoadResult.failure(MERGE_CONFLICT_MESSAGE),
        )

    # Everything the load is about to do is settled, so the gate judges it before
    # any of it happens. Reading the target's size is an action of its own, and a
    # change too small to breach any target the gate applies to cannot breach
    # this one either, so the precondition decides whether to ask.
    breach = None
    if not ignore_stability_threshold and contract.may_breach(
        deleting=deleted, updating=updated
    ):
        breach = contract.breaches(
            target_rows=_count(spark, names["target"]),
            deleting=deleted,
            updating=updated,
        )
    if breach:
        # A breach never writes. Tolerating one would be tolerating exactly the
        # change the threshold was declared to prevent, so what fault_tolerant
        # decides here is only whether the refusal is raised or returned.
        keep(staging=staging_view, delete=deleting)
        refused = LoadResult.failure(
            BREACH_MESSAGE.format(reason=breach),
            rows_read=rows_read,
            rows_rejected=rows_rejected,
        )
        if not fault_tolerant:
            raise LoadError(f"{contract.qualified}: {breach}", result=refused)
        return refused

    # Nothing is submitted for a phase that decided on no rows: a zero-row merge
    # is a Delta commit and a scan for work that does not exist.
    if deleted:
        _apply_deletes(spark, names, delete_view, contract)
    if inserted or updated:
        _apply_changes(spark, names, change_view, contract, columns)

    result = LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=inserted,
        rows_updated=updated,
        # What the target lost is what the classification settled on. Those keys
        # are already narrowed to ones the target holds, and are disjoint from
        # the rows written, so each of them is one row gone.
        rows_deleted=deleted,
        rows_rejected=rows_rejected,
    )
    if rows_rejected:
        keep(delete=deleting)
        return result.rejected(f"{rows_rejected} {TOLERATED_MESSAGE}")
    return result


# --- phases ------------------------------------------------------------------


def _discover_rejects(
    spark, held, target, staging_view, contract: LoadContract, columns, signature
):
    """Apply unique keys in declaration order so earlier rejects cannot survive later."""

    chain, rejects = _validation_chain(staging_view, contract, columns, signature)
    union = "\nUNION ALL\n".join(f"SELECT * FROM {name}" for name in rejects)
    return _hold(spark, held, f"WITH {chain}\n{union}", target, "reject")


def _purge_staging(
    spark, held, target, staging_view, contract: LoadContract, columns, signature
) -> str:
    """Derive survivors from the same chain that produced the rejects."""

    chain, _rejects = _validation_chain(staging_view, contract, columns, signature)
    clean, clean_view = _hold(
        spark,
        held,
        f"WITH {chain}\n"
        f"SELECT {qualified('s', columns)} "
        f"FROM {_surviving_relation(contract)} AS s",
        target,
        "clean",
    )
    clean.count()
    _give_back_one(spark, held, staging_view)
    return clean_view


def _validation_chain(
    staging_view, contract: LoadContract, columns, signature: str
) -> tuple[str, list[str]]:
    named = qualified("s", columns)
    violation = violation_predicate(contract)
    ctes = [
        (
            "weaver_null_reject",
            f"SELECT {named}, {_violation_reason(contract)} AS `{REJECTION_REASON}`\n"
            f"FROM {staging_view} AS s WHERE {violation}",
        ),
        (
            "weaver_valid",
            f"SELECT {named}, {signature} AS `{WORKING_SIGNATURE_COLUMN}`\n"
            f"FROM {staging_view} AS s WHERE NOT ({violation})",
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
    those groups are ranked, over the duplicate groups alone and never over the
    whole population.
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
    if not contract.unique_keys:
        return "weaver_unique_key"
    return f"weaver_unique_{len(contract.unique_keys)}_survivor"


def _violation_reason(contract: LoadContract, alias: str = "s") -> str:
    """Choose one reason per rejected row so threshold counts remain row counts."""

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


def _settled_changes(
    spark, held, names, staging_view, contract: LoadContract, columns, signature
) -> str:
    """Settle the exact insert, update and delete rows before any mutation."""

    proposed = (
        f"WITH weaver_proposed AS (\n"
        f"    SELECT {qualified('s', columns)}, "
        f"{signature} AS `{delta_signature_name()}`\n"
        f"    FROM {staging_view} AS s\n"
        f")\n"
    )
    body = (
        _incremental_changes(names, contract, columns)
        if contract.incremental
        else _full_changes(names, contract, columns)
    )
    _frame, view = _hold(spark, held, proposed + body, names["target"], "change")
    return view


def _incremental_changes(names, contract: LoadContract, columns) -> str:
    """Classify writes without treating absence from an incremental window as deletion."""

    stored = delta_signature_name()
    missing = f"t.`{contract.primary_key[0]}` IS NULL"
    return (
        f"SELECT\n"
        f"  CASE WHEN {missing} THEN '{INSERT_OP}' ELSE '{UPDATE_OP}' END "
        f"AS `{OPERATION_COLUMN}`,\n"
        f"  {qualified('q', columns)}, q.`{stored}`\n"
        f"FROM weaver_proposed AS q\n"
        f"LEFT JOIN {names['target']} AS t "
        f"ON {key_join('q', 't', contract.primary_key)}\n"
        f"WHERE {missing} OR q.`{stored}` <> t.`{stored}`"
    )


def _full_changes(names, contract: LoadContract, columns) -> str:
    """Classify all changes from one full outer join of staging and target."""

    stored = delta_signature_name()
    absent_from_target = f"t.`{contract.primary_key[0]}` IS NULL"
    absent_from_staging = f"q.`{contract.primary_key[0]}` IS NULL"
    key = ", ".join(
        f"coalesce(q.`{c}`, t.`{c}`) AS `{c}`" for c in contract.primary_key
    )
    rest = [f"q.`{c}`" for c in columns if c not in contract.primary_key]
    named = ", ".join([key, *rest])
    return (
        f"SELECT\n"
        f"  CASE WHEN {absent_from_target} THEN '{INSERT_OP}'\n"
        f"       WHEN {absent_from_staging} THEN '{DELETE_OP}'\n"
        f"       ELSE '{UPDATE_OP}' END AS `{OPERATION_COLUMN}`,\n"
        f"  {named}, q.`{stored}`\n"
        f"FROM weaver_proposed AS q\n"
        f"FULL OUTER JOIN {names['target']} AS t "
        f"ON {key_join('q', 't', contract.primary_key)}\n"
        f"WHERE {absent_from_target} OR {absent_from_staging} "
        f"OR q.`{stored}` <> t.`{stored}`"
    )


def _change_counts(spark, change_view: str) -> tuple[int, int, int]:
    rows = spark.sql(
        f"SELECT `{OPERATION_COLUMN}` AS op, count(*) AS n "
        f"FROM {change_view} GROUP BY `{OPERATION_COLUMN}`"
    ).collect()
    counts = {str(row["op"]): int(row["n"]) for row in rows}
    return (
        counts.get(INSERT_OP, 0),
        counts.get(UPDATE_OP, 0),
        counts.get(DELETE_OP, 0),
    )


def _delete_keys(spark, held, names, change_view: str, contract: LoadContract) -> str:
    return _name(
        spark,
        held,
        f"SELECT {qualified('', contract.primary_key)} FROM {change_view}\n"
        f"WHERE `{OPERATION_COLUMN}` = '{DELETE_OP}'",
        names["target"],
        "delete",
    )


def _merge_conflicts(
    spark, names, change_view, delete_view, contract: LoadContract, *, has_claim: bool
) -> int:
    """Reject incremental changes that collide with retained unique-key holders."""

    branches = [
        _conflict_branch(
            names, change_view, delete_view, contract, unique_key, has_claim
        )
        for unique_key in contract.unique_keys
    ]
    union = "\nUNION ALL\n".join(branches)
    return int(
        spark.sql(
            f"SELECT count(*) AS n FROM (\n{union}\n) AS weaver_merge_conflict"
        ).collect()[0]["n"]
    )


def _conflict_branch(
    names, change_view, delete_view, contract: LoadContract, unique_key, has_claim: bool
) -> str:
    differs = " OR ".join(f"holder.`{c}` <> u.`{c}`" for c in contract.primary_key)
    vacated = [
        f"AND NOT EXISTS (SELECT 1 FROM {change_view} AS moving\n"
        f"    WHERE {key_join('moving', 'holder', contract.primary_key)}\n"
        f"      AND ({moves_off(unique_key)}))"
    ]
    if has_claim:
        vacated.insert(
            0,
            f"AND NOT EXISTS (SELECT 1 FROM {delete_view} AS d\n"
            f"    WHERE {key_join('d', 'holder', contract.primary_key)})",
        )
    return (
        f"SELECT {qualified('u', contract.primary_key)}\n"
        f"FROM {change_view} AS u\n"
        f"JOIN {names['target']} AS holder\n"
        f"    ON {key_join('holder', 'u', unique_key)} AND ({differs})\n"
        f"WHERE {participates(unique_key, 'u')}\n" + "\n".join(vacated)
    )


def _apply_changes(spark, names, change_view, contract: LoadContract, columns) -> None:
    """Apply inserts and updates in one Delta merge."""

    audit = delta_audit_names()
    stored = delta_signature_name()
    written = [*columns, stored]
    named = qualified("", written)
    audit_columns = qualified("", audit)
    sets = [
        f"t.`{column}` = chg.`{column}`"
        for column in written
        if column not in contract.primary_key
    ] + [
        f"t.`{audit[1]}` = current_timestamp()",
        f"t.`{audit[2]}` = {live_delete_literal()}",
    ]
    spark.sql(
        f"MERGE INTO {names['target']} AS t\n"
        f"USING (SELECT * FROM {change_view} "
        f"WHERE `{OPERATION_COLUMN}` <> '{DELETE_OP}') AS chg\n"
        f"   ON {key_join('chg', 't', contract.primary_key)}\n"
        f"WHEN MATCHED AND chg.`{OPERATION_COLUMN}` = '{UPDATE_OP}' "
        f"THEN UPDATE SET {', '.join(sets)}\n"
        f"WHEN NOT MATCHED AND chg.`{OPERATION_COLUMN}` = '{INSERT_OP}' "
        f"THEN INSERT ({named}, {audit_columns})\n"
        f"     VALUES ({qualified('chg', written)}, current_timestamp(), "
        f"current_timestamp(), {live_delete_literal()})"
    )


def _delete_driver(contract: LoadContract, deletes):
    if contract.incremental:
        return deletes
    if deletes is not None:
        raise LoadError(
            f"{contract.qualified}: read() returned explicit deletes for a "
            "non-incremental table; return only staging or declare Incremental: true"
        )
    return None


def _claimed_deletes(spark, held, names, staging_view, contract: LoadContract, deletes):
    """Settle claims to target keys absent from clean staging before the guard."""

    keys = qualified("", contract.primary_key)
    target_keys = qualified("t", contract.primary_key)
    claimed = _register(spark, held, deletes, names["target"], "delete_keys")
    return _hold(
        spark,
        held,
        f"SELECT {target_keys}\n"
        f"FROM {names['target']} AS t "
        f"JOIN (SELECT DISTINCT {keys} FROM {claimed}) AS d "
        f"ON {key_join('d', 't', contract.primary_key)}\n"
        f"WHERE NOT EXISTS (SELECT 1 FROM {staging_view} AS s "
        f"WHERE {key_join('s', 't', contract.primary_key)})",
        names["target"],
        "delete",
    )


def _apply_deletes(spark, names, delete_view, contract) -> None:
    spark.sql(
        f"MERGE INTO {names['target']} AS t USING {delete_view} AS d "
        f"ON {key_join('d', 't', contract.primary_key)} WHEN MATCHED THEN DELETE"
    )


def _full_replace(spark, names, staging_view, columns, rows_read: int) -> LoadResult:
    """Write staging to Delta before emptying a target the source may read."""

    audit = delta_audit_names()
    named = qualified("", columns)
    audit_columns = qualified("", audit)
    with _exact_case(spark):
        spark.sql(
            f"CREATE TABLE {names['staging']} USING delta {COLUMN_MAPPING} AS "
            f"SELECT {named} FROM {staging_view}"
        )
    rows_deleted = _count(spark, names["target"])
    spark.sql(f"DELETE FROM {names['target']}")
    spark.sql(
        f"INSERT INTO {names['target']} ({named}, {audit_columns})\n"
        f"SELECT {named}, current_timestamp(), current_timestamp(), "
        f"{live_delete_literal()} FROM {names['staging']}"
    )
    spark.sql(f"DROP TABLE IF EXISTS {names['staging']}")
    return LoadResult(
        succeeded=True,
        rows_read=rows_read,
        rows_inserted=rows_read,
        rows_deleted=rows_deleted,
    )


# --- what a load keeps, and what it gives back -------------------------------


def _view_name(role: str, target: str) -> str:
    """Name one working relation, in a spelling Spark's own normalisation keeps.

    A temporary view is held under the key ``spark.sql.caseSensitive`` makes of
    its name: lower case while the conf is off, as written while it is on. A load
    turns the conf on so a durable artefact is created with the object's own
    spelling (see :func:`_exact_case`), and that statement reads a view
    registered while it was off. A lower-case name is the same key either way.
    """

    return ("weaver_" + role + "_" + _clean(target)).lower()


def _hold(spark, held, sql: str, target: str, role: str):
    """Persist and name one phase without writing a Delta table."""

    frame = spark.sql(sql).persist()
    view = _view_name(role, target)
    frame.createOrReplaceTempView(view)
    held.append((frame, view))
    return frame, view


def _register(spark, held, frame, target: str, role: str) -> str:
    view = _view_name(role, target)
    frame.createOrReplaceTempView(view)
    held.append((None, view))
    return view


def _name(spark, held, sql: str, target: str, role: str) -> str:
    view = _view_name(role, target)
    spark.sql(sql).createOrReplaceTempView(view)
    held.append((None, view))
    return view


def _give_back_one(spark, held, view: str) -> None:
    for index, (frame, name) in enumerate(held):
        if name == view:
            held.pop(index)
            _give_back(spark, frame, name)
            return


def _release(spark, held) -> None:
    """Release all persisted relations on every exit path."""

    while held:
        frame, view = held.pop()
        _give_back(spark, frame, view)


def _give_back(spark, frame, view: str) -> None:
    """Suppress cleanup failures so they never mask the load outcome."""

    if frame is not None:
        try:
            frame.unpersist()
        except Exception:  # noqa: BLE001 - see above
            pass
    try:
        spark.catalog.dropTempView(view)
    except Exception:  # noqa: BLE001 - see above
        pass


# --- evidence ----------------------------------------------------------------


def _drop_evidence(spark, names) -> None:
    """Remove evidence from the preceding faulted load before this run writes."""

    for role in ("reject", "delete", "staging"):
        spark.sql(f"DROP TABLE IF EXISTS {names[role]}")


def _keep_evidence(spark, names, kept: set, **relations) -> None:
    """Write each failure-evidence relation once as a Delta table."""

    for role, view in relations.items():
        if view is None or role in kept:
            continue
        with _exact_case(spark):
            spark.sql(
                f"CREATE TABLE {names[role]} USING delta {COLUMN_MAPPING} AS "
                f"SELECT * FROM {view}"
            )
        # Recorded after the write, so being in ``kept`` means the table is
        # there. A write that failed leaves the role to be attempted again.
        kept.add(role)


def _keep_unclassified_evidence(spark, names, kept: set, evidence: dict) -> None:
    """Try every evidence write without masking the original failure."""

    for role, view in evidence.items():
        try:
            _keep_evidence(spark, names, kept, **{role: view})
        except Exception:  # noqa: BLE001 - see above
            pass


# --- helpers -----------------------------------------------------------------


def _count(spark, relation: str) -> int:
    return int(spark.sql(f"SELECT count(*) AS n FROM {relation}").collect()[0]["n"])


def _comparison_columns(contract: LoadContract, columns) -> tuple[str, ...]:
    """Default to target business columns when Spark SQL inferred the schema."""

    if contract.comparison_columns:
        return contract.comparison_columns
    return tuple(column for column in columns if column not in contract.primary_key)


def _business_columns(spark, target: str) -> tuple[tuple[str, ...], dict[str, str]]:
    """Read current target business columns and signature-relevant Spark types."""

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
    produced = set(frame.columns)
    missing = [name for name in columns if name not in produced]
    if missing:
        raise LoadError(
            f"{contract.qualified}: staged rows are missing table columns "
            f"{', '.join(repr(name) for name in missing)}; return every declared "
            "column from read(), using the exact names"
        )
    key_missing = [name for name in contract.primary_key if name not in produced]
    if key_missing:
        raise LoadError(
            f"{contract.qualified}: staged rows are missing primary key columns "
            f"{', '.join(repr(name) for name in key_missing)}; return them from read()"
        )


def _clean(name: str) -> str:
    return name.replace(".", "_").replace("`", "").replace(" ", "_").replace("-", "_")


__all__ = [
    "INTOLERANT_MESSAGE",
    "OPERATION_COLUMN",
    "MERGE_CONFLICT_MESSAGE",
    "TOLERATED_MESSAGE",
    "clear_table",
    "load_table",
]
