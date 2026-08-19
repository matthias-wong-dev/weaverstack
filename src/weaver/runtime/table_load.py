"""Loading a Delta table — the mechanics behind ``Table.load()``.

The authored class proposes rows; this owns everything that happens to them.
``read()`` returns staging, and an incremental one may return
``(staging, deletes)``. Neither touches the target. A generated
``SparkSqlTable`` enters here too, so both authoring languages reconcile through
one implementation.

The first value is *staging*: unvalidated, with nothing yet rejected or
classified. It goes through the phases the Warehouse procedure runs: raw
staging, every refusal discovered, the rejection gate, the purge that makes
staging the clean incoming state, the change set the load has settled on, merge
uniqueness, the stability gate, then the target. What each phase means, and
where the two engines differ physically, is ``design/keyed-load.md``.

The change set is one relation, and every insert, update and delete is a row in
it saying which it is. It is settled before anything moves, so the gates judge
exactly the rows the mutations then act on, one grouped pass answers all three
counts, and one merge writes the inserts and the updates together.

Each phase's result is a **persisted Spark relation** named by a temporary view,
not a Delta table. The reconciliation needs the phases to be settled before the
target moves and needs to read their sizes; it does not need them to outlive the
session, and a Delta table per phase costs a write, a commit and a drop for
state nothing else ever reads. Durable Delta artefacts are written only when a
load ends with something to troubleshoot, whether a refusal at a gate or a
failure Weaver has no outcome for; see ``_keep_evidence``.

``Incremental`` chooses what a deletion means, and there is only ever one
answer. An incremental source is a window on the truth, so absence proves
nothing and the object states what went (``read()[1]``); with no claim there is
nothing to remove and no relation is derived to say so. A non-incremental source
is the whole truth, so absence is the statement, an explicit list is refused,
and the deletions fall out of the same join that classifies the writes. Whatever
settles them also feeds the stability count, so the guard cannot protect against
a number the load never intended to delete.

Written in SQL rather than the DataFrame API because
``tests/test_core_boundary.py`` forbids importing ``pyspark`` or ``delta``
anywhere in ``weaver``: every phase is a statement, and what is held is the
statement's result. ``fault_tolerant`` is Weaver's own addition: an intolerant
run returns before any target mutation.
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

#: The suffixes of the durable artefacts a faulted load leaves behind, matching
#: the Warehouse so one vocabulary describes a load whichever engine ran it.
#: These are evidence, not execution state: what did the source propose, what did
#: Weaver refuse and why, and what was it proposing to remove.
STAGING_SUFFIX = "_Staging"
REJECT_SUFFIX = "_Reject"
DELETE_SUFFIX = "_Delete"

#: Columns a working relation carries. Weaver's, and named so: they sit beside
#: the author's own columns wherever one is written out as evidence.
RANK_COLUMN = "__weaver_rank"
WORKING_SIGNATURE_COLUMN = "__weaver_signature"
SURVIVOR_COLUMN = "__weaver_survivor"

#: What one row of the settled change relation says this load decided about it.
#: Membership means something happens to the row; this says what.
OPERATION_COLUMN = "__weaver_operation"
INSERT_OP = "I"
UPDATE_OP = "U"
DELETE_OP = "D"

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
    """The state machine, top to bottom, one phase per step.

    Each derivation hands back a relation and the name later phases read it by.
    Nothing is written to the target until every gate has passed, and the durable
    artefacts are written only where an outcome owes an explanation.

    ``kept`` records what has been written out and ``evidence`` the relations a
    reader would want if this run stopped here. Both belong to the caller, which
    is where a failure with no outcome of its own reads them.
    """

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
    """Everything this load refuses, in one statement.

    One statement because the stages are sequential and the chain says so
    directly: each unique key reads the rows that survived the ones before it, so
    a row already refused never becomes the arbitrary survivor of a later group.

    Every scan is narrow. A duplicate is found by grouping, and the only window is
    over rows already known to sit in a duplicate primary key group.
    """

    chain, rejects = _validation_chain(staging_view, contract, columns, signature)
    union = "\nUNION ALL\n".join(f"SELECT * FROM {name}" for name in rejects)
    return _hold(spark, held, f"WITH {chain}\n{union}", target, "reject")


def _purge_staging(
    spark, held, target, staging_view, contract: LoadContract, columns, signature
) -> str:
    """The rows the refusals left, as the clean incoming state from here on.

    Assembled from the same chain discovery ran, so what survives and what was
    refused cannot disagree, which matters most where the choice is arbitrary, as
    it is inside a duplicate primary key group. There is no predicate that would
    do instead: rows sharing a primary key may be identical in every column.

    Only ever reached when something was refused, so an ordinary load derives
    staging once. The relation this supersedes is given back once the survivors
    are materialised, its evidence already written.
    """

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


def _settled_changes(
    spark, held, names, staging_view, contract: LoadContract, columns, signature
) -> str:
    """Everything this load has decided to do to the target, in one relation.

    One row per change, each saying which of insert, update or delete it is.
    Settled as its own relation rather than left for the mutation to work out, so
    the size of the change can be read before a row moves, so the gates judge
    exactly the rows the mutations then act on, and so a run that stops has
    something to show for what it was going to do.

    A row carries the operation, the business columns and the signature the
    target will store, which is what the mutation needs. What an archive would
    add is the target's own values beside them, and the shape leaves room for
    that rather than reducing to counts.

    Two derivations, because absence means two different things. See
    :func:`_full_changes` and :func:`_incremental_changes`.
    """

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
    """What an incremental source proposes: new rows, and changed ones.

    A window on the truth, so a key it does not carry says nothing about the
    target's row and no absence is a deletion. What such a load removes is the
    explicit claim, settled separately.
    """

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
    """What a whole-truth source proposes: the target becomes clean staging.

    One outer join answers all three questions where a delete set and an upsert
    set asked the same join twice. A key only staging holds is an insert, one
    only the target holds is a deletion, and one both hold is an update when the
    signatures differ and nothing at all when they agree.

    A delete row carries its key and nothing else. The key is what retires the
    row and what ``_Delete`` is written from; the target's own values are the
    archive payload this shape leaves room for rather than reads today.
    """

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
    """How many rows this load inserts, updates and deletes.

    One grouped pass. The relation already says what each row is, so the three
    counts are a partition of it rather than three questions, and the same action
    materialises the relation the gates and the mutations then read.
    """

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
    """The keys the settled changes retire, under the name a reader knows.

    A projection of a relation that is already settled, so nothing is computed
    here. The delete mutation reads it and ``_Delete`` evidence is written from
    it, which keeps that artefact the list of keys it has always been.
    """

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
    """The one question an incremental load with unique keys has to ask.

    If every settled change were applied, would a declared unique key still be
    held by another target row? A non-incremental load never asks: it
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
    """Write the new rows and the changed ones, in one merge.

    The classification is settled, so this consumes it rather than working out
    again which rows differ: one pass over the target rather than an insert and a
    merge over the same relation, and a row's own operation decides which clause
    acts on it.

    An inserted row is stamped inserted, updated and live; an updated row keeps
    the insert time it already had. Both carry the signature the classification
    computed, which is what makes the next load see them as unchanged.
    """

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
    """Which delete claim this object made, refusing the one it may not make.

    ``Incremental`` decides exclusively. A non-incremental source is the whole
    truth, so a row's absence from it is what retires the row, and a second value
    states the same thing again.

    Refused on the value being there at all. Reading it to find out whether it
    holds any rows is a Spark job run to learn that a frame returned nothing, and
    an incremental table's claim is taken as a claim for the same reason: what
    the target loses is settled by the reconciliation rather than by whether the
    author's frame was empty.
    """

    if contract.incremental:
        return deletes
    if deletes is not None:
        raise LoadError(
            f"{contract.qualified}: a non-incremental table returns staging on "
            "its own. The source is the whole truth, so a row's absence from it "
            "is what retires the row. Return the staging frame, or declare "
            "Incremental: true."
        )
    return None


def _claimed_deletes(spark, held, names, staging_view, contract: LoadContract, deletes):
    """The keys an incremental load's claim actually retires, settled first.

    Reached only where the object returned a claim. A number obtained by deleting
    would be a report rather than a check, and the guard exists to decide not to
    delete.

    Narrowed twice. Only keys the target holds, because a delete for a row that
    was never there is not a deletion, and counting it would make the guard
    protect against work the load was never going to do. And only keys clean
    staging no longer carries: a key the source still produces is not retired,
    whether or not its row changed, so the claim gives it up and the row is
    loaded normally, which keeps its insert time and leaves an unchanged row
    alone.
    """

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
    """Remove exactly the keys the driver settled on."""

    spark.sql(
        f"MERGE INTO {names['target']} AS t USING {delete_view} AS d "
        f"ON {key_join('d', 't', contract.primary_key)} WHEN MATCHED THEN DELETE"
    )


def _full_replace(spark, names, staging_view, columns, rows_read: int) -> LoadResult:
    """No key, so no row can be matched: the target's contents become these.

    The one path that writes staging to Delta on its way through, and the reason
    is that it empties the target first. A persisted relation is recomputed from
    its source if its cache is lost, and this is the only phase where that source
    could be read after the target it may depend on has been emptied.
    """

    audit = delta_audit_names()
    named = qualified("", columns)
    audit_columns = qualified("", audit)
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


def _hold(spark, held, sql: str, target: str, role: str):
    """Run one statement, keep its result in Spark, and name it for the next phase.

    A persisted relation, not a table. Every phase after this one reads it by the
    view name, the caller forces it with the count it wanted anyway, and nothing
    outside the load ever needs it. Registered under the object's own name, so two
    loads in one session cannot read each other's phases.
    """

    frame = spark.sql(sql).persist()
    view = "weaver_" + role + "_" + _clean(target)
    frame.createOrReplaceTempView(view)
    held.append((frame, view))
    return frame, view


def _register(spark, held, frame, target: str, role: str) -> str:
    """A temporary view over a frame the caller made, so SQL can name it.

    ``read()``'s own output and an explicit delete claim arrive as frames rather
    than statements. Nothing is persisted here: what is held is the relation
    derived from it.
    """

    view = "weaver_" + role + "_" + _clean(target)
    frame.createOrReplaceTempView(view)
    held.append((None, view))
    return view


def _name(spark, held, sql: str, target: str, role: str) -> str:
    """Name a statement's result without materialising it.

    For a projection of a relation that is already settled and persisted, where
    what the name buys is a shape a reader and a mutation both know. Nothing is
    computed here: whatever reads it computes it, from the phase underneath.
    """

    view = "weaver_" + role + "_" + _clean(target)
    spark.sql(sql).createOrReplaceTempView(view)
    held.append((None, view))
    return view


def _give_back_one(spark, held, view: str) -> None:
    """Release one relation early, once no later phase will read it."""

    for index, (frame, name) in enumerate(held):
        if name == view:
            held.pop(index)
            _give_back(spark, frame, name)
            return


def _release(spark, held) -> None:
    """Release everything this load kept in Spark, however it ended.

    Reached on a clean load, on a refusal at any gate, and on an unexpected
    failure. An evicted cache would be recomputed rather than lost, so what this
    prevents is a finished load holding executor memory.
    """

    while held:
        frame, view = held.pop()
        _give_back(spark, frame, view)


def _give_back(spark, frame, view: str) -> None:
    """Unpersist one relation and drop its view, and never mask the real outcome.

    Cleanup runs while an exception may be on its way out. A failure to release
    something is not the failure worth reporting, and stopping here would leave
    the rest of the load's relations held.
    """

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
    """Remove what an earlier faulted run left, before this run writes any.

    Attempted rather than looked up. A missing table is the ordinary case, and an
    inventory read to avoid asking for a drop that does nothing would cost more
    than the drop.
    """

    for role in ("reject", "delete", "staging"):
        spark.sql(f"DROP TABLE IF EXISTS {names[role]}")


def _keep_evidence(spark, names, kept: set, **relations) -> None:
    """Write the relations a reader will want as Delta tables beside the object.

    Only for an outcome with something to troubleshoot, which is what separates
    evidence from execution state:

    .. code-block:: text

        _Staging   what did the source propose?
        _Reject    what did Weaver refuse, and why?
        _Delete    what was Weaver proposing to remove?

    Each is written once, and ``kept`` names what is on disk rather than what was
    asked for. A load can pass more than one gate that owes evidence, and the
    relation holding what the source proposed is released as soon as the purge
    supersedes it.
    """

    for role, view in relations.items():
        if view is None or role in kept:
            continue
        spark.sql(
            f"CREATE TABLE {names[role]} USING delta {COLUMN_MAPPING} AS "
            f"SELECT * FROM {view}"
        )
        # Recorded after the write, so being in ``kept`` means the table is
        # there. A write that failed leaves the role to be attempted again.
        kept.add(role)


def _keep_unclassified_evidence(spark, names, kept: set, evidence: dict) -> None:
    """Write what a load had settled when an unclassified failure ended it.

    The same three artefacts a refusal leaves, and never the classification,
    which describes work rather than a proposal. A relation already written stays as it
    was written.

    The failure on its way out is the one worth reporting, so each write is
    attempted on its own: one that cannot be made is left out and the others are
    still tried.
    """

    for role, view in evidence.items():
        try:
            _keep_evidence(spark, names, kept, **{role: view})
        except Exception:  # noqa: BLE001 - see above
            pass


# --- helpers -----------------------------------------------------------------


def _count(spark, relation: str) -> int:
    """How many rows a relation holds.

    Unfiltered, so Delta answers it from the transaction log rather than by
    scanning. It is still an action, which is why the stability gate asks only
    where its own precondition says the answer could matter.
    """

    return int(spark.sql(f"SELECT count(*) AS n FROM {relation}").collect()[0]["n"])


def _comparison_columns(contract: LoadContract, columns) -> tuple[str, ...]:
    """Which columns decide whether a matched row changed.

    What the declaration named, and otherwise every business column except the
    key — read from the target, because a Spark SQL table may infer its schema and
    then the contract has no column list to default from. Left empty, every row
    would sign identically and no change would ever be detected.

    The same rule the Warehouse installer applies against ``sys.columns``.
    """

    if contract.comparison_columns:
        return contract.comparison_columns
    return tuple(column for column in columns if column not in contract.primary_key)


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


def _clean(name: str) -> str:
    return name.replace(".", "_").replace("`", "").replace(" ", "_").replace("-", "_")


__all__ = [
    "INTOLERANT_MESSAGE",
    "OPERATION_COLUMN",
    "MERGE_CONFLICT_MESSAGE",
    "TOLERATED_MESSAGE",
    "load_table",
]
