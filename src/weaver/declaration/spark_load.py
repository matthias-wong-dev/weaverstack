"""Spark SQL load generation — one runnable program per Spark SQL table.

The Warehouse counterpart is a stored procedure: it has parameters, variables
and ``if``. Spark SQL has none of those, so the same algorithm has to be written
without control flow, and that constraint shapes everything here.

Three consequences, each deliberate:

**The program is an ordered list of statements, not one statement.** Spark
executes one statement per ``spark.sql`` call, so the file is delimited and run
in order — the same shape the ``spark_sql_batch`` executor already installs
with. The last statement projects the result row.

**The counts are measured, not accumulated.** A procedure adds up ``@@rowcount``
as it goes; nothing here can hold a running total, so every count is a query
against the staged data taken *before* the writes, and materialised into a small
result table the final statement reads. Measuring first is also what makes the
counts describe the same instant as the decision they justify.

**Fault tolerance is a predicate, not a branch.** A commented literal reading
0 is substituted with 1 to tolerate rejects, and the valid-rows view is gated on
it: with rejects present and no tolerance the view is empty, so the merge and
the delete run against nothing and the target is untouched. An ``if`` that
Spark does not have becomes a ``where`` that it does — and the statement list
stays the same length either way, which is what keeps the program readable as
one thing rather than two.

Object names are ``{{object:Schema.Object}}`` tokens for the same reason the
build payloads are: a bundle must be destination-free so the same repository
generates the same bytes everywhere. The installed *file* is addressed, because
by then the destination is known — see
:class:`weaver.build_bundle.executors.load_file.LoadFileExecutor`.
"""

from __future__ import annotations

from ..runtime.load_contract import LoadContract
from ..runtime.load_result import RESULT_COLUMNS
from ..spark.tokens import object_token
from .dependencies import rewrite_sql_references
from .sql_shaping import split_statements, split_trailing_query
from .metadata import (
    AUDIT_COLUMNS,
    AUDIT_LIVE_DELETE_DATETIME,
    PYTHON,
    SesDocument,
    audit_column_name,
)

#: What separates one statement from the next in an installed program. A bare
#: ``;`` will not do: a statement may legitimately contain one inside a string
#: literal, and splitting on it would cut a program in half at the worst
#: possible moment. This marker is a SQL comment, so the file also stays valid
#: to paste into a notebook whole.
STATEMENT_DELIMITER = "-- weaver:statement"

#: The first line of every generated program. It is what the installer keys its
#: token expansion on: a generated load keeps its *authored* filename, so the
#: file cannot be recognised by its name — only by what it says it is.
GENERATED_LOAD_MARKER = "-- Weaver generated load"

#: The one hole the installer does *not* fill. Tolerance of rejects is a run's
#: own choice rather than anything about where the object lives, so it is
#: answered by whoever runs the program.
#:
#: Deliberately not a ``{{...}}`` token: that namespace belongs to the
#: installer's destination resolution, which refuses any token it does not
#: itself resolve — correctly, since a name left unresolved must never reach the
#: engine. This is a comment wrapped around a literal instead, so an installed
#: program is valid SQL with nothing substituted at all, and what it does then
#: is refuse — the safe answer for anyone who ran the file without choosing.
FAULT_TOLERANT_MARKER = "/*weaver:fault_tolerant*/"
FAULT_TOLERANT_DEFAULT = f"{FAULT_TOLERANT_MARKER}0"

#: The second answer a run gives, in the same comment-wrapped form and for the
#: same reason: an installed program is valid SQL with nothing substituted, and
#: what it does then is enforce the declared thresholds.
IGNORE_THRESHOLD_MARKER = "/*weaver:ignore_stability_threshold*/"
IGNORE_THRESHOLD_DEFAULT = f"{IGNORE_THRESHOLD_MARKER}0"

#: The rank a duplicate key gets, and the suffixes of the intermediate relations.
RANK_COLUMN = "__weaver_pk_row_number"
STAGING_SUFFIX = "_Staging"
REJECT_SUFFIX = "_Reject"
UPSERT_SUFFIX = "_Upsert"
RESULT_SUFFIX = "_LoadResult"
DELETE_SUFFIX = "_Delete"

#: What marks a row of the upsert set as new rather than merely changed. The
#: same column the Warehouse procedure and the Python load use, so one query
#: reads a change set whichever engine produced it.
IS_NEW_COLUMN = "_Is new row"

#: Re-exported from the runtime so the generators and the Python loads write one
#: vocabulary. A reject table is read by people, and a Warehouse reject that said
#: "null primary key" beside a Delta one that said "blank_primary_key" would make
#: the same refusal look like two different problems.
from ..runtime.load_contract import (  # noqa: E402
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
)

#: Every table this program creates carries Delta column mapping, for the same
#: reason :func:`weaver.declaration.ddl._create_table_sql` does: a declared
#: column name may contain spaces, and Delta refuses those in a physical schema
#: unless mapping is on. Staging carries the author's own columns forward, so a
#: table created without it fails on exactly the declarations Weaver permits.
COLUMN_MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"

#: Banners marking where the author's own code sits in the generated program.
#: A generated artefact is read by people — usually when something has gone
#: wrong — and the first question is always "which of this did I write?".
PREPROCESSING_BANNER = "-- Pre-processing"
TRANSFORMATION_BANNER = "-- Data transformation (authored)"
POSTPROCESSING_BANNER = "-- Post-processing"

INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"


def generate_spark_load_program(document: SesDocument, body: str) -> str:
    """The runnable Spark SQL program that loads one table."""

    contract = LoadContract.from_document(document)
    names = _names(document)
    addressed = _addressed(body.strip().rstrip(";"))

    if contract.primary_key:
        statements = _keyed_program(names, addressed, contract)
    else:
        statements = _full_replace_program(names, addressed, contract)

    # The header must not quote the delimiter. It is a comment, but the splitter
    # looks for the marker anywhere, so a header that spelled it out would be
    # cut in half and its first line offered to Spark as a statement.
    header = (
        f"{GENERATED_LOAD_MARKER} for {document.qualified}.\n"
        f"-- Statements run in order, separated by the marker below.\n"
        f"-- Substitute {FAULT_TOLERANT_MARKER}0 with {FAULT_TOLERANT_MARKER}1 to load "
        "valid rows despite rejects. Unsubstituted, it refuses.\n"
    )
    joined = f"\n\n{STATEMENT_DELIMITER}\n\n".join(
        statement.strip() for statement in statements
    )
    # A delimiter after the header, so the header is a chunk of its own and the
    # splitter drops it. Without one it rides along with the first statement and
    # is re-sent to the engine on every run.
    return f"{header}\n{STATEMENT_DELIMITER}\n\n{joined}\n"


def statements_of(program: str) -> tuple[str, ...]:
    """Split an installed program back into the statements it is made of.

    The inverse of the join above, and the only supported way to read one: a
    caller that split on ``;`` would eventually cut through a string literal.
    """

    parts = []
    for chunk in program.split(STATEMENT_DELIMITER):
        statement = chunk.strip()
        # A chunk of nothing but comments is the file's header, or the tail of a
        # marker line — never something to hand to Spark, which would reject it
        # as a syntax error at end of input.
        if statement and not _is_all_comment(statement):
            parts.append(statement)
    return tuple(parts)


def _is_all_comment(statement: str) -> bool:
    return all(
        not line.strip() or line.lstrip().startswith("--")
        for line in statement.splitlines()
    )


# --- the two programs --------------------------------------------------------


def _keyed_program(names: dict, body: str, contract: LoadContract) -> list[str]:
    preamble, query = split_trailing_query(body)
    audit = delta_audit_names()
    business = _business_columns(contract)
    blank = blank_key_predicate(contract.primary_key)
    rejected = f"({blank} OR s.`{RANK_COLUMN}` > 1)"

    statements = [
        f"{PREPROCESSING_BANNER}\nDROP TABLE IF EXISTS {names['reject']}",
        f"DROP TABLE IF EXISTS {names['upsert']}",
        f"DROP TABLE IF EXISTS {names['staging']}",
        f"DROP TABLE IF EXISTS {names['result']}",
        # Only when this program creates one. Dropping a table it never makes
        # would leave a reader looking for the statement that creates it.
        *(
            [f"DROP TABLE IF EXISTS {names['delete']}"]
            if contract.deletes_absent_rows
            else []
        ),
        # The authored preamble, as written. A body may set a temporary view up
        # before selecting from it, and only the trailing query fills staging —
        # wrapping the whole body in a subquery would put a CREATE inside a FROM.
        *(f"{TRANSFORMATION_BANNER}\n{statement}" for statement in split_statements(preamble)),
        # Staging is a real table, not a view: the source query must run once,
        # and a view would re-run it for every count and again for the merge.
        f"{TRANSFORMATION_BANNER}\n"
        f"CREATE TABLE {names['staging']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT\n    s.*\n"
        f"  , row_number() OVER (\n"
        f"        PARTITION BY {_columns('s', contract.primary_key)}\n"
        f"        ORDER BY (SELECT NULL)\n"
        f"    ) AS `{RANK_COLUMN}`\n"
        f"FROM (\n{_indent(query, 4)}\n) AS s",
        f"{POSTPROCESSING_BANNER}\n"
        f"CREATE TABLE {names['reject']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT\n    {_columns('s', business)}\n"
        f"  , CASE WHEN {blank} THEN '{REASON_BLANK_PK}'\n"
        f"         ELSE '{REASON_DUPLICATE_PK}' END AS `{REJECTION_REASON}`\n"
        f"FROM {names['staging']} AS s\n"
        f"WHERE {rejected}",
        # The gate. With rejects present and no tolerance this view is empty, so
        # every write below it touches nothing and the target is left exactly as
        # it was — the branch a procedure would take, written as a predicate.
        f"CREATE OR REPLACE TEMP VIEW {names['valid']} AS\n"
        f"SELECT s.*\nFROM {names['staging']} AS s\n"
        f"WHERE NOT {rejected}\n"
        f"  AND {_tolerated(names)}",
        _upsert_table(names, contract, business),
        # After the upsert set, which it counts, and before any write — so the
        # counts and the changes they describe are one decision.
        _result_table(names, contract, rejected),
        # The stability gate, narrowing the upsert set the writes read. With a
        # breach and no tolerance this is empty, so the merge below touches
        # nothing and the target is left exactly as it was — the `if` a
        # procedure would use, written as a `where`.
        f"CREATE OR REPLACE TEMP VIEW {names['permitted']} AS\n"
        f"SELECT u.*\nFROM {names['upsert']} AS u\n"
        f"WHERE {_permitted(names)}",
        _merge(names, contract, business, audit),
    ]
    if contract.deletes_absent_rows:
        statements.extend(_delete_absent(names, contract))
    statements.append(_guard(names))
    statements.append(_final_select(names))
    return statements


def _full_replace_program(names: dict, body: str, contract: LoadContract) -> list[str]:
    """No key, so no match, no update and nothing to reject.

    The target's contents become the source's. There is no reject table at all
    here — rejection is a statement about keys, and there are none.
    """

    audit = delta_audit_names()
    business = _business_columns(contract)
    columns = ", ".join(f"`{name}`" for name in business)
    preamble, query = split_trailing_query(body)
    return [
        f"{PREPROCESSING_BANNER}\nDROP TABLE IF EXISTS {names['staging']}",
        f"DROP TABLE IF EXISTS {names['result']}",
        *(f"{TRANSFORMATION_BANNER}\n{statement}" for statement in split_statements(preamble)),
        f"{TRANSFORMATION_BANNER}\n"
        f"CREATE TABLE {names['staging']} USING delta {COLUMN_MAPPING} AS\n{query}",
        f"{POSTPROCESSING_BANNER}\n"
        f"CREATE TABLE {names['result']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT\n"
        f"    (SELECT count(*) FROM {names['staging']}) AS rows_read\n"
        f"  , (SELECT count(*) FROM {names['staging']}) AS rows_inserted\n"
        f"  , CAST(0 AS BIGINT) AS rows_updated\n"
        f"  , (SELECT count(*) FROM {names['target']}) AS rows_deleted\n"
        f"  , CAST(0 AS BIGINT) AS rows_rejected",
        f"DELETE FROM {names['target']}",
        f"INSERT INTO {names['target']} ({columns}, {_audit_list(audit)})\n"
        f"SELECT {columns}, {_audit_values(audit)}\n"
        f"FROM {names['staging']}",
        _final_select(names),
    ]


# --- statements --------------------------------------------------------------


def _result_table(names: dict, contract: LoadContract, rejected: str) -> str:
    """Every count, measured against the staged rows before any write.

    ``rows_inserted`` and ``rows_updated`` are what the merge *will* do, counted
    from the same predicates the merge uses, and ``rows_deleted`` likewise. A
    Delta merge does report its own metrics, but only through the table history,
    which would make reading them a second round trip against state that a
    concurrent write could have moved on.
    """

    valid = f"(SELECT * FROM {names['staging']} AS s WHERE NOT {rejected})"
    join = key_join("v", "t", contract.primary_key)
    tolerated = _tolerated(names)
    deleted = (
        f"    , CASE WHEN {tolerated} THEN (\n"
        f"          SELECT count(*) FROM {names['target']} AS t\n"
        f"          WHERE NOT EXISTS (SELECT 1 FROM {valid} AS v WHERE {join})\n"
        f"      ) ELSE 0 END AS proposed_deleted\n"
        if contract.deletes_absent_rows
        else "    , CAST(0 AS BIGINT) AS proposed_deleted\n"
    )
    return (
        f"CREATE TABLE {names['result']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT *\n"
        f"    , {_outcome_columns()}\n"
        f"FROM (\n"
        f"  SELECT *\n"
        f"    , {_threshold_predicate(names, contract)} AS within_thresholds\n"
        f"  FROM (\n"
        f"  SELECT\n"
        f"      (SELECT count(*) FROM {names['staging']}) AS rows_read\n"
        f"    , (SELECT count(*) FROM {names['target']}) AS target_rows\n"
        f"    , (SELECT count(*) FROM {names['upsert']} "
        f"WHERE `{IS_NEW_COLUMN}` = 1) AS proposed_inserted\n"
        f"    , (SELECT count(*) FROM {names['upsert']} "
        f"WHERE `{IS_NEW_COLUMN}` = 0) AS proposed_updated\n"
        f"{deleted}"
        f"    , (SELECT count(*) FROM {names['reject']}) AS rows_rejected\n"
        f"  ) AS proposed\n"
        f") AS decided"
    )


def _outcome_columns() -> str:
    """``succeeded`` and ``error_message``, derived where every reader sees them.

    Both are known before a single row moves, so they are settled with the
    counts rather than at the end — which is what lets the guard that raises and
    the row that reports read one decision.
    """

    return (
        f"      (rows_rejected = 0 AND within_thresholds) AS succeeded\n"
        f"    , CASE\n"
        f"        WHEN NOT within_thresholds THEN\n"
        f"          concat('the proposed change is over this object''s stability "
        f"thresholds: ',\n"
        f"                 proposed_deleted, ' deletes and ', proposed_updated,\n"
        f"                 ' updates against ', target_rows, ' rows; "
        f"the target was not modified')\n"
        f"        WHEN rows_rejected = 0 THEN CAST(NULL AS STRING)\n"
        f"        WHEN {FAULT_TOLERANT_DEFAULT} = 0 THEN '{INTOLERANT_MESSAGE}'\n"
        f"        ELSE '{TOLERATED_MESSAGE}'\n"
        f"      END AS error_message"
    )


def _guard(names: dict) -> str:
    """Raise when the run failed and was not asked to tolerate it.

    Native, because ``exec [_].[Load S.N]`` and ``.load()`` must fail the same
    way — a primitive that returned a quiet row where its sibling raised would
    make every caller special-case which one it was talking to.

    Safe at the end: both failing cases empty the relations the writes read, so
    nothing has been written by the time this runs.
    """

    return (
        f"{POSTPROCESSING_BANNER}\n"
        f"SELECT CASE\n"
        f"         WHEN succeeded THEN 'ok'\n"
        f"         WHEN {FAULT_TOLERANT_DEFAULT} = 1 THEN 'reported'\n"
        f"         ELSE raise_error(error_message)\n"
        f"       END AS guard\n"
        f"FROM {names['result']}"
    )


def _threshold_predicate(names: dict, contract: LoadContract) -> str:
    """Whether the proposed change is within what the object allows.

    Decided *once*, here, and recorded as a column — because three things need
    the answer: the writes that must not happen, the delete set that must stay
    empty, and the result that has to say so. Recomputing it in each would let
    them disagree, and a load that reported one thing and did another is the
    failure this whole guard exists to prevent.

    An explicit ``CASE`` rather than an ``OR`` chain, because SQL does not
    promise to short-circuit and the arithmetic divides by ``target_rows``. An
    empty target has no proportion to be a percentage of, and a first load into
    one is the case the guard must never stand in the way of.
    """

    return (
        f"CASE\n"
        f"        WHEN {IGNORE_THRESHOLD_DEFAULT} = 1 THEN true\n"
        f"        WHEN target_rows = 0 THEN true\n"
        f"        WHEN target_rows < {contract.stability_rows} THEN true\n"
        f"        ELSE proposed_deleted * 100.0 / target_rows "
        f"<= {contract.delete_threshold}\n"
        f"         AND proposed_updated * 100.0 / target_rows "
        f"<= {contract.update_threshold}\n"
        f"      END"
    )


def _upsert_table(names: dict, contract: LoadContract, business) -> str:
    """What this load has decided to change, materialised before it changes it.

    The Warehouse procedure and the Python load both build this table; the
    program used to derive the same set inline, three times, in three subqueries.
    Materialising it means the counts and the writes read one set rather than
    re-deriving it, and it survives the run — so what Weaver decided is
    inspectable afterwards, like what it staged and what it refused.

    A matched row appears only when a comparison column differs. Including every
    matched row would be simpler and wrong: it would rewrite the update
    timestamp of rows nothing changed, so "when did this row last change" would
    come to mean "when was this table last loaded".
    """

    join = key_join("s", "t", contract.primary_key)
    changed = changed_predicate("s", "t", contract)
    missing = f"t.`{contract.primary_key[0]}` IS NULL"
    return (
        f"CREATE TABLE {names['upsert']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT\n    {_columns('s', business)}\n"
        f"  , CASE WHEN {missing} THEN 1 ELSE 0 END AS `{IS_NEW_COLUMN}`\n"
        f"FROM {names['valid']} AS s\n"
        f"LEFT JOIN {names['target']} AS t ON {join}\n"
        f"WHERE {missing} OR ({changed})"
    )


def _merge(names: dict, contract: LoadContract, business, audit) -> str:
    """Insert the new rows and update the changed ones, from the upsert set.

    One statement rather than two, because Delta has no ``UPDATE ... FROM`` and a
    merge against a set whose rows are already classified applies exactly the
    change that set recorded.
    """

    join = key_join("s", "t", contract.primary_key)
    updates = ", ".join(
        f"t.`{name}` = s.`{name}`"
        for name in business
        if name not in contract.primary_key
    )
    update_set = ", ".join(
        part
        for part in (
            updates,
            f"t.`{audit[1]}` = current_timestamp()",
            f"t.`{audit[2]}` = {live_delete_literal()}",
        )
        if part
    )
    columns = ", ".join(f"`{name}`" for name in business)
    values = ", ".join(f"s.`{name}`" for name in business)
    return (
        f"MERGE INTO {names['target']} AS t\n"
        f"USING {names['permitted']} AS s\n"
        f"   ON {join}\n"
        f"WHEN MATCHED AND s.`{IS_NEW_COLUMN}` = 0 THEN UPDATE SET {update_set}\n"
        f"WHEN NOT MATCHED AND s.`{IS_NEW_COLUMN}` = 1 "
        f"THEN INSERT ({columns}, {_audit_list(audit)})\n"
        f"     VALUES ({values}, {_audit_values(audit)})"
    )


def _tolerated(names: dict) -> str:
    """Whether this run is permitted to write: no rejects, or tolerance asked for.

    One definition, used by the counts, the valid view and the delete key set.
    They must agree — a count computed under one condition and a write performed
    under another would report a load that did not happen.
    """

    return (
        f"({FAULT_TOLERANT_DEFAULT} = 1 "
        f"OR (SELECT count(*) FROM {names['reject']}) = 0)"
    )


def _permitted(names: dict) -> str:
    """The decision the result table already recorded.

    Read rather than recomputed, so the writes, the delete set and the reported
    result cannot disagree about whether this load was allowed to happen.
    """

    return f"(SELECT within_thresholds FROM {names['result']})"


def _delete_absent(names: dict, contract: LoadContract) -> list[str]:
    """Remove target rows the source stopped producing, in two statements.

    Delta refuses a subquery in ``DELETE``, and ``WHEN NOT MATCHED BY SOURCE``
    would be worse than unavailable — it would be dangerous. The valid view is
    empty whenever a run is refusing to write, and "not matched by source"
    against an empty source matches *every* target row, so the one case that
    must leave the target untouched would empty it instead.

    Materialising the keys first removes the ``DELETE`` restriction: the
    subquery lives in a ``CREATE TABLE AS``, where it is allowed.

    The gate has to be repeated here, and this is the sharp edge. Every other
    statement is made harmless by an empty valid view, but *this* one inverts
    it: "in the target and not in valid" selects everything precisely when valid
    is empty. So an intolerant run would delete the whole table — which is what
    a test caught, and why the tolerance condition is stated on the key set
    itself rather than inherited from the view.
    """

    join = key_join("v", "t", contract.primary_key)
    keys = ", ".join(f"t.`{c}`" for c in contract.primary_key)
    return [
        f"CREATE TABLE {names['delete']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT {keys}\n"
        f"FROM {names['target']} AS t\n"
        f"WHERE NOT EXISTS (\n"
        f"    SELECT 1 FROM {names['valid']} AS v WHERE {join}\n"
        f")\n"
        f"  AND {_tolerated(names)}\n"
        f"  AND {_permitted(names)}",
        f"MERGE INTO {names['target']} AS t\n"
        f"USING {names['delete']} AS d\n"
        f"   ON {key_join('d', 't', contract.primary_key)}\n"
        f"WHEN MATCHED THEN DELETE",
    ]


def _final_select(names: dict) -> str:
    """The result row, in the shape every transport reports.

    ``rows_deleted`` is reconciled from the target's own cardinality rather than
    taken from the delete driver: the driver says what the load *intended*, and
    this says what happened. The two differ whenever a key named for deletion
    was not there to begin with.
    """

    inserted = "CASE WHEN within_thresholds THEN proposed_inserted ELSE 0 END"
    return (
        f"SELECT\n"
        f"      succeeded\n"
        f"    , rows_read\n"
        f"    , {inserted} AS rows_inserted\n"
        f"    , CASE WHEN within_thresholds THEN proposed_updated ELSE 0 END\n"
        f"        AS rows_updated\n"
        f"    , target_rows + {inserted} - (SELECT count(*) FROM {names['target']})\n"
        f"        AS rows_deleted\n"
        f"    , rows_rejected\n"
        f"    , error_message\n"
        f"FROM {names['result']}"
    )


# --- names and fragments -----------------------------------------------------


def _names(document: SesDocument) -> dict:
    schema = document.object_id.schema
    obj = document.object_id.object
    return {
        "target": object_token(schema, obj),
        "staging": object_token(schema, obj + STAGING_SUFFIX),
        "reject": object_token(schema, obj + REJECT_SUFFIX),
        "upsert": object_token(schema, obj + UPSERT_SUFFIX),
        "result": object_token(schema, obj + RESULT_SUFFIX),
        "delete": object_token(schema, obj + DELETE_SUFFIX),
        # A temp view is session-scoped and unqualified: it is the one relation
        # here that is not a managed object, because it holds no rows of its own.
        "valid": f"weaver_valid_{schema}__{obj}".replace(" ", "_"),
        "permitted": f"weaver_permitted_{schema}__{obj}".replace(" ", "_"),
    }


def _business_columns(contract: LoadContract) -> tuple[str, ...]:
    """The columns a load writes.

    **This is wrong and is a known defect.** Comparison columns are the subset
    whose change means a matched row was updated; they are not the table's
    shape, and a declaration may narrow them to one column out of many. A table
    declaring ``Customer id, Customer name, Amount`` with
    ``Comparison columns: Amount`` therefore has ``Customer name`` dropped from
    staging, rejects, the upsert set, inserts and updates. The branch's own
    ``Sales.OrderSummary`` example is affected.

    The fix needs the *declared* shape, which ``LoadContract`` does not carry,
    and it has to work for a Spark SQL table that leaves its schema to be
    inferred at build — where the generator cannot know the columns at all.
    Projecting around the helper columns instead of naming them was tried and
    does not work on OSS Spark: ``SELECT * EXCEPT`` is a Databricks extension,
    and Delta's ``MERGE ... UPDATE SET *`` requires the source to carry every
    target column including ``row_insert_datetime``, which an update must not
    overwrite.

    So the shape of the fix is: carry the declared columns on the contract and
    use them here, and settle separately what an inferred table does — most
    likely reading its columns at install, as the Warehouse installer already
    reads ``sys.columns``.
    """

    return tuple(contract.primary_key) + tuple(
        column
        for column in contract.comparison_columns
        if column not in contract.primary_key
    )


def delta_audit_names() -> tuple[str, str, str]:
    return tuple(audit_column_name(logical, PYTHON) for logical in AUDIT_COLUMNS)


def _audit_list(audit) -> str:
    return ", ".join(f"`{name}`" for name in audit)


def _audit_values(audit) -> str:
    return f"current_timestamp(), current_timestamp(), {live_delete_literal()}"


def live_delete_literal() -> str:
    return f"CAST('{AUDIT_LIVE_DELETE_DATETIME}' AS TIMESTAMP)"


def key_join(left: str, right: str, columns) -> str:
    return " AND ".join(f"{left}.`{c}` = {right}.`{c}`" for c in columns)


def changed_predicate(left: str, right: str, contract: LoadContract) -> str:
    """Whether a matched row differs, null-safely.

    ``<=>`` rather than ``<>`` because a column going to or from null is a
    change, and ``<>`` answers null to that question — so a row that lost a
    value would silently never be updated.
    """

    comparison = [
        column
        for column in contract.comparison_columns
        if column not in contract.primary_key
    ]
    if not comparison:
        # Nothing to compare: every matched row is unchanged by definition, and
        # saying so as `false` keeps the merge's shape identical either way.
        return "false"
    return " OR ".join(
        f"NOT ({left}.`{c}` <=> {right}.`{c}`)" for c in comparison
    )


def blank_key_predicate(columns, alias: str = "s") -> str:
    """A key column that is null, empty or only spaces is not a key.

    Blank is rejected alongside null deliberately: a key of whitespace matches
    nothing a human would call a match, and letting it through would create a
    row nobody can find again.

    ``alias`` is empty when the predicate is applied to a frame rather than
    inside a join, where there is no relation to qualify.
    """

    prefix = f"{alias}." if alias else ""
    predicates = [
        f"nullif(trim(CAST({prefix}`{c}` AS STRING)), '') IS NULL" for c in columns
    ]
    if len(predicates) == 1:
        return predicates[0]
    return "(" + " OR ".join(predicates) + ")"


def _columns(alias: str, columns) -> str:
    return ", ".join(f"{alias}.`{c}`" for c in columns)


def _addressed(body: str) -> str:
    """Name every managed reference in the query, as the build payloads do."""

    def rewrite(reference):
        object_id = reference.object_id
        if object_id is None:
            return None
        return object_token(object_id.schema, object_id.object)

    return rewrite_sql_references(body, rewrite)


def _indent(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line.strip() else line for line in text.splitlines())


__all__ = [
    "COLUMN_MAPPING",
    "GENERATED_LOAD_MARKER",
    "FAULT_TOLERANT_DEFAULT",
    "FAULT_TOLERANT_MARKER",
    "REJECTION_REASON",
    "blank_key_predicate",
    "changed_predicate",
    "delta_audit_names",
    "key_join",
    "live_delete_literal",
    "INTOLERANT_MESSAGE",
    "STATEMENT_DELIMITER",
    "TOLERATED_MESSAGE",
    "generate_spark_load_program",
    "statements_of",
]
