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

**Fault tolerance is a predicate, not a branch.** ``{{fault_tolerant}}`` is
substituted with 0 or 1 before execution, and the valid-rows view is gated on
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

#: Substituted with 1 or 0 before execution. Not an object token: it is a run's
#: own choice rather than anything about where the object lives, so it is
#: resolved by whoever runs the program, not by the installer that placed it.
FAULT_TOLERANT_TOKEN = "{{fault_tolerant}}"

#: The rank a duplicate key gets, and the suffixes of the intermediate relations.
RANK_COLUMN = "__weaver_pk_row_number"
STAGING_SUFFIX = "_Staging"
REJECT_SUFFIX = "_Reject"
RESULT_SUFFIX = "_LoadResult"
DELETE_SUFFIX = "_Delete"

REJECTION_REASON = "Rejection reason"

#: Every table this program creates carries Delta column mapping, for the same
#: reason :func:`weaver.declaration.ddl._create_table_sql` does: a declared
#: column name may contain spaces, and Delta refuses those in a physical schema
#: unless mapping is on. Staging carries the author's own columns forward, so a
#: table created without it fails on exactly the declarations Weaver permits.
COLUMN_MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"

INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"


def generate_spark_load_program(document: SesDocument, body: str) -> str:
    """The runnable Spark SQL program that loads one table."""

    contract = LoadContract.from_document(document)
    names = _names(document)
    query = _addressed(body.strip().rstrip(";"))

    if contract.primary_key:
        statements = _keyed_program(names, query, contract)
    else:
        statements = _full_replace_program(names, query, contract)

    # The header must not quote the delimiter. It is a comment, but the splitter
    # looks for the marker anywhere, so a header that spelled it out would be
    # cut in half and its first line offered to Spark as a statement.
    header = (
        f"-- Weaver generated load for {document.qualified}.\n"
        f"-- Statements run in order, separated by the marker below.\n"
        f"-- Substitute {FAULT_TOLERANT_TOKEN} with 1 to load valid rows despite "
        "rejects, or 0 to refuse.\n"
    )
    joined = f"\n\n{STATEMENT_DELIMITER}\n\n".join(
        statement.strip() for statement in statements
    )
    return f"{header}\n{joined}\n"


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


def _keyed_program(names: dict, query: str, contract: LoadContract) -> list[str]:
    audit = delta_audit_names()
    business = _business_columns(contract)
    blank = blank_key_predicate(contract.primary_key)
    rejected = f"({blank} OR s.`{RANK_COLUMN}` > 1)"

    statements = [
        f"DROP TABLE IF EXISTS {names['reject']}",
        f"DROP TABLE IF EXISTS {names['staging']}",
        f"DROP TABLE IF EXISTS {names['result']}",
        f"DROP TABLE IF EXISTS {names['delete']}",
        # Staging is a real table, not a view: the source query must run once,
        # and a view would re-run it for every count and again for the merge.
        f"CREATE TABLE {names['staging']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT\n    s.*\n"
        f"  , row_number() OVER (\n"
        f"        PARTITION BY {_columns('s', contract.primary_key)}\n"
        f"        ORDER BY (SELECT NULL)\n"
        f"    ) AS `{RANK_COLUMN}`\n"
        f"FROM (\n{_indent(query, 4)}\n) AS s",
        f"CREATE TABLE {names['reject']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT\n    s.*\n"
        f"  , CASE WHEN {blank} THEN 'null primary key'\n"
        f"         ELSE 'duplicate primary key' END AS `{REJECTION_REASON}`\n"
        f"FROM {names['staging']} AS s\n"
        f"WHERE {rejected}",
        # Measured before anything is written, so the counts and the decision
        # they justify describe one instant.
        _result_table(names, contract, rejected),
        # The gate. With rejects present and no tolerance this view is empty, so
        # every write below it touches nothing and the target is left exactly as
        # it was — the branch a procedure would take, written as a predicate.
        f"CREATE OR REPLACE TEMP VIEW {names['valid']} AS\n"
        f"SELECT s.*\nFROM {names['staging']} AS s\n"
        f"WHERE NOT {rejected}\n"
        f"  AND (\n"
        f"        {FAULT_TOLERANT_TOKEN} = 1\n"
        f"     OR (SELECT count(*) FROM {names['reject']}) = 0\n"
        f"  )",
        _merge(names, contract, business, audit),
    ]
    if contract.deletes_absent_rows:
        statements.extend(_delete_absent(names, contract))
    statements.append(_final_select(names))
    return statements


def _full_replace_program(names: dict, query: str, contract: LoadContract) -> list[str]:
    """No key, so no match, no update and nothing to reject.

    The target's contents become the source's. There is no reject table at all
    here — rejection is a statement about keys, and there are none.
    """

    audit = delta_audit_names()
    business = _business_columns(contract)
    columns = ", ".join(f"`{name}`" for name in business)
    return [
        f"DROP TABLE IF EXISTS {names['staging']}",
        f"DROP TABLE IF EXISTS {names['result']}",
        f"CREATE TABLE {names['staging']} USING delta {COLUMN_MAPPING} AS\n{query}",
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
    changed = changed_predicate("v", "t", contract)
    tolerated = f"({FAULT_TOLERANT_TOKEN} = 1 OR (SELECT count(*) FROM {names['reject']}) = 0)"
    deleted = (
        f"    , CASE WHEN {tolerated} THEN (\n"
        f"          SELECT count(*) FROM {names['target']} AS t\n"
        f"          WHERE NOT EXISTS (SELECT 1 FROM {valid} AS v WHERE {join})\n"
        f"      ) ELSE 0 END AS rows_deleted\n"
        if contract.deletes_absent_rows
        else "    , CAST(0 AS BIGINT) AS rows_deleted\n"
    )
    return (
        f"CREATE TABLE {names['result']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT\n"
        f"      (SELECT count(*) FROM {names['staging']}) AS rows_read\n"
        f"    , CASE WHEN {tolerated} THEN (\n"
        f"          SELECT count(*) FROM {valid} AS v\n"
        f"          WHERE NOT EXISTS (SELECT 1 FROM {names['target']} AS t WHERE {join})\n"
        f"      ) ELSE 0 END AS rows_inserted\n"
        f"    , CASE WHEN {tolerated} THEN (\n"
        f"          SELECT count(*) FROM {valid} AS v\n"
        f"          JOIN {names['target']} AS t ON {join}\n"
        f"          WHERE {changed}\n"
        f"      ) ELSE 0 END AS rows_updated\n"
        f"{deleted}"
        f"    , (SELECT count(*) FROM {names['reject']}) AS rows_rejected"
    )


def _merge(names: dict, contract: LoadContract, business, audit) -> str:
    """Insert new rows and update changed ones, in Delta's own single operation.

    A matched row is only updated when a comparison column differs. Updating
    every matched row would be simpler and wrong: it would rewrite the update
    timestamp of rows nothing changed, so "when did this row last change" would
    come to mean "when was this table last loaded".
    """

    join = key_join("s", "t", contract.primary_key)
    changed = changed_predicate("s", "t", contract)
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
        f"USING {names['valid']} AS s\n"
        f"   ON {join}\n"
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {update_set}\n"
        f"WHEN NOT MATCHED THEN INSERT ({columns}, {_audit_list(audit)})\n"
        f"     VALUES ({values}, {_audit_values(audit)})"
    )


def _delete_absent(names: dict, contract: LoadContract) -> list[str]:
    """Remove target rows the source stopped producing, in two statements.

    Delta refuses a subquery in ``DELETE``, and ``WHEN NOT MATCHED BY SOURCE``
    would be worse than unavailable — it would be dangerous. The valid view is
    empty whenever a run is refusing to write, and "not matched by source"
    against an empty source matches *every* target row, so the one case that
    must leave the target untouched would empty it instead.

    Materialising the keys first removes both problems. The subquery lives in a
    ``CREATE TABLE AS``, where it is allowed, and an intolerant run produces an
    empty key set, so the merge below it deletes nothing.
    """

    join = key_join("v", "t", contract.primary_key)
    keys = ", ".join(f"t.`{c}`" for c in contract.primary_key)
    return [
        f"CREATE TABLE {names['delete']} USING delta {COLUMN_MAPPING} AS\n"
        f"SELECT {keys}\n"
        f"FROM {names['target']} AS t\n"
        f"WHERE NOT EXISTS (\n"
        f"    SELECT 1 FROM {names['valid']} AS v WHERE {join}\n"
        f")",
        f"MERGE INTO {names['target']} AS t\n"
        f"USING {names['delete']} AS d\n"
        f"   ON {key_join('d', 't', contract.primary_key)}\n"
        f"WHEN MATCHED THEN DELETE",
    ]


def _final_select(names: dict) -> str:
    """The result row, in the shape every transport reports.

    ``succeeded`` is derived here rather than stored, because it is not an
    independent fact: a load succeeded exactly when it rejected nothing.
    """

    columns = ", ".join(RESULT_COLUMNS[1:6])
    return (
        f"SELECT\n"
        f"      rows_rejected = 0 AS succeeded\n"
        f"    , {columns.replace(', ', chr(10) + '    , ')}\n"
        f"    , CASE WHEN rows_rejected = 0 THEN CAST(NULL AS STRING)\n"
        f"           WHEN rows_inserted = 0 AND rows_updated = 0 AND rows_deleted = 0\n"
        f"           THEN '{INTOLERANT_MESSAGE}'\n"
        f"           ELSE '{TOLERATED_MESSAGE}' END AS error_message\n"
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
        "result": object_token(schema, obj + RESULT_SUFFIX),
        "delete": object_token(schema, obj + DELETE_SUFFIX),
        # A temp view is session-scoped and unqualified: it is the one relation
        # here that is not a managed object, because it holds no rows of its own.
        "valid": f"weaver_valid_{schema}__{obj}".replace(" ", "_"),
    }


def _business_columns(contract: LoadContract) -> tuple[str, ...]:
    """The columns a load writes: the declaration's, never the audit ones.

    A Delta table has no identity column to exclude — that is a Warehouse
    declaration — so what remains is exactly what the author declared.
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
    "FAULT_TOLERANT_TOKEN",
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
