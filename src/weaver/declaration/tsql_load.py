"""Generate one Warehouse table's load procedure.

The build payload installs a procedure that derives target columns from
``sys.columns``. Procedure result counts use output parameters, and rejected
rows remain in the reject table for inspection.

A keyed load runs one state machine and the generated procedure shows it in
order: raw staging, every refusal discovered into ``_Reject``, the rejection
gate, the purge that makes staging the clean incoming state, ``_Delete``, an
``_Upsert`` of new and changed rows only, merge uniqueness, the stability gate,
then the target. What each phase means, and why, is
``design/keyed-load.md``; the Delta half of it is
:mod:`weaver.runtime.table_load`.
"""

from __future__ import annotations

from ..catalogue.tables import (
    BOOKMARK,
    CATALOGUE_SCHEMA,
)
from ..catalogue.tsql import identifier
from ..errors import DiscoveryError
from ..runtime.load_contract import (
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
    REJECTION_REASON_WIDTH,
    LoadContract,
    duplicate_unique_reason,
    null_column_reason,
)
from .metadata import (
    AUDIT_COLUMNS,
    AUDIT_LIVE_DELETE_DATETIME,
    SIGNATURE_COLUMN,
    SesDocument,
)
from .sql_shaping import insert_select_into, render_sql_template, temp_table_name
from .tsql_program import TsqlProgram, parse_tsql_program, validate_query_contract

#: Columns the generated procedure adds to a working relation. Named to be
#: unmistakably Weaver's: they sit beside the author's own columns, so a name an
#: author might have chosen would be a collision waiting to happen.
RANK_COLUMN = "__weaver_rank"
WORKING_SIGNATURE_COLUMN = "__weaver_signature"
SURVIVOR_COLUMN = "__weaver_survivor"

#: What marks a row of the upsert set as one the target does not yet hold.
#: Membership already means new or changed, so this only says which.
IS_NEW_COLUMN = "_Is new row"

#: The placeholder the installer fills with the canonical payload the row
#: signature is taken over. Left to install time because the payload has to name
#: each comparison column's physical type, and an inferred table's types are only
#: known once the table exists.
SIGNATURE_PAYLOAD = "__SIGNATURE_PAYLOAD__"

#: The procedure's result, as parameters rather than as a projection, and their
#: T-SQL types. One definition, read from both ends: the generator writes the
#: signature from it and :mod:`weaver.load_execution` declares locals to match,
#: so a field cannot be added to one and forgotten in the other.
#:
#: Ordered as :data:`weaver.runtime.load_result.RESULT_COLUMNS` is, and named
#: identically, because a caller reads the two as one contract.
RESULT_PARAMETERS = (
    ("succeeded", "bit"),
    ("rows_read", "bigint"),
    ("rows_inserted", "bigint"),
    ("rows_updated", "bigint"),
    ("rows_deleted", "bigint"),
    ("rows_rejected", "bigint"),
    ("error_message", "varchar(4000)"),
    ("bookmark_datetime", "datetime2(6)"),
    ("is_static_skip", "bit"),
)

#: The generated procedure's private parameter namespace, mapped back to the
#: stable logical result contract above. Callers project the logical names, so
#: this physical ABI never leaks into :class:`LoadResult`.
RESULT_PARAMETER_NAMES = {
    logical: f"weaver_{logical}" for logical, _type_name in RESULT_PARAMETERS
}

#: The physical names and T-SQL types passed to the generic SQL executor.
PROCEDURE_RESULT_PARAMETERS = tuple(
    (RESULT_PARAMETER_NAMES[logical], type_name)
    for logical, type_name in RESULT_PARAMETERS
)


def logical_result_row(row) -> dict:
    """Map a generated procedure's private output names to ``LoadResult`` names."""

    return {
        logical: row[RESULT_PARAMETER_NAMES[logical]]
        for logical, _type_name in RESULT_PARAMETERS
    }


#: The suffixes of the intermediate tables, in the object's own schema.
STAGING_SUFFIX = "_Staging"
UPSERT_SUFFIX = "_Upsert"
REJECT_SUFFIX = "_Reject"
DELETE_SUFFIX = "_Delete"

#: What a run reports when it refused to start, and when it went ahead anyway.
#: Both say rejects occurred; they differ in what happened to the target, which
#: is the only thing ``fault_tolerant`` changes.
INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"

#: What a run reports when its proposed changes do not describe a valid target.
#: Not a row-level reject and not governed by ``fault_tolerant``: the incoming
#: rows are individually fine, and it is the state they would leave that is not.
MERGE_CONFLICT_MESSAGE = (
    "the proposed changes would leave a declared unique key held by two rows, "
    "so the target was not modified"
)

#: Banners marking where the author's own code sits in the generated procedure.
#: A generated artefact is read when something has gone wrong, and the first
#: question is which of it the author wrote.
PREPROCESSING_BANNER = "/*-- Pre-processing --*/"
TRANSFORMATION_BANNER = "/*---- Data transformation ----*/"
END_TRANSFORMATION_BANNER = "/*---- End data transformation ----*/"
POSTPROCESSING_BANNER = "/*-- Post-processing --*/"

#: The physical Warehouse types a built table can hold, and the canonical text
#: each is spelled as before it enters the row signature. Read at install time
#: against ``sys.types``, so the payload is type-aware for an inferred table too.
#:
#: Each spelling is exact and stable for one value: a style is named wherever the
#: default rendering is locale- or precision-dependent, because a signature that
#: moved with the session's settings would report every row as changed.
_CANONICAL_TEXT = {
    "date": "convert(varchar(10), {column}, 23)",
    "datetime2": "convert(varchar(27), {column}, 126)",
    "time": "convert(varchar(16), {column}, 114)",
    "bit": "cast(cast({column} as int) as varchar(1))",
    "float": "convert(varchar(32), {column}, 3)",
    "real": "convert(varchar(32), {column}, 3)",
    "varbinary": "convert(varchar(max), {column}, 2)",
    "uniqueidentifier": "cast({column} as varchar(36))",
}

#: What every other type is spelled as. A Warehouse table holds no type whose
#: text form is ambiguous once the cases above are named: ``varchar`` is itself,
#: and the exact numerics render their declared scale.
_CANONICAL_FALLBACK = "cast({column} as varchar(max))"

#: The text of a null comparison value. It cannot be confused with a present
#: value, because a present value is written as its byte length, a colon, and
#: then itself, so it always begins with a digit.
_NULL_MARKER = "~"


def generate_tsql_load_script(
    document: SesDocument, body: str, *, procedure_name: str, item
) -> str:
    """The installer script for one Warehouse table's load procedure.

    ``body`` is the table's own query, the same text its build materialises to
    settle its shape. A load runs it for real.

    ``item`` is the logical Weaver item the table belongs to. A document knows
    its ``Schema.Object`` and not which item declares it, and the procedure needs
    both: its bookmark row is keyed by the same four-part identity the Registry
    uses, and the procedure maintains that row itself when it is run by hand.
    """

    contract = LoadContract.from_document(document)
    program = parse_tsql_program(body, what=document.qualified, error=DiscoveryError)
    validate_query_contract(
        program,
        what=document.qualified,
        primary_key=document.primary_key,
        incremental=document.is_incremental,
        error=DiscoveryError,
    )

    names = _table_names(document, procedure_name)
    claims_deletes = program.deletes is not None
    staging_sql = _staging_sql(names, program, contract)

    if contract.primary_key:
        load_body = _primary_key_body(names, contract, claims_deletes)
    else:
        load_body = _full_replace_body(names)

    procedure = render_sql_template(
        "load/load_procedure",
        load_procedure=names["procedure"],
        result_parameters=_result_parameters(),
        result_assignment=_indent(
            _result_assignment(
                # The instant this load began, reported only when the load was
                # clean. A caller advances a bookmark to an instant a load
                # established, and one that rejected a row established none.
                bookmark_datetime=(
                    "case when @weaver_error is null and @weaver_rows_rejected = 0 "
                    "then @weaver_load_datetime end"
                )
            ),
            4,
        ),
        bookmark_key=_indent(_bookmark_key(document, item, contract), 4),
        live_delete_datetime=AUDIT_LIVE_DELETE_DATETIME,
        preprocessing_banner=_indent(PREPROCESSING_BANNER, 4),
        postprocessing_banner=_indent(POSTPROCESSING_BANNER, 4),
        static_gate=_indent(_static_gate(contract), 4),
        start_artifact_cleanup=_indent(_cleanup(names, contract, claims_deletes), 4),
        staging_sql=_indent(staging_sql, 4),
        staging_table=names["staging"],
        target_table=names["target"],
        load_body=_indent(load_body, 4),
        end_artifact_cleanup=_indent(_end_cleanup(names, contract, claims_deletes), 4),
    ).rstrip()

    return render_sql_template(
        "load/install_load_procedure",
        column_metadata_sql=_column_metadata_sql(names, contract),
        procedure_template_sql_literal=_sql_literal(procedure),
    )


# --- the result, as a signature ----------------------------------------------


def _result_parameters() -> str:
    """The result fields, declared as optional outputs on the procedure.

    Optional so ``exec [_].[Load Sales.Customer];`` still works by hand without
    declaring seven variables first.
    """

    return "\n".join(
        f"  , @{RESULT_PARAMETER_NAMES[name]} {type_name} = null output"
        for name, type_name in RESULT_PARAMETERS
    )


def _result_assignment(**values: str) -> str:
    """Fill the output parameters, at one of the procedure's exits.

    Every exit assigns all of them: an unset field would read as its ``null``
    default and be indistinguishable from a real one. The defaults exist to make
    the parameters optional rather than to be observed.
    """

    defaults = {
        "succeeded": "cast(case when @weaver_error is null then 1 else 0 end as bit)",
        "rows_read": "@weaver_rows_read",
        "rows_inserted": "@weaver_rows_inserted",
        "rows_updated": "@weaver_rows_updated",
        "rows_deleted": "@weaver_rows_deleted",
        "rows_rejected": "@weaver_rows_rejected",
        "error_message": "@weaver_error",
        # Null unless a clean load actually ran. A caller advances a bookmark
        # only to an instant a load established. An exit that read nothing, being
        # a Static skip, a refused breach or a load that rejected rows, reports
        # none and the bookmark it already had stands.
        "bookmark_datetime": "null",
        # Reported rather than inferred from the counts: a Static skip and a load
        # that read an empty window are both a success with nothing moved, and
        # only the procedure knows which of the two happened.
        "is_static_skip": "cast(0 as bit)",
    }
    defaults.update(values)
    return "\n".join(
        f"set @{RESULT_PARAMETER_NAMES[name]} = {defaults[name]};"
        for name, _ in RESULT_PARAMETERS
    )


# --- the pieces of the procedure ---------------------------------------------


def _static_gate(contract: LoadContract) -> str:
    """The check a ``Static`` object makes before it does anything else.

    Baked into the procedure rather than performed by its caller, because the
    procedure is independently runnable: running it by hand must give the same
    answer an orchestrated run gets.

    The bookmark answers it, not the target's contents. What ``Static`` means is
    "load this once", and the record of whether that has happened is the
    bookmark, so a table populated by hand is still loaded and a table a clean
    load emptied is still skipped. Before the staging query, so a
    loaded object costs no source read.

    ``@reload`` passes through it: a reload asks for this one to be loaded again.

    A non-static object gets a comment rather than a disabled branch.
    """

    if not contract.static:
        return "-- Not static: this object is loaded on every run."
    seeded = _result_assignment(
        succeeded="cast(1 as bit)", is_static_skip="cast(1 as bit)"
    )
    return (
        "-- Static: loaded once. A bookmark row means a clean load has run for\n"
        "-- this incarnation, so this reports a successful load of nothing. A\n"
        "-- reload asks for it again.\n"
        "if @reload = 0 and @weaver_bookmark is not null\n"
        "begin\n"
        f"{_indent(seeded, 4)}\n"
        "    return;\n"
        "end;"
    )


def _bookmark_key(document: SesDocument, item, contract: LoadContract) -> str:
    """This object's bookmark row, read into a local before anything else.

    Only a ``Static`` object reads it, because only a ``Static`` object decides
    anything from it: every other load writes its bookmark at the end and never
    asks what it was. In every Warehouse but the catalogue's, this table is a
    view across databases, so the read a load does not need is a round trip it
    should not pay for.

    The identity is baked in: the procedure is one object's, so which row it
    means is a fact about the procedure rather than an argument to it.

    No row leaves the local null, and that is the answer rather than a missing
    one: no clean load has run since this object's current physical incarnation.
    """

    if not contract.static:
        return (
            "-- Not static: nothing here reads a bookmark, so nothing reads the table."
        )
    predicate = " and ".join(
        f"{identifier(BOOKMARK.public_name_of(column))} = {_key_literal(value)}"
        for column, value in _bookmark_identity(document, item).items()
    )
    return (
        f"select @weaver_bookmark = "
        f"{identifier(BOOKMARK.public_name_of('bookmark_datetime'))}\n"
        f"  from {_bookmark_table()}\n"
        f" where {predicate};"
    )


def _bookmark_table() -> str:
    return f"{identifier(CATALOGUE_SCHEMA)}.{identifier(BOOKMARK.name)}"


def _bookmark_identity(document: SesDocument, item) -> dict:
    """The four values that key this object's bookmark row.

    Built through the identity the catalogue writers use, so a procedure reads
    the row a run wrote. A Warehouse relation names no Lakehouse area, and the
    one rule is what says so.
    """

    from ..catalogue.claims import bookmark_row
    from .model import WeaverDocumentId

    if item is None:
        raise DiscoveryError(
            f"{document.qualified}: a load procedure is keyed by the logical item "
            "that declares it, and none was supplied"
        )
    return bookmark_row(WeaverDocumentId(item, document.object_id))


def _key_literal(value: str) -> str:
    return "N'" + _escape_literal(str(value)) + "'"


def _staging_sql(names: dict, program: TsqlProgram, contract: LoadContract) -> str:
    """Run the author's program, materialising each query it produces.

    The body is emitted in the order it was written, so a setup statement
    between two queries runs between them: an author may build a working table,
    stage from it, then build another and name the retired keys from that.
    """

    pieces = [TRANSFORMATION_BANNER]
    query_number = 0
    for statement in program.statements:
        if not statement.produces_result:
            pieces.append(f"{statement.sql};")
            continue
        query_number += 1
        if query_number == 1:
            pieces.append(_staging_table_sql(names, statement.sql, contract))
        else:
            pieces.append(_delete_claim_sql(names, statement.sql, contract))
    pieces.append(END_TRANSFORMATION_BANNER)
    return "\n\n".join(pieces)


def _staging_table_sql(names: dict, query: str, contract: LoadContract) -> str:
    """Materialise the object's rows, exactly as the author produced them.

    Staging carries business columns and nothing else. Weaver adds no rank and no
    signature here: what it needs of both is computed later, over the rows that
    survive validation, rather than over every row a source produced.

    A keyed load places ``INTO`` in the query by the same offset-exact transform
    the shape-only build uses, because ``with … select …`` is a legal statement
    and an illegal derived table, so a body opening with a CTE cannot be wrapped
    and has to be run as the statement it is.
    """

    if not contract.primary_key:
        return f"create table {names['staging']} as\n{query};"
    return f"{insert_select_into(query, names['staging'])};"


def _delete_claim_sql(names: dict, query: str, contract: LoadContract) -> str:
    """Settle which target rows the author's second query actually names.

    Narrowed here rather than at the delete, so the stability threshold is
    checked against what will really be removed rather than what was asked for:

    .. code-block:: text

        distinct      naming a key twice is one deletion, not two
        not blank     whitespace identifies no row a person would call a match
        in the target claiming a key that was never there deletes nothing

    The table is both the count and the driver, so ``rows_deleted`` reports rows
    actually removed. The target is read before anything modifies it, which
    makes the guard a decision not to start rather than an unwind.
    """

    claim = temp_table_name("#weaver_delete_claim", names["object"])
    keys = ", ".join(f"c.{_quote(column)}" for column in contract.primary_key)
    join = _join("d", "c", contract.primary_key)
    return (
        f"if object_id('tempdb..{claim}') is not null drop table {claim};\n"
        f"{insert_select_into(query, claim)};\n\n"
        f"create table {names['delete']} as\n"
        f"select distinct {keys}\n"
        f"from {names['target']} as c\n"
        f"inner join {claim} as d\n"
        f"    on {join}\n"
        f"where not ({_blank_key_predicate(contract.primary_key, alias='d')});\n\n"
        f"drop table {claim};"
    )


def _primary_key_body(names: dict, contract: LoadContract, claims_deletes: bool) -> str:
    has_delete = _has_delete_relation(contract, claims_deletes)
    return render_sql_template(
        "load/primary_key_body",
        reject_table=names["reject"],
        upsert_table=names["upsert"],
        staging_table=names["staging"],
        target_table=names["target"],
        signature_column=SIGNATURE_COLUMN,
        signature_expression=_signature_expression(),
        is_new_column=IS_NEW_COLUMN,
        rejection_reason=REJECTION_REASON,
        reason_width=REJECTION_REASON_WIDTH,
        reject_discovery=_reject_discovery(names, contract),
        duplicate_key_count=_duplicate_key_count(names, contract),
        staging_purge=_staging_purge(names, contract),
        delete_derivation=_delete_derivation(names, contract, claims_deletes),
        merge_uniqueness=_merge_uniqueness(names, contract, has_delete),
        query_target_join=_join("q", "t", contract.primary_key),
        target_upsert_join=_join("c", "u", contract.primary_key),
        target_missing_predicate=f"t.{_quote(contract.primary_key[0])} is null",
        missing_reconciliation=_reconciliation(names, contract, claims_deletes),
        prospective_deletes=_prospective_deletes(names, contract, claims_deletes),
        delete_threshold=contract.delete_threshold,
        update_threshold=contract.update_threshold,
        stability_rows=contract.stability_rows,
        intolerant_message=_escape_literal(INTOLERANT_MESSAGE),
        tolerated_message=_escape_literal(TOLERATED_MESSAGE),
        breach_result_assignment=_indent(
            # A refused load wrote nothing, so the three counts of what it wrote
            # are zero rather than whatever they had reached. rows_read stands:
            # the source really was read, which is how the breach was measured.
            _result_assignment(
                succeeded="cast(0 as bit)",
                rows_inserted="cast(0 as bigint)",
                rows_updated="cast(0 as bigint)",
                rows_deleted="cast(0 as bigint)",
            ),
            8,
        ),
    ).rstrip()


def _full_replace_body(names: dict) -> str:
    return render_sql_template(
        "load/full_replace_body",
        target_table=names["target"],
        staging_table=names["staging"],
    ).rstrip()


# --- the row signature -------------------------------------------------------


def _signature_expression() -> str:
    """The digest of one staged row's comparison state.

    ``N''`` opens the payload so the expression is complete even for a table
    whose comparison columns are empty. Every row then signs identically, which
    is what "nothing to compare" means.
    """

    return f"convert(varbinary(32), hashbytes('SHA2_256', N''{SIGNATURE_PAYLOAD}))"


# --- discovering what to refuse ----------------------------------------------


def _reject_discovery(names: dict, contract: LoadContract) -> str:
    """Everything this load refuses, in one statement.

    One statement because the stages are sequential and the chain says so
    directly: each unique key reads the rows that survived the ones before it, so
    a row already refused never becomes the arbitrary survivor of a later group.
    Splitting them would mean either mutating staging before the gate, or
    reading a half-written reject table to find out what had been refused.

    Every scan here is narrow. A duplicate is found by grouping, and the only
    window is over rows already known to sit in a duplicate primary key group.
    """

    ctes = [
        (
            "weaver_null_reject",
            f"select\n{_reject_projection(_violation_reason(contract))}\n"
            f"from {names['staging']} as s\n"
            f"where {_violation_predicate(contract)}",
        ),
        (
            "weaver_valid",
            f"select\n"
            f"    __STAGING_SELECT_COLUMNS__\n"
            f"  , {_signature_expression()} as {_quote(WORKING_SIGNATURE_COLUMN)}\n"
            f"from {names['staging']} as s\n"
            f"where not ({_violation_predicate(contract)})",
        ),
        (
            "weaver_duplicate_key",
            f"select {_bare_columns(contract.primary_key)}\n"
            f"from weaver_valid\n"
            f"group by {_bare_columns(contract.primary_key)}\n"
            f"having count(*) > 1",
        ),
        (
            "weaver_ranked_key",
            f"select\n"
            f"    __STAGING_SELECT_COLUMNS__\n"
            f"  , row_number() over (\n"
            f"        partition by {_aliased_columns('s', contract.primary_key)}\n"
            f"        order by s.{_quote(WORKING_SIGNATURE_COLUMN)}) "
            f"as {_quote(RANK_COLUMN)}\n"
            f"from weaver_valid as s\n"
            f"inner join weaver_duplicate_key as d\n"
            f"    on {_join('d', 's', contract.primary_key)}",
        ),
        (
            "weaver_key_reject",
            f"select\n{_reject_projection(_reason_literal(REASON_DUPLICATE_PK))}\n"
            f"from weaver_ranked_key as s\n"
            f"where s.{_quote(RANK_COLUMN)} > 1",
        ),
    ]
    rejects = ["weaver_null_reject", "weaver_key_reject"]

    if contract.unique_keys:
        ctes.append(
            (
                "weaver_unique_key",
                # One row per surviving primary key. From here on the key
                # identifies a row, which is what lets a unique key name its
                # losers by key rather than by materialising them.
                f"select __STAGING_SELECT_COLUMNS__\n"
                f"from weaver_valid as s\n"
                f"where not exists (\n"
                f"    select 1 from weaver_duplicate_key as d\n"
                f"    where {_join('d', 's', contract.primary_key)}\n"
                f")\n"
                f"union all\n"
                f"select __STAGING_SELECT_COLUMNS__\n"
                f"from weaver_ranked_key as s\n"
                f"where s.{_quote(RANK_COLUMN)} = 1",
            )
        )
        source = "weaver_unique_key"
        for index, unique_key in enumerate(contract.unique_keys, start=1):
            last = index == len(contract.unique_keys)
            ctes.extend(
                _unique_key_ctes(
                    contract,
                    unique_key,
                    index=index,
                    source=source,
                    followed=not last,
                )
            )
            rejects.append(f"weaver_unique_{index}_reject")
            source = f"weaver_unique_{index}_survivor"

    chain = ",\n".join(f"{name} as (\n{_indent(sql, 4)}\n)" for name, sql in ctes)
    union = "\nunion all\n".join(f"select * from {name}" for name in rejects)
    return f";with {chain}\ninsert into {names['reject']}\n{union};"


def _reject_projection(reason: str) -> str:
    """One refused row, and why: the staged row itself plus the reason."""

    return f"    __STAGING_SELECT_COLUMNS__\n  , {reason} as {_quote(REJECTION_REASON)}"


def _reason_literal(reason: str) -> str:
    return f"cast('{reason}' as varchar({REJECTION_REASON_WIDTH}))"


def _unique_key_ctes(
    contract: LoadContract,
    unique_key: tuple[str, ...],
    *,
    index: int,
    source: str,
    followed: bool,
) -> list[tuple[str, str]]:
    """One unique key's duplicate group, its losers, and what survives it.

    A row whose key tuple contains a null does not take part: a null is not a
    value, so two rows carrying one are not two rows claiming the same thing.
    ``group by`` would put them in one group, which would refuse rows the
    declaration permits.

    Which row survives a group is arbitrary and settled cheaply. A single-column
    primary key gives an aggregate to settle it with; a composite one has none,
    so those groups are ranked, over the duplicate groups alone and never over
    the whole population.
    """

    reason = duplicate_unique_reason(unique_key)
    participates = " and ".join(
        f"s.{_quote(column)} is not null" for column in unique_key
    )
    bare_participates = " and ".join(
        f"{_quote(column)} is not null" for column in unique_key
    )
    ctes: list[tuple[str, str]] = []

    if len(contract.primary_key) == 1:
        key = _quote(contract.primary_key[0])
        ctes.append(
            (
                f"weaver_unique_{index}_duplicate",
                f"select\n"
                f"    {_bare_columns(unique_key)}\n"
                f"  , min({key}) as {_quote(SURVIVOR_COLUMN)}\n"
                f"from {source}\n"
                f"where {bare_participates}\n"
                f"group by {_bare_columns(unique_key)}\n"
                f"having count(*) > 1",
            )
        )
        ctes.append(
            (
                f"weaver_unique_{index}_reject",
                f"select\n{_reject_projection(_reason_literal(reason))}\n"
                f"from {source} as s\n"
                f"inner join weaver_unique_{index}_duplicate as d\n"
                f"    on {_join('d', 's', unique_key)}\n"
                f"where s.{key} <> d.{_quote(SURVIVOR_COLUMN)}",
            )
        )
    else:
        ctes.append(
            (
                f"weaver_unique_{index}_duplicate",
                f"select {_bare_columns(unique_key)}\n"
                f"from {source}\n"
                f"where {bare_participates}\n"
                f"group by {_bare_columns(unique_key)}\n"
                f"having count(*) > 1",
            )
        )
        ctes.append(
            (
                f"weaver_unique_{index}_ranked",
                f"select\n"
                f"    __STAGING_SELECT_COLUMNS__\n"
                f"  , row_number() over (\n"
                f"        partition by {_aliased_columns('s', unique_key)}\n"
                f"        order by {_aliased_columns('s', contract.primary_key)}) "
                f"as {_quote(RANK_COLUMN)}\n"
                f"from {source} as s\n"
                f"inner join weaver_unique_{index}_duplicate as d\n"
                f"    on {_join('d', 's', unique_key)}\n"
                f"where {participates}",
            )
        )
        ctes.append(
            (
                f"weaver_unique_{index}_reject",
                f"select\n{_reject_projection(_reason_literal(reason))}\n"
                f"from weaver_unique_{index}_ranked as s\n"
                f"where s.{_quote(RANK_COLUMN)} > 1",
            )
        )

    if followed:
        ctes.append(
            (
                f"weaver_unique_{index}_survivor",
                f"select __STAGING_SELECT_COLUMNS__\n"
                f"from {source} as s\n"
                f"where not exists (\n"
                f"    select 1 from weaver_unique_{index}_reject as r\n"
                f"    where {_join('r', 's', contract.primary_key)}\n"
                f")",
            )
        )
    return ctes


def _violation_predicate(contract: LoadContract, alias: str = "s") -> str:
    """A row that cannot be loaded whatever else is true of it.

    An unusable primary key, and a declared not-null column left empty. Only
    declared ones: a business column is nullable unless the object said
    otherwise, so checking every column would refuse rows the declaration
    permits.
    """

    prefix = f"{alias}." if alias else ""
    predicates = [_blank_key_predicate(contract.primary_key, alias=alias)]
    predicates.extend(
        f"{prefix}{_quote(column)} is null" for column in contract.not_null_columns
    )
    return "\n   or ".join(predicates)


def _violation_reason(contract: LoadContract, alias: str = "s") -> str:
    """Which of those a row failed, taking the first that applies.

    One reason per refused row. A row that is wrong twice over is still one row
    the load will not take, and counting it twice would let it weigh twice
    against the rejection threshold.
    """

    width = REJECTION_REASON_WIDTH
    if not contract.not_null_columns:
        return f"cast('{REASON_BLANK_PK}' as varchar({width}))"
    branches = [
        f"        when {_blank_key_predicate(contract.primary_key, alias=alias)}\n"
        f"            then cast('{REASON_BLANK_PK}' as varchar({width}))"
    ]
    branches.extend(
        f"        when {alias}.{_quote(column)} is null\n"
        f"            then cast('{null_column_reason(column)}' as varchar({width}))"
        for column in contract.not_null_columns
    )
    return "case\n" + "\n".join(branches) + "\n    end"


def _duplicate_key_count(names: dict, contract: LoadContract) -> str:
    """How many rows lost a duplicate primary key group.

    Read from the reject table, so the staging purge knows whether it has any
    physical duplicates to remove without asking staging a second time.
    """

    return (
        f"select @weaver_duplicate_keys = count(*)\n"
        f"from {names['reject']}\n"
        f"where {_quote(REJECTION_REASON)} = '{REASON_DUPLICATE_PK}';"
    )


# --- staging becomes the clean incoming state --------------------------------


def _staging_purge(names: dict, contract: LoadContract) -> str:
    """Remove the refused rows, once the gate has let the load continue.

    Nothing here runs for a load that refused nothing, which is the ordinary
    case: staging is then already the clean incoming state.

    In order, and the order is what makes it agree with discovery. Unusable rows
    go first, so the duplicate ranking sees the same population discovery ranked;
    each unique key then reads a staging table the keys before it have already
    been taken out of, which is the sequence the chain expressed with CTEs.
    """

    steps = [
        f"delete from {names['staging']}\nwhere {_violation_predicate(contract, '')};"
    ]
    steps.append(
        # Fabric will delete through a CTE only when it reads one base table, so
        # the rank cannot be narrowed to duplicate groups by joining. It is
        # narrowed by not running at all unless a duplicate was found.
        f"if @weaver_duplicate_keys > 0\n"
        f"begin\n"
        f"{_indent(_ranked_purge(names, contract), 4)}\n"
        f"end;"
    )
    steps.extend(
        _unique_key_purge(names, contract, unique_key)
        for unique_key in contract.unique_keys
    )
    body = "\n\n".join(steps)
    return f"if @weaver_rows_rejected > 0\nbegin\n{_indent(body, 4)}\nend;"


def _ranked_purge(names: dict, contract: LoadContract) -> str:
    """Keep one physical row per duplicate primary key group.

    Ordered by the row signature rather than arbitrarily, so this keeps the row
    discovery kept: an arbitrary order would be free to choose differently, and
    the reject table would then name a row the load had gone on to write.

    A delete through a ranked CTE, because rows sharing a key may be identical
    in every column and no predicate can tell one of them from the other.
    """

    return (
        f";with weaver_ranked as (\n"
        f"    select row_number() over (\n"
        f"        partition by {_aliased_columns('s', contract.primary_key)}\n"
        f"        order by {_signature_expression()}) as {_quote(RANK_COLUMN)}\n"
        f"    from {names['staging']} as s\n"
        f")\n"
        f"delete from weaver_ranked where {_quote(RANK_COLUMN)} > 1;"
    )


def _unique_key_purge(
    names: dict, contract: LoadContract, unique_key: tuple[str, ...]
) -> str:
    """Remove the rows that lost one unique key's duplicate groups.

    By key: staging holds one row per primary key by now, so naming the losers is
    enough and nothing has to be materialised to identify them.
    """

    key_columns = _bare_columns(unique_key)
    participates = " and ".join(
        f"{_quote(column)} is not null" for column in unique_key
    )
    if len(contract.primary_key) == 1:
        key = _quote(contract.primary_key[0])
        loser = (
            f"    select s.{key}\n"
            f"    from {names['staging']} as s\n"
            f"    inner join weaver_duplicate as d\n"
            f"        on {_join('d', 's', unique_key, indent=8)}\n"
            f"    where s.{key} <> d.{_quote(SURVIVOR_COLUMN)}"
        )
        duplicate = (
            f"    select\n"
            f"        {key_columns}\n"
            f"      , min({key}) as {_quote(SURVIVOR_COLUMN)}\n"
            f"    from {names['staging']}\n"
            f"    where {participates}\n"
            f"    group by {key_columns}\n"
            f"    having count(*) > 1"
        )
    else:
        keys = _aliased_columns("s", contract.primary_key)
        loser = (
            f"    select {keys}\n"
            f"    from (\n"
            f"        select\n"
            f"            {_aliased_columns('s', contract.primary_key)}\n"
            f"          , row_number() over (\n"
            f"                partition by {_aliased_columns('s', unique_key)}\n"
            f"                order by {_aliased_columns('s', contract.primary_key)}) "
            f"as {_quote(RANK_COLUMN)}\n"
            f"        from {names['staging']} as s\n"
            f"        inner join weaver_duplicate as d\n"
            f"            on {_join('d', 's', unique_key, indent=12)}\n"
            f"        where {' and '.join(f's.{_quote(c)} is not null' for c in unique_key)}\n"
            f"    ) as s\n"
            f"    where s.{_quote(RANK_COLUMN)} > 1"
        )
        duplicate = (
            f"    select {key_columns}\n"
            f"    from {names['staging']}\n"
            f"    where {participates}\n"
            f"    group by {key_columns}\n"
            f"    having count(*) > 1"
        )
    return (
        f";with weaver_duplicate as (\n{duplicate}\n),\n"
        f"weaver_loser as (\n{loser}\n)\n"
        f"delete s\n"
        f"from {names['staging']} as s\n"
        f"where exists (\n"
        f"    select 1 from weaver_loser as l where {_join('l', 's', contract.primary_key)}\n"
        f");"
    )


# --- what leaves the target --------------------------------------------------


def _has_delete_relation(contract: LoadContract, claims_deletes: bool) -> bool:
    """Whether this load materialises a delete table at all.

    An incremental object that named no keys to retire deletes nothing, so it has
    no relation and nothing reads one.
    """

    return claims_deletes or contract.deletes_absent_rows


def _delete_derivation(
    names: dict, contract: LoadContract, claims_deletes: bool
) -> str:
    """Settle the keys this load removes, before it removes any.

    Which rows those are is what ``Incremental`` decides. A non-incremental
    source is the whole truth, so a key clean staging no longer carries is
    retired, including one whose only staged row was refused, which is why this
    reads staging after the purge rather than before it. An incremental source is
    a window, so only an explicit second query can retire anything, and that
    query's claim was already narrowed when it ran.
    """

    if claims_deletes:
        join = _join("s", "d", contract.primary_key)
        return (
            "-- Named by the author's second query, narrowed to keys the target\n"
            "-- holds. Narrowed again here, now that staging is clean: a key the\n"
            "-- source still produces is not retired, whether or not its row\n"
            "-- changed, so the claim gives it up and the row is loaded normally.\n"
            f"delete d\n"
            f"from {names['delete']} as d\n"
            f"where exists (\n"
            f"    select 1 from {names['staging']} as s where {join}\n"
            f");"
        )
    if not contract.deletes_absent_rows:
        return "-- Incremental, and no delete query: absence retires nothing."
    keys = ", ".join(f"t.{_quote(column)}" for column in contract.primary_key)
    join = _join("s", "t", contract.primary_key)
    return (
        f"create table {names['delete']} as\n"
        f"select {keys}\n"
        f"from {names['target']} as t\n"
        f"where not exists (\n"
        f"    select 1 from {names['staging']} as s where {join}\n"
        f");"
    )


def _prospective_deletes(
    names: dict, contract: LoadContract, claims_deletes: bool
) -> str:
    """Count prospective deletes before the load applies them."""

    if not _has_delete_relation(contract, claims_deletes):
        return "-- Incremental: nothing is deleted, so there is nothing to count."
    return (
        "-- Only keys the target holds, so this is what will really go.\n"
        f"select @weaver_prospective_deletes = count(*) from {names['delete']};"
    )


def _reconciliation(names: dict, contract: LoadContract, claims_deletes: bool) -> str:
    """Remove the target rows this load retires, as a physical delete."""

    if not _has_delete_relation(contract, claims_deletes):
        return "-- Incremental, and no delete query: absence retires nothing."
    join = _join("d", "c", contract.primary_key)
    return (
        f"delete c\n"
        f"from {names['target']} as c\n"
        f"inner join {names['delete']} as d\n"
        f"    on {join};"
    )


# --- would the proposed changes leave a valid target? ------------------------


def _merge_uniqueness(names: dict, contract: LoadContract, has_delete: bool) -> str:
    """The one question an incremental load with unique keys has to ask.

    If every surviving delete and upsert were applied, would a declared unique
    key still be held by another target row? A non-incremental load never asks:
    it leaves the target equal to clean staging, and staging has already been
    made unique.

    A holder gives its value up in two ways: the load deletes it, or the load
    moves it off that value. Being in the upsert set is not one of them, and a
    row may be changing something else entirely and keeping the value it has. So
    a swap, and a cycle whose proposed state is unique, both pass, and a claim
    against an untouched holder does not.

    Any collision that remains stops the load. There is no partial application
    and no closure to compute: the proposed target state is either valid under
    the declared keys or it is not.
    """

    if not contract.checks_merge_uniqueness:
        return ""
    branches = [
        _merge_conflict_branch(names, contract, unique_key, has_delete)
        for unique_key in contract.unique_keys
    ]
    union = "\n\n    union all\n\n".join(branches)
    return (
        f"select @weaver_merge_conflicts = count(*)\n"
        f"from (\n{union}\n) as weaver_merge_conflict;\n\n"
        f"-- Fatal whatever @fault_tolerant says: that governs incoming rows.\n"
        f"if @weaver_merge_conflicts > 0\n"
        f"    throw 51022, '{_escape_literal(MERGE_CONFLICT_MESSAGE)}', 1;\n"
    )


def _merge_conflict_branch(
    names: dict,
    contract: LoadContract,
    unique_key: tuple[str, ...],
    has_delete: bool,
) -> str:
    key = _aliased_columns("u", contract.primary_key)
    participates = "\n          and ".join(
        f"u.{_quote(column)} is not null" for column in unique_key
    )
    differs = " or ".join(
        f"holder.{_quote(column)} <> u.{_quote(column)}"
        for column in contract.primary_key
    )
    vacated = []
    if has_delete:
        vacated.append(
            f"          /* not leaving */\n"
            f"          and not exists (\n"
            f"              select 1 from {names['delete']} as d\n"
            f"              where {_join('d', 'holder', contract.primary_key)}\n"
            f"          )"
        )
    moved = " or ".join(
        f"moving.{_quote(column)} <> holder.{_quote(column)}"
        f" or moving.{_quote(column)} is null"
        for column in unique_key
    )
    vacated.append(
        f"          /* not moving off this value */\n"
        f"          and not exists (\n"
        f"              select 1 from {names['upsert']} as moving\n"
        f"              where {_join('moving', 'holder', contract.primary_key)}\n"
        f"                and ({moved})\n"
        f"          )"
    )
    return (
        f"    select {key}\n"
        f"    from {names['upsert']} as u\n"
        f"    inner join {names['target']} as holder\n"
        f"        on {_join('holder', 'u', unique_key, indent=8)}\n"
        f"       and ({differs})\n"
        f"    where {participates}\n" + "\n".join(vacated)
    )


# --- the working tables ------------------------------------------------------


def _cleanup(names: dict, contract: LoadContract, claims_deletes: bool) -> str:
    """Drop whatever a previous run left behind, newest dependency first.

    Only the tables this procedure makes. An unkeyed load has no reject, upsert
    or delete table; dropping them anyway would hide the
    statement that creates them.
    """

    if not contract.primary_key:
        keys = ("staging",)
    elif _has_delete_relation(contract, claims_deletes):
        keys = ("reject", "upsert", "delete", "staging")
    else:
        keys = ("reject", "upsert", "staging")
    return "\n".join(
        f"if object_id({_sql_literal(names[key])}, N'U') is not null "
        f"drop table {names[key]};"
        for key in keys
    )


def _end_cleanup(names: dict, contract: LoadContract, claims_deletes: bool) -> str:
    """Clear the intermediate tables, unless they are the evidence of a problem.

    A run that rejected nothing leaves nothing to look at, so its artefacts go.
    One that rejected rows keeps them all: the reject table names what was
    refused, and the others make the rejection explicable.

    A run that stopped at a gate never reaches here, so its tables stand too.
    """

    cleanup = _cleanup(names, contract, claims_deletes)
    if not contract.primary_key:
        return cleanup
    return f"if @weaver_rows_rejected = 0\nbegin\n{_indent(cleanup, 4)}\nend;"


# --- the installer's column metadata -----------------------------------------


def _column_metadata_sql(names: dict, contract: LoadContract) -> str:
    """Read the target's loadable columns, and what an update sets.

    Weaver's own columns are excluded because the load supplies them itself, and
    identity is excluded because the engine does.
    """

    reserved = ", ".join(
        _sql_literal(name) for name in (*AUDIT_COLUMNS, SIGNATURE_COLUMN)
    )
    source_column_filter = f"c.name not in ({reserved})\n        and c.is_identity = 0"
    return render_sql_template(
        "load/column_metadata",
        target_table_literal=_sql_literal(names["target"]),
        source_column_filter=source_column_filter,
        signature_payload_select=_signature_payload_select(
            names, contract, source_column_filter
        ),
        update_select=_update_select(names, contract, source_column_filter),
    )


def _signature_payload_select(
    names: dict, contract: LoadContract, source_column_filter: str
) -> str:
    """Build the canonical payload the row signature is taken over.

    Assembled here rather than by the generator because it names each comparison
    column's physical type, and an inferred table's types are settled by the
    build rather than by the declaration.

    Each value is written as its byte length, a colon, and its canonical text, so
    a value containing whatever separator was chosen cannot be read as two
    values, and a null, written ``~``, cannot be read as an empty string.
    """

    if not contract.primary_key:
        return "-- No primary key, so no row is compared and there is no signature."
    if contract.comparison_columns:
        names_in = ", ".join(
            _sql_literal(column.lower()) for column in contract.comparison_columns
        )
        comparison_filter = f"lower(c.name) in ({names_in})"
    else:
        # No declared comparison set and no declared schema: every business
        # column except the key, which is what a declared schema's default is.
        keys = ", ".join(
            _sql_literal(column.lower()) for column in contract.primary_key
        )
        comparison_filter = f"lower(c.name) not in ({keys})"

    cases = "\n".join(
        f"                when '{type_name}' then "
        f"{_sql_literal(_CANONICAL_TEXT[type_name].format(column='__COLUMN__'))}"
        for type_name in sorted(_CANONICAL_TEXT)
    )
    fallback = _sql_literal(_CANONICAL_FALLBACK.format(column="__COLUMN__"))
    return (
        ";with comparison_columns as (\n"
        "    select\n"
        "        c.name\n"
        "      , c.column_id\n"
        "      , replace(\n"
        "            case lower(t.name)\n"
        f"{cases}\n"
        f"                else {fallback}\n"
        "            end,\n"
        "            N'__COLUMN__',\n"
        "            N's.' + quotename(c.name)\n"
        "        ) as canonical_text\n"
        "    from sys.columns as c\n"
        "    inner join sys.types as t on t.user_type_id = c.user_type_id\n"
        f"    where c.[object_id] = object_id({_sql_literal(names['target'])})\n"
        f"        and {source_column_filter}\n"
        f"        and {comparison_filter}\n"
        ")\n"
        "select\n"
        "    @weaver_signature_payload = string_agg(\n"
        "        convert(nvarchar(max), char(10) + N'        + case when s.' + quotename(name)\n"
        "            + N' is null then N''"
        + _NULL_MARKER
        + "'' else concat(cast(datalength('\n"
        "            + canonical_text + N') as varchar(20)), N'':'', '\n"
        "            + canonical_text + N') end'),\n"
        "        N''\n"
        "    ) within group (order by column_id)\n"
        "from comparison_columns;"
    )


def _update_select(
    names: dict, contract: LoadContract, source_column_filter: str
) -> str:
    """Build the UPDATE SET list: every loadable column except the key.

    The key is excluded because it is what matched the rows. Weaver's own columns
    are appended unconditionally: an updated row's update time, live sentinel and
    row signature are Weaver's to state, and the signature is copied from the
    upsert set rather than computed again.
    """

    if not contract.primary_key:
        return "set @weaver_update_set_columns = N'';"
    key_values = ", ".join(
        _sql_literal(column.lower()) for column in contract.primary_key
    )
    signature = _quote(SIGNATURE_COLUMN)
    return (
        ";with update_columns as (\n"
        "    select\n"
        "        c.name\n"
        "      , c.column_id\n"
        "      , row_number() over (order by c.column_id) as row_ordinal\n"
        "    from sys.columns as c\n"
        f"    where c.[object_id] = object_id({_sql_literal(names['target'])})\n"
        f"        and {source_column_filter}\n"
        f"        and lower(c.name) not in ({key_values})\n"
        ")\n"
        "select\n"
        "    @weaver_update_set_columns =\n"
        "        coalesce(\n"
        "            string_agg(\n"
        "                convert(nvarchar(max), case\n"
        "                    when row_ordinal = 1 then N'c.' + quotename(name) + N' = u.' + quotename(name)\n"
        "                    else char(10) + N'      , c.' + quotename(name) + N' = u.' + quotename(name)\n"
        "                end),\n"
        "                N''\n"
        "            ) within group (order by column_id)\n"
        "            + char(10) + N'      , ',\n"
        "            N''\n"
        "        )\n"
        f"        + N'c.{signature} = u.{signature}'\n"
        "        + char(10) + N'      , c.[Row update datetime] = @weaver_load_datetime'\n"
        "        + char(10) + N'      , c.[Row delete datetime] = @weaver_live_datetime'\n"
        "from update_columns;"
    )


# --- names and text ----------------------------------------------------------


def _table_names(document: SesDocument, procedure_name: str) -> dict:
    """The five names one load deals in, quoted once here and reused.

    The intermediate tables sit in the object's own schema beside it: they
    belong to the object being loaded, not to the generated ``_`` schema, which
    holds procedures.
    """

    schema = document.object_id.schema
    obj = document.object_id.object
    qualified = f"{_quote(schema)}."
    return {
        "target": f"{qualified}{_quote(obj)}",
        "staging": f"{qualified}{_quote(obj + STAGING_SUFFIX)}",
        "upsert": f"{qualified}{_quote(obj + UPSERT_SUFFIX)}",
        "reject": f"{qualified}{_quote(obj + REJECT_SUFFIX)}",
        "delete": f"{qualified}{_quote(obj + DELETE_SUFFIX)}",
        "object": document.qualified,
        "procedure": procedure_name,
    }


def _join(left: str, right: str, columns: tuple[str, ...], *, indent: int = 4) -> str:
    """Two relations matched on every key column.

    ``indent`` is where a continued ``and`` line starts, so a composite key reads
    straight however deeply the join is nested.
    """

    separator = "\n" + " " * indent + "and "
    return separator.join(
        f"{left}.{_quote(column)} = {right}.{_quote(column)}" for column in columns
    )


def _bare_columns(columns: tuple[str, ...]) -> str:
    return ", ".join(_quote(column) for column in columns)


def _aliased_columns(alias: str, columns: tuple[str, ...]) -> str:
    return ", ".join(f"{alias}.{_quote(column)}" for column in columns)


def _blank_key_predicate(columns: tuple[str, ...], *, alias: str = "s") -> str:
    """A key column that is null, empty or only spaces is not a key.

    Blank is rejected alongside null: a whitespace key matches nothing a person
    would call a match, and would create a row nobody can find again, or claim
    one on the delete side.

    ``alias`` is empty where the predicate is applied to one table with no
    relation to qualify, as the staging purge does.
    """

    prefix = f"{alias}." if alias else ""
    predicates = [
        f"nullif(trim(cast({prefix}{_quote(column)} as varchar(max))), '') is null"
        for column in columns
    ]
    if len(predicates) == 1:
        return predicates[0]
    return "(" + "\n       or ".join(predicates) + ")"


def _quote(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _sql_literal(text: str) -> str:
    return "N'" + text.replace("'", "''") + "'"


def _escape_literal(text: str) -> str:
    """Text going inside an already-quoted literal in the procedure template.

    The procedure is itself embedded in a string literal by the installer, so a
    quote here is doubled twice over: once at each layer.
    """

    return text.replace("'", "''")


def _indent(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line.strip() else line for line in text.splitlines())


__all__ = [
    "INTOLERANT_MESSAGE",
    "IS_NEW_COLUMN",
    "MERGE_CONFLICT_MESSAGE",
    "RANK_COLUMN",
    "TOLERATED_MESSAGE",
    "logical_result_row",
    "generate_tsql_load_script",
]
