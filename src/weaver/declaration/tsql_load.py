"""Generate an independently runnable Warehouse load procedure.

The build payload installs a procedure that derives target columns from
``sys.columns``. Procedure result counts use output parameters, and rejected
rows remain in the reject table for inspection.
"""

from __future__ import annotations

from ..errors import DiscoveryError
from ..runtime.load_contract import (
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
    LoadContract,
)
from .metadata import (
    AUDIT_COLUMNS,
    AUDIT_LIVE_DELETE_DATETIME,
    SesDocument,
)
from .sql_shaping import insert_select_into, render_sql_template, temp_table_name
from .tsql_program import TsqlProgram, parse_tsql_program, validate_query_contract

#: The column a staged row's duplicate rank lands in. Named to be unmistakably
#: Weaver's: it sits beside the author's own columns in a real table, so a name
#: an author might have chosen would be a collision waiting to happen.
RANK_COLUMN = "__weaver_pk_row_number"

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
)

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

#: Banners marking where the author's own code sits in the generated procedure.
#: A generated artefact is read by people — usually when something has gone
#: wrong — and the first question is always "which of this did I write?".
PREPROCESSING_BANNER = "/*-- Pre-processing --*/"
TRANSFORMATION_BANNER = "/*---- Data transformation ----*/"
END_TRANSFORMATION_BANNER = "/*---- End data transformation ----*/"
POSTPROCESSING_BANNER = "/*-- Post-processing --*/"


def generate_tsql_load_script(
    document: SesDocument, body: str, *, procedure_name: str
) -> str:
    """The installer script for one Warehouse table's load procedure.

    ``body`` is the table's own query — the same text its build materialises to
    settle its shape. A load runs it for real.
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
        result_assignment=_indent(_result_assignment(), 4),
        live_delete_datetime=AUDIT_LIVE_DELETE_DATETIME,
        preprocessing_banner=_indent(PREPROCESSING_BANNER, 4),
        postprocessing_banner=_indent(POSTPROCESSING_BANNER, 4),
        static_gate=_indent(_static_gate(names, contract), 4),
        start_artifact_cleanup=_indent(_cleanup(names, contract, claims_deletes), 4),
        staging_sql=_indent(staging_sql, 4),
        staging_table=names["staging"],
        target_table=names["target"],
        load_body=_indent(load_body, 4),
        end_artifact_cleanup=_indent(
            _end_cleanup(names, contract, claims_deletes), 4
        ),
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
        f"  , @{name} {type_name} = null output" for name, type_name in RESULT_PARAMETERS
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
    }
    defaults.update(values)
    return "\n".join(f"set @{name} = {defaults[name]};" for name, _ in RESULT_PARAMETERS)


# --- the pieces of the procedure ---------------------------------------------


def _static_gate(names: dict, contract: LoadContract) -> str:
    """The check a ``Static`` object makes before it does anything else.

    Baked into the procedure rather than performed by its caller, because the
    procedure is independently runnable: running it by hand must give the same
    answer an orchestrated run gets.

    Before the staging query, so a populated static table costs one existence
    check rather than a full source read — and ``exists`` rather than a count,
    because the question is whether it has been seeded.

    A non-static object gets a comment rather than a disabled branch.
    """

    if not contract.static:
        return "-- Not static: this object is loaded on every run."
    seeded = _result_assignment(succeeded="cast(1 as bit)")
    return (
        "-- Static: seeded once, into an empty target. Already populated means\n"
        "-- the load has nothing to do, and reports a successful load of nothing\n"
        "-- rather than repeating work or being skipped from outside.\n"
        f"if exists (select 1 from {names['target']})\n"
        "begin\n"
        f"{_indent(seeded, 4)}\n"
        "    return;\n"
        "end;"
    )


def _staging_sql(names: dict, program: TsqlProgram, contract: LoadContract) -> str:
    """Run the author's program, materialising each query it produces.

    The body is emitted in the order it was written, so a setup statement
    between two queries runs between them: an author may build a working table,
    stage from it, then build another and name the retired keys from that.

    The staging query is wrapped rather than inlined because a duplicate key is
    identified by its rank, and ranking later would read the staged rows twice.
    With no key there is nothing to rank.
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


def _hoisted(query: str, temp_table: str) -> str:
    """The author's query, diverted into a session temp table of its own.

    A query cannot always be a subquery: ``with … select …`` is a legal
    statement and an illegal derived table, so ``from (<query>) as s`` is a
    syntax error for any body opening with a CTE. Running the query as the
    statement it is, into a table, works whatever its shape.

    The ``INTO`` is placed by the same offset-exact transform the shape-only
    build uses, so a CTE gets it on the body ``SELECT`` and a ``UNION`` on its
    first branch.
    """

    return (
        f"if object_id('tempdb..{temp_table}') is not null drop table {temp_table};\n"
        f"{insert_select_into(query, temp_table)};"
    )


def _staging_table_sql(names: dict, query: str, contract: LoadContract) -> str:
    """Materialise the object's rows, ranking duplicate keys as it goes.

    The rank identifies a duplicate, and is computed here because computing it
    later would read the staged rows twice. With no key there is nothing to
    rank, and the query becomes the staging table directly.
    """

    if not contract.primary_key:
        return f"create table {names['staging']} as\n{query};"

    source = temp_table_name("#weaver_staging_source", names["object"])
    partition = ", ".join(f"s.{_quote(column)}" for column in contract.primary_key)
    return (
        f"{_hoisted(query, source)}\n\n"
        f"create table {names['staging']} as\n"
        f"select\n"
        f"    s.*\n"
        f"  , row_number() over (partition by {partition} order by (select null)) "
        f"as {_quote(RANK_COLUMN)}\n"
        f"from {source} as s;\n\n"
        f"drop table {source};"
    )


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
        f"{_hoisted(query, claim)}\n\n"
        f"create table {names['delete']} as\n"
        f"select distinct {keys}\n"
        f"from {names['target']} as c\n"
        f"inner join {claim} as d\n"
        f"    on {join}\n"
        f"where not ({_blank_key_predicate(contract.primary_key, alias='d')});\n\n"
        f"drop table {claim};"
    )


def _primary_key_body(
    names: dict, contract: LoadContract, claims_deletes: bool
) -> str:
    blank = _blank_key_predicate(contract.primary_key)
    return render_sql_template(
        "load/primary_key_body",
        reject_table=names["reject"],
        upsert_table=names["upsert"],
        staging_table=names["staging"],
        target_table=names["target"],
        rank_column=RANK_COLUMN,
        staging_blank_predicate=blank,
        staging_target_join=_join("s", "t", contract.primary_key),
        target_upsert_join=_join("c", "u", contract.primary_key),
        target_missing_predicate=f"t.{_quote(contract.primary_key[0])} is null",
        missing_reconciliation=_reconciliation(names, contract, claims_deletes),
        prospective_deletes=_prospective_deletes(names, contract, claims_deletes),
        delete_threshold=contract.delete_threshold,
        update_threshold=contract.update_threshold,
        stability_rows=contract.stability_rows,
        rejection_reason=REJECTION_REASON,
        blank_reason=REASON_BLANK_PK,
        duplicate_reason=REASON_DUPLICATE_PK,
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


def _prospective_deletes(
    names: dict, contract: LoadContract, claims_deletes: bool
) -> str:
    """Count prospective deletes before the load applies them."""

    if claims_deletes:
        return (
            "-- The delete claim was already narrowed to keys the target holds,\n"
            "-- so its size is the number of rows that will really go.\n"
            f"select @weaver_prospective_deletes = count(*) from {names['delete']};"
        )
    if not contract.deletes_absent_rows:
        return (
            "-- Incremental: nothing is deleted, so there is nothing to count."
        )
    join = _join("s", "c", contract.primary_key)
    return (
        f"select @weaver_prospective_deletes = count(*)\n"
        f"from {names['target']} as c\n"
        f"where not exists (\n"
        f"    select 1 from {names['staging']} as s where {join}\n"
        f");"
    )


def _reconciliation(
    names: dict, contract: LoadContract, claims_deletes: bool
) -> str:
    """Remove the target rows this load retires — a physical delete.

    Which rows those are is what ``Incremental`` decides. A non-incremental
    source is the whole truth, so absence retires a row; an incremental source
    is a window, so only an explicit second query can retire anything.
    """

    if claims_deletes:
        join = _join("d", "c", contract.primary_key)
        return (
            f"delete c\n"
            f"from {names['target']} as c\n"
            f"inner join {names['delete']} as d\n"
            f"    on {join};"
        )
    if not contract.deletes_absent_rows:
        return (
            "-- Incremental, and no delete query. Absence from the source is\n"
            "-- not a retirement, so nothing is deleted."
        )
    join = _join("s", "c", contract.primary_key)
    return (
        f"delete c\n"
        f"from {names['target']} as c\n"
        f"where not exists (\n"
        f"    select 1 from {names['staging']} as s where {join}\n"
        f");"
    )


def _cleanup(names: dict, contract: LoadContract, claims_deletes: bool) -> str:
    """Drop whatever a previous run left behind, newest dependency first.

    Only the tables this procedure makes. An unkeyed load has no reject or
    upsert table, and one whose author named no deletes has no delete table;
    dropping them anyway would send a reader hunting for the statement that
    creates them.
    """

    if not contract.primary_key:
        keys = ("staging",)
    elif claims_deletes:
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
    """

    cleanup = _cleanup(names, contract, claims_deletes)
    if not contract.primary_key:
        return cleanup
    return (
        "if @weaver_rows_rejected = 0\n"
        "begin\n"
        f"{_indent(cleanup, 4)}\n"
        "end;"
    )


# --- the installer's column metadata -----------------------------------------


def _column_metadata_sql(names: dict, contract: LoadContract) -> str:
    """Read the target's loadable columns, and what an update sets.

    The audit columns are excluded because the load supplies them itself, and
    identity is excluded because the engine does.
    """

    audit = ", ".join(_sql_literal(name) for name in AUDIT_COLUMNS)
    source_column_filter = (
        f"c.name not in ({audit})\n        and c.is_identity = 0"
    )
    return render_sql_template(
        "load/column_metadata",
        target_table_literal=_sql_literal(names["target"]),
        source_column_filter=source_column_filter,
        update_select=_update_select(names, contract, source_column_filter),
    )


def _update_select(names: dict, contract: LoadContract, source_column_filter: str) -> str:
    """Build the UPDATE SET list: every loadable column except the key.

    The key is excluded because it is what matched the rows. The audit columns
    are appended unconditionally: an updated row's update time and live sentinel
    are Weaver's to state.
    """

    if not contract.primary_key:
        return "set @weaver_update_set_columns = N'';"
    key_values = ", ".join(
        _sql_literal(column.lower()) for column in contract.primary_key
    )
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
        "                case\n"
        "                    when row_ordinal = 1 then N'c.' + quotename(name) + N' = u.' + quotename(name)\n"
        "                    else char(10) + N'      , c.' + quotename(name) + N' = u.' + quotename(name)\n"
        "                end,\n"
        "                N''\n"
        "            ) within group (order by column_id)\n"
        "            + char(10) + N'      , ',\n"
        "            N''\n"
        "        )\n"
        "        + N'c.[Row update datetime] = @weaver_load_datetime'\n"
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


def _join(left: str, right: str, columns: tuple[str, ...]) -> str:
    return "\n    and ".join(
        f"{left}.{_quote(column)} = {right}.{_quote(column)}" for column in columns
    )


def _blank_key_predicate(columns: tuple[str, ...], *, alias: str = "s") -> str:
    """A key column that is null, empty or only spaces is not a key.

    Blank is rejected alongside null: a whitespace key matches nothing a person
    would call a match, and would create a row nobody can find again — or, on
    the delete side, claim one.
    """

    predicates = [
        f"nullif(trim(cast({alias}.{_quote(column)} as varchar(max))), '') is null"
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
    """Text going *inside* an already-quoted literal in the procedure template.

    The procedure is itself embedded in a string literal by the installer, so a
    quote here is doubled twice over: once at each layer.
    """

    return text.replace("'", "''")


def _indent(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line.strip() else line for line in text.splitlines())


__all__ = [
    "INTOLERANT_MESSAGE",
    "RANK_COLUMN",
    "TOLERATED_MESSAGE",
    "generate_tsql_load_script",
]
