"""T-SQL load generation — one independently runnable procedure per table.

What this produces is not the procedure. It is a script that *installs* the
procedure, and the difference is the whole design.

.. code-block:: text

    payload            an installer script, frozen at build
      reads            sys.columns of the target table
      assembles        the procedure text
      exec             sp_executesql

The column lists cannot be written at generation time. A Warehouse table may
infer its shape from its query, so the generator does not always know its
columns — and even when it does, the *physical* table is what the procedure must
name. Reading ``sys.columns`` at install settles both, and it excludes the
identity column for free: the engine generates that column, so ``is_identity =
0`` keeps it out of every insert list without this module having to know which
column it was. It is the same two-phase shape
:mod:`weaver.declaration.tsql_ddl` uses to build the table, for the same reason.

The installed procedure is independently runnable, which is the point of a
primitive::

    exec [_].[Load Sales.Customer] @fault_tolerant = 1;

It reads no repository, consults no catalogue and calls nothing else Weaver
owns. It returns one row of :data:`weaver.runtime.load_result.RESULT_COLUMNS`.

**The intermediate tables are real.** ``Sales.Customer_Staging``,
``_Upsert`` and ``_Reject`` are ordinary tables in the object's own schema, as
they were in the reference implementation. Real tables are what make a failed
load inspectable: when rows are rejected the evidence is still there afterwards,
addressable by anyone with a query tool. They are dropped at the start of every
run and again at the end, but only when the run was clean — the whole point of
keeping a reject table is that a run which rejected rows leaves it behind.

**No history.** The reference carried a ``_Current``/``_History`` pair behind a
view. Weaver builds only the authored table, so a load updates it directly and a
row absent from a non-incremental source is deleted outright. The audit columns
still record insert and update times; the delete sentinel stays live because
there is nowhere for a deleted row to go.

Ported from ``weaver_runtime.dbrep.sql.etl``, with the history layout removed and
the ``fault_tolerant`` contract and structured result added.
"""

from __future__ import annotations

from .metadata import (
    AUDIT_COLUMNS,
    AUDIT_LIVE_DELETE_DATETIME,
    SesDocument,
)
from .sql_shaping import render_sql_template, split_trailing_query
from ..runtime.load_contract import (
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
    LoadContract,
)

#: The column a staged row's duplicate rank lands in. Named to be unmistakably
#: Weaver's: it sits beside the author's own columns in a real table, so a name
#: an author might have chosen would be a collision waiting to happen.
RANK_COLUMN = "__weaver_pk_row_number"

#: The suffixes of the three intermediate tables, in the object's own schema.
STAGING_SUFFIX = "_Staging"
UPSERT_SUFFIX = "_Upsert"
REJECT_SUFFIX = "_Reject"

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
    names = _table_names(document, procedure_name)
    staging_sql = _staging_sql(names, body, contract)

    if contract.primary_key:
        load_body = _primary_key_body(names, contract)
    else:
        load_body = _full_replace_body(names)

    procedure = render_sql_template(
        "load/load_procedure",
        load_procedure=names["procedure"],
        live_delete_datetime=AUDIT_LIVE_DELETE_DATETIME,
        preprocessing_banner=_indent(PREPROCESSING_BANNER, 4),
        postprocessing_banner=_indent(POSTPROCESSING_BANNER, 4),
        start_artifact_cleanup=_indent(_cleanup(names, contract), 4),
        staging_sql=_indent(staging_sql, 4),
        staging_table=names["staging"],
        load_body=_indent(load_body, 4),
        end_artifact_cleanup=_indent(_end_cleanup(names, contract), 4),
    ).rstrip()

    return render_sql_template(
        "load/install_load_procedure",
        column_metadata_sql=_column_metadata_sql(names, contract),
        procedure_template_sql_literal=_sql_literal(procedure),
    )


# --- the pieces of the procedure ---------------------------------------------


def _staging_sql(names: dict, body: str, contract: LoadContract) -> str:
    """Materialise the object's query, ranking duplicate keys as it goes.

    One pass, because the rank is what identifies a duplicate and computing it
    later would mean reading the staged rows twice. With no key there is nothing
    to rank and the query is staged as it stands.

    Only the *last standalone query* is staged. A body may set a temporary view
    up first and select from it, and wrapping the whole body in a subquery would
    put a ``create`` inside a ``from`` — so the preamble runs as it was written,
    and the query it leads to is what fills staging.
    """

    preamble, query = split_trailing_query(body)
    lead = f"{preamble};\n\n" if preamble else ""
    if not contract.primary_key:
        staged = f"create table {names['staging']} as\n{query};"
    else:
        partition = ", ".join(f"s.{_quote(column)}" for column in contract.primary_key)
        staged = (
            f"create table {names['staging']} as\n"
            f"select\n"
            f"    s.*\n"
            f"  , row_number() over (partition by {partition} order by (select null)) "
            f"as {_quote(RANK_COLUMN)}\n"
            f"from (\n{_indent(query, 4)}\n) as s;"
        )
    return f"{TRANSFORMATION_BANNER}\n{lead}{staged}\n{END_TRANSFORMATION_BANNER}"


def _primary_key_body(names: dict, contract: LoadContract) -> str:
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
        missing_reconciliation=_missing_reconciliation(names, contract),
        rejection_reason=REJECTION_REASON,
        blank_reason=REASON_BLANK_PK,
        duplicate_reason=REASON_DUPLICATE_PK,
        intolerant_message=_escape_literal(INTOLERANT_MESSAGE),
        tolerated_message=_escape_literal(TOLERATED_MESSAGE),
    ).rstrip()


def _full_replace_body(names: dict) -> str:
    return render_sql_template(
        "load/full_replace_body",
        target_table=names["target"],
        staging_table=names["staging"],
    ).rstrip()


def _missing_reconciliation(names: dict, contract: LoadContract) -> str:
    """Delete target rows the source stopped producing — a physical delete.

    Only for a keyed, non-incremental load. An incremental source shows a window
    rather than the whole truth, so absence from it is not a retirement and
    deleting on it would destroy rows the source never claimed to describe.
    """

    if not contract.deletes_absent_rows:
        return (
            "-- Incremental: absence from the source is not a retirement, so\n"
            "-- nothing is deleted."
        )
    join = _join("s", "c", contract.primary_key)
    return (
        f"delete c\n"
        f"from {names['target']} as c\n"
        f"where not exists (\n"
        f"    select 1 from {names['staging']} as s where {join}\n"
        f");\n"
        f"\n"
        f"set @weaver_rows_deleted = @@rowcount;"
    )


def _cleanup(names: dict, contract: LoadContract) -> str:
    """Drop whatever a previous run left behind, newest dependency first.

    Only the tables this procedure actually makes. An unkeyed load has no reject
    or upsert table — with no key nothing can be matched, so there is nothing to
    reject and no upsert set — and dropping them anyway would leave a reader
    hunting for the statement that creates them.
    """

    keys = ("reject", "upsert", "staging") if contract.primary_key else ("staging",)
    return "\n".join(
        f"if object_id({_sql_literal(names[key])}, N'U') is not null "
        f"drop table {names[key]};"
        for key in keys
    )


def _end_cleanup(names: dict, contract: LoadContract) -> str:
    """Clear the intermediate tables, unless they are the evidence of a problem.

    A run that rejected nothing leaves nothing to look at, so its artefacts go.
    A run that rejected rows keeps all three: the reject table names what was
    refused, and staging and upsert are what make the rejection explicable.
    """

    cleanup = _cleanup(names, contract)
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

    The key is excluded because it is what matched the rows — setting a column
    to the value it was joined on is work that cannot change anything. The audit
    columns are appended unconditionally, since an updated row's update time and
    live sentinel are Weaver's to state.
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

    The intermediate tables sit in the object's own schema beside it, which is
    the reference's arrangement: they belong to the object being loaded, not to
    the generated ``_`` schema, which holds procedures.
    """

    schema = document.object_id.schema
    obj = document.object_id.object
    qualified = f"{_quote(schema)}."
    return {
        "target": f"{qualified}{_quote(obj)}",
        "staging": f"{qualified}{_quote(obj + STAGING_SUFFIX)}",
        "upsert": f"{qualified}{_quote(obj + UPSERT_SUFFIX)}",
        "reject": f"{qualified}{_quote(obj + REJECT_SUFFIX)}",
        "procedure": procedure_name,
    }


def _join(left: str, right: str, columns: tuple[str, ...]) -> str:
    return "\n    and ".join(
        f"{left}.{_quote(column)} = {right}.{_quote(column)}" for column in columns
    )


def _blank_key_predicate(columns: tuple[str, ...]) -> str:
    """A key column that is null, empty or only spaces is not a key.

    Blank is rejected alongside null deliberately: a key that is whitespace
    matches nothing a human would call a match, and letting it through would
    create a row nobody can find again.
    """

    predicates = [
        f"nullif(trim(cast(s.{_quote(column)} as varchar(max))), '') is null"
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
    quote here is doubled twice over. Escaping once at each layer is what keeps
    the two levels straight.
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
