"""Generate a Warehouse table's load procedure.

Keyed loads preserve this order: stage, discover rejects, apply the rejection
gate, purge rejects, settle deletes and upserts, validate the proposed target,
then mutate the target. Rejected rows remain available for inspection.
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

#: Reserved working-column names avoid collisions with authored columns.
RANK_COLUMN = "__weaver_rank"
WORKING_SIGNATURE_COLUMN = "__weaver_signature"
SURVIVOR_COLUMN = "__weaver_survivor"

#: Distinguishes inserts from updates within the upsert set.
IS_NEW_COLUMN = "_Is new row"

#: Filled at install time, after inferred physical column types are known.
SIGNATURE_PAYLOAD = "__SIGNATURE_PAYLOAD__"

#: Ordered and named to match ``runtime.load_result.RESULT_COLUMNS``.
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

#: Maps private procedure parameters to the stable logical result contract.
RESULT_PARAMETER_NAMES = {
    logical: f"weaver_{logical}" for logical, _type_name in RESULT_PARAMETERS
}

#: The physical names and T-SQL types passed to the generic SQL executor.
PROCEDURE_RESULT_PARAMETERS = tuple(
    (RESULT_PARAMETER_NAMES[logical], type_name)
    for logical, type_name in RESULT_PARAMETERS
)


def logical_result_row(row) -> dict:
    return {
        logical: row[RESULT_PARAMETER_NAMES[logical]]
        for logical, _type_name in RESULT_PARAMETERS
    }


STAGING_SUFFIX = "_Staging"
UPSERT_SUFFIX = "_Upsert"
REJECT_SUFFIX = "_Reject"
DELETE_SUFFIX = "_Delete"

#: Rejection status records whether the target was left unchanged.
INTOLERANT_MESSAGE = (
    "rows were rejected and fault_tolerant = 0, so the target was not modified"
)
TOLERATED_MESSAGE = "rows were rejected and excluded from the load"

#: Proposed-target conflicts are not row-level rejects or fault-tolerant.
MERGE_CONFLICT_MESSAGE = (
    "the proposed changes would leave a declared unique key held by two rows, "
    "so the target was not modified"
)

#: Delimit authored SQL in the generated procedure.
PREPROCESSING_BANNER = "/*-- Pre-processing --*/"
TRANSFORMATION_BANNER = "/*---- Data transformation ----*/"
END_TRANSFORMATION_BANNER = "/*---- End data transformation ----*/"
POSTPROCESSING_BANNER = "/*-- Post-processing --*/"

#: Canonical, locale-independent text for physical values in row signatures.
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

#: Remaining supported types have stable default text representations.
_CANONICAL_FALLBACK = "cast({column} as varchar(max))"

#: Present values start with their byte length, so this cannot collide with one.
_NULL_MARKER = "~"


def generate_tsql_load_script(
    document: SesDocument, body: str, *, procedure_name: str, item
) -> str:
    """Generate an installer script for one Warehouse load procedure.

    ``item`` completes the four-part identity used for the procedure's bookmark.
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
                # Only a clean load establishes a new bookmark instant.
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


def _result_parameters() -> str:
    """Declare optional outputs so the procedure also runs directly."""

    return "\n".join(
        f"  , @{RESULT_PARAMETER_NAMES[name]} {type_name} = null output"
        for name, type_name in RESULT_PARAMETERS
    )


def _result_assignment(**values: str) -> str:
    """Assign every output at each procedure exit.

    Defaults make parameters optional; they are never observable results.
    """

    defaults = {
        "succeeded": "cast(case when @weaver_error is null then 1 else 0 end as bit)",
        "rows_read": "@weaver_rows_read",
        "rows_inserted": "@weaver_rows_inserted",
        "rows_updated": "@weaver_rows_updated",
        "rows_deleted": "@weaver_rows_deleted",
        "rows_rejected": "@weaver_rows_rejected",
        "error_message": "@weaver_error",
        # Static skips, rejection gates and loads with rejects establish no bookmark.
        "bookmark_datetime": "null",
        # Counts cannot distinguish a Static skip from an empty successful load.
        "is_static_skip": "cast(0 as bit)",
    }
    defaults.update(values)
    return "\n".join(
        f"set @{RESULT_PARAMETER_NAMES[name]} = {defaults[name]};"
        for name, _ in RESULT_PARAMETERS
    )


def _static_gate(contract: LoadContract) -> str:
    """Skip an already-loaded Static object before reading its source.

    The bookmark, not target contents, records whether it loaded. ``@reload``
    bypasses the gate. Keeping this check in the procedure makes direct and
    orchestrated runs agree.
    """

    if not contract.static:
        return "-- Not static: this object is loaded on every run."
    seeded = _result_assignment(
        succeeded="cast(1 as bit)", is_static_skip="cast(1 as bit)"
    )
    return (
        "-- Static: loaded once. A bookmark past the sentinel means a clean load\n"
        "-- has run for this incarnation, so this reports a successful load of\n"
        "-- nothing. A reload asks for it again.\n"
        f"if @reload = 0 and @weaver_bookmark > {_sentinel_literal()}\n"
        "begin\n"
        f"{_indent(seeded, 4)}\n"
        "    return;\n"
        "end;"
    )


def _sentinel_literal() -> str:
    from ..catalogue.tables import BOOKMARK_SENTINEL_TEXT

    return f"convert(datetime2(6), '{BOOKMARK_SENTINEL_TEXT}')"


def _bookmark_key(document: SesDocument, item, contract: LoadContract) -> str:
    """Read only a Static object's bookmark before any source work.

    Other dispositions avoid the cross-Warehouse read. Null and the sentinel
    both mean that no clean load established a cursor.
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
    """Return the four values that key the object's bookmark row."""

    from ..catalogue.claims import bookmark_row
    from .model import WeaverDocumentId

    if item is None:
        raise DiscoveryError(
            f"{document.qualified}: no declaring item was supplied for the load "
            "procedure. Supply the table's logical item."
        )
    return bookmark_row(WeaverDocumentId(item, document.object_id))


def _key_literal(value: str) -> str:
    return "N'" + _escape_literal(str(value)) + "'"


def _staging_sql(names: dict, program: TsqlProgram, contract: LoadContract) -> str:
    """Materialise result queries without moving intervening setup.

    Later setup may depend on staging and prepare the delete query.
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
    """Divert the staging query without changing its authored text.

    Offset-exact ``INTO`` insertion supports CTE-led queries that cannot be
    wrapped as derived tables.
    """

    if not contract.primary_key:
        return f"create table {names['staging']} as\n{query};"
    return f"{insert_select_into(query, names['staging'])};"


def _delete_claim_sql(names: dict, query: str, contract: LoadContract) -> str:
    """Materialise distinct, nonblank delete keys that exist in the target.

    This happens before target mutation so stability checks can stop before any
    write and ``rows_deleted`` counts rows actually removed.
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
            # A refusal preserves rows_read but reports no target writes.
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


def _signature_expression() -> str:
    """Hash one staged row's canonical comparison payload.

    The empty prefix keeps the expression valid with no comparison columns; all
    rows then share a signature.
    """

    return f"convert(varbinary(32), hashbytes('SHA2_256', N''{SIGNATURE_PAYLOAD}))"


def _reject_discovery(names: dict, contract: LoadContract) -> str:
    """Discover rejects sequentially in one CTE chain.

    Each unique key sees only rows that survived earlier checks, preserving key
    declaration order without mutating staging before the rejection gate.
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
                # Later unique keys identify losers by the surviving primary key.
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
    """Choose one survivor from each duplicate unique-key group.

    Null-bearing keys do not participate. Composite primary keys require ranking,
    but only rows in duplicate groups are ranked.
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
    """Match rows with an unusable key or a declared not-null violation."""

    prefix = f"{alias}." if alias else ""
    predicates = [_blank_key_predicate(contract.primary_key, alias=alias)]
    predicates.extend(
        f"{prefix}{_quote(column)} is null" for column in contract.not_null_columns
    )
    return "\n   or ".join(predicates)


def _violation_reason(contract: LoadContract, alias: str = "s") -> str:
    """Return one reason per refused row so thresholds count each row once."""

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
    """Count primary-key duplicates from the collected reject evidence."""

    return (
        f"select @weaver_duplicate_keys = count(*)\n"
        f"from {names['reject']}\n"
        f"where {_quote(REJECTION_REASON)} = '{REASON_DUPLICATE_PK}';"
    )


def _staging_purge(names: dict, contract: LoadContract) -> str:
    """Remove rejects in the same order in which they were discovered.

    Unusable rows go first. Each unique key then sees the population left by the
    preceding keys.
    """

    steps = [
        f"delete from {names['staging']}\nwhere {_violation_predicate(contract, '')};"
    ]
    steps.append(
        # Fabric deletes through a CTE only when it reads one base table. Avoid a
        # full ranking pass by running it only when a duplicate was discovered.
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
    """Keep the same duplicate row that reject discovery selected.

    Identical rows cannot be separated by a predicate, so deletion uses a CTE
    ranked by row signature.
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
    """Remove unique-key losers by their now-unique primary keys."""

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


def _has_delete_relation(contract: LoadContract, claims_deletes: bool) -> bool:
    return claims_deletes or contract.deletes_absent_rows


def _delete_derivation(
    names: dict, contract: LoadContract, claims_deletes: bool
) -> str:
    """Settle delete keys before target mutation.

    Non-incremental loads compare the target with staging after rejects are
    purged. Incremental loads use only an explicit delete query.
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

    if not _has_delete_relation(contract, claims_deletes):
        return "-- Incremental: nothing is deleted, so there is nothing to count."
    return (
        "-- Only keys the target holds, so this is what will really go.\n"
        f"select @weaver_prospective_deletes = count(*) from {names['delete']};"
    )


def _reconciliation(names: dict, contract: LoadContract, claims_deletes: bool) -> str:
    if not _has_delete_relation(contract, claims_deletes):
        return "-- Incremental, and no delete query: absence retires nothing."
    join = _join("d", "c", contract.primary_key)
    return (
        f"delete c\n"
        f"from {names['target']} as c\n"
        f"inner join {names['delete']} as d\n"
        f"    on {join};"
    )


def _merge_uniqueness(names: dict, contract: LoadContract, has_delete: bool) -> str:
    """Reject an incremental change that would violate a target unique key.

    A holder releases a value only when deleted or moved off it. Swaps and cycles
    pass when the complete proposed target is unique. This gate precedes mutation.
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


def _cleanup(names: dict, contract: LoadContract, claims_deletes: bool) -> str:
    """Drop only this procedure's working tables, newest dependency first."""

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
    """Keep working tables when a load rejects rows or stops at a gate."""

    cleanup = _cleanup(names, contract, claims_deletes)
    if not contract.primary_key:
        return cleanup
    return f"if @weaver_rows_rejected = 0\nbegin\n{_indent(cleanup, 4)}\nend;"


def _column_metadata_sql(names: dict, contract: LoadContract) -> str:
    """Read target columns, excluding engine- and Weaver-supplied values."""

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
    """Build an unambiguous, type-aware row-signature payload.

    Inferred physical types are available only at install time. Prefixing each
    canonical value with its byte length keeps separators and nulls distinct.
    """

    if not contract.primary_key:
        return "-- No primary key, so no row is compared and there is no signature."
    if contract.comparison_columns:
        names_in = ", ".join(
            _sql_literal(column.lower()) for column in contract.comparison_columns
        )
        comparison_filter = f"lower(c.name) in ({names_in})"
    else:
        # With no declared schema or comparison set, compare every non-key column.
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
    """Build updates from non-key loadable columns plus managed state."""

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


def _table_names(document: SesDocument, procedure_name: str) -> dict:
    """Quote target and working-table names in the object's own schema."""

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
    """Join two relations on every key column with stable indentation."""

    separator = "\n" + " " * indent + "and "
    return separator.join(
        f"{left}.{_quote(column)} = {right}.{_quote(column)}" for column in columns
    )


def _bare_columns(columns: tuple[str, ...]) -> str:
    return ", ".join(_quote(column) for column in columns)


def _aliased_columns(alias: str, columns: tuple[str, ...]) -> str:
    return ", ".join(f"{alias}.{_quote(column)}" for column in columns)


def _blank_key_predicate(columns: tuple[str, ...], *, alias: str = "s") -> str:
    """Match key columns that are null, empty or whitespace."""

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
    """Escape text for both nested T-SQL string-literal layers."""

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
