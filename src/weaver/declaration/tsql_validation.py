"""Generate T-SQL validation procedures and direct file-run batches.

Installed and direct validation runs share the same validation body.
"""

from __future__ import annotations

from ..errors import DiscoveryError
from .metadata import ASSUMPTION, TEST, SesDocument
from .sql_shaping import (
    insert_select_into,
    query_spans,
    selects_into,
    temp_table_name,
)
from .tsql_program import parse_tsql_program
from .validation_program import validate_validation_contract

#: Match the reserved Spark diagnostic columns.
SIDE_COLUMN = "_weaver_side"
SK_COLUMN = "_weaver_sk"
EXPECTED = "expected"
ACTUAL = "actual"

#: Shared by generated signatures and caller-local declarations.
TEST_PARAMETERS = (("missing_count", "bigint"), ("unexpected_count", "bigint"))
ASSUMPTION_PARAMETERS = (("violation_count", "bigint"),)
RESULT_PARAMETERS = {TEST: TEST_PARAMETERS, ASSUMPTION: ASSUMPTION_PARAMETERS}

#: Direct calls return diagnostics unless orchestration suppresses them.
SUPPRESS_PARAMETER = "suppress_result_set"

#: Keep authored SQL identifiable inside generated procedures.
SETUP_BANNER = "/*-- Pre-processing --*/"
POSTPROCESSING_BANNER = "/*-- Post-processing --*/"


def generate_tsql_validation_script(
    document: SesDocument, body: str, *, procedure_name: str
) -> str:

    core = validation_body(document, body)
    # Result counts precede the suppression flag in the public procedure contract.
    declared = [
        f"@{name} {type_name} = null output"
        for name, type_name in RESULT_PARAMETERS[document.kind]
    ] + [f"@{SUPPRESS_PARAMETER} bit = 0"]
    parameters = "\n".join(
        f"    {parameter}" if index == 0 else f"  , {parameter}"
        for index, parameter in enumerate(declared)
    )
    return (
        f"create or alter procedure {procedure_name}\n"
        f"{parameters}\n"
        "as\n"
        "begin\n"
        "    set nocount on;\n"
        "\n"
        f"{_indent(core, 4)}\n"
        "end;\n"
    )


def generate_tsql_validation_batch(document: SesDocument, body: str) -> str:
    """Render the validation body as a directly runnable batch.

    Locals replace procedure output parameters and are projected at the end.
    """

    parameters = RESULT_PARAMETERS[document.kind]
    declarations = "\n".join(
        f"declare @{name} {type_name};" for name, type_name in parameters
    )
    projection = ", ".join(f"@{name} as {_quote(name)}" for name, _type in parameters)
    return (
        f"declare @{SUPPRESS_PARAMETER} bit = 0;\n"
        f"{declarations}\n"
        "\n"
        f"{validation_body(document, body)}\n"
        "\n"
        f"select {projection};\n"
    )


def validation_body(document: SesDocument, body: str) -> str:
    """Render the SQL shared by installed and direct validation runs."""

    what = document.qualified
    program = parse_tsql_program(body, what=what, error=DiscoveryError)
    validate_validation_contract(
        program, what=what, kind=document.kind, error=DiscoveryError
    )
    if document.kind == ASSUMPTION:
        return _assumption_body(document, body)
    return _test_body(document, body)


def _assumption_body(document: SesDocument, body: str) -> str:
    violations = temp_table_name("#weaver_violations", document.qualified)
    return "\n\n".join(
        [
            _drop(violations),
            SETUP_BANNER,
            _capture_contract_queries(body, (violations,)),
            POSTPROCESSING_BANNER,
            f"select @violation_count = count(*) from {violations};",
            (
                f"if @{SUPPRESS_PARAMETER} = 0\n"
                "begin\n"
                f"    select * from {violations};\n"
                "end;"
            ),
            _drop(violations),
        ]
    )


def _test_body(document: SesDocument, body: str) -> str:
    """Materialise both sides before computing either difference.

    Both ``EXCEPT`` operations therefore compare the same snapshots.
    """

    qualified = document.qualified
    expected = temp_table_name("#weaver_expected", qualified)
    actual = temp_table_name("#weaver_actual", qualified)
    missing = temp_table_name("#weaver_missing", qualified)
    unexpected = temp_table_name("#weaver_unexpected", qualified)
    tables = (expected, actual, missing, unexpected)

    sections = [
        "\n".join(_drop(table) for table in tables),
        SETUP_BANNER,
        _capture_contract_queries(body, (expected, actual)),
        POSTPROCESSING_BANNER,
        _shape_guard(expected, actual, qualified),
    ]
    if document.primary_key:
        sections.append(_key_guard(expected, EXPECTED, document, qualified))
        sections.append(_key_guard(actual, ACTUAL, document, qualified))
    sections.extend(
        [
            _difference(missing, expected, actual),
            _difference(unexpected, actual, expected),
            (
                f"select @missing_count = count(*) from {missing};\n"
                f"select @unexpected_count = count(*) from {unexpected};"
            ),
            _diagnostics(document, missing, unexpected, qualified),
            "\n".join(_drop(table) for table in tables),
        ]
    )
    return "\n\n".join(section for section in sections if section)


def _difference(into: str, left: str, right: str) -> str:
    """Use a derived table to avoid ``INTO`` placement inside a set operation."""

    return (
        f"select * into {into} from (\n"
        f"    select * from {left}\n"
        "    except\n"
        f"    select * from {right}\n"
        ") as weaver_difference;"
    )


def _diagnostics(
    document: SesDocument, missing: str, unexpected: str, qualified: str
) -> str:
    """Render discrepancy rows only when the caller has not suppressed them."""

    if document.primary_key:
        return _correlated_diagnostics(document, missing, unexpected, qualified)
    return _unpaired_diagnostics(missing, unexpected)


def _correlated_diagnostics(
    document: SesDocument, missing: str, unexpected: str, qualified: str
) -> str:
    """Rank distinct keys separately from each side's complete rows.

    Weaver does not know the Test's columns, so ranking a union of complete rows
    would duplicate the added diagnostic column.
    """

    keys = temp_table_name("#weaver_keys", qualified)
    key_columns = ", ".join(_quote(column) for column in document.primary_key)
    join = " and ".join(
        f"k.{_quote(column)} = d.{_quote(column)}" for column in document.primary_key
    )
    return (
        f"if @{SUPPRESS_PARAMETER} = 0\n"
        "begin\n"
        f"    {_drop(keys)}\n"
        f"    select {key_columns}\n"
        f"         , dense_rank() over (order by {key_columns}) "
        f"as {_quote(SK_COLUMN)}\n"
        f"    into {keys}\n"
        "    from (\n"
        f"        select {key_columns} from {missing}\n"
        "        union\n"
        f"        select {key_columns} from {unexpected}\n"
        "    ) as weaver_key_values;\n"
        "\n"
        f"    select '{EXPECTED}' as {_quote(SIDE_COLUMN)}, "
        f"k.{_quote(SK_COLUMN)}, d.*\n"
        f"    from {missing} as d join {keys} as k on {join}\n"
        "    union all\n"
        f"    select '{ACTUAL}', k.{_quote(SK_COLUMN)}, d.*\n"
        f"    from {unexpected} as d join {keys} as k on {join};\n"
        "\n"
        f"    {_drop(keys)}\n"
        "end;"
    )


def _unpaired_diagnostics(missing: str, unexpected: str) -> str:
    """Give each unpaired row a unique diagnostic key.

    Actual-side numbering starts after the missing count.
    """

    numbered = "row_number() over (order by (select null))"
    return (
        f"if @{SUPPRESS_PARAMETER} = 0\n"
        "begin\n"
        f"    select '{EXPECTED}' as {_quote(SIDE_COLUMN)}, "
        f"{numbered} as {_quote(SK_COLUMN)}, d.*\n"
        f"    from {missing} as d\n"
        "    union all\n"
        f"    select '{ACTUAL}', @missing_count + {numbered}, d.*\n"
        f"    from {unexpected} as d;\n"
        "end;"
    )


def _shape_guard(expected: str, actual: str, qualified: str) -> str:
    """Treat incompatible relation shapes as execution failure, not evidence.

    Compare column counts after both sides are materialised.
    """

    return (
        "declare @weaver_expected_columns int = (\n"
        "    select count(*) from tempdb.sys.columns\n"
        f"    where object_id = object_id('tempdb..{expected}'));\n"
        "declare @weaver_actual_columns int = (\n"
        "    select count(*) from tempdb.sys.columns\n"
        f"    where object_id = object_id('tempdb..{actual}'));\n"
        "if @weaver_expected_columns <> @weaver_actual_columns\n"
        "begin\n"
        "    declare @weaver_shape nvarchar(400) = concat(\n"
        f"        N'{_escape(qualified)}: expected has ', @weaver_expected_columns,\n"
        "        N' column(s) and actual has ', @weaver_actual_columns,\n"
        "        N', the two sides of a Test must be the same shape to be compared');\n"
        "    throw 51020, @weaver_shape, 1;\n"
        "end;"
    )


def _key_guard(table: str, side: str, document: SesDocument, qualified: str) -> str:
    """Reject blank, null or duplicate keys that cannot correlate rows."""

    columns = document.primary_key
    blank = " or ".join(
        f"nullif(ltrim(rtrim(cast({_quote(column)} as nvarchar(max)))), '') is null"
        for column in columns
    )
    grouped = ", ".join(_quote(column) for column in columns)
    named = ", ".join(columns)
    return (
        f"if exists (select 1 from {table} where {blank})\n"
        "begin\n"
        f"    throw 51021, N'{_escape(qualified)}: the declared Primary key "
        f"({_escape(named)}) is null or blank on the {side} side, so it cannot "
        "identify a row', 1;\n"
        "end;\n"
        f"if exists (select 1 from {table} group by {grouped} having count(*) > 1)\n"
        "begin\n"
        f"    throw 51022, N'{_escape(qualified)}: the declared Primary key "
        f"({_escape(named)}) repeats on the {side} side, so it cannot correlate "
        "the two sides of the comparison', 1;\n"
        "end;"
    )


def _capture_contract_queries(body: str, into: tuple[str, ...]) -> str:
    """Divert contract queries while preserving all other source text.

    Offset splicing keeps setup, comments, formatting and separators unchanged.
    The shared ``INTO`` transform handles CTEs and set operations.
    """

    contract = [span for span in query_spans(body) if not selects_into(body, span)]
    pieces: list[str] = []
    cursor = 0
    for span, table in zip(contract, into):
        pieces.append(body[cursor : span.start])
        pieces.append(insert_select_into(body[span.start : span.end], table))
        cursor = span.end
    pieces.append(body[cursor:])
    return "".join(pieces).strip()


def _drop(table: str) -> str:
    return f"if object_id('tempdb..{table}') is not null drop table {table};"


def _quote(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _escape(text: str) -> str:
    return text.replace("'", "''")


def _indent(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line.strip() else line for line in text.splitlines())


__all__ = [
    "ASSUMPTION_PARAMETERS",
    "RESULT_PARAMETERS",
    "SK_COLUMN",
    "SIDE_COLUMN",
    "SUPPRESS_PARAMETER",
    "TEST_PARAMETERS",
    "generate_tsql_validation_batch",
    "generate_tsql_validation_script",
    "validation_body",
]
