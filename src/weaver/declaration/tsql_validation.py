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

#: The diagnostic columns, ahead of the validation's own. Snake case and
#: reserved, matching what :mod:`weaver.runtime.test_compare` adds on the Spark
#: side. One vocabulary, whichever engine produced the rows.
SIDE_COLUMN = "_weaver_side"
SK_COLUMN = "_weaver_sk"
EXPECTED = "expected"
ACTUAL = "actual"

#: The output parameters each kind exposes, and their T-SQL types. One
#: definition, read from both ends: the generator writes the signature from it
#: and the caller declares locals to match, so a parameter cannot be added to
#: one and forgotten in the other.
TEST_PARAMETERS = (("missing_count", "bigint"), ("unexpected_count", "bigint"))
ASSUMPTION_PARAMETERS = (("violation_count", "bigint"),)
RESULT_PARAMETERS = {TEST: TEST_PARAMETERS, ASSUMPTION: ASSUMPTION_PARAMETERS}

#: The flag every validation procedure carries, defaulting to returning the
#: diagnostics. A person running one by hand needs the evidence; orchestration
#: is the caller that has to ask for silence, and it knows to.
SUPPRESS_PARAMETER = "suppress_result_set"

#: Banners marking where the author's own code sits in the generated procedure,
#: so authored text is distinguishable from what Weaver added. The same two
#: words a generated load procedure uses.
SETUP_BANNER = "/*-- Pre-processing --*/"
POSTPROCESSING_BANNER = "/*-- Post-processing --*/"


def generate_tsql_validation_script(
    document: SesDocument, body: str, *, procedure_name: str
) -> str:
    """The installable procedure for one Warehouse validation."""

    core = validation_body(document, body)
    # The counts first and the flag last, because that is the order the
    # meets them in: what the validation found, then how much of it to return.
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
    """The same body, runnable directly, without installing anything.

    What ``weaver test --file`` executes. The locals stand in for the
    procedure's output parameters and are projected at the end, so a file run
    gives the same counts from the same SQL a build installs.
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
    """The core SQL both the procedure and the direct batch run.

    Everything between ``set nocount on`` and the procedure's ``end``: the
    author's setup, their contract queries captured into temp tables, the
    comparison, the counts, and the diagnostics behind the suppression flag.
    """

    what = document.qualified
    program = parse_tsql_program(body, what=what, error=DiscoveryError)
    validate_validation_contract(
        program, what=what, kind=document.kind, error=DiscoveryError
    )
    if document.kind == ASSUMPTION:
        return _assumption_body(document, body)
    return _test_body(document, body)


# --- Assumption ---------------------------------------------------------------


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


# --- Test ---------------------------------------------------------------------


def _test_body(document: SesDocument, body: str) -> str:
    """Capture both sides, difference them both ways, then correlate.

    The same order the Spark comparison uses: both relations are materialised
    first, so the two ``EXCEPT``s read one snapshot of each rather than
    differencing data that moved between two runs of the author's queries.
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
    """One side of the symmetric difference, as a set.

    A derived table rather than ``select … into … except …``, which avoids the
    placement rules for ``INTO`` inside a set operation.
    """

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
    """The discrepancy rows, keyed so the two sides pair.

    Returned only when the caller needs them, because they may be large and may
    carry sensitive business data.
    """

    if document.primary_key:
        return _correlated_diagnostics(document, missing, unexpected, qualified)
    return _unpaired_diagnostics(missing, unexpected)


def _correlated_diagnostics(
    document: SesDocument, missing: str, unexpected: str, qualified: str
) -> str:
    """Rank the distinct key values once, then join both sides to them.

    Ranking over a union of the rows does not work: they are projected with
    ``*``, since Weaver does not know a Test's columns, so a ``_weaver_side``
    added to the union would appear twice. Ranking the keys needs only the
    declared key names and leaves ``d.*`` to carry the Test's own columns.
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
    """No key, so nothing is paired and every row is keyed on its own.

    Each side is numbered within itself and the actual side offset past the
    known missing count: distinct keys throughout, and no claim that two rows
    describe one entity.
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


# --- guards, which are execution failures rather than evidence -----------------


def _shape_guard(expected: str, actual: str, qualified: str) -> str:
    """Two sides that cannot be compared are a broken Test, not a failing one.

    ``EXCEPT`` would report it as a query-plan error, while the mistake is about
    two relations the author believes are the same shape. The column counts come
    from ``tempdb.sys.columns``, once both sides are materialised.
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
    """A declared key that does not identify rows cannot correlate them.

    Blank, null and duplicate keys are refused on the same terms a load refuses
    them: a Test whose key repeats would pair rows arbitrarily.
    """

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


# --- capturing the authored queries -------------------------------------------


def _capture_contract_queries(body: str, into: tuple[str, ...]) -> str:
    """The authored body, with each contract query diverted into a temp table.

    A single pass over the original text, so everything other than the contract
    queries travels verbatim and in place. Setup, comments, formatting,
    separators. That is why this splices offsets rather than reassembling
    statements.

    The ``INTO`` is placed by the same offset-exact transform the shape-only
    build uses, so a CTE gets it on the body ``SELECT`` and a set operation on
    its first branch.
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
