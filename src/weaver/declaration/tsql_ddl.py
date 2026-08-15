"""Generate a self-contained T-SQL build script for a Warehouse object.

The script materialises query shape, validates metadata, and creates the main
table in one server-side execution. Declared and inferred schemas share the
same shape validation path.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import yaml

from ..errors import DiscoveryError
from .columns import metadata_column_references
from .metadata import SesDocument
from .sql_shaping import (
    insert_select_into,
    insert_where_one_eq_zero,
    query_spans,
    render_sql_template,
    selects_into,
)
from .tsql_program import parse_tsql_program, validate_query_contract

TYPE_MAPPING_PATH = Path(__file__).resolve().parent / "warehouse_type_mapping.yml"


# --- entry points -----------------------------------------------------------


def generate_tsql_table_script(document: SesDocument, body: str) -> str:
    """A self-contained T-SQL script that builds ``document``'s main table.

    The table's shape comes from the *staging* query and only from it. A body
    that also names the keys to delete describes two things — what the object
    holds, and which rows are leaving — and only the first is the table. The
    second is still materialised, but into a temp table of its own, so the build
    can say now that it names the primary key and nothing else rather than
    leaving the load to discover it.
    """

    mapping = _load_type_mapping()
    program = parse_tsql_program(body, what=document.qualified, error=DiscoveryError)
    validate_query_contract(
        program,
        what=document.qualified,
        primary_key=document.primary_key,
        incremental=document.is_incremental,
        error=DiscoveryError,
    )

    temp_table = _weaver_temp_table_name("#weaver_shape", document.qualified)
    delete_temp_table = _weaver_temp_table_name(
        "#weaver_delete_shape", document.qualified
    )
    shape_sql = _ensure_terminated(
        _materialise_shapes(
            body,
            len(program.queries),
            temp_table,
            delete_temp_table,
            what=document.qualified,
        )
    )

    if document.has_declared_schema:
        create_sql = _render_declared_create(document, temp_table)
    else:
        create_sql = _render_inferred_create(document, temp_table, mapping)

    deletes = program.deletes is not None
    return (
        "/* weaver generated table build script. */\n"
        "set nocount on;\n\n"
        f"{_drop_temp_tables(temp_table, delete_temp_table if deletes else None)}\n\n"
        f"{shape_sql}\n\n"
        f"{_render_delete_shape_validation(document, delete_temp_table) if deletes else ''}"
        f"{create_sql}\n"
        f"\n{_drop_temp_tables(temp_table, delete_temp_table if deletes else None)}\n"
    )


def _materialise_shapes(
    body: str,
    query_count: int,
    temp_table: str,
    delete_temp_table: str,
    *,
    what: str,
) -> str:
    """The whole body in shape-only form, each query diverted to its temp table.

    Guarded first and re-read afterwards, because guarding rewrites the text:
    the spans are recomputed over what will actually run, so the ``INTO`` lands
    in the query the server will see rather than in the one the author wrote.

    Later query first. Inserting text moves everything after it, and going
    backwards means no offset needs adjusting for an edit that has already
    happened.

    **The guard reaches only what it can see.** A body whose setup builds its
    working table through ``EXEC`` or ``sp_executesql`` runs that setup for
    real, because the alternative is reading the SQL inside a string literal —
    which Weaver does not do. Shape-only is a promise about the queries Weaver
    interprets, not about statements it merely passes on.
    """

    guarded = insert_where_one_eq_zero(body)
    spans = tuple(
        span for span in query_spans(guarded) if not selects_into(guarded, span)
    )
    if len(spans) != query_count:
        raise DiscoveryError(
            f"{what}: the shape-only form of this body has {len(spans)} result "
            f"queries where the body has {query_count}. The build and the load "
            "would stage different statements, so neither is generated."
        )

    shaped = guarded
    if len(spans) > 1:
        shaped = insert_select_into(shaped, delete_temp_table, span=spans[1])
    return insert_select_into(shaped, temp_table, span=spans[0])


def _drop_temp_tables(*names: str | None) -> str:
    return "\n".join(
        f"if object_id('tempdb..{name}') is not null drop table {name};"
        for name in names
        if name
    )


def _render_delete_shape_validation(document: SesDocument, temp_table: str) -> str:
    """Refuse a delete query that does not name exactly the primary key.

    The load deletes target rows by key, so a delete query producing anything
    else is either missing part of the key — in which case it would retire rows
    it never named — or carrying columns that mean nothing to a deletion. Both
    are authoring mistakes, and both are cheaper to state here than to discover
    in a load that has already begun.

    Case-exact, under a binary collation, like every other column-name contract
    in a Weaver build.
    """

    columns = _leading_comma_list(
        [f"({_sql_literal(name)})" for name in document.primary_key],
        first_indent="        ",
        comma_indent="      ",
    )
    return (
        render_sql_template(
            "ddl/delete_shape_validation",
            temp_object_literal=_sql_literal(f"tempdb..{temp_table}"),
            primary_key_columns_cte=(
                "    select column_name\n"
                "    from (values\n"
                f"{columns}\n"
                "    ) as pk(column_name)"
            ),
        ).rstrip()
        + "\n\n"
    )


def generate_tsql_view_script(document: SesDocument, body: str) -> str:
    """A strict ``CREATE VIEW`` over the validated query body."""

    return (
        f"create view {_quote_multipart(document.qualified)} as\n"
        f"{_normalise_view_body(body)}\n"
    )


# --- inferred path ----------------------------------------------------------


def _render_inferred_create(
    document: SesDocument, temp_table: str, mapping: dict
) -> str:
    target = _quote_multipart(document.qualified)
    temp_literal = _sql_literal(f"tempdb..{temp_table}")
    identity = document.identity_column
    return render_sql_template(
        "ddl/infer_create_table",
        temp_object_literal=temp_literal,
        metadata_validation_sql=_render_metadata_validation(document, temp_literal),
        identity_guard_sql=_render_identity_guard(identity, temp_literal),
        identity_column_sql=_render_identity_union(identity),
        first_ordinal="0" if identity else "1",
        primary_key_columns_cte=_render_primary_key_cte(document.primary_key),
        not_null_columns_cte=_render_name_only_cte(document.not_null),
        type_case=_render_type_case(mapping),
        target_table=target,
        target_table_literal=_sql_literal(target),
        pk_constraint=_pk_constraint_name(document.qualified),
    )


def _render_identity_union(column) -> str:
    """A leading ``all_columns`` entry (ordinal 0) for the identity column."""

    if column is None:
        return ""
    definition = _column_definition(column)
    # This is the leading SELECT of the all_columns CTE, so it must name both
    # columns — a CTE takes its column names from its first SELECT, and an
    # unnamed literal there is a T-SQL error ("No column name was specified").
    return (
        f"    select 0 as column_ordinal, {_sql_literal(definition)} as column_definition\n"
        "    union all\n\n"
    )


def _render_identity_guard(column, temp_literal: str) -> str:
    """Refuse an inferred query that already produces the identity column's name.

    The identity column is Weaver's own; if the query also produces it the create
    would carry two same-named columns. Checked case-insensitively, like Spark.
    """

    if column is None:
        return ""
    return (
        "if exists (\n"
        "    select 1 from tempdb.sys.columns\n"
        f"    where [object_id] = object_id({temp_literal})\n"
        f"        and lower(name) = lower({_sql_literal(column.name)})\n"
        ")\n"
        "begin\n"
        f"    throw 51006, {_sql_literal(f'Identity {column.name} collides with a query column')}, 1;\n"
        "end;\n"
    )


# --- declared path ----------------------------------------------------------


def _render_declared_create(document: SesDocument, temp_table: str) -> str:
    target = _quote_multipart(document.qualified)
    temp_literal = _sql_literal(f"tempdb..{temp_table}")
    return render_sql_template(
        "ddl/declared_create_table",
        temp_object_literal=temp_literal,
        metadata_validation_sql=_render_metadata_validation(document, temp_literal),
        declared_columns_cte=_render_declared_columns_cte(document),
        declared_column_definitions=_render_declared_definitions(document),
        target_table=target,
        target_table_literal=_sql_literal(target),
        pk_alter_sql=_render_declared_pk(document, target),
    )


def _render_declared_definitions(document: SesDocument) -> str:
    """Static column definitions: identity, declared business, then audit columns."""

    lines = [_column_definition(column) for column in document.effective_schema]
    return _leading_comma_list(lines, first_indent="        ", comma_indent="      ")


def _render_declared_columns_cte(document: SesDocument) -> str:
    values = _leading_comma_list(
        [f"({_sql_literal(column.name)})" for column in document.schema],
        first_indent="        ",
        comma_indent="      ",
    )
    return (
        "    select column_name\n"
        "    from (values\n"
        f"{values}\n"
        "    ) as declared(column_name)"
    )


def _render_declared_pk(document: SesDocument, target: str) -> str:
    if not document.primary_key:
        return ""
    columns = ", ".join(_quote_part(name) for name in document.primary_key)
    constraint = _pk_constraint_name(document.qualified)
    return (
        f"\nalter table {target} add constraint {constraint} "
        f"primary key nonclustered ({columns}) not enforced;\n"
    )


# --- shared rendering -------------------------------------------------------


def _column_definition(column) -> str:
    """One column's physical definition, identity included.

    The identity column is the only one whose type is not simply what it
    declares: the Warehouse generates its values, so the definition carries
    ``identity`` and a load never names the column in an insert. Nullability
    still comes from the column, as it does for every other definition here.

    Bare ``identity``, with no seed and increment: Fabric does not let either be
    chosen and refuses the parenthesised form even where it would spell Fabric's
    own behaviour. So the values a load sees are the engine's to decide, and
    nothing may assume they start at one or rise by one.
    """

    identity = " identity" if column.is_identity else ""
    return (
        f"{_quote_part(column.name)} {column.type}{identity}"
        f"{_nullability(column.not_null)}"
    )


def _render_metadata_validation(document: SesDocument, temp_literal: str) -> str:
    references = metadata_column_references(document)
    if not references:
        return ""
    return render_sql_template(
        "ddl/metadata_column_validation",
        temp_object_literal=temp_literal,
        metadata_columns_cte=_render_metadata_columns_cte(references),
        identity_available_sql=_render_identity_available(document.identity),
    ).rstrip()


def _render_identity_available(identity: str | None) -> str:
    """Make the identity column an *available* column for the metadata check.

    The identity is Weaver's own column, not one the query produces, so the
    primary key may name it — but the query-shape temp table does not contain it.
    Union it into the ``described`` set so a primary key on the surrogate resolves
    (mirrors the Python validator's available-set in ``weaver.ses.columns``).
    """

    if identity is None:
        return ""
    return f"\n\n    union all\n\n    select {_sql_literal(identity)} as column_name"


def _render_metadata_columns_cte(references: tuple[tuple[str, str], ...]) -> str:
    lines = []
    for index, (kind, column) in enumerate(references):
        prefix = "    select" if index == 0 else "    union all\n\n    select"
        lines.append(
            f"{prefix}\n"
            f"        {_sql_literal(kind)} as metadata_kind\n"
            f"      , {_sql_literal(column)} as column_name"
        )
    return "\n".join(lines)


def _render_primary_key_cte(primary_key: tuple[str, ...]) -> str:
    if not primary_key:
        return (
            "    select\n"
            "        convert(int, null) as column_ordinal\n"
            "      , convert(nvarchar(128), null) as column_name\n"
            "    where 1 = 0"
        )
    values = _leading_comma_list(
        [
            f"({index}, {_sql_literal(name)})"
            for index, name in enumerate(primary_key, start=1)
        ],
        first_indent="        ",
        comma_indent="      ",
    )
    return (
        "    select column_ordinal, column_name\n"
        "    from (values\n"
        f"{values}\n"
        "    ) as pk(column_ordinal, column_name)"
    )


def _render_name_only_cte(names: tuple[str, ...]) -> str:
    """A single-column ``(column_name)`` CTE, empty when there are no names."""

    if not names:
        return "    select convert(nvarchar(128), null) as column_name\n    where 1 = 0"
    values = _leading_comma_list(
        [f"({_sql_literal(name)})" for name in names],
        first_indent="        ",
        comma_indent="      ",
    )
    return (
        "    select column_name\n"
        "    from (values\n"
        f"{values}\n"
        "    ) as names(column_name)"
    )


# --- type mapping (ported) --------------------------------------------------


def _load_type_mapping() -> dict:
    with TYPE_MAPPING_PATH.open("r", encoding="utf-8") as mapping_file:
        loaded = yaml.safe_load(mapping_file) or {}
    if "mappings" not in loaded:
        raise ValueError("warehouse type mapping must define a mappings block")
    return loaded


def _render_type_case(mapping: dict) -> str:
    fallback = mapping.get("fallback_type", "varchar(max)")
    mappings = mapping.get("mappings", {})
    lines = ["case bt.base_type"]
    for source_type in sorted(mappings):
        expression = _render_target_type_expression(source_type, mappings[source_type])
        lines.append(f"            when '{source_type.lower()}' then {expression}")
    lines.append(f"            else N'{_escape_literal(fallback)}'")
    lines.append("        end")
    return "\n        ".join(lines)


def _render_target_type_expression(source_type: str, mapping: dict) -> str:
    target = mapping["target"]
    if "precision" in mapping and "scale" in mapping:
        precision = _numeric_part_expression(mapping["precision"], "precision")
        scale = _numeric_part_expression(mapping["scale"], "scale")
        return f"N'{target}(' + {precision} + N',' + {scale} + N')'"
    if "scale" in mapping:
        return f"N'{target}(' + {_scale_expression(mapping['scale'])} + N')'"
    if "length" in mapping:
        return f"N'{target}(' + {_length_expression(source_type, mapping['length'])} + N')'"
    return f"N'{target}'"


def _numeric_part_expression(value, column_name: str) -> str:
    if value == "source":
        default_value = "38" if column_name == "precision" else "0"
        return (
            f"convert(nvarchar(20), "
            f"coalesce(nullif(convert(int, d.{column_name}), 0), {default_value}))"
        )
    return f"N'{value}'"


def _scale_expression(value) -> str:
    if value == "min_source_6":
        return (
            "convert(nvarchar(20), "
            "case "
            "when d.scale is null then 6 "
            "when convert(int, d.scale) > 6 then 6 "
            "when convert(int, d.scale) < 0 then 0 "
            "else convert(int, d.scale) "
            "end)"
        )
    return f"N'{value}'"


def _length_expression(source_type: str, value) -> str:
    if value == "max":
        return "N'max'"
    if value == "source":
        divisor = "2" if source_type.lower() in {"nchar", "nvarchar"} else "1"
        source_length = f"convert(int, d.max_length) / {divisor}"
        return (
            "case "
            "when d.max_length = -1 then N'max' "
            "when d.max_length is null or d.max_length = 0 then N'1' "
            f"else convert(nvarchar(20), case when {source_length} < 1 then 1 else {source_length} end) "
            "end"
        )
    return f"N'{value}'"


# --- identifiers and literals -----------------------------------------------


def _normalise_view_body(body: str) -> str:
    text = body.strip()
    if text.endswith(";"):
        text = text[:-1].rstrip()
    if text[:1] == ";" and text[1:].lstrip().upper().startswith("WITH"):
        return text[1:].lstrip()
    return text


def _ensure_terminated(sql_text: str) -> str:
    stripped = sql_text.rstrip()
    return stripped if stripped.endswith(";") else f"{stripped};"


def _nullability(not_null: bool) -> str:
    return " not null" if not_null else " null"


def _pk_constraint_name(qualified: str) -> str:
    object_name = _unquote_part(_split_identifier(qualified)[-1])
    return _quote_part(f"PK_{object_name}")


def _weaver_temp_table_name(prefix: str, qualified: str) -> str:
    normalised_prefix = prefix if prefix.startswith("#") else f"#{prefix}"
    safe = re.sub(r"[^A-Za-z0-9_]", "_", qualified)
    candidate = f"{normalised_prefix}_{safe}"
    if len(candidate) > 111:
        digest = hashlib.sha1(qualified.encode("utf-8")).hexdigest()[:12]
        candidate = f"{candidate[:98]}_{digest}"
    return candidate


def _leading_comma_list(
    items: list[str], *, first_indent: str = "    ", comma_indent: str = "  "
) -> str:
    if not items:
        return ""
    lines = [f"{first_indent}{items[0]}"]
    lines.extend(f"{comma_indent}, {item}" for item in items[1:])
    return "\n".join(lines)


def _quote_multipart(identifier: str) -> str:
    parts = _split_identifier(identifier)
    if not parts:
        raise ValueError("identifier must not be empty")
    return ".".join(_quote_part(part) for part in parts)


def _split_identifier(identifier: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_brackets = False
    for character in identifier.strip():
        if character == "[" and not in_brackets:
            in_brackets = True
            current.append(character)
            continue
        if character == "]" and in_brackets:
            in_brackets = False
            current.append(character)
            continue
        if character == "." and not in_brackets:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(character)
    part = "".join(current).strip()
    if part:
        parts.append(part)
    return parts


def _quote_part(part: str) -> str:
    stripped = _unquote_part(part)
    return f"[{stripped.replace(']', ']]')}]"


def _unquote_part(part: str) -> str:
    stripped = part.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        return stripped[1:-1].replace("]]", "]")
    return stripped


def _sql_literal(value: str) -> str:
    return f"N'{_escape_literal(value)}'"


def _escape_literal(value: str) -> str:
    return value.replace("'", "''")
