"""Generate a self-contained T-SQL build script for a Warehouse object."""

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


def generate_tsql_table_script(document: SesDocument, body: str) -> str:
    """Generate a script that builds the main table from its staging-query shape.

    A delete query is materialised separately and validated against the primary
    key; it never contributes to table shape.
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
    """Divert each result query in the shape-only body to its temp table.

    Guarding changes offsets, so spans are recomputed afterwards. Insertions run
    from the later query backwards so earlier offsets remain valid.

    Dynamic SQL inside ``EXEC`` or ``sp_executesql`` remains opaque and runs
    unchanged; shape-only guards apply only to parsed queries.
    """

    guarded = insert_where_one_eq_zero(body)
    spans = tuple(
        span for span in query_spans(guarded) if not selects_into(guarded, span)
    )
    if len(spans) != query_count:
        raise DiscoveryError(
            f"{what}: shape-only conversion changes the result-query count from "
            f"{query_count} to {len(spans)}, so the table cannot be generated."
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
    """Validate that a delete query returns exactly the primary key.

    Column names are compared case-exactly under a binary collation.
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
    return (
        f"create view {_quote_multipart(document.qualified)} as\n"
        f"{_normalise_view_body(body)}\n"
    )


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
        identity_guard_sql=_render_identity_guard(identity, temp_literal)
        + _render_internal_guard(document.signature_column, temp_literal),
        identity_column_sql=_render_identity_union(identity),
        signature_column_sql=_render_signature_union(document.signature_column),
        first_ordinal="0" if identity else "1",
        primary_key_columns_cte=_render_primary_key_cte(document.primary_key),
        not_null_columns_cte=_render_name_only_cte(document.not_null),
        type_case=_render_type_case(mapping),
        target_table=target,
        target_table_literal=_sql_literal(target),
        pk_constraint=_pk_constraint_name(document.qualified),
    )


def _render_identity_union(column) -> str:
    if column is None:
        return ""
    definition = _column_definition(column)
    # The leading SELECT defines both CTE column names.
    return (
        f"    select 0 as column_ordinal, {_sql_literal(definition)} as column_definition\n"
        "    union all\n\n"
    )


def _render_signature_union(column) -> str:
    """Append the row signature after the template's audit columns."""

    if column is None:
        return ""
    return (
        "    union all\n"
        f"    select 1000004, {_sql_literal(_column_definition(column))}\n"
    )


def _render_identity_guard(column, temp_literal: str) -> str:
    """Guard the managed identity name case-insensitively, as Spark does."""

    if column is None:
        return ""
    return _render_internal_guard(column, temp_literal, what=f"Identity {column.name}")


def _render_internal_guard(column, temp_literal: str, what: str | None = None) -> str:
    """Guard a managed column name case-insensitively, as Spark does."""

    if column is None:
        return ""
    subject = what or f"{column.name} is Weaver's own column and"
    return (
        "if exists (\n"
        "    select 1 from tempdb.sys.columns\n"
        f"    where [object_id] = object_id({temp_literal})\n"
        f"        and lower(name) = lower({_sql_literal(column.name)})\n"
        ")\n"
        "begin\n"
        f"    throw 51006, {_sql_literal(f'{subject} collides with a query column')}, 1;\n"
        "end;\n"
    )


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


def _column_definition(column) -> str:
    """Render a column definition, including a bare Fabric identity clause.

    Fabric does not accept a seed or increment. Identity values are therefore
    engine-defined and must not be assumed to start at one or increase by one.
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
    """Include the managed identity in metadata-reference validation."""

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
