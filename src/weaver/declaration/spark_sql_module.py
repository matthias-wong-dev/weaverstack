"""Compile Spark SQL documents into deployed Python primitives."""

from __future__ import annotations

from ..objects import CLASS_ID_SEPARATOR
from .dependencies import address_managed_references
from .metadata import ASSUMPTION, TABLE, TEST, ObjectId, SesDocument

#: Keep the marker above the docstring so the metadata remains ``__doc__``.
GENERATED_MODULE_MARKER = "# Weaver generated"

SQL_ATTRIBUTE = "SQL"


def deployed_module_name(object_id: ObjectId) -> str:
    """Return the canonical deployed file name for an object."""

    return f"{class_name(object_id)}.py"


def class_name(object_id: ObjectId) -> str:
    return f"{object_id.schema}{CLASS_ID_SEPARATOR}{object_id.object}"


#: Every generated kind must define both its base class and marker label.
GENERATED_BASE = {
    TABLE: ("SparkSqlTable", "load"),
    TEST: ("SparkSqlTest", "test"),
    ASSUMPTION: ("SparkSqlAssumption", "assumption"),
}


def render_spark_sql_module(
    document: SesDocument, *, header: str, body: str, source_name: str
) -> str:
    """Render a deployed module while preserving authored metadata and SQL.

    ``header`` becomes the module docstring. ``body`` already contains addressed
    object tokens for the installer to resolve.
    """

    name = class_name(document.object_id)
    base, what = GENERATED_BASE[document.kind]
    return (
        # This signed line is generated output; even punctuation changes rebuilds.
        f"{GENERATED_MODULE_MARKER} {what} \u2014 {document.qualified}, "
        f"from {source_name}\n"
        f"{python_string(header)}\n"
        "\n"
        f"from weaver import {base}\n"
        "\n"
        "\n"
        f"{SQL_ATTRIBUTE} = {python_string(body)}\n"
        "\n"
        "\n"
        f"class {name}({base}):\n"
        f"    sql = {SQL_ATTRIBUTE}\n"
    )


def addressed(body: str, destination) -> str:
    return address_managed_references(body, destination)


def python_string(text: str) -> str:
    """Encode text as a readable triple-quoted literal with the same value.

    Backslashes are doubled. Quotes are escaped only where a quote run or the end
    of the text could close the literal. The opening continuation removes its own
    newline from the value.
    """

    doubled = text.replace("\\", "\\\\")
    escaped = "".join(
        '\\"'
        if character == '"' and (index + 1 == len(doubled) or doubled[index + 1] == '"')
        else character
        for index, character in enumerate(doubled)
    )
    return f'"""\\\n{escaped}"""'


__all__ = [
    "GENERATED_BASE",
    "GENERATED_MODULE_MARKER",
    "SQL_ATTRIBUTE",
    "addressed",
    "class_name",
    "deployed_module_name",
    "python_string",
    "render_spark_sql_module",
]
