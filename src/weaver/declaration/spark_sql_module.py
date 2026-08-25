"""Compile Spark SQL documents into deployed Python primitives.

Generated modules retain authored metadata and SQL while using the ordinary
Delta load or validation runtime.
"""

from __future__ import annotations

from ..objects import CLASS_ID_SEPARATOR
from .dependencies import address_managed_references
from .metadata import ASSUMPTION, TABLE, TEST, ObjectId, SesDocument

#: The first line of every generated module. A comment, so it sits above the
#: docstring without displacing it as the module's ``__doc__``.
#:
#: A prefix rather than the whole line: what follows says which kind of
#: primitive was generated, and the installer only needs to know that one was.
#: A generated module carries object tokens for it to resolve, and an authored
#: one does not.
GENERATED_MODULE_MARKER = "# Weaver generated"

#: What the module calls its program. Public because the generated class refers
#: to it, and the file presents its SQL under an obvious name.
SQL_ATTRIBUTE = "SQL"


def deployed_module_name(object_id: ObjectId) -> str:
    """``Sales.OrderSummary`` → ``Sales__OrderSummary.py``.

    One spelling, used by the build that writes the file and by the planner that
    later looks for it, because a build and a load disagreeing about where a
    primitive lives is a defect neither of them can see.
    """

    return f"{class_name(object_id)}.py"


def class_name(object_id: ObjectId) -> str:
    """``Sales.OrderSummary`` → ``Sales__OrderSummary``."""

    return f"{object_id.schema}{CLASS_ID_SEPARATOR}{object_id.object}"


#: Which generated base carries which authored kind, and what the marker line
#: calls it. One mapping, so a new kind cannot acquire a module shape without
#: acquiring a name for it.
GENERATED_BASE = {
    TABLE: ("SparkSqlTable", "load"),
    TEST: ("SparkSqlTest", "test"),
    ASSUMPTION: ("SparkSqlAssumption", "assumption"),
}


def render_spark_sql_module(
    document: SesDocument, *, header: str, body: str, source_name: str
) -> str:
    """The complete deployed module for one authored Spark SQL declaration.

    A table, a Test and an Assumption differ in exactly two characters of this
    file, the base class and the word on the marker line, because the three are
    one arrangement: authored SQL, carried verbatim, under a docstring that
    is the contract, with a Weaver base supplying everything else.

    ``header`` is the authored metadata block verbatim and ``body`` the authored
    SQL with its object references already addressed as tokens. The installer
    resolves those on the way down, so the bundle stays destination-free while
    the installed file is runnable by anyone who opens it.
    """

    name = class_name(document.object_id)
    base, what = GENERATED_BASE[document.kind]
    return (
        # The em dash stays: this text is signed into every deployed module, so
        # changing it rebuilds every load artefact in every estate.
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
    """Name every managed reference in the program, as the build payloads do.

    The deployed module carries final Fabric names, so opening it shows exactly
    what it reads and writes.
    """

    return address_managed_references(body, destination)


def python_string(text: str) -> str:
    """``text`` as a Python triple-quoted literal that evaluates back to it.

    The one encoder, and it is the readable one: SQL stays legible
    in the installed file rather than becoming an escaped single line, because
    the file is something an operator opens when a load misbehaves.

    Two rules, and between them they are exhaustive. Every backslash is doubled,
    so nothing in the text can be read as an escape. Then every quote that is
    followed by another quote, or that ends the text, is escaped. That is exactly
    enough: no run of three unescaped quotes can survive it, so the
    literal cannot be closed early, and the text cannot merge with the closing
    delimiter. A lone quote between other characters is left alone, which is what
    keeps ordinary SQL readable.

    The opening ``\\`` continuation swallows the newline after it, so the value
    starts at the text's first character rather than one line late.
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
