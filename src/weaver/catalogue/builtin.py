"""Generate the built-in ``Warehouse/_weaver`` declaration Item."""

from __future__ import annotations

import textwrap
from dataclasses import replace

from ..declaration.model import WAREHOUSE, WeaverItemId
from .tables import (
    BOOKMARK,
    CATALOGUE_TABLES,
    CATALOGUE_SCHEMA,
    LOG,
    RUNTIME_TABLES,
    CatalogueColumn,
)

#: The reserved Item that owns the catalogue declaration.
BUILTIN_ITEM = WeaverItemId(WAREHOUSE, "_weaver")
ITEM_ROOT = str(BUILTIN_ITEM)
SCHEMA_PATH = f"{ITEM_ROOT}/schemas/{CATALOGUE_SCHEMA}.yml"

LINEAGE = (
    "Projected from validated Weaver document declarations by Weaver's own build, and "
    "maintained only by the catalogue DML a build appends. Never populated by a "
    "load."
)

LOG_LINEAGE = (
    "Appended by Weaver's own runs as each unit of work settles. Never "
    "authored, never projected from a declaration, and never populated by a "
    "load."
)

BOOKMARK_LINEAGE = (
    "Maintained by Weaver's own build and load lifecycle: a build resets the "
    "objects it rebuilds and removes the rows of objects it no longer loads, and "
    "a clean load advances the object it loaded. Never authored, never projected "
    "from a declaration, and never populated by a load."
)

#: What each runtime-maintained table says about where its rows come from. Held
#: here rather than on the table, because a lineage is declaration prose.
RUNTIME_LINEAGE = {LOG.name: LOG_LINEAGE, BOOKMARK.name: BOOKMARK_LINEAGE}

SCHEMA_DESCRIPTION = (
    "Weaver's own catalogue. These tables record what Weaver has built and "
    "what it certifies as installed; they are declared as ordinary Weaver document and built "
    "by Weaver itself, and are never authored or loaded by hand."
)

_WIDTH = 76


def _escaped(text: str) -> str:
    """Escape dollars that metadata would parse as object references."""

    return text.replace("$", "$$")


def _folded(key: str, text: str, *, indent: int = 0) -> str:
    """Render wrapped prose as one YAML scalar."""

    pad = " " * indent
    body = textwrap.fill(
        _escaped(text),
        width=_WIDTH - indent - 2,
        initial_indent="",
        subsequent_indent="",
    )
    lines = "\n".join(f"{pad}  {line}" for line in body.splitlines())
    return f"{pad}{key}: >-\n{lines}"


def render_schema_file() -> str:
    """The ``schemas/_.yml`` declaration for the catalogue schema."""

    return (
        f"Schema ID: {CATALOGUE_SCHEMA}\n"
        "\n"
        f"{_folded('Description', SCHEMA_DESCRIPTION)}\n"
    )


def _body(table) -> str:
    """A T-SQL query that declares the shape and returns no rows."""

    def line(column: CatalogueColumn, first: bool) -> str:
        lead = "select" if first else "     ,"
        return f"{lead} cast(null as {column.warehouse_type}) as [{column.public_name}]"

    lines = [line(column, index == 0) for index, column in enumerate(table.columns)]
    return "\n".join(lines) + "\n where 1 = 0\n"


def render_source(table, *, lineage: str = LINEAGE) -> str:
    """The complete Weaver document source file for one ``_`` schema table.

    Projected and runtime-maintained tables are declared the same way and differ
    only in what their lineage says, because what differs between them is where
    their rows come from rather than what they are.
    """

    not_null = [
        column.public_name
        for column in table.columns
        if column.not_null and column.name not in table.key
    ]

    sections: list[str] = [
        f"Table ID: {table.qualified}",
        _folded("Description", table.description),
        _folded("Lineage", lineage),
        "Dependencies: []",
        "Static: true",
        "Prohibit rebuild: true",
        # Written by the catalogue's own DML, never by a load. So there is no load
        # to install and no row signature to keep for one.
        "Has load procedure: false",
        # The key is declared as the primary key, so the catalogue's own tables
        # describe themselves: Weaver document makes key columns not null, and the projection
        # records the key in the catalogue like any other object's.
        "Primary key: " + ", ".join(table.public_name_of(name) for name in table.key),
    ]
    if not_null:
        sections.append("Not null:\n" + "\n".join(f"  - {name}" for name in not_null))
    sections.append(
        "Schema:\n"
        + "\n".join(
            f"  {column.public_name}: {column.warehouse_type}"
            for column in table.columns
        )
    )
    sections.append(
        "Column notes:\n"
        + "\n".join(
            _folded(column.public_name, column.description, indent=2)
            for column in table.columns
        )
    )

    header = "\n\n".join(sections)
    return f"/*\n{header}\n*/\n{_body(table)}"


def render_item_sources() -> dict[str, str]:
    sources = {SCHEMA_PATH: render_schema_file()}
    runtime = {table.name for table in RUNTIME_TABLES}
    for table in CATALOGUE_TABLES:
        documented = replace(
            table,
            columns=tuple(
                replace(
                    column,
                    description=column.description
                    or f"The catalogue value for {column.public_name}.",
                )
                for column in table.columns
            ),
        )
        sources[f"{ITEM_ROOT}/{table.qualified}.sql"] = render_source(
            documented,
            lineage=RUNTIME_LINEAGE[table.name] if table.name in runtime else LINEAGE,
        )
    return sources


def item_repository_files() -> dict[str, bytes]:
    return {path: text.encode("utf-8") for path, text in render_item_sources().items()}
