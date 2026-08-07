"""The built-in Weaver document repository that declares the catalogue tables.

Weaver's catalogue is built by Weaver, from ordinary Weaver document, through the ordinary
planner and installer. There is no second "create the control tables" path — that
recursion is the point, and it is the proof that a catalogue table is an ordinary
Weaver object rather than a privileged one.

The item is rendered from :mod:`weaver.catalogue.tables`, so the declaration and
the table definitions cannot drift: there is one source of truth and the text is
derived from it. ``Lakehouse/_weaver`` is composed into the parsed repository
in memory and built through the ordinary planner.

Every table declares:

``Static: true``
    Its rows are not produced by a load. Catalogue rows are maintained only by
    the DML a build appends.

``Prohibit rebuild: true``
    This stops an ordinary build treating the catalogue as a disposable
    application object.

``Dependencies: []``
    Explicitly nothing. The body is literals, so there is nothing to discover
    and nothing to declare.
"""

from __future__ import annotations

import textwrap
from dataclasses import replace

from .tables import CATALOGUE_SCHEMA, CATALOGUE_TABLES, CatalogueColumn, CatalogueTable

#: The reserved item Weaver generates and manages inside the declaration.
ITEM_ROOT = "Lakehouse/_weaver"
SCHEMA_PATH = f"{ITEM_ROOT}/schemas/{CATALOGUE_SCHEMA}.yml"

#: The folder every top-level Weaver task writes its evidence beneath, and the
#: document that declares it. ``_`` + ``__`` + ``Log`` spells ``___Log``, which
#: the parser reads as ``_.Log`` — see
#: :func:`weaver.declaration.source.python_id_parts`.
LOG_FOLDER = "Log"
LOG_FOLDER_ID = f"{CATALOGUE_SCHEMA}.{LOG_FOLDER}"
LOG_PATH = f"{ITEM_ROOT}/Files/{CATALOGUE_SCHEMA}{'__'}{LOG_FOLDER}.py"

#: Where the folder puts its files, relative to the Weaver Lakehouse's ``Files``
#: area. Derived from the identity exactly as any Folder's location is, so the
#: logger addresses the *declared* folder rather than a path it happens to know.
LOG_ROOT = f"{CATALOGUE_SCHEMA}/{LOG_FOLDER}"

_LOG_CLASS = f"{CATALOGUE_SCHEMA}{'__'}{LOG_FOLDER}"

#: One sentence, the same on every table, saying where the rows come from. It is
#: not boilerplate: "never loaded" is the fact that makes ``Static: true``
#: correct, and a reader of any one file should be told it.
LINEAGE = (
    "Projected from validated Weaver document declarations by Weaver's own build, and "
    "maintained only by the catalogue DML a build appends. Never populated by a "
    "load."
)

SCHEMA_DESCRIPTION = (
    "Weaver's own control plane. These tables record what Weaver has built and "
    "what it certifies as installed; they are declared as ordinary Weaver document and built "
    "by Weaver itself, and are never authored or loaded by hand."
)

_WIDTH = 76


def _escaped(text: str) -> str:
    """Metadata text with dollars escaped.

    A ``$`` opens a ``$Schema.Object`` reference, and several catalogue columns are
    described in terms of one — ``description_reference`` holds "the
    $Schema.Object the description was copied from". Written raw, that would parse
    as a reference and be refused, so it is escaped the way Weaver document specifies.
    """

    return text.replace("$", "$$")


def _folded(key: str, text: str, *, indent: int = 0) -> str:
    """A YAML folded block, so prose can wrap without becoming multi-line text.

    ``>-`` folds newlines into spaces and strips the trailing one, which keeps the
    parsed value a single sentence however it is laid out in the file. Used
    uniformly rather than only when needed: a plain scalar is fine until a
    description happens to contain a colon, and this removes the class of problem.
    """

    pad = " " * indent
    body = textwrap.fill(
        _escaped(text), width=_WIDTH - indent - 2, initial_indent="", subsequent_indent=""
    )
    lines = "\n".join(f"{pad}  {line}" for line in body.splitlines())
    return f"{pad}{key}: >-\n{lines}"


def render_schema_file() -> str:
    """The ``_schemas/_.yml`` declaration for the catalogue schema."""

    return (
        f"Schema ID: {CATALOGUE_SCHEMA}\n"
        "\n"
        f"{_folded('Description', SCHEMA_DESCRIPTION)}\n"
    )


def _body(table: CatalogueTable) -> str:
    """A query that declares the shape and returns no rows.

    ``where 1 = 0`` with no ``FROM`` is valid Spark SQL and is the whole trick: the
    executor resolves the query's schema to create the table, and resolving a
    schema reads no rows. Build creates structure; this is the smallest possible
    statement that describes one.

    Terminated, like every other authored Spark SQL statement: what Weaver
    generates has to satisfy the rule Weaver enforces, or the built-in item
    would be the one repository nobody could have written by hand.
    """

    def line(column: CatalogueColumn, first: bool) -> str:
        lead = "select" if first else "     ,"
        return f"{lead} cast(null as {column.type}) as `{column.name}`"

    lines = [line(column, index == 0) for index, column in enumerate(table.columns)]
    return "\n".join(lines) + "\n where 1 = 0;\n"


def render_source(table: CatalogueTable) -> str:
    """The complete Weaver document source file for one catalogue table."""

    not_null = [
        column.name
        for column in table.columns
        if column.not_null and column.name not in table.key
    ]

    sections: list[str] = [
        f"Table ID: {table.qualified}",
        _folded("Description", table.description),
        _folded("Lineage", LINEAGE),
        "Dependencies: []",
        "Static: true",
        "Prohibit rebuild: true",
        # The key is declared as the primary key, so the catalogue's own tables
        # describe themselves: Weaver document makes key columns not null, and the projection
        # records the key in the catalogue like any other object's.
        f"Primary key: {', '.join(table.key)}",
    ]
    if not_null:
        sections.append("Not null:\n" + "\n".join(f"  - {name}" for name in not_null))
    sections.append(
        "Schema:\n"
        + "\n".join(f"  {column.name}: {column.type}" for column in table.columns)
    )
    sections.append(
        "Column notes:\n"
        + "\n".join(
            _folded(column.name, column.description, indent=2)
            for column in table.columns
        )
    )

    header = "\n\n".join(sections)
    return f"/*\n{header}\n*/\n{_body(table)}"


def render_log_file() -> str:
    """The declaration for ``Files/_/Log`` — where task evidence is written.

    An ordinary Folder document, and that is the whole point. A task log could
    have been a path the logger alone knew about, but then its creation, its
    registration, its survival through a prune and its removal would each need a
    rule of their own. Declared here it is claimed, projected, inventoried,
    installed, converged and protected by the machinery that already exists, and
    the logger asks the *folder* where to write rather than composing a path.

    ``Static: true`` because nothing loads into it: a task writes its own
    evidence beneath it, exactly as a Folder object's authored code writes files
    into its destination. ``Incremental: false`` for the same reason the runtime
    tree declares it — the folder itself accumulates nothing that Weaver claims
    file by file.

    ``File key`` claims everything beneath, because everything beneath *is*
    Weaver's — task evidence and nothing else. It is an accurate statement of
    ownership rather than a licence to delete: nothing loads this folder, and a
    written task file is never rewritten.
    """

    return f'''\
"""
Folder ID: {LOG_FOLDER_ID}

Description: >-
  Where every top-level Weaver task — wipe, mirror, build, load and test —
  writes its immutable evidence. One folder per task, partitioned by the UTC
  date the task started.

Lineage: >-
  Written by Weaver's own task logger as each top-level task runs. Never
  authored, and never populated by a load.

File key: "**/*"

Incremental: false

Static: true
"""
from weaver import Folder


class {_LOG_CLASS}(Folder):
    def read(self):
        return self.staging_folder(), []
'''


def render_item_sources() -> dict[str, str]:
    sources = {SCHEMA_PATH: render_schema_file(), LOG_PATH: render_log_file()}
    for table in CATALOGUE_TABLES:
        documented = replace(
            table,
            columns=tuple(
                replace(
                    column,
                    description=column.description
                    or f"The catalogue value for {column.name.replace('_', ' ')}.",
                )
                for column in table.columns
            ),
        )
        sources[f"{ITEM_ROOT}/{table.qualified}.sql"] = render_source(documented)
    return sources


def item_repository_files() -> dict[str, bytes]:
    return {
        path: text.encode("utf-8") for path, text in render_item_sources().items()
    }
