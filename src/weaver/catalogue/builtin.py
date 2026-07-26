"""The built-in SES repository that declares the catalogue tables.

Weaver's catalogue is built by Weaver, from ordinary SES, through the ordinary
planner and installer. There is no second "create the control tables" path — that
recursion is the point, and it is the proof that a catalogue table is an ordinary
Weaver object rather than a privileged one.

The repository ships as package resources under ``weaver/builtin/catalogue``, so
an installed wheel carries it and setup materialises it deterministically. The
text is committed rather than generated at runtime because it is the *contract*:
a reviewer should be able to read the declaration of ``_.Registry`` as SES, the
same way they would read any other object.

Committed text and generated shape could drift, so they cannot be allowed to.
:func:`render_sources` produces the canonical text from
:mod:`weaver.catalogue.tables`, and a test asserts the shipped resources match it
byte for byte. Adding a column to a table definition therefore fails the suite
until the resource is regenerated — which is the loud failure the whole
arrangement exists to produce.

Every table declares:

``Static: true``
    Its rows are not produced by a load. Catalogue rows are maintained only by
    the DML a build appends.

``Prohibit rebuild: true``
    Once drop policy lands, this is what stops an ordinary build treating the
    catalogue as a disposable application object.

``Dependencies: []``
    Explicitly nothing. The body is literals, so there is nothing to discover
    and nothing to declare.
"""

from __future__ import annotations

import textwrap
from importlib.resources import files

from .tables import CATALOGUE_SCHEMA, CATALOGUE_TABLES, CatalogueColumn, CatalogueTable

#: Where the resources live, as an importable package path. Read through
#: ``importlib.resources`` so it works from a source tree and from a wheel alike.
RESOURCE_PACKAGE = "weaver.builtin"
RESOURCE_DIRECTORY = "catalogue"

SCHEMA_FILE = f"_schemas/{CATALOGUE_SCHEMA}.yml"

#: One sentence, the same on every table, saying where the rows come from. It is
#: not boilerplate: "never loaded" is the fact that makes ``Static: true``
#: correct, and a reader of any one file should be told it.
LINEAGE = (
    "Projected from validated SES declarations by Weaver's own build, and "
    "maintained only by the catalogue DML a build appends. Never populated by a "
    "load."
)

SCHEMA_DESCRIPTION = (
    "Weaver's own control plane. These tables record what Weaver has built and "
    "what it certifies as installed; they are declared as ordinary SES and built "
    "by Weaver itself, and are never authored or loaded by hand."
)

_WIDTH = 76


def _escaped(text: str) -> str:
    """Metadata text with dollars escaped.

    A ``$`` opens a ``$Schema.Object`` reference, and several catalogue columns are
    described in terms of one — ``description_reference`` holds "the
    $Schema.Object the description was copied from". Written raw, that would parse
    as a reference and be refused, so it is escaped the way SES specifies.
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
    """

    def line(column: CatalogueColumn, first: bool) -> str:
        lead = "select" if first else "     ,"
        return f"{lead} cast(null as {column.type}) as `{column.name}`"

    lines = [line(column, index == 0) for index, column in enumerate(table.columns)]
    return "\n".join(lines) + "\n where 1 = 0\n"


def render_source(table: CatalogueTable) -> str:
    """The complete SES source file for one catalogue table."""

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
        # describe themselves: SES makes key columns not null, and the projection
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


def render_sources() -> dict[str, str]:
    """The canonical text of every file in the built-in repository.

    Keyed by repository-relative path, which is exactly what setup materialises
    and what the drift test compares against the shipped resources.
    """

    sources = {SCHEMA_FILE: render_schema_file()}
    for table in CATALOGUE_TABLES:
        sources[f"{table.qualified}.spark.sql"] = render_source(table)
    return sources


def repository_files() -> dict[str, bytes]:
    """The shipped resources, read the way an installed wheel exposes them.

    Only the SES files are returned — the package's own ``__init__`` and any
    compiled artefacts are not part of the repository, and would otherwise travel
    into the Weaver Lakehouse as support files and into its signature.
    """

    directory = files(RESOURCE_PACKAGE) / RESOURCE_DIRECTORY
    return {
        relative: (directory / relative).read_bytes() for relative in sorted(render_sources())
    }
