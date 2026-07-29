"""How one logical Lakehouse is *named* in a Spark session.

:class:`~weaver.locations.LakehouseSparkLocation` answers where a destination's
bytes live. This answers the other half: what a statement has to write to reach
that destination's catalogue, given that the session is attached somewhere else.

The two workspaces disagree, and the disagreement is **data**, not behaviour — which
is why there is one class here and two constructors rather than two classes.

**Fabric** has a native namespace for exactly this. Under ``spark_catalog`` a
schema is a three-level name, so an object is four parts::

    `Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer`
     ^workspace ^lakehouse       ^schema ^object

One session can create, read and drop through that name in any Lakehouse in the
workspace, and can build a view in one over a table in another. Nothing has to be
attached, and nothing has to be switched.

**Local Spark** has no such namespace, and cannot be given one: a Delta catalogue
can only be the *session* catalogue (``DeltaCatalog`` extends
``DelegatingCatalogExtension``, and registered as an ordinary named catalogue its
delegate is null), so ``spark.sql.catalog.<lakehouse>`` is not available. Its one
namespace level is the database. The proxy therefore folds the Lakehouse into
that level::

    `sales_lh__sales`.`Customer`

which is not Fabric syntax and is not meant to be. What it reproduces is the
*property* Fabric's namespace provides and a bare ``Sales.Customer`` does not:
two destinations that declare a schema of the same name stay apart. Storage is
untouched by the folding — the database still carries an explicit ``LOCATION`` of
``<lakehouse>/Tables/<schema>``, so a managed table lands exactly where the
Fabric layout puts it and the emulator keeps mirroring OneLake. The folded
database identifier is lower-case because the local session catalogue registers
it that way; declared object identifiers remain exact-case under the emulator's
case-sensitive analysis policy.

A destination is never carried in a build bundle. It is derived at install time
from the item the bundle names, because a Fabric namespace is workspace-specific
and a local one is rooted in a temporary directory — see
:class:`~weaver.locations.LakehouseSparkLocation` for why a bundle that moved
with either would stop being comparable between environments
(how-does-build-work §15).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ..errors import IdentityError

#: Local Spark accepts word characters in a database name and nothing else, so a
#: folded name is only legal if the Lakehouse's own name is.
_LEGAL_LOCAL_NAME = re.compile(r"\A\w+\Z")

#: What separates the Lakehouse from the schema in the folded local name. Two
#: characters, so a single underscore in either half cannot be mistaken for it.
LOCAL_SEPARATOR = "__"


def identifier(name: str) -> str:
    """A back-tick quoted Spark identifier, safe for spaces and keywords."""

    return "`" + name.replace("`", "``") + "`"


@dataclass(frozen=True)
class SparkDestination:
    """One logical Lakehouse, as a Spark session addresses it.

    ``item`` is the logical name, and is what appears in a message. ``namespace``
    is whatever sits above the schema — the workspace and Lakehouse on Fabric,
    nothing locally. ``schema_prefix`` is the local fold, empty on Fabric.

    ``tables_root`` is present only for the platform that needs it: local Spark
    drops a managed table into its own warehouse directory unless the database
    says otherwise, so the proxy supplies a ``LOCATION``. A schema-enabled Fabric
    Lakehouse pins its own, and answers None.
    """

    item: str
    namespace: tuple[str, ...] = ()
    schema_prefix: str = ""
    tables_root: str | None = None
    preserve_table_identifier_case: bool = False
    lowercase_schema_identifier: bool = False
    case_sensitive_analysis: bool = False

    def schema_identifier(self, schema: str) -> str:
        """The schema's name at its own namespace level, unquoted."""

        name = f"{self.schema_prefix}{_checked(schema, what='schema')}"
        return name.lower() if self.lowercase_schema_identifier else name

    def qualified_schema(self, schema: str) -> str:
        """The schema, fully qualified — what ``CREATE SCHEMA`` is given."""

        return ".".join(
            identifier(part)
            for part in (*self.namespace, self.schema_identifier(schema))
        )

    def qualify(self, schema: str, name: str) -> str:
        """One object, fully qualified — what every statement names it by."""

        return (
            f"{self.qualified_schema(schema)}"
            f".{identifier(_checked(name, what='object name'))}"
        )

    def schema_location(self, schema: str) -> str | None:
        """Where this destination's managed tables for a schema must be pinned.

        None when the platform pins them itself, which is the Fabric answer and
        the reason no path reaches a Fabric ``CREATE SCHEMA``.
        """

        if self.tables_root is None:
            return None
        return f"{self.tables_root.rstrip('/')}/{_checked(schema, what='schema')}"

    def __str__(self) -> str:
        return self.item if not self.namespace else ".".join(self.namespace)


def fabric_destination(*, workspace: str, lakehouse: str) -> SparkDestination:
    """A Fabric Lakehouse, addressed by its native four-part name.

    Both names are display names, deliberately: this is what Fabric's Spark
    namespace is spelled with, and it is what a reviewer reading a statement can
    recognise. The workspace and item *ids* stay where they belong — in
    resolution, and in the bundle's target block.
    """

    return SparkDestination(
        item=lakehouse,
        namespace=(
            _checked(workspace, what="workspace"),
            _checked(lakehouse, what="lakehouse"),
        ),
        # Fabric otherwise folds a quoted table identifier to lower-case at
        # creation.
        preserve_table_identifier_case=True,
    )


def local_destination(*, item: str, tables_root: str) -> SparkDestination:
    """A local Lakehouse, folded into the one namespace level Spark offers.

    The Lakehouse name becomes part of every database name, so it has to be a
    legal one. Refused rather than sanitised: a silently altered name would make
    two destinations collide again, which is the single thing this exists to
    prevent.
    """

    name = _checked(item, what="lakehouse")
    if not _LEGAL_LOCAL_NAME.match(name):
        raise IdentityError(
            f"local Spark folds the Lakehouse name into its database names, and "
            f"only accepts letters, digits and underscores there — {item!r} cannot "
            "be addressed locally"
        )
    return SparkDestination(
        item=name,
        schema_prefix=f"{name}{LOCAL_SEPARATOR}",
        tables_root=tables_root,
        # The emulator mirrors Fabric's case-preserving table directories. Its
        # folded schema itself was registered under Spark's case-insensitive
        # policy, so every statement addresses it by its canonical lower case.
        preserve_table_identifier_case=True,
        lowercase_schema_identifier=True,
        # Unlike Fabric's catalogue, Spark's local session catalogue cannot find
        # a PascalCase table again after analysis returns to case-insensitive
        # mode. The emulator therefore uses one exact-case policy for its life.
        case_sensitive_analysis=True,
    )


def _checked(value: object, *, what: str) -> str:
    """One name part, checked rather than trusted.

    These strings are concatenated into identifiers and into paths, so a part
    carrying a delimiter would name something other than what it says.
    """

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    name = value.strip()
    if not name:
        raise IdentityError(f"{what} must not be empty")
    if "." in name or "/" in name or "\\" in name:
        raise IdentityError(f"{what} must not contain a separator: {value!r}")
    return name
