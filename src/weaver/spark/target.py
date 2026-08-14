"""Name one Fabric Lakehouse in a Spark session.

Fabric addresses a Lakehouse object by its four-part name,
``workspace.lakehouse.schema.object``. Rendering that is this module's whole
responsibility: a build freezes final names into its payloads, so nothing below
the Builder decides what an object is called.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import IdentityError


def identifier(name: str) -> str:
    """A back-tick quoted Spark identifier, safe for spaces and keywords."""

    return "`" + name.replace("`", "``") + "`"


def escaped(value: str) -> str:
    """One string literal's content, safe inside single quotes."""

    return value.replace("\\", "\\\\").replace("'", "\\'")


@dataclass(frozen=True)
class FabricSparkTarget:
    """One Fabric Lakehouse, as Fabric Spark addresses it.

    Both names are display names: that is what Fabric's Spark namespace is
    spelled with, and what a reviewer reading a frozen statement can recognise.
    The workspace and item *ids* stay in resolution and in the bundle's target
    block.
    """

    workspace: str
    lakehouse: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "workspace", _checked(self.workspace, what="workspace")
        )
        object.__setattr__(
            self, "lakehouse", _checked(self.lakehouse, what="lakehouse")
        )

    @property
    def item(self) -> str:
        """The Lakehouse's name, which is what a message calls this."""

        return self.lakehouse

    @property
    def namespace(self) -> tuple[str, str]:
        """What sits above the schema: the workspace and the Lakehouse."""

        return (self.workspace, self.lakehouse)

    def qualified_schema(self, schema: str) -> str:
        """The schema, fully qualified — what ``CREATE SCHEMA`` is given."""

        return ".".join(
            identifier(part)
            for part in (*self.namespace, _checked(schema, what="schema"))
        )

    def qualify(self, schema: str, name: str) -> str:
        """One object, fully qualified — what every statement names it by."""

        return (
            f"{self.qualified_schema(schema)}"
            f".{identifier(_checked(name, what='object name'))}"
        )

    def create_schema_statement(
        self, schema: str, *, if_not_exists: bool = True
    ) -> str:
        """The ``CREATE SCHEMA`` this target needs, ready to run.

        No ``LOCATION``: a schema-enabled Fabric Lakehouse pins its own storage
        and refuses one.
        """

        qualifier = " IF NOT EXISTS" if if_not_exists else ""
        return f"CREATE SCHEMA{qualifier} {self.qualified_schema(schema)}"

    def __str__(self) -> str:
        return f"{self.workspace}.{self.lakehouse}"


def _checked(value: object, *, what: str) -> str:
    """One name part, checked rather than trusted.

    These strings are concatenated into identifiers, so a part carrying a
    delimiter would name something other than what it says.
    """

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    name = value.strip()
    if not name:
        raise IdentityError(f"{what} must not be empty")
    if "." in name or "/" in name or "\\" in name:
        raise IdentityError(f"{what} must not contain a separator: {value!r}")
    return name


__all__ = ["FabricSparkTarget", "escaped", "identifier"]
