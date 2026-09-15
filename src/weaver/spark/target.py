"""Render a Fabric Lakehouse's four-part Spark names.

Builds freeze ``workspace.lakehouse.schema.object`` names into their payloads.
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
    """A Fabric Lakehouse as Spark addresses it.

    Spark namespaces use Workspace and Lakehouse display names, not IDs.
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
        return self.lakehouse

    @property
    def namespace(self) -> tuple[str, str]:
        return (self.workspace, self.lakehouse)

    def qualified_schema(self, schema: str) -> str:
        """The schema, fully qualified: what ``CREATE SCHEMA`` is given."""

        return ".".join(
            identifier(part)
            for part in (*self.namespace, _checked(schema, what="schema"))
        )

    def qualify(self, schema: str, name: str) -> str:
        """One object, fully qualified: what every statement names it by."""

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
    """Require a non-empty name part.

    Backtick quoting makes dots and spaces unambiguous. Creation-time and path
    restrictions do not apply when addressing an existing Lakehouse.
    """

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    name = value.strip()
    if not name:
        raise IdentityError(f"{what} must not be empty")
    return name


__all__ = ["FabricSparkTarget", "escaped", "identifier"]
