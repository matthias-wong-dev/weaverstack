"""Pure logical repository, item and document identities.

These values describe authored Weaver structure. They deliberately know
nothing about Fabric item names, workspaces, stores or build execution: those are
physical bindings applied later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Mapping

from ..errors import DiscoveryError, IdentityError
from ..locations import Location
from .metadata import ObjectId

if TYPE_CHECKING:
    from .schemas import SchemaSes
    from .source import SourceDocument

LAKEHOUSE = "Lakehouse"
WAREHOUSE = "Warehouse"
ITEM_TYPES = frozenset({LAKEHOUSE, WAREHOUSE})
FILES = "Files"


def _logical_name(value: object, *, what: str) -> str:
    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    if not value or value != value.strip():
        raise IdentityError(f"{what} must be a non-empty name without surrounding whitespace")
    if any(character in value for character in ("/", "\\", ".", ":")):
        raise IdentityError(f"{what} must be one logical name, got {value!r}")
    return value


def _item_type(value: object) -> str:
    if not isinstance(value, str) or value not in ITEM_TYPES:
        expected = ", ".join(sorted(ITEM_TYPES))
        raise IdentityError(f"item type must be exactly one of {expected}, got {value!r}")
    return value


def _split(text: object, *, what: str) -> tuple[str, ...]:
    if not isinstance(text, str):
        raise IdentityError(f"{what} must be a string, got {type(text).__name__}")
    if not text or text != text.strip():
        raise IdentityError(f"{what} must not be empty or padded with whitespace")
    return tuple(text.split("/"))


def _object_id(text: str) -> ObjectId:
    if text.count(".") != 1:
        raise IdentityError(f"object identity must be Schema.Object, got {text!r}")
    schema, object_name = text.split(".")
    return ObjectId(
        schema=_logical_name(schema, what="schema name"),
        object=_logical_name(object_name, what="object name"),
    )


@dataclass(frozen=True, order=True)
class WeaverItemId:
    """An exact-case logical item identity: ``ItemType/ItemName``."""

    item_type: str
    item_name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "item_type", _item_type(self.item_type))
        object.__setattr__(
            self, "item_name", _logical_name(self.item_name, what="item name")
        )

    @classmethod
    def parse(cls, text: str) -> "WeaverItemId":
        parts = _split(text, what="item identity")
        if len(parts) != 2:
            raise IdentityError(
                f"item identity must be ItemType/ItemName, got {text!r}"
            )
        return cls(parts[0], parts[1])

    def __str__(self) -> str:
        return f"{self.item_type}/{self.item_name}"


@dataclass(frozen=True, order=True)
class WeaverSchemaId:
    """An item-owned schema identity."""

    item: WeaverItemId
    schema: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "schema", _logical_name(self.schema, what="schema name"))

    @classmethod
    def parse(cls, text: str) -> "WeaverSchemaId":
        parts = _split(text, what="schema identity")
        if len(parts) != 3:
            raise IdentityError(
                f"schema identity must be ItemType/ItemName/Schema, got {text!r}"
            )
        return cls(WeaverItemId(parts[0], parts[1]), parts[2])

    def __str__(self) -> str:
        return f"{self.item}/{self.schema}"


@dataclass(frozen=True, order=True)
class WeaverDocumentId:
    """An item-qualified table-style or Lakehouse Files document identity."""

    item: WeaverItemId
    object_id: ObjectId
    is_files: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "object_id",
            ObjectId(
                schema=_logical_name(self.object_id.schema, what="schema name"),
                object=_logical_name(self.object_id.object, what="object name"),
            ),
        )
        if self.is_files and self.item.item_type != LAKEHOUSE:
            raise IdentityError("Files documents may only belong to a Lakehouse item")

    @classmethod
    def parse(cls, text: str) -> "WeaverDocumentId":
        parts = _split(text, what="document identity")
        if len(parts) == 3:
            return cls(WeaverItemId(parts[0], parts[1]), _object_id(parts[2]))
        if len(parts) == 4 and parts[2] == FILES:
            return cls(
                WeaverItemId(parts[0], parts[1]),
                _object_id(parts[3]),
                is_files=True,
            )
        raise IdentityError(
            "document identity must be ItemType/ItemName/Schema.Object or "
            f"Lakehouse/ItemName/Files/Schema.Object, got {text!r}"
        )

    @classmethod
    def parse_local(cls, item: "WeaverItemId", text: str) -> "WeaverDocumentId":
        """Parse the item-relative spelling — the inverse of :attr:`relative`.

        Used where the item is already known from context, such as an item's own
        ``alias.yml``, so the declaration does not repeat it.
        """

        parts = _split(text, what="document identity")
        if len(parts) == 1:
            return cls(item, _object_id(parts[0]))
        if len(parts) == 2 and parts[0] == FILES:
            return cls(item, _object_id(parts[1]), is_files=True)
        raise IdentityError(
            "an item-relative document identity must be Schema.Object or "
            f"Files/Schema.Object, got {text!r}"
        )

    @property
    def relative(self) -> str:
        prefix = f"{FILES}/" if self.is_files else ""
        return f"{prefix}{self.object_id.qualified}"

    def __str__(self) -> str:
        return f"{self.item}/{self.relative}"


@dataclass(frozen=True, order=True)
class RepositoryAlias:
    """One destination-keyed logical alias from ``alias.yml``."""

    destination: WeaverDocumentId
    source: WeaverDocumentId

    def __str__(self) -> str:
        return f"{self.destination}: {self.source}"


@dataclass(frozen=True, order=True)
class ItemDependency:
    """One consumer-owned dependency declaration and its logical resolution."""

    consumer: WeaverDocumentId
    reference: str
    producer: WeaverDocumentId | None = None
    resolution_kind: str = "native"  # native | alias | physical
    is_within_item: bool = False

    @property
    def uses_alias(self) -> bool:
        return self.resolution_kind == "alias"

    @property
    def is_physical(self) -> bool:
        return self.resolution_kind == "physical"


def _reject_duplicates(values: tuple[object, ...], *, what: str) -> None:
    exact: set[str] = set()
    folded: dict[str, str] = {}
    for value in values:
        rendered = str(value)
        if rendered in exact:
            raise DiscoveryError(f"{what} is declared more than once: {rendered}")
        prior = folded.get(rendered.casefold())
        if prior is not None and prior != rendered:
            raise DiscoveryError(
                f"{rendered} and {prior} differ only by case and cannot coexist"
            )
        exact.add(rendered)
        folded[rendered.casefold()] = rendered


@dataclass(frozen=True)
class WeaverItem:
    """The pure identity-level contents owned by one logical item."""

    identity: WeaverItemId
    schemas: tuple[WeaverSchemaId, ...] = ()
    documents: tuple[WeaverDocumentId, ...] = ()
    signature: str = ""

    def __post_init__(self) -> None:
        if any(schema.item != self.identity for schema in self.schemas):
            raise DiscoveryError(f"every schema must belong to item {self.identity}")
        if any(document.item != self.identity for document in self.documents):
            raise DiscoveryError(f"every document must belong to item {self.identity}")
        _reject_duplicates(self.schemas, what="schema")
        _reject_duplicates(self.documents, what="document")

    def __getitem__(self, relative: str) -> WeaverDocumentId:
        for document in self.documents:
            if document.relative == relative:
                return document
        raise DiscoveryError(f"{relative!r} is not a document in item {self.identity}")


@dataclass(frozen=True)
class WeaverRepository:
    """One exact-case logical repository containing typed Weaver items."""

    name: str
    items: tuple[WeaverItem, ...]
    root: Location | None = None
    source_documents: Mapping[WeaverDocumentId, "SourceDocument"] = field(
        default_factory=dict
    )
    schema_documents: Mapping[WeaverSchemaId, "SchemaSes"] = field(default_factory=dict)
    support_files: tuple[str, ...] = ()
    signature: str = ""
    aliases: tuple[RepositoryAlias, ...] = ()
    dependency_edges: tuple[ItemDependency, ...] = ()
    dependency_graph: object | None = None
    #: The item-level graph over :attr:`items`, and its topological layers.
    #: The document graph orders work *inside* an item; this orders the items
    #: themselves, and is the outer structure a build is planned against. It is
    #: derived once, here, so no later stage reconstructs an ordering of its own.
    item_graph: object | None = None
    item_layers: tuple[tuple[WeaverItemId, ...], ...] = ()
    generated_files: Mapping[str, bytes] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _logical_name(self.name, what="repository name"))
        _reject_duplicates(tuple(item.identity for item in self.items), what="item")

    def __getitem__(self, identity: str) -> WeaverItem:
        for item in self.items:
            if str(item.identity) == identity:
                return item
        raise DiscoveryError(f"{identity!r} is not an item in repository {self.name!r}")
