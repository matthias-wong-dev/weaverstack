"""Pure logical repository, item and document identities.

These values describe authored Weaver structure. They deliberately know
nothing about Fabric item names, workspaces, stores or build execution: those are
physical bindings applied later.
"""

from __future__ import annotations

import hashlib
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

#: What an identity's two parts *mean*, which is what decides how they are
#: validated and spelled. Weaver has one identity — a schema and an object
#: within an item — and three kinds of target wear it differently:
#:
#: ``OBJECT``     ``Sales`` + ``Customer``: a table, view or folder.
#: ``FILE``       ``_/Load/lib`` + ``dates.py``: the containing path is the
#:                schema and the complete leaf filename is the object.
#: ``PROCEDURE``  ``_`` + ``Load Sales.Customer``: an ordinary schema, and an
#:                object name that carries the dot and space of the object it
#:                loads.
#:
#: Validation branches on this rather than assuming table-style naming
#: everywhere, and the Registry stores the real logical target name rather than
#: something encoded to fit one validator.
OBJECT_SHAPE = "object"
FILE_SHAPE = "file"
PROCEDURE_SHAPE = "procedure"
SHAPES = (OBJECT_SHAPE, FILE_SHAPE, PROCEDURE_SHAPE)

#: How a non-object shape marks itself in the one-line spelling. A file's schema
#: is a path and its object carries an extension, so ``Schema.Object`` cannot
#: tell the two halves apart without being told which shape it is reading.
_SHAPE_MARKERS = {FILE_SHAPE: "file:", PROCEDURE_SHAPE: "procedure:"}


def _logical_name(value: object, *, what: str) -> str:
    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    if not value or value != value.strip():
        raise IdentityError(f"{what} must be a non-empty name without surrounding whitespace")
    if any(character in value for character in ("/", "\\", ".", ":")):
        raise IdentityError(f"{what} must be one logical name, got {value!r}")
    return value


def _relative_path(value: object, *, what: str) -> str:
    """One relative, canonical path — a file identity's schema half.

    A file's schema is where it sits, so it may contain ``/``. Everything that
    would make it ambiguous or let it escape its root may not: an absolute path,
    a backslash, an empty component, or a ``.``/``..`` component. The result is
    a path that joins onto a root exactly once and means the same thing however
    it is read back.
    """

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    if not value or value != value.strip():
        raise IdentityError(f"{what} must be a non-empty path without surrounding whitespace")
    if "\\" in value:
        raise IdentityError(f"{what} must use '/' between components, got {value!r}")
    components = value.split("/")
    if any(not component for component in components):
        raise IdentityError(f"{what} must be relative and canonical, got {value!r}")
    if any(component in (".", "..") for component in components):
        raise IdentityError(f"{what} must not contain '.' or '..', got {value!r}")
    return value


def _file_name(value: object, *, what: str) -> str:
    """One complete leaf filename, extension included — a file identity's object."""

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    if not value or value != value.strip():
        raise IdentityError(f"{what} must be a non-empty name without surrounding whitespace")
    if "/" in value or "\\" in value:
        raise IdentityError(f"{what} must be one filename, got {value!r}")
    if value in (".", ".."):
        raise IdentityError(f"{what} must name a file, got {value!r}")
    return value


def _procedure_name(value: object, *, what: str) -> str:
    """One procedure name, which carries the identity of what it loads.

    ``Load Sales.Customer`` is the real name of the real object, so the dot and
    the space are part of it rather than something to encode away. It is still
    one name in one schema, so a path separator is refused.
    """

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    if not value or value != value.strip():
        raise IdentityError(f"{what} must be a non-empty name without surrounding whitespace")
    if any(character in value for character in ("/", "\\")):
        raise IdentityError(f"{what} must be one object name, got {value!r}")
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
    """An item-qualified target identity: one schema and one object, in one item.

    One identity model for everything Weaver builds. A table, a deployed Python
    module and a generated stored procedure are all *a schema and an object
    inside an item* — what differs is only the shape of those two parts, which
    :data:`SHAPES` names and which decides both how they are validated and how
    they are spelled on one line. The Registry stores the two real parts, so
    nothing is encoded to fit a validator and nothing has to be decoded to be
    used.
    """

    item: WeaverItemId
    object_id: ObjectId
    is_files: bool = False
    shape: str = OBJECT_SHAPE

    def __post_init__(self) -> None:
        if self.shape not in SHAPES:
            expected = ", ".join(SHAPES)
            raise IdentityError(
                f"identity shape must be one of {expected}, got {self.shape!r}"
            )
        if self.shape == FILE_SHAPE:
            schema = _relative_path(self.object_id.schema, what="file path")
            name = _file_name(self.object_id.object, what="file name")
        elif self.shape == PROCEDURE_SHAPE:
            schema = _logical_name(self.object_id.schema, what="schema name")
            name = _procedure_name(self.object_id.object, what="object name")
        else:
            schema = _logical_name(self.object_id.schema, what="schema name")
            name = _logical_name(self.object_id.object, what="object name")
        object.__setattr__(self, "object_id", ObjectId(schema=schema, object=name))
        if self.is_files and self.shape != OBJECT_SHAPE:
            raise IdentityError(
                "the Files/ prefix belongs to a Folder document; a "
                f"{self.shape} identity carries its own location"
            )
        if self.is_files and self.item.item_type != LAKEHOUSE:
            raise IdentityError("Files documents may only belong to a Lakehouse item")
        if self.shape == FILE_SHAPE and self.item.item_type != LAKEHOUSE:
            raise IdentityError("a file identity may only belong to a Lakehouse item")
        if self.shape == PROCEDURE_SHAPE and self.item.item_type != WAREHOUSE:
            raise IdentityError(
                "a stored procedure identity may only belong to a Warehouse item"
            )

    @classmethod
    def parse(cls, text: str) -> "WeaverDocumentId":
        parts = _split(text, what="document identity")
        if len(parts) >= 4:
            marker = _SHAPE_MARKERS[FILE_SHAPE]
            if parts[2].startswith(marker):
                # ``file:<path>/<name>`` — the last component is the filename and
                # everything before it, marker stripped, is the containing path.
                head = (parts[2][len(marker) :],) + parts[3:-1]
                return cls(
                    WeaverItemId(parts[0], parts[1]),
                    ObjectId(schema="/".join(head), object=parts[-1]),
                    shape=FILE_SHAPE,
                )
        if len(parts) == 4:
            marker = _SHAPE_MARKERS[PROCEDURE_SHAPE]
            if parts[2].startswith(marker):
                return cls(
                    WeaverItemId(parts[0], parts[1]),
                    ObjectId(schema=parts[2][len(marker) :], object=parts[3]),
                    shape=PROCEDURE_SHAPE,
                )
            if parts[2] == FILES:
                return cls(
                    WeaverItemId(parts[0], parts[1]),
                    _object_id(parts[3]),
                    is_files=True,
                )
        if len(parts) == 3:
            return cls(WeaverItemId(parts[0], parts[1]), _object_id(parts[2]))
        raise IdentityError(
            "document identity must be ItemType/ItemName/Schema.Object, "
            "Lakehouse/ItemName/Files/Schema.Object, "
            "Lakehouse/ItemName/file:Path/Name.ext or "
            f"Warehouse/ItemName/procedure:Schema/Object, got {text!r}"
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
        if self.shape == FILE_SHAPE:
            marker = _SHAPE_MARKERS[FILE_SHAPE]
            return f"{marker}{self.object_id.schema}/{self.object_id.object}"
        if self.shape == PROCEDURE_SHAPE:
            marker = _SHAPE_MARKERS[PROCEDURE_SHAPE]
            return f"{marker}{self.object_id.schema}/{self.object_id.object}"
        prefix = f"{FILES}/" if self.is_files else ""
        return f"{prefix}{self.object_id.qualified}"

    @property
    def is_load_artefact(self) -> bool:
        """Whether this identity names something a load layer produces.

        The two load shapes against the one structural shape. Asked wherever a
        selection has to be partitioned, so that the question is answered from
        the identity rather than by each caller keeping its own set.
        """

        return self.shape in (FILE_SHAPE, PROCEDURE_SHAPE)

    def __str__(self) -> str:
        return f"{self.item}/{self.relative}"


@dataclass(frozen=True, order=True)
class RepositoryAlias:
    """One destination-keyed logical alias from ``alias.yml``."""

    destination: WeaverDocumentId
    source: WeaverDocumentId

    @property
    def signature(self) -> str:
        """What this alias *is*, hashed — and nothing about what it points to.

        An alias declares one thing: this destination stands for that source. So
        its signature is the pair, and only the pair. The source document's own
        content is deliberately absent: a rebuilt source does not redefine the
        alias, and treating it as a change would replace every downstream
        shortcut whenever a table was reloaded.

        A source that was rebuilt is still a reason to remake the alias — but
        that is freshness, answered by comparing build epochs in the Registry,
        not by this signature. Keeping the two apart is what lets an unchanged
        alias over an unchanged source be left alone.
        """

        declaration = f"{self.destination}\0{self.source}".encode("utf-8")
        return hashlib.sha256(declaration).hexdigest()

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
    #: The item's Tests and Assumptions. Held apart from :attr:`documents`
    #: because they are logical declarations that materialise nothing: a
    #: projection that walks an item's documents is asking what this item puts
    #: in the estate, and the answer must not include a Test merely because a
    #: Test has a Schema.Object identity too.
    validations: tuple[WeaverDocumentId, ...] = ()
    signature: str = ""

    def __post_init__(self) -> None:
        if any(schema.item != self.identity for schema in self.schemas):
            raise DiscoveryError(f"every schema must belong to item {self.identity}")
        for declared in (self.documents, self.validations):
            if any(document.item != self.identity for document in declared):
                raise DiscoveryError(
                    f"every document must belong to item {self.identity}"
                )
        _reject_duplicates(self.schemas, what="schema")
        # One namespace across both, so a Test and a Table cannot both claim
        # Sales.Order inside one item.
        _reject_duplicates(self.documents + self.validations, what="document")

    @property
    def declarations(self) -> tuple[WeaverDocumentId, ...]:
        """Everything this item declares — objects and validation alike.

        The common view, for the readers that genuinely span both: dependency
        resolution, reference checking and the item signature. Anything asking
        what the item *materialises* wants :attr:`documents`.
        """

        return self.documents + self.validations

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
    #: The bytes of the support files a build has to *carry*, by the same
    #: repository-relative path. A ``lib/`` module is authored source that no
    #: Weaver document declares, and the load layer deploys it — so its content
    #: has to reach signature derivation and the bundle without either of them
    #: reopening the repository. Files nothing deploys, such as ``alias.yml``,
    #: are listed in :attr:`support_files` and not held here.
    support_file_contents: Mapping[str, bytes] = field(default_factory=dict)
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
