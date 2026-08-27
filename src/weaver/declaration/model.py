"""Pure logical repository, item and document identities.

These values describe authored Weaver structure. They know
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
    from .programmable import Programmable
    from .schemas import SchemaSes
    from .source import SourceDocument

LAKEHOUSE = "Lakehouse"
WAREHOUSE = "Warehouse"
ITEM_TYPES = frozenset({LAKEHOUSE, WAREHOUSE})
FILES = "Files"

#: What an identity's two parts mean, which is what decides how they are
#: validated and spelled. Weaver has one identity, a schema and an object within
#: an item, and three kinds of target wear it differently:
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
        raise IdentityError(
            f"{what} must be a non-empty name without surrounding whitespace"
        )
    if any(character in value for character in ("/", "\\", ".", ":")):
        raise IdentityError(f"{what} must be one logical name, got {value!r}")
    return value


def _relative_path(value: object, *, what: str) -> str:
    """One relative, canonical path, being a file identity's schema half.

    A file's schema is where it sits, so it may contain ``/``. What would make
    it ambiguous or let it escape its root may not: an absolute path, a
    backslash, an empty component, or ``.``/``..``.
    """

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    if not value or value != value.strip():
        raise IdentityError(
            f"{what} must be a non-empty path without surrounding whitespace"
        )
    if "\\" in value:
        raise IdentityError(f"{what} must use '/' between components, got {value!r}")
    components = value.split("/")
    if any(not component for component in components):
        raise IdentityError(f"{what} must be relative and canonical, got {value!r}")
    if any(component in (".", "..") for component in components):
        raise IdentityError(f"{what} must not contain '.' or '..', got {value!r}")
    return value


def _file_name(value: object, *, what: str) -> str:
    """One complete leaf filename with its extension, a file identity's object."""

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    if not value or value != value.strip():
        raise IdentityError(
            f"{what} must be a non-empty name without surrounding whitespace"
        )
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
        raise IdentityError(
            f"{what} must be a non-empty name without surrounding whitespace"
        )
    if any(character in value for character in ("/", "\\")):
        raise IdentityError(f"{what} must be one object name, got {value!r}")
    return value


def _item_type(value: object) -> str:
    if not isinstance(value, str) or value not in ITEM_TYPES:
        expected = ", ".join(sorted(ITEM_TYPES))
        raise IdentityError(
            f"item type must be exactly one of {expected}, got {value!r}"
        )
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
        object.__setattr__(
            self, "schema", _logical_name(self.schema, what="schema name")
        )

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
    module and a generated stored procedure are all a schema and an object inside
    an item. What differs is the shape of those two parts, which
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
                # ``file:<path>/<name>``, where the last component is the filename and
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
        """Parse the item-relative spelling, the inverse of :attr:`relative`.

        Used where the item is already known from context, so a declaration
        does not repeat it.
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

    def __str__(self) -> str:
        return f"{self.item}/{self.relative}"


def parse_installed_identity(text: str):
    """One identity a build installs, whichever kind it is.

    Almost everything Weaver installs is a :class:`WeaverDocumentId`. A schema
    shortcut is the exception: it presents a namespace rather than an object, so
    its identity is a :class:`WeaverSchemaId`, and a plan that recorded it has to
    be able to read it back.
    """

    parts = _split(text, what="installed identity")
    if len(parts) == 3 and "." not in parts[2]:
        return WeaverSchemaId(WeaverItemId(parts[0], parts[1]), parts[2])
    return WeaverDocumentId.parse(text)


@dataclass(frozen=True, order=True)
class RepositoryShortcut:
    """One logical pair a ``logical`` shortcut stands for.

    Internal. Dependency resolution, ordering and freshness are computed over
    these, because a logical target is a Weaver document like any other. A
    physical shortcut names an item Weaver does not manage and has no logical
    source, so it never becomes one of these.
    """

    destination: WeaverDocumentId
    source: WeaverDocumentId

    @property
    def signature(self) -> str:
        """What this shortcut is, hashed, and nothing about what it points to.

        A shortcut declares one thing: this destination stands for that source.
        So its signature is the pair and only the pair. The source document's own
        content is absent: a rebuilt source does not redeclare the shortcut, and
        treating it as a change would replace every downstream shortcut whenever
        a table was reloaded.

        A source that was rebuilt is still a reason to remake the shortcut, but
        that is freshness, answered by comparing build datetimes in the Registry
        rather than by this signature. Keeping the two apart is what lets an
        unchanged shortcut over an unchanged source be left alone.
        """

        declaration = f"{self.destination}\0{self.source}".encode("utf-8")
        return hashlib.sha256(declaration).hexdigest()

    def __str__(self) -> str:
        return f"{self.destination}: {self.source}"


#: What a shortcut points at, which decides how its paths are built and what the
#: destination is known by. ``view`` is the Warehouse form: it reaches another
#: item over TDS rather than through OneLake, and it is still a shortcut.
TABLE_SHORTCUT = "table"
SCHEMA_SHORTCUT = "schema"
FOLDER_SHORTCUT = "folder"
VIEW_SHORTCUT = "view"
SHORTCUT_TYPES = (TABLE_SHORTCUT, SCHEMA_SHORTCUT, FOLDER_SHORTCUT, VIEW_SHORTCUT)

#: How the target is read. ``logical`` names a Weaver item and is followed
#: through its current binding; ``physical`` names the Fabric item itself.
LOGICAL_TARGET = "logical"
PHYSICAL_TARGET = "physical"
TARGET_TYPES = (LOGICAL_TARGET, PHYSICAL_TARGET)

#: How an authored declaration name spells a two-part identity, matching the
#: module naming an item's own documents already use.
NAME_SEPARATOR = "__"


def _one_of(value: object, allowed: tuple[str, ...], *, what: str) -> str:
    """One value from a closed vocabulary, compared rather than coerced."""

    if not isinstance(value, str) or value not in allowed:
        expected = ", ".join(allowed)
        raise IdentityError(f"{what} must be one of {expected}, got {value!r}")
    return value


@dataclass(frozen=True, order=True)
class ShortcutDeclaration:
    """One shortcut an item declares, exactly as it was authored.

    The declaration is the author's intent and nothing more. Which workspace and
    item a target resolves to, and what path each side becomes, are settled
    during planning against the workspace the build is bound to.

    ``name`` is the authored symbol, and it names the destination:
    ``Sales__Customer`` for a table, folder or view, and ``Reference`` for a
    schema. ``target`` is kept as written, because whether its item half is a
    Weaver item or a Fabric one is what ``target_type`` says.
    """

    owner: WeaverItemId
    name: str
    shortcut_type: str
    target_type: str
    target: str
    workspace: str | None = None
    #: Where this was written, carried from the reader so nothing downstream
    #: reconstructs an authored path.
    relative_path: str = ""
    #: Package-owned declarations may already have an identity. Authored names
    #: leave this empty and are decoded from ``Schema__Object`` below.
    destination_identity: "WeaverDocumentId | WeaverSchemaId | None" = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "shortcut_type",
            _one_of(self.shortcut_type, SHORTCUT_TYPES, what="shortcut_type"),
        )
        object.__setattr__(
            self,
            "target_type",
            _one_of(self.target_type, TARGET_TYPES, what="target_type"),
        )
        if not isinstance(self.target, str) or not self.target.strip():
            raise IdentityError("a shortcut target must be a non-empty string")
        object.__setattr__(self, "target", self.target.strip())
        if self.workspace is not None:
            object.__setattr__(
                self, "workspace", _logical_name(self.workspace, what="workspace")
            )
        if self.is_logical and self.workspace is not None:
            raise IdentityError(
                f"shortcut {self.name!r} has a logical target, so it cannot name "
                "a workspace: Weaver follows the target item's own binding"
            )
        if self.is_logical and self.shortcut_type == SCHEMA_SHORTCUT:
            raise IdentityError(
                f"shortcut {self.name!r} is a schema shortcut, so its target must "
                "be physical: a schema's contents belong to the item it points "
                "at, and Weaver binds objects rather than namespaces"
            )
        if self.shortcut_type == VIEW_SHORTCUT:
            if self.owner.item_type != WAREHOUSE:
                raise IdentityError(
                    "a view shortcut is a Warehouse view, so it belongs to a "
                    f"Warehouse item, got {self.owner}"
                )
            if self.workspace is not None:
                raise IdentityError(
                    f"shortcut {self.name!r} is a Warehouse view, which reaches "
                    "another item in the same workspace, so it cannot name one"
                )
        elif self.owner.item_type != LAKEHOUSE:
            raise IdentityError(
                f"a {self.shortcut_type} shortcut is a OneLake shortcut, so it "
                f"belongs to a Lakehouse item, got {self.owner}"
            )
        # Validated here so a malformed declaration is refused where it is
        # written rather than where something tries to resolve it.
        self.destination
        self.target_item
        self.target_tail
        if self.is_logical and self.target_item == self.owner:
            raise IdentityError(
                f"shortcut {self.name!r} has a logical target in {self.owner}, "
                "which is the item declaring it. A shortcut crosses items, so "
                "reference the object directly instead."
            )

    @property
    def is_logical(self) -> bool:
        return self.target_type == LOGICAL_TARGET

    @property
    def is_schema(self) -> bool:
        return self.shortcut_type == SCHEMA_SHORTCUT

    @property
    def is_view(self) -> bool:
        return self.shortcut_type == VIEW_SHORTCUT

    @property
    def is_files(self) -> bool:
        return self.shortcut_type == FOLDER_SHORTCUT

    @property
    def destination(self):
        """What this shortcut is called in the item that declares it.

        A schema shortcut is a :class:`WeaverSchemaId`: it establishes a
        namespace rather than an object, and what appears inside belongs to the
        item it points at. Everything else is an ordinary
        :class:`WeaverDocumentId` in the owning item.
        """

        if self.destination_identity is not None:
            if self.destination_identity.item != self.owner:
                raise IdentityError(
                    f"shortcut destination {self.destination_identity} belongs to "
                    f"{self.destination_identity.item}, not {self.owner}"
                )
            return self.destination_identity
        if self.is_schema:
            return WeaverSchemaId(self.owner, self.name)
        parts = self.name.split(NAME_SEPARATOR)
        if len(parts) != 2 or not all(parts):
            raise IdentityError(
                f"shortcut {self.name!r} names a {self.shortcut_type}, so its "
                f"name must be Schema{NAME_SEPARATOR}Object"
            )
        return WeaverDocumentId(
            self.owner,
            ObjectId(
                schema=_logical_name(parts[0], what="schema name"),
                object=_logical_name(parts[1], what="object name"),
            ),
            is_files=self.is_files,
        )

    @property
    def schema(self) -> str:
        """The schema this shortcut occupies in the item that declares it."""

        return self.name if self.is_schema else self.destination.object_id.schema

    @property
    def shortcut_id(self) -> str:
        """The shortcut as its author declared it, in this item's own terms.

        ``Sales.Customer`` for a table or a folder, ``Reference`` for a schema.
        The authored symbol spells the same thing with ``__`` because a Python
        name cannot carry a dot.
        """

        if self.is_schema:
            return self.name
        return self.destination.object_id.qualified

    @property
    def target_item(self) -> WeaverItemId:
        """The item half of the target, typed.

        The same spelling either way, because it is the same two questions:
        which kind of item, and which one. ``target_type`` says whether the
        answer is a Weaver item or a Fabric one.
        """

        parts = _split(self.target, what="shortcut target")
        if len(parts) < 3:
            raise IdentityError(
                f"shortcut target {self.target!r} must be ItemType/ItemName "
                "followed by what it points at"
            )
        return WeaverItemId(parts[0], parts[1])

    @property
    def target_tail(self) -> str:
        """What the target names inside its item, validated for this type.

        A table or a view names ``Schema.Object``, a schema names one schema, and
        a folder names a canonical relative path beneath ``Files``.
        """

        parts = _split(self.target, what="shortcut target")
        tail = "/".join(parts[2:])
        if self.shortcut_type in (TABLE_SHORTCUT, VIEW_SHORTCUT):
            _object_id(tail)
        elif self.is_schema:
            _logical_name(tail, what="target schema")
        else:
            if parts[2] != FILES or len(parts) < 4:
                raise IdentityError(
                    f"shortcut target {self.target!r} must name a path beneath "
                    f"{FILES}/, because a folder shortcut points into a "
                    "Lakehouse's Files area"
                )
            # The same canonical-relative-path rule a file identity is held to.
            _relative_path("/".join(parts[3:]), what="target folder path")
        return tail

    @property
    def target_object(self) -> ObjectId | None:
        """The object the target names, or None where it names a namespace."""

        if self.shortcut_type in (TABLE_SHORTCUT, VIEW_SHORTCUT):
            return _object_id(self.target_tail)
        return None

    @property
    def target_schema(self) -> str:
        """The schema or path the target sits in, however it is spelled."""

        if self.is_schema:
            return self.target_tail
        if self.is_files:
            return self.target_tail.split("/", 1)[1]
        return _object_id(self.target_tail).schema

    @property
    def logical_source(self) -> "WeaverDocumentId":
        """The Weaver document a logical shortcut names."""

        if not self.is_logical:
            raise IdentityError(
                f"shortcut {self.name!r} has a physical target, so it names a "
                "Fabric item rather than a Weaver document"
            )
        return WeaverDocumentId.parse(self.target)

    @property
    def signature(self) -> str:
        """What this shortcut is, hashed.

        The declaration and nothing about what it points at, for the reason
        :attr:`RepositoryShortcut.signature` gives.
        """

        declaration = "\0".join(
            (
                str(self.owner),
                self.name,
                self.shortcut_type,
                self.target_type,
                self.target,
                self.workspace or "",
            )
        ).encode("utf-8")
        return hashlib.sha256(declaration).hexdigest()

    def __str__(self) -> str:
        where = f" in {self.workspace}" if self.workspace else ""
        return (
            f"{self.owner}/{self.name}: {self.target_type} "
            f"{self.shortcut_type} {self.target}{where}"
        )


@dataclass(frozen=True, order=True)
class ItemDependency:
    """One consumer-owned dependency declaration and its logical resolution."""

    consumer: WeaverDocumentId
    reference: str
    producer: WeaverDocumentId | None = None
    resolution_kind: str = "native"  # native | shortcut | physical
    is_within_item: bool = False

    @property
    def uses_shortcut(self) -> bool:
        return self.resolution_kind == "shortcut"

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
    #: in the estate, and the answer must not include a Test because a
    #: Test has a Schema.Object identity too.
    validations: tuple[WeaverDocumentId, ...] = ()
    #: The stored procedures this item manages, authored and generated alike.
    programmables: tuple["Programmable", ...] = ()
    signature: str = ""

    def __post_init__(self) -> None:
        if any(schema.item != self.identity for schema in self.schemas):
            raise DiscoveryError(f"every schema must belong to item {self.identity}")
        for declared in (self.documents, self.validations):
            if any(document.item != self.identity for document in declared):
                raise DiscoveryError(
                    f"every document must belong to item {self.identity}"
                )
        for programmable in self.programmables:
            if programmable.identity.item != self.identity:
                raise DiscoveryError(
                    f"every programmable must belong to item {self.identity}"
                )
        _reject_duplicates(self.schemas, what="schema")
        # One namespace across both, so a Test and a Table cannot both claim
        # Sales.Order inside one item.
        _reject_duplicates(self.documents + self.validations, what="document")
        _reject_duplicates(
            tuple(each.identity for each in self.programmables),
            what="programmable",
        )

    @property
    def declarations(self) -> tuple[WeaverDocumentId, ...]:
        """Everything this item declares, objects and validation alike.

        The common view, for the readers that span both: dependency
        resolution, reference checking and the item signature. Anything asking
        what the item materialises needs :attr:`documents`.
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
    #: The stored procedures the repository manages, by identity. Authored
    #: content, generated infrastructure and Weaver's own fragments alike.
    programmables: Mapping[WeaverDocumentId, "Programmable"] = field(
        default_factory=dict
    )
    support_files: tuple[str, ...] = ()
    #: The bytes of those files, by the same path. A ``lib/`` module is authored
    #: source that no Weaver document declares, and the load layer deploys it,
    #: so its content has to reach signature derivation and the bundle without
    #: either of them reopening the repository.
    support_file_contents: Mapping[str, bytes] = field(default_factory=dict)
    signature: str = ""
    #: The logical pairs the ``logical`` shortcuts stand for, which resolution,
    #: ordering and freshness are computed over.
    logical_shortcuts: tuple[RepositoryShortcut, ...] = ()
    #: Every shortcut each item declares, as authored and as generated. This
    #: holds the intent, including the physical declarations that name a Fabric
    #: item and so have no logical source, and the Weaver-owned standard
    #: catalogue surface composed in before resolution.
    shortcuts: tuple[ShortcutDeclaration, ...] = ()
    dependency_edges: tuple[ItemDependency, ...] = ()
    dependency_graph: object | None = None
    #: The item-level graph over :attr:`items`, and its topological layers.
    #: The document graph orders work inside an item; this orders the items
    #: themselves, and is the outer structure a build is planned against. It is
    #: derived once, here, so no later stage reconstructs an ordering of its own.
    item_graph: object | None = None
    item_layers: tuple[tuple[WeaverItemId, ...], ...] = ()
    generated_files: Mapping[str, bytes] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "name", _logical_name(self.name, what="repository name")
        )
        _reject_duplicates(tuple(item.identity for item in self.items), what="item")

    def __getitem__(self, identity: str) -> WeaverItem:
        for item in self.items:
            if str(item.identity) == identity:
                return item
        raise DiscoveryError(f"{identity!r} is not an item in repository {self.name!r}")
