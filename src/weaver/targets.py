"""Typed physical target identities.

Lakehouse, Warehouse and Environment names identify Fabric items within a
Workspace. Resolution to paths, IDs and endpoints happens elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import IdentityError

#: The Lakehouse area holding folder materialisations. Written explicitly in a
#: folder target because it is what the user sees in the Fabric UI. The Delta
#: area (``Tables``) is implicit for the same reason: a Delta target names a
#: Lakehouse, and the area follows from the object kind.
FILES_AREA = "Files"

_ILLEGAL_IN_NAME = ("/", "\\", ":", "*", "?", '"', "<", ">", "|")


def validate_name(value: object, *, what: str) -> str:
    """Validate and strip one Fabric item or path name."""

    if not isinstance(value, str):
        raise IdentityError(f"{what} must be a string, got {type(value).__name__}")
    name = value.strip()
    if not name:
        raise IdentityError(f"{what} must not be empty")
    for character in _ILLEGAL_IN_NAME:
        if character in name:
            raise IdentityError(f"{what} must not contain {character!r}: {value!r}")
    if set(name) == {"."}:
        raise IdentityError(f"{what} must not be {name!r}")
    return name


def _split(text: object, *, what: str) -> list[str]:
    if not isinstance(text, str):
        raise IdentityError(f"{what} must be a string, got {type(text).__name__}")
    if not text.strip():
        raise IdentityError(f"{what} must not be empty")
    return [segment for segment in text.strip().strip("/").split("/")]


@dataclass(frozen=True)
class ItemRef:
    """A Fabric item name within a Workspace.

    Its slot determines whether it names a Lakehouse, Warehouse or Environment.
    """

    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", validate_name(self.name, what="item name"))

    @classmethod
    def parse(cls, text: str) -> "ItemRef":
        segments = _split(text, what="item name")
        if len(segments) != 1:
            raise IdentityError(f"item name must be a single name, got {text!r}")
        return cls(name=segments[0])

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class FolderTarget:
    """A Lakehouse Files area, written ``Sales/Files``."""

    lakehouse: ItemRef

    @classmethod
    def parse(cls, text: str) -> "FolderTarget":
        segments = _split(text, what="folder target")
        if len(segments) != 2:
            raise IdentityError(
                f"folder target must be '<Lakehouse>/{FILES_AREA}', got {text!r}"
                + (
                    f". Remove everything after '/{FILES_AREA}'"
                    if len(segments) > 2
                    else ""
                )
            )
        if segments[1] != FILES_AREA:
            raise IdentityError(
                f"folder target must name the {FILES_AREA!r} area after the Lakehouse, "
                f"got {segments[1]!r} in {text!r}"
            )
        return cls(lakehouse=ItemRef(segments[0]))

    def __str__(self) -> str:
        return f"{self.lakehouse.name}/{FILES_AREA}"


@dataclass(frozen=True)
class DeltaTarget:
    """A Lakehouse holding Delta tables.

    Named bare, as ``Sales``. The ``Tables`` area is implicit because the object
    kind already determines it.
    """

    lakehouse: ItemRef

    @classmethod
    def parse(cls, text: str) -> "DeltaTarget":
        segments = _split(text, what="delta target")
        if len(segments) != 1:
            raise IdentityError(
                "delta target must name a Lakehouse only. The 'Tables' area is "
                "implicit, "
                f"got {text!r}"
            )
        return cls(lakehouse=ItemRef(segments[0]))

    def __str__(self) -> str:
        return self.lakehouse.name


@dataclass(frozen=True)
class WarehouseTarget:
    """A Warehouse holding SQL tables, views and generated load procedures."""

    warehouse: ItemRef

    @classmethod
    def parse(cls, text: str) -> "WarehouseTarget":
        segments = _split(text, what="warehouse target")
        if len(segments) != 1:
            raise IdentityError(
                f"warehouse target must name a Warehouse only, got {text!r}"
            )
        return cls(warehouse=ItemRef(segments[0]))

    def __str__(self) -> str:
        return self.warehouse.name


# --- the physical target grammar shared by public operations ------------------

LAKEHOUSE_KIND = "Lakehouse"
WAREHOUSE_KIND = "Warehouse"

#: Physical target kinds, in the order used in errors.
PHYSICAL_KINDS = (LAKEHOUSE_KIND, WAREHOUSE_KIND)

_PHYSICAL_TYPES = {LAKEHOUSE_KIND: DeltaTarget, WAREHOUSE_KIND: WarehouseTarget}


def parse_physical_target(
    text: object, *, what: str = "target", error: type[Exception] = IdentityError
):
    """Parse ``Lakehouse/Name`` or ``Warehouse/Name``.

    ``what`` names the target in errors. ``error`` preserves the calling
    operation's exception type.
    """

    if not isinstance(text, str):
        raise error(f"{what}s must be strings, got {type(text).__name__}")
    parts = text.strip().strip("/").split("/")
    if len(parts) != 2 or not all(part.strip() for part in parts):
        raise error(
            f"a {what} must name a whole physical item as "
            + " or ".join(f"{kind}/Name" for kind in PHYSICAL_KINDS)
            + f", got {text!r}"
        )
    kind, name = parts[0].strip(), parts[1].strip()
    if kind not in _PHYSICAL_TYPES:
        raise error(
            f"a {what} must start with "
            + " or ".join(PHYSICAL_KINDS)
            + f", got {kind!r}"
        )
    return _PHYSICAL_TYPES[kind](ItemRef.parse(name))


def physical_kind(target) -> str:
    """``Lakehouse`` or ``Warehouse`` for one typed physical target."""

    if isinstance(target, DeltaTarget):
        return LAKEHOUSE_KIND
    if isinstance(target, WarehouseTarget):
        return WAREHOUSE_KIND
    raise IdentityError(f"{type(target).__name__} is not a typed physical target")


def physical_item(target) -> ItemRef:
    """The item one typed physical target names."""

    if isinstance(target, DeltaTarget):
        return target.lakehouse
    if isinstance(target, WarehouseTarget):
        return target.warehouse
    raise IdentityError(f"{type(target).__name__} is not a typed physical target")


def physical_target_text(target) -> str:
    """One typed physical target, spelled back in the grammar it was parsed from."""

    return f"{physical_kind(target)}/{physical_item(target).name}"


# --- catalogue and plan target kinds ------------------------------------------

LAKEHOUSE_TARGET = "lakehouse"
WAREHOUSE_TARGET = "warehouse"

_GRAMMAR_KIND = {LAKEHOUSE_TARGET: LAKEHOUSE_KIND, WAREHOUSE_TARGET: WAREHOUSE_KIND}


@dataclass(frozen=True)
class PhysicalTargetRef:
    """One physical item, as the public grammar names it."""

    kind: str
    name: str

    @classmethod
    def of(cls, target) -> "PhysicalTargetRef":
        """Convert a typed target to catalogue and plan vocabulary."""

        return cls(
            kind=LAKEHOUSE_TARGET
            if isinstance(target, DeltaTarget)
            else WAREHOUSE_TARGET,
            name=physical_item(target).name,
        )

    def __str__(self) -> str:
        return f"{_GRAMMAR_KIND[self.kind]}/{self.name}"

    @property
    def is_lakehouse(self) -> bool:
        return self.kind == LAKEHOUSE_TARGET


@dataclass(frozen=True)
class PhysicalObjectRef:
    """One installed object, addressed as its physical target holds it.

    ``schema`` is the catalogue's ``schema_name`` unchanged: for a folder that
    carries its ``Files/`` prefix, and for a deployed file it is the path
    beneath ``Files``. Keeping the stored spelling lets a reference go straight
    to :meth:`weaver.build_bundle.prune.TargetInventory.has_object`.
    """

    target_id: str
    target_kind: str
    schema: str
    object: str
    object_type: str
    shape: str | None = None

    def __str__(self) -> str:
        return f"{self.schema}.{self.object}"


def lakehouse_names(targets) -> tuple[str, ...]:
    """Return Lakehouse names in the order given.

    Any may be attached for Livy because generated statements name their target
    in full.
    """

    return tuple(target.name for target in targets if target.is_lakehouse)
