"""Physical identities — the third level of the four-level model.

Weaver names things the way SQL does::

    Server . Database . Schema . Object

    4         3          2        1

+-------+------------------------------------+
| Level | Fabric                             |
+=======+====================================+
| 4     | workspace                          |
| 3     | Lakehouse, Warehouse, Environment  |
| 2     | schema                             |
| 1     | table, view, folder, procedure     |
+-------+------------------------------------+

Level 4 is the only level written down in Workspace configuration. A level-3
item is unique within its workspace, so it is named directly rather than
aliased — but unique is not invariant, so those names are always supplied at the
call site and never inferred.

Levels 2 and 1 come from the object's own metadata (``Schema.Object``) and do
not appear here.

This module is pure identity. Nothing here resolves an item to a path, an ID or
an endpoint — that is the resolver's job.
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
    """Validate one level-3 or path name and return it stripped."""

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
    """A uniquely-named item within a workspace — level three.

    A Lakehouse, a Warehouse or a Fabric Environment. Which of those it must be
    is decided by the slot it is used in, never by the name itself: the same
    string passed as a ``delta_target`` names a Lakehouse and passed as a
    ``sql_target`` names a Warehouse.
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
    """A Lakehouse Files area — ``Sales/Files``, and nothing further.

    A folder object's physical location is derived from its identity alone:
    ``Files/<Schema>/<Object>``. A binding-level subpath used to be accepted here,
    and it made that derivation untrue — the same object landed in different
    places depending on how its item was bound, so authored code could not compose
    its own path and neither could anything else without carrying the binding
    around. One deterministic location per object is worth more than a
    configurable root.
    """

    lakehouse: ItemRef

    @classmethod
    def parse(cls, text: str) -> "FolderTarget":
        segments = _split(text, what="folder target")
        if len(segments) != 2:
            raise IdentityError(
                f"folder target must be '<Lakehouse>/{FILES_AREA}', got {text!r}"
                + (
                    " — a folder object lands at Files/<Schema>/<Object>, so there is "
                    "nothing to configure beneath the area"
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

    Named bare — ``Sales``. The ``Tables`` area is implicit because the object
    kind already determines it.
    """

    lakehouse: ItemRef

    @classmethod
    def parse(cls, text: str) -> "DeltaTarget":
        segments = _split(text, what="delta target")
        if len(segments) != 1:
            raise IdentityError(
                "delta target must name a Lakehouse only — the 'Tables' area is implicit, "
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
            raise IdentityError(f"warehouse target must name a Warehouse only, got {text!r}")
        return cls(warehouse=ItemRef(segments[0]))

    def __str__(self) -> str:
        return self.warehouse.name


# --- the one typed physical grammar the public operations share ---------------
#
# ``Lakehouse/Name`` and ``Warehouse/Name`` are what a caller writes at every
# boundary that names a whole physical item: a build binding's left-hand side, a
# wipe target, an unbind target, a load target. One parser, deliberately — four
# spellings of one grammar is four places for it to drift, and the drift would
# show up as one operation accepting a target another refuses.
#
# It returns the *existing* typed targets rather than a fifth wrapper, so what a
# caller gets back is what the resolvers and executors already take.

LAKEHOUSE_KIND = "Lakehouse"
WAREHOUSE_KIND = "Warehouse"

#: How the grammar spells each kind, in the order the error message lists them.
PHYSICAL_KINDS = (LAKEHOUSE_KIND, WAREHOUSE_KIND)

_PHYSICAL_TYPES = {LAKEHOUSE_KIND: DeltaTarget, WAREHOUSE_KIND: WarehouseTarget}


def parse_physical_target(
    text: object, *, what: str = "target", error: type[Exception] = IdentityError
):
    """``Lakehouse/Name`` or ``Warehouse/Name``, as the typed physical target.

    ``what`` names the caller's own noun so the message reads in that operation's
    vocabulary — "a wipe target must …", "a load target must …". ``error`` is the
    class the caller's boundary raises, because *which* error a malformed request
    produces belongs to the operation and not to the grammar.
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
    raise IdentityError(
        f"{type(target).__name__} is not a typed physical target"
    )


def physical_item(target) -> ItemRef:
    """The item one typed physical target names."""

    if isinstance(target, DeltaTarget):
        return target.lakehouse
    if isinstance(target, WarehouseTarget):
        return target.warehouse
    raise IdentityError(
        f"{type(target).__name__} is not a typed physical target"
    )


def physical_target_text(target) -> str:
    """One typed physical target, spelled back in the grammar it was parsed from."""

    return f"{physical_kind(target)}/{physical_item(target).name}"
