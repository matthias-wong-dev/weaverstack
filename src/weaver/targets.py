"""Physical identities — the third level of the four-level model.

Weaver names things the way SQL does::

    Server . Database . Schema . Object

    4         3          2        1

+-------+-------------------+-------------------------------+
| Level | Fabric            | Local                         |
+=======+===================+===============================+
| 4     | workspace         | root directory                |
| 3     | Lakehouse,        | subdirectory                  |
|       | Warehouse,        |                               |
|       | Environment       |                               |
| 2     | schema            | schema directory              |
| 1     | table, view,      | table or folder               |
|       | folder, procedure |                               |
+-------+-------------------+-------------------------------+

Level 4 is the only level that is written down and named, in ``hosts:``
configuration. Level 3 needs no configuration because an item is *uniquely
identifiable within its host* — so it is referred to by its real name, never by
an alias. That is uniqueness, not invariance: promoting ``Dev_Lakehouse`` to
``Prod_Lakehouse`` inside one workspace is ordinary, so level-3 names are always
supplied explicitly at the call site and never inferred.

Levels 2 and 1 come from the object's own metadata (``Schema.Object``) and do
not appear here.

This module is pure identity. Nothing here resolves an item to a path, an ID or
an endpoint — that is the local host's job (checkpoint 2) and the Fabric host's
job (checkpoint 7).
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
    """A uniquely-named item within a host — level three.

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
    """A directory inside a Lakehouse Files area.

    ``Sales/Files`` or ``Sales/Files/Extracts``. The optional subpath is a
    root *within* level three, not a level of its own — a folder object's
    ``Schema.Object`` still materialises beneath it.
    """

    lakehouse: ItemRef
    subpath: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "subpath",
            tuple(validate_name(part, what="folder subpath segment") for part in self.subpath),
        )

    @classmethod
    def parse(cls, text: str) -> "FolderTarget":
        segments = _split(text, what="folder target")
        if len(segments) < 2:
            raise IdentityError(
                f"folder target must be '<Lakehouse>/{FILES_AREA}[/<subpath>]', got {text!r}"
            )
        if segments[1] != FILES_AREA:
            raise IdentityError(
                f"folder target must name the {FILES_AREA!r} area after the Lakehouse, "
                f"got {segments[1]!r} in {text!r}"
            )
        return cls(lakehouse=ItemRef(segments[0]), subpath=tuple(segments[2:]))

    def __str__(self) -> str:
        return "/".join((self.lakehouse.name, FILES_AREA, *self.subpath))


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


@dataclass(frozen=True)
class RepositoryRef:
    """An installed repository, named within a Weaver Lakehouse.

    The same rule as any other level-3 name: unique inside its container, so it
    is referred to by name and never by path. Where it physically lives —
    ``<weaver-lakehouse>/Files/repos/<name>`` — is resolution, not identity.
    """

    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", validate_name(self.name, what="repository name"))

    @classmethod
    def parse(cls, text: str) -> "RepositoryRef":
        segments = _split(text, what="repository name")
        if len(segments) != 1:
            raise IdentityError(f"repository must be a single name, got {text!r}")
        return cls(name=segments[0])

    def __str__(self) -> str:
        return self.name
