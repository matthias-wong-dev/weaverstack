"""First-class Warehouse stored-procedure declarations.

A Programmable is a managed stored procedure: authored content under
``Warehouse/<Item>/programmables/<Schema>.<Procedure>.sql``, generated
infrastructure derived from a logical declaration, or one of Weaver's own fixed
entry points. One representation and one lifecycle -- discover, validate,
sign, select, install, register, prune -- whichever kind it is.

Identity is the existing ``PROCEDURE_SHAPE`` document identity: an ordinary
schema and an object name that carries what the procedure is for. Nothing here
invents a second procedure naming scheme.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ..errors import DiscoveryError
from .metadata import ObjectId
from .model import PROCEDURE_SHAPE, WAREHOUSE, WeaverDocumentId, WeaverItemId
from .source import content_hash

#: Where an item authors its stored procedures.
PROGRAMMABLES_DIRECTORY = "programmables"

#: What an authored file's SQL must say, so a changed Programmable replaces
#: rather than collides: the installer runs the text verbatim, and a plain
#: ``CREATE`` fails once the procedure exists.
_CREATE_PATTERN = re.compile(
    r"create\s+or\s+alter\s+procedure\s+"
    r"(?P<schema>\[[^\]]+(?:\]\][^\]]*)*\]|[\w@#$]+)"
    r"\s*\.\s*"
    r"(?P<name>\[[^\]]+(?:\]\][^\]]*)*\]|[\w@#$]+)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Programmable:
    """One managed stored procedure declaration, whatever its provenance.

    ``identity`` is the catalogue key, ``text`` the complete statement the
    installer runs, and ``signature`` what incremental selection compares.
    ``role`` says what the procedure is for and is carried rather than inferred,
    exactly as it is for every runtime artefact.

    ``relative_path`` marks authored content and records where it was written;
    ``origin`` marks generated content and records the logical declaration it
    was derived from. A Programmable never carries both: Weaver-owned content
    such as the fixed entry points carries neither.
    """

    identity: WeaverDocumentId
    text: str
    signature: str
    role: str
    relative_path: str | None = None
    origin: WeaverDocumentId | None = None

    def __post_init__(self) -> None:
        if self.identity.shape != PROCEDURE_SHAPE:
            raise DiscoveryError(
                f"{self.identity}: a Programmable is a stored procedure, so its "
                f"identity has the {PROCEDURE_SHAPE} shape"
            )
        if self.identity.item.item_type != WAREHOUSE:
            raise DiscoveryError(
                f"{self.identity}: a Programmable belongs to a Warehouse item"
            )
        if self.relative_path is not None and self.origin is not None:
            raise DiscoveryError(
                f"{self.identity}: a Programmable is authored or generated, "
                "never both"
            )

    @property
    def payload(self) -> bytes:
        return self.text.encode("utf-8")


def read_programmable(
    relative_path: str, data: bytes, *, owner: WeaverItemId
) -> Programmable:
    """One authored stored procedure, validated against its own filename.

    The file lives at ``programmables/<Schema>.<Procedure>.sql`` and its SQL
    must create that exact procedure, so the identity the catalogue registers
    and the object the statement creates cannot drift apart.
    """

    if owner.item_type != WAREHOUSE:
        raise DiscoveryError(
            f"{relative_path}: programmables belong to a Warehouse item, not "
            f"{owner}"
        )
    stem = relative_path.rsplit("/", 1)[-1]
    if not stem.endswith(".sql"):
        raise DiscoveryError(
            f"{relative_path}: a programmable is a .sql file"
        )
    stem = stem[: -len(".sql")]
    parts = stem.split(".")
    if len(parts) != 2 or not all(parts):
        raise DiscoveryError(
            f"{relative_path}: name it <Schema>.<Procedure>.sql, one dot between "
            "the schema and the procedure"
        )
    object_id = ObjectId(
        schema=parts[0],
        object=parts[1],
    )
    identity = WeaverDocumentId(owner, object_id, shape=PROCEDURE_SHAPE)

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DiscoveryError(f"{relative_path}: must be UTF-8 text ({exc})") from exc

    match = _CREATE_PATTERN.search(text)
    if match is None:
        raise DiscoveryError(
            f"{relative_path}: must contain one 'create or alter procedure "
            "<Schema>.<Procedure>' statement, so a changed programmable can "
            "replace what is installed"
        )
    created = ObjectId(
        schema=_unbracket(match.group("schema")),
        object=_unbracket(match.group("name")),
    )
    if (
        created.schema.casefold() != object_id.schema.casefold()
        or created.object.casefold() != object_id.object.casefold()
    ):
        raise DiscoveryError(
            f"{relative_path}: the statement creates {created.qualified}, but the "
            f"file is named {object_id.qualified}. The two must agree."
        )
    _refuse_reserved(created, relative_path)

    return Programmable(
        identity=identity,
        text=text,
        signature=content_hash(data),
        role=_AUTHORED_ROLE,
        relative_path=relative_path,
    )


def _refuse_reserved(object_id: ObjectId, relative_path: str) -> None:
    """Weaver's reserved namespace stays Weaver's."""

    from ..etl import ETL_SCHEMA

    if object_id.schema == ETL_SCHEMA:
        raise DiscoveryError(
            f"{relative_path}: schema {ETL_SCHEMA!r} is reserved for Weaver's "
            "generated infrastructure, so an authored programmable may not "
            "create into it"
        )


def _unbracket(name: str) -> str:
    if name.startswith("[") and name.endswith("]"):
        return name[1:-1].replace("]]", "]")
    return name


def generated_programmable(
    identity: WeaverDocumentId,
    *,
    text: str,
    signature: str,
    role: str,
    origin: WeaverDocumentId | None = None,
) -> Programmable:
    """One Weaver-generated or package-owned Programmable."""

    return Programmable(
        identity=identity,
        text=text,
        signature=signature,
        role=role,
        origin=origin,
    )


#: What authored content carries in the Registry. It is managed structure
#: rather than scheduled work, so it is outside the runnable roles.
_AUTHORED_ROLE = "programmable"

__all__ = [
    "PROGRAMMABLES_DIRECTORY",
    "Programmable",
    "generated_programmable",
    "read_programmable",
]
