"""First-class Warehouse stored-procedure declarations.

A Programmable is a managed stored procedure, whether authored under
``Warehouse/<Item>/programmables/<Schema>.<Procedure>.sql``, generated from a
logical declaration, or supplied by a Weaver fragment. One representation and
one lifecycle: discover, validate, sign, select, install, register, prune.

Identity is the ``PROCEDURE_SHAPE`` document identity, an ordinary schema and an
object name carrying what the procedure is for.
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

#: What authored content carries in the Registry. Managed structure rather than
#: scheduled work: nothing runs a Programmable but a caller.
ROLE_PROGRAMMABLE = "programmable"

#: The installer runs a Programmable's text verbatim, and a plain ``CREATE``
#: fails once the procedure exists, so a replacement-safe form is required.
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
    installer runs, ``signature`` what incremental selection compares, and
    ``role`` what the procedure is for.

    ``relative_path`` marks authored item source and records where it was
    written; ``origin`` marks generated content and records the declaration it
    was derived from. Weaver's own fragments carry neither, so they sign
    themselves and no item signature moves when one changes.
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
    relative_path: str,
    data: bytes,
    *,
    owner: WeaverItemId,
    weaver_owned: bool = False,
) -> Programmable:
    """One stored procedure from a ``.sql`` file, validated against its name.

    The file lives at ``programmables/<Schema>.<Procedure>.sql`` and its SQL
    creates that exact procedure, so the identity the catalogue registers and
    the object the statement creates cannot drift apart. ``weaver_owned`` reads
    a Weaver fragment, which may claim the reserved ``_`` schema and is not item
    source.
    """

    if owner.item_type != WAREHOUSE:
        raise DiscoveryError(
            f"{relative_path}: programmables belong to a Warehouse item, not "
            f"{owner}"
        )
    stem = relative_path.rsplit("/", 1)[-1]
    if not stem.endswith(".sql"):
        raise DiscoveryError(f"{relative_path}: a programmable is a .sql file")
    parts = stem[: -len(".sql")].split(".")
    if len(parts) != 2 or not all(parts):
        raise DiscoveryError(
            f"{relative_path}: name it <Schema>.<Procedure>.sql, one dot between "
            "the schema and the procedure"
        )
    object_id = ObjectId(schema=parts[0], object=parts[1])

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DiscoveryError(f"{relative_path}: must be UTF-8 text ({exc})") from exc

    declared = _CREATE_PATTERN.findall(text)
    if len(declared) != 1:
        raise DiscoveryError(
            f"{relative_path}: must contain exactly one 'create or alter "
            "procedure <Schema>.<Procedure>' statement, so what Weaver installs "
            "is what it registers and prunes"
        )
    created = ObjectId(
        schema=_unbracket(declared[0][0]), object=_unbracket(declared[0][1])
    )
    if (
        created.schema.casefold() != object_id.schema.casefold()
        or created.object.casefold() != object_id.object.casefold()
    ):
        raise DiscoveryError(
            f"{relative_path}: the statement creates {created.qualified}, but the "
            f"file is named {object_id.qualified}. The two must agree."
        )
    if not weaver_owned and created.schema == _reserved_schema():
        raise DiscoveryError(
            f"{relative_path}: schema {created.schema!r} is reserved for Weaver, "
            "so an authored programmable may not create into it"
        )

    return Programmable(
        identity=WeaverDocumentId(owner, object_id, shape=PROCEDURE_SHAPE),
        text=text,
        signature=content_hash(data),
        role=ROLE_PROGRAMMABLE,
        relative_path=None if weaver_owned else relative_path,
    )


def generated_programmable(
    identity: WeaverDocumentId,
    *,
    text: str,
    signature: str,
    role: str,
    origin: WeaverDocumentId | None = None,
) -> Programmable:
    """One Programmable Weaver generated from a logical declaration."""

    return Programmable(
        identity=identity,
        text=text,
        signature=signature,
        role=role,
        origin=origin,
    )


def _reserved_schema() -> str:
    from ..etl import ETL_SCHEMA

    return ETL_SCHEMA


def _unbracket(name: str) -> str:
    if name.startswith("[") and name.endswith("]"):
        return name[1:-1].replace("]]", "]")
    return name


__all__ = [
    "PROGRAMMABLES_DIRECTORY",
    "ROLE_PROGRAMMABLE",
    "Programmable",
    "generated_programmable",
    "read_programmable",
]
