"""Schema documents, one per file under an item's ``schemas`` directory.

A managed item-owned identity implies its schema. ``Warehouse/MyWH/Sales.Customer.sql``
establishes the schema ``Sales`` in that Warehouse, and so does
``Lakehouse/MyLH/Tables/Sales.Customer.sql``.

::

    Warehouse/MyWH/
      schemas/
        Sales.yml
      Sales.Customer.sql

The file is optional metadata for the same schema, and it may also declare a
schema no object sits in yet. Each file names exactly one schema, and its
filename without ``.yml`` must match the declared ``Schema ID`` exactly, case
included. Declaring a schema creates nothing physical: the build stage plans
that from the identities an item owns.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from typing import Any

import yaml

from ..errors import DiscoveryError, MetadataError
from .metadata import _UniqueKeyLoader

SCHEMA_SUFFIX = ".yml"

_SCHEMA_ID = "Schema ID"
_DESCRIPTION = "Description"
_ALLOWED_KEYS = {_SCHEMA_ID, _DESCRIPTION}


@dataclass(frozen=True)
class SchemaSes:
    """One schema an item owns, whether a file declares it or an object implies it."""

    schema_id: str
    description: str | None
    #: Where the declaration was read from. ``None`` for an inferred schema,
    #: which :attr:`is_explicit` reads back.
    relative_path: str | None
    #: The declaration's content hash, on the same terms as an object's. It is
    #: what the catalogue records as the signature of a schema row. Empty for a
    #: schema parsed from text rather than read from a file.
    source_hash: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def is_explicit(self) -> bool:
        """Whether a ``schemas/<Schema>.yml`` declared this schema."""

        return self.relative_path is not None


def inferred_schema(schema_id: str) -> SchemaSes:
    """The schema a managed identity implies, carrying no metadata.

    The signature covers the schema name and nothing else, so editing the object
    that happened to imply the schema does not read as a schema metadata change.
    """

    digest = hashlib.sha256()
    digest.update(b"weaver:inferred-schema\n")
    digest.update(schema_id.encode("utf-8"))
    return SchemaSes(
        schema_id=schema_id,
        description=None,
        relative_path=None,
        source_hash=digest.hexdigest(),
    )


def schema_id_for_filename(relative_path: str) -> str:
    """The Schema ID a ``schemas/`` filename claims, before the file is read."""

    filename = relative_path.rsplit("/", 1)[-1]
    return filename[: -len(SCHEMA_SUFFIX)]


def read_schema_document(relative_path: str, data: bytes) -> SchemaSes:
    """Parse and validate one schema Weaver document file against its filename."""

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DiscoveryError(f"{relative_path}: must be UTF-8 text ({exc})") from exc

    from .source import content_hash

    filename_id = schema_id_for_filename(relative_path)
    schema = parse_schema_document(text, relative_path)
    schema = replace(schema, source_hash=content_hash(data))
    if schema.schema_id != filename_id:
        raise DiscoveryError(
            f"{relative_path}: declares Schema ID {schema.schema_id!r} but the filename "
            f"names {filename_id!r}. They must match exactly, case included"
        )
    return schema


def parse_schema_document(text: str, relative_path: str) -> SchemaSes:
    """Parse the YAML of a schema Weaver document file."""

    try:
        loaded = yaml.load(text, Loader=_UniqueKeyLoader)
    except MetadataError:
        raise
    except yaml.YAMLError as exc:
        raise DiscoveryError(f"{relative_path}: invalid schema YAML: {exc}") from exc

    if not isinstance(loaded, dict):
        raise DiscoveryError(f"{relative_path}: schema metadata must be a YAML mapping")

    unknown = {str(key) for key in loaded} - _ALLOWED_KEYS
    if unknown:
        raise DiscoveryError(
            f"{relative_path}: unknown schema key(s): " + ", ".join(sorted(unknown))
        )

    schema_id = loaded.get(_SCHEMA_ID)
    if not isinstance(schema_id, str) or not schema_id.strip():
        raise DiscoveryError(
            f"{relative_path}: {_SCHEMA_ID} is required and must be non-empty"
        )
    schema_id = schema_id.strip()
    if "." in schema_id or any(character.isspace() for character in schema_id):
        raise DiscoveryError(
            f"{relative_path}: {_SCHEMA_ID} must be a single bare name, got {schema_id!r}"
        )

    description = loaded.get(_DESCRIPTION)
    if description is not None:
        if not isinstance(description, str) or not description.strip():
            raise DiscoveryError(
                f"{relative_path}: {_DESCRIPTION} must be non-empty when present"
            )
        description = description.strip()

    return SchemaSes(
        schema_id=schema_id,
        description=description,
        relative_path=relative_path,
        raw=dict(loaded),
    )
