"""Read optional schema metadata from an item's ``schemas`` directory.

Object identities imply their schemas. A schema file can add metadata or declare
an otherwise empty schema, and its filename must match ``Schema ID`` exactly.
Reading the declaration creates nothing physically; a build creates schemas from
the item's declarations.
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
    """A schema declared by a file or implied by an object identity."""

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
        return self.relative_path is not None


def inferred_schema(schema_id: str) -> SchemaSes:
    """Build the metadata-free schema implied by an object identity.

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
    filename = relative_path.rsplit("/", 1)[-1]
    return filename[: -len(SCHEMA_SUFFIX)]


def read_schema_document(relative_path: str, data: bytes) -> SchemaSes:
    """Read a schema document and check its filename."""

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DiscoveryError(
            f"{relative_path}: schema file is not UTF-8 text ({exc}). Save it as UTF-8."
        ) from exc

    from .source import content_hash

    filename_id = schema_id_for_filename(relative_path)
    schema = parse_schema_document(text, relative_path)
    schema = replace(schema, source_hash=content_hash(data))
    if schema.schema_id != filename_id:
        raise DiscoveryError(
            f"{relative_path}: declares Schema ID {schema.schema_id!r} but the filename "
            f"names {filename_id!r}. Rename the file or change Schema ID so they "
            "match exactly, including case."
        )
    return schema


def parse_schema_document(text: str, relative_path: str) -> SchemaSes:
    try:
        loaded = yaml.load(text, Loader=_UniqueKeyLoader)
    except MetadataError:
        raise
    except yaml.YAMLError as exc:
        raise DiscoveryError(f"{relative_path}: invalid schema YAML: {exc}") from exc

    if not isinstance(loaded, dict):
        raise DiscoveryError(
            f"{relative_path}: schema metadata must be a YAML mapping with a Schema ID."
        )

    unknown = {str(key) for key in loaded} - _ALLOWED_KEYS
    if unknown:
        allowed = ", ".join(sorted(_ALLOWED_KEYS))
        raise DiscoveryError(
            f"{relative_path}: unknown schema key(s): "
            + ", ".join(sorted(unknown))
            + f". Use only {allowed}."
        )

    schema_id = loaded.get(_SCHEMA_ID)
    if not isinstance(schema_id, str) or not schema_id.strip():
        raise DiscoveryError(
            f"{relative_path}: {_SCHEMA_ID} is required and must be non-empty. "
            "Add a schema name."
        )
    schema_id = schema_id.strip()
    if "." in schema_id or any(character.isspace() for character in schema_id):
        raise DiscoveryError(
            f"{relative_path}: {_SCHEMA_ID} must be a single bare name, not "
            f"{schema_id!r}. Remove dots and whitespace."
        )

    description = loaded.get(_DESCRIPTION)
    if description is not None:
        if not isinstance(description, str) or not description.strip():
            raise DiscoveryError(
                f"{relative_path}: {_DESCRIPTION} must be non-empty when present. "
                "Add a description or remove the key."
            )
        description = description.strip()

    return SchemaSes(
        schema_id=schema_id,
        description=description,
        relative_path=relative_path,
        raw=dict(loaded),
    )
