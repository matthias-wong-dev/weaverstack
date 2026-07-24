"""Schema SES files — one declared schema per file under ``_schemas``.

A repository declares its schemas explicitly rather than letting a two-part
object ID conjure one on the fly. Every ``Schema.Object`` an object or an alias
uses implies a schema, and that schema must be declared here or the repository
is invalid.

::

    repo/
      _schemas/
        Sales.yml
        Reporting.yml
      Sales__Order.py
      Reporting.OrderReport.sql

Each file names exactly one schema, and its filename — without ``.yml`` — must
match the declared ``Schema ID`` exactly, case included. A schema is a
repository resource: it is not owned by a Lakehouse, a Warehouse, a tier or an
object folder, and declaring one does not create anything physical.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from ..errors import DiscoveryError, MetadataError
from .metadata import _UniqueKeyLoader

#: The directory schema SES files live in, relative to the repository root.
SCHEMAS_DIRECTORY = "_schemas"
SCHEMA_SUFFIX = ".yml"

_SCHEMA_ID = "Schema ID"
_DESCRIPTION = "Description"
_ALLOWED_KEYS = {_SCHEMA_ID, _DESCRIPTION}


@dataclass(frozen=True)
class SchemaSes:
    """One declared schema, kept as a distinct repository resource."""

    schema_id: str
    description: str | None
    relative_path: str
    raw: dict[str, Any] = field(default_factory=dict)


def schema_id_for_filename(relative_path: str) -> str:
    """The Schema ID a ``_schemas`` filename claims, before the file is read."""

    filename = relative_path.rsplit("/", 1)[-1]
    return filename[: -len(SCHEMA_SUFFIX)]


def is_schema_file(relative_path: str) -> bool:
    """True for ``_schemas/<name>.yml`` at the repository root."""

    parts = relative_path.split("/")
    return (
        len(parts) == 2
        and parts[0] == SCHEMAS_DIRECTORY
        and parts[1].endswith(SCHEMA_SUFFIX)
        and len(parts[1]) > len(SCHEMA_SUFFIX)
    )


def read_schema_document(relative_path: str, data: bytes) -> SchemaSes:
    """Parse and validate one schema SES file against its filename."""

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DiscoveryError(f"{relative_path}: must be UTF-8 text ({exc})") from exc

    filename_id = schema_id_for_filename(relative_path)
    schema = parse_schema_document(text, relative_path)
    if schema.schema_id != filename_id:
        raise DiscoveryError(
            f"{relative_path}: declares Schema ID {schema.schema_id!r} but the filename "
            f"names {filename_id!r} — they must match exactly, case included"
        )
    return schema


def parse_schema_document(text: str, relative_path: str) -> SchemaSes:
    """Parse the YAML of a schema SES file."""

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
        raise DiscoveryError(f"{relative_path}: {_SCHEMA_ID} is required and must be non-empty")
    schema_id = schema_id.strip()
    if "." in schema_id or any(character.isspace() for character in schema_id):
        raise DiscoveryError(
            f"{relative_path}: {_SCHEMA_ID} must be a single bare name, got {schema_id!r}"
        )

    description = loaded.get(_DESCRIPTION)
    if description is not None:
        if not isinstance(description, str) or not description.strip():
            raise DiscoveryError(f"{relative_path}: {_DESCRIPTION} must be non-empty when present")
        description = description.strip()

    return SchemaSes(
        schema_id=schema_id,
        description=description,
        relative_path=relative_path,
        raw=dict(loaded),
    )
