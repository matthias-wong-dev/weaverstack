"""Versioned Weaver contract for staged Delta Table metadata.

The profile compiles catalogue shape before a writer runs. A writer allocates
physical names, Table ID and creation time for one instance. Read-back records
those allocations and verifies every committed profile field before publication.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from uuid import UUID

from ..delta_protocol import resolve_protocol_minima
from ..locations import Location
from ..store import Store

PROFILE_VERSION = "weaver-delta/v1"
WRITER_VERSION = "1.6.6"

_PROPERTIES = {
    "delta.columnMapping.mode": "name",
    "delta.checkpointInterval": "10",
    "delta.checkpointPolicy": "classic",
    "delta.checkpoint.writeStatsAsJson": "true",
    "delta.checkpoint.writeStatsAsStruct": "false",
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
}

_ALIASES = {
    "bigint": "long",
    "integer": "integer",
    "int": "integer",
    "smallint": "short",
    "tinyint": "byte",
    "bool": "boolean",
}
_PRIMITIVES = {
    "long",
    "integer",
    "short",
    "byte",
    "float",
    "double",
    "boolean",
    "string",
    "binary",
    "date",
    "timestamp",
    "timestamp_ntz",
    "variant",
}
_DECIMAL = re.compile(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.I)
_PHYSICAL = re.compile(
    r"col-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)


@dataclass(frozen=True)
class DeltaProfileV1:
    version: str
    writer_version: str
    protocol: dict[str, Any]
    configuration: dict[str, str]
    schema: dict[str, Any]
    partition_columns: tuple[str, ...]
    final_version: int


@dataclass(frozen=True)
class DeltaAllocation:
    table_id: str
    created_time: int
    physical_names: dict[str, str]


def _delta_type(spark_type: str) -> str:
    normalized = spark_type.strip().lower()
    if match := _DECIMAL.fullmatch(normalized):
        precision, scale = map(int, match.groups())
        if not (1 <= precision <= 38 and 0 <= scale <= precision):
            raise ValueError(f"unsupported Delta decimal: {spark_type}")
        return f"decimal({precision},{scale})"
    resolved = _ALIASES.get(normalized, normalized)
    if resolved not in _PRIMITIVES:
        raise ValueError(f"unsupported Weaver Delta v1 type: {spark_type}")
    return resolved


def compile_delta_profile_v1(
    columns: Sequence[Sequence[Any]],
    *,
    identity_column: str | None = None,
    generated_columns: Mapping[str, str] | None = None,
    partition_columns: Sequence[str] = (),
    protocol_minima: Mapping[str, int] | None = None,
) -> DeltaProfileV1:
    """Compile physical fields, protocol and properties without asking the writer."""
    if not columns:
        raise ValueError("Weaver Delta v1 requires at least one physical column")
    generated = generated_columns or {}
    names = [column[0] for column in columns]
    if len(set(name.casefold() for name in names)) != len(names):
        raise ValueError("duplicate physical Delta column")
    if identity_column is not None and identity_column not in names:
        raise ValueError("identity column absent from physical schema")
    if set(generated) - set(names):
        raise ValueError("generated column absent from physical schema")
    partitions = tuple(partition_columns)
    if len(set(partitions)) != len(partitions) or set(partitions) - set(names):
        raise ValueError("partition column absent or repeated")
    fields = []
    for index, (name, type_, not_null) in enumerate(columns, 1):
        delta_type = _delta_type(type_)
        metadata: dict[str, Any] = {"delta.columnMapping.id": index}
        if name == identity_column:
            if delta_type != "long" or not not_null or name in generated:
                raise ValueError("identity requires a distinct non-null bigint column")
            metadata.update(
                {
                    "delta.identity.start": 1,
                    "delta.identity.step": 1,
                    "delta.identity.allowExplicitInsert": False,
                }
            )
        if name in generated:
            metadata["delta.generationExpression"] = generated[name]
        fields.append(
            {
                "name": name,
                "type": delta_type,
                "nullable": not bool(not_null),
                "metadata": metadata,
            }
        )
    variant = any(field["type"] == "variant" for field in fields)
    minima = resolve_protocol_minima(protocol_minima)
    reader = max(2, minima["minReaderVersion"])
    writer = max(5, minima["minWriterVersion"])
    if (
        variant
        or identity_column is not None
        or any(field["type"] == "timestamp_ntz" for field in fields)
        or reader >= 3
        or writer >= 7
    ):
        reader, writer = max(3, reader), max(7, writer)
    feature_table = writer >= 7
    protocol: dict[str, Any] = {
        "minReaderVersion": reader,
        "minWriterVersion": writer,
    }
    if feature_table:
        protocol["readerFeatures"] = ["columnMapping"]
        protocol["writerFeatures"] = ["columnMapping"]
        if any(field["type"] == "timestamp_ntz" for field in fields):
            protocol["readerFeatures"].append("timestampNtz")
            protocol["writerFeatures"].append("timestampNtz")
        if variant:
            protocol["readerFeatures"].append("variantType")
            protocol["writerFeatures"].append("variantType")
        if identity_column is not None:
            protocol["writerFeatures"].append("identityColumns")
        if any(not field["nullable"] for field in fields):
            protocol["writerFeatures"].append("invariants")
        if generated:
            protocol["writerFeatures"].append("generatedColumns")
    configuration = {
        **_PROPERTIES,
        "delta.columnMapping.maxColumnId": str(len(fields)),
    }
    return DeltaProfileV1(
        version=PROFILE_VERSION,
        writer_version=WRITER_VERSION,
        protocol=protocol,
        configuration=configuration,
        schema={"type": "struct", "fields": fields},
        partition_columns=partitions,
        final_version=1 if feature_table else 0,
    )


def _canonical_protocol(protocol: Any) -> Any:
    if not isinstance(protocol, dict):
        raise ValueError("invalid Delta protocol")
    result = dict(protocol)
    for key in ("readerFeatures", "writerFeatures"):
        if key in result:
            values = result[key]
            if not isinstance(values, list) or len(values) != len(set(values)):
                raise ValueError("duplicate or malformed Delta protocol features")
            result[key] = sorted(values)
    return result


def verify_delta_profile_v1(
    profile: DeltaProfileV1, snapshot: Mapping[str, Any]
) -> DeltaAllocation:
    """Fail on committed metadata drift; return validated per-instance allocations."""
    if snapshot.get("version") != profile.final_version:
        raise ValueError("Delta commit version differs from Weaver profile")
    if _canonical_protocol(snapshot.get("protocol")) != _canonical_protocol(
        profile.protocol
    ):
        raise ValueError("Delta protocol differs from Weaver profile")
    metadata = snapshot.get("metaData")
    if not isinstance(metadata, dict) or set(metadata) != {
        "id",
        "name",
        "description",
        "createdTime",
        "format",
        "schemaString",
        "partitionColumns",
        "configuration",
    }:
        raise ValueError("Delta metadata fields differ from Weaver profile")
    table_id = metadata["id"]
    try:
        if not isinstance(table_id, str) or UUID(table_id).version != 4:
            raise ValueError
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("invalid allocated Delta Table ID") from exc
    created = metadata["createdTime"]
    if isinstance(created, bool) or not isinstance(created, int) or created <= 0:
        raise ValueError("invalid allocated Delta creation time")
    expected = {
        "name": None,
        "description": None,
        "format": {"provider": "parquet", "options": {}},
        "partitionColumns": list(profile.partition_columns),
        "configuration": profile.configuration,
    }
    for key, value in expected.items():
        if metadata[key] != value:
            raise ValueError(f"Delta {key} differs from Weaver profile")
    try:
        actual_schema = json.loads(metadata["schemaString"])
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid committed Delta schemaString") from exc
    if not isinstance(actual_schema, dict) or set(actual_schema) != {"type", "fields"}:
        raise ValueError("Delta schema structure differs from Weaver profile")
    if actual_schema["type"] != "struct":
        raise ValueError("Delta schema type differs from Weaver profile")
    fields = actual_schema["fields"]
    if not isinstance(fields, list) or len(fields) != len(profile.schema["fields"]):
        raise ValueError("Delta schema field count differs from Weaver profile")
    physical_names = {}
    for actual, expected_field in zip(fields, profile.schema["fields"]):
        if not isinstance(actual, dict) or set(actual) != set(expected_field):
            raise ValueError("Delta schema field shape differs from Weaver profile")
        actual_metadata = actual["metadata"]
        if not isinstance(actual_metadata, dict):
            raise ValueError("Delta columnMapping metadata missing")
        physical = actual_metadata.get("delta.columnMapping.physicalName")
        if not isinstance(physical, str) or not _PHYSICAL.fullmatch(physical):
            raise ValueError("Delta columnMapping.physicalName invalid")
        normalized = {
            **actual,
            "metadata": {
                key: value
                for key, value in actual_metadata.items()
                if key != "delta.columnMapping.physicalName"
            },
        }
        if normalized != expected_field:
            raise ValueError(
                "Delta schema or columnMapping.id differs from Weaver profile"
            )
        physical_names[actual["name"]] = physical
    if len(set(physical_names.values())) != len(physical_names):
        raise ValueError("duplicate allocated Delta columnMapping.physicalName")
    return DeltaAllocation(
        table_id=table_id, created_time=created, physical_names=physical_names
    )


def read_delta_snapshot(store: Store, root: Location) -> dict[str, Any]:
    """Read the committed protocol and metadata across the private stage's logs."""
    actions: dict[str, Any] = {}
    last = -1
    for version in range(3):
        log = root / "_delta_log" / f"{version:020d}.json"
        if not store.exists(log):
            break
        if version == 2:
            raise ValueError("Weaver Delta v1 has unexpected additional commits")
        last = version
        for line in store.read(log).splitlines():
            try:
                entry = json.loads(line)
            except ValueError as exc:
                raise ValueError("invalid committed Delta log JSON") from exc
            if not isinstance(entry, dict) or len(entry) != 1:
                raise ValueError("malformed committed Delta log action")
            key, value = next(iter(entry.items()))
            if key not in {"protocol", "metaData", "commitInfo"}:
                raise ValueError(f"unexpected Delta creation action: {key}")
            if key != "commitInfo":
                actions[key] = value
    if last < 0 or "protocol" not in actions or "metaData" not in actions:
        raise ValueError("Delta creation log lacks protocol or metadata")
    return {
        "version": last,
        "protocol": actions["protocol"],
        "metaData": actions["metaData"],
    }
