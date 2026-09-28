"""Write a private Delta Table and publish only its verified v1 metadata."""

from __future__ import annotations

import json
import re
import time
from importlib import metadata
from typing import Callable, Sequence
from uuid import uuid4

from ..fabric.onelake import abfss_root, onelake_url
from ..fabric.resources import LAKEHOUSE
from ..locations import Location
from ..spark import FabricSparkTarget
from ..store import Store
from ..targets import ItemRef, validate_name
from .delta_profile import (
    WRITER_VERSION,
    DeltaAllocation,
    compile_delta_profile_v1,
    read_delta_snapshot,
    verify_delta_profile_v1,
)

_PART = r"`((?:``|[^`])*)`"
_OBJECT = re.compile(r"\.".join((_PART,) * 4) + r"\Z")


def direct_profile_supported(
    columns: Sequence[Sequence], identity_column: str | None, column_mapping: bool
) -> bool:
    """Use v1 for scalar Tables; retain TableBuilder for other supported types."""
    if any(re.search(r"\bvariant\b", str(column[1]), re.I) for column in columns):
        raise ValueError("VARIANT columns are not supported in Weaver Table Build")
    if not column_mapping:
        return False
    try:
        compile_delta_profile_v1(columns, identity_column=identity_column)
    except ValueError as exc:
        if str(exc).startswith("unsupported Weaver Delta v1 type:"):
            return False
        raise
    return True


def run_direct_delta_actions(create, actions, *, workspace=None):
    """Settle each independent direct commit without losing later siblings."""
    origin = time.monotonic()
    outcomes = []
    for label, qualified, columns, identity in actions:
        started = time.monotonic()
        try:
            create(
                qualified,
                columns,
                identity_column=identity,
                workspace=workspace,
            )
        except Exception as exc:
            outcome = {
                "label": label,
                "succeeded": False,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            }
        else:
            outcome = {"label": label, "succeeded": True}
        outcome["started_after_seconds"] = started - origin
        outcome["duration_seconds"] = time.monotonic() - started
        outcomes.append(outcome)
    return outcomes


def table_identity(target: FabricSparkTarget, qualified: str) -> tuple[str, str]:
    """Recover a Table's authored path only within its frozen bound target."""
    match = _OBJECT.fullmatch(qualified)
    if match is None:
        raise ValueError("Delta Table has no four-part bound target identity")
    workspace, lakehouse, schema, name = (
        part.replace("``", "`") for part in match.groups()
    )
    if (workspace, lakehouse) != target.namespace or target.qualify(
        schema, name
    ) != qualified:
        raise ValueError("Delta Table does not match its bound target")
    return schema, name


def create_bound_delta_table(
    *,
    qualified_name: str,
    columns: Sequence[Sequence],
    identity_column: str | None,
    resolver,
    store,
) -> DeltaAllocation:
    """Resolve a frozen four-part Table name to one typed Lakehouse path."""
    match = _OBJECT.fullmatch(qualified_name)
    if match is None:
        raise ValueError("Delta Table has no four-part bound target identity")
    names = [part.replace("``", "`") for part in match.groups()]
    target = FabricSparkTarget(resolver.configuration.workspace, names[1])
    schema, name = table_identity(target, qualified_name)
    schema = validate_name(schema, what="schema")
    name = validate_name(name, what="object name")
    item = resolver.resolve(ItemRef(target.lakehouse), item_type=LAKEHOUSE)
    workspace_id = resolver.workspace.id
    stage_relative = f"Files/weaver-stage-{uuid4().hex}"
    stage = Location(onelake_url(workspace_id, item.id, stage_relative))
    destination = Location(
        onelake_url(workspace_id, item.id, f"Tables/{schema}/{name}")
    )
    return create_staged_delta_table(
        uri=f"{abfss_root(workspace_id, item.id)}/{stage_relative}",
        stage=stage,
        destination=destination,
        store=store,
        columns=columns,
        identity_column=identity_column,
        storage_options={
            "bearer_token": store.token,
            "use_fabric_endpoint": "true",
        },
        publish=store.rename_directory,
    )


def create_staged_delta_table(
    *,
    uri: str,
    stage: Location,
    destination: Location,
    store: Store,
    columns: Sequence[Sequence],
    identity_column: str | None,
    storage_options: dict[str, str] | None,
    publish: Callable[[Location, Location], None],
) -> DeltaAllocation:
    """Create in private storage; verify the final log before conditional publish."""
    profile = compile_delta_profile_v1(columns, identity_column=identity_column)
    if any(field["type"] == "variant" for field in profile.schema["fields"]):
        raise ValueError("VARIANT columns are not supported in Weaver Table Build")
    if metadata.version("deltalake") != WRITER_VERSION:
        raise ValueError("Delta writer version differs from Weaver profile")
    if store.exists(stage) or store.exists(destination):
        raise ValueError("Delta Table stage or destination already exists")

    from deltalake import DeltaTable, TableFeatures
    from deltalake.schema import Schema

    fields = [
        {
            **field,
            "metadata": {
                key: value
                for key, value in field["metadata"].items()
                if key != "delta.columnMapping.id"
            },
        }
        for field in profile.schema["fields"]
    ]
    schema = Schema.from_json(json.dumps({"type": "struct", "fields": fields}))
    table = DeltaTable.create(
        uri,
        schema,
        configuration={
            key: value
            for key, value in profile.configuration.items()
            if key != "delta.columnMapping.maxColumnId"
        },
        partition_by=list(profile.partition_columns),
        storage_options=storage_options,
        mode="error",
    )
    if profile.final_version:
        features = {
            "columnMapping": TableFeatures.ColumnMapping,
            "identityColumns": TableFeatures.IdentityColumns,
            "invariants": TableFeatures.Invariants,
            "generatedColumns": TableFeatures.GeneratedColumns,
        }
        table.alter.add_feature(
            [features[name] for name in profile.protocol["writerFeatures"]],
            allow_protocol_versions_increase=True,
        )
        table.update_incremental()
    original = read_delta_snapshot(store, stage)
    allocation = verify_delta_profile_v1(profile, original)
    publish(stage, destination)
    if store.exists(stage) or not store.exists(destination):
        raise ValueError("Delta Table publication did not move the validated stage")
    if read_delta_snapshot(store, destination) != original:
        raise ValueError("Published Delta log differs from validated stage")
    return allocation
