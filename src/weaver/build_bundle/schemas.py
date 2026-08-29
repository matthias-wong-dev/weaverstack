"""Plan item-owned schema creation."""

from __future__ import annotations

from ..declaration.model import WeaverItemId
from .changes import SCHEMA as SCHEMA_KIND
from .changes import added
from .models import CREATE_SCHEMA, BuildBatch, InstallAction
from .payloads import sha256_hex
from .sql_templates import render_sql_statement, tsql_ident
from .stages import SCHEMA, PlannedStage


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-").replace(":", "-")


def lakehouse_schema_stage(
    selected_ids,
    *,
    item: WeaverItemId,
    target,
    inventory,
    extra_schemas=(),
) -> PlannedStage | None:
    """Plan missing schemas for one Lakehouse item."""

    return _schema_stage(
        selected_ids,
        item=item,
        target=target,
        inventory=inventory,
        extra_schemas=extra_schemas,
        render=lambda schema: render_sql_statement(
            "spark_sql",
            "create_schema",
            statement=target.spark_target.create_schema_statement(schema),
        ).encode("utf-8"),
        executor="spark_sql",
        extension=".spark.sql",
    )


def warehouse_schema_stage(
    selected_ids,
    *,
    item: WeaverItemId,
    target,
    inventory,
    extra_schemas=(),
) -> PlannedStage | None:
    """Plan missing schemas for one Warehouse item."""

    return _schema_stage(
        selected_ids,
        item=item,
        target=target,
        inventory=inventory,
        extra_schemas=extra_schemas,
        render=lambda schema: render_sql_statement(
            "tsql", "create_schema", schema=tsql_ident(schema)
        ).encode("utf-8"),
        executor="tsql",
        extension=".sql",
    )


def _schema_stage(
    selected_ids,
    *,
    item: WeaverItemId,
    target,
    inventory,
    extra_schemas,
    render,
    executor: str,
    extension: str,
) -> PlannedStage | None:
    present = {schema.casefold() for schema in inventory.schemas}
    wanted = {
        identity.object_id.schema
        for identity in selected_ids
        if identity.item == item and not identity.is_files
    } | set(extra_schemas)
    schemas = sorted(schema for schema in wanted if schema.casefold() not in present)
    if not schemas:
        return None

    item_slug = _slug(item)
    payloads: dict[str, bytes] = {}
    actions = []
    changes = []
    for schema in schemas:
        content = render(schema)
        filename = f"create-{item_slug}-{schema}{extension}"
        payloads[filename] = content
        action_id = f"schema-{item_slug}-{schema}"
        actions.append(
            InstallAction(
                id=action_id,
                kind=CREATE_SCHEMA,
                resource_node_id=None,
                executor=executor,
                payload=filename,
                payload_sha256=sha256_hex(content),
            )
        )
        changes.append(added(SCHEMA_KIND, schema, action_id))
    return PlannedStage(
        phase=SCHEMA,
        slug="create-schemas",
        description="create item-owned schemas",
        payloads=payloads,
        changes={target.id: tuple(changes)},
        batches=(
            BuildBatch(id=f"{item_slug}", target_id=target.id, actions=tuple(actions)),
        ),
    )
