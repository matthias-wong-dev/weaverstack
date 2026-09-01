"""Plan managed object removal before rebuild."""

from __future__ import annotations

from dataclasses import replace
from typing import Mapping

from ..catalogue.tables import ROLE_SHORTCUT
from ..declaration.metadata import FOLDER, TABLE, VIEW
from ..declaration.model import WeaverItemId
from ..errors import BuildError
from ..graph import Graph
from .changes import FOLDER as FOLDER_KIND
from .changes import TABLE as TABLE_KIND
from .changes import VIEW as VIEW_KIND
from .changes import removed
from .models import (
    DROP_FOLDER,
    DROP_SHORTCUT,
    DROP_TABLE,
    DROP_VIEW,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .sql_templates import render_sql_statement, tsql_ident
from .stages import DROP, PlannedStage

_DROP_KIND = {FOLDER: DROP_FOLDER, TABLE: DROP_TABLE, VIEW: DROP_VIEW}
_DECLARATION_KIND = {"folder": FOLDER, "table": TABLE, "view": VIEW}
_CHANGE_KIND = {"folder": FOLDER_KIND, "table": TABLE_KIND, "view": VIEW_KIND}


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-").replace(":", "-")


def _refuse_protected(schema: str, name: str, what: str) -> None:
    """Stop a destructive action against a Weaver catalogue table."""

    from ..catalogue.tables import is_protected

    if is_protected(schema, name):
        raise BuildError(
            f"{what} is a Weaver catalogue table and cannot be dropped. It holds "
            "installed state no declaration reproduces, so a build may create it "
            "and write it but never replace it."
        )


def lakehouse_drop_stages(
    repository,
    selected_for_drop,
    *,
    item: WeaverItemId,
    target,
    inventory,
    registered: Mapping | None = None,
    reused_names=(),
) -> tuple[PlannedStage, ...]:
    """Plan one Lakehouse item's managed drops."""

    return _item_drop_stages(
        repository,
        selected_for_drop,
        item=item,
        target=target,
        inventory=inventory,
        registered=registered,
        renderer=_lakehouse_drop_action,
        reused_names=reused_names,
    )


def warehouse_drop_stages(
    repository,
    selected_for_drop,
    *,
    item: WeaverItemId,
    target,
    inventory,
    registered: Mapping | None = None,
    reused_names=(),
) -> tuple[PlannedStage, ...]:
    """Plan one Warehouse item's managed drops."""

    return _item_drop_stages(
        repository,
        selected_for_drop,
        item=item,
        target=target,
        inventory=inventory,
        registered=registered,
        renderer=_warehouse_drop_action,
        reused_names=reused_names,
    )


def _item_drop_stages(
    repository,
    selected_for_drop,
    *,
    item,
    target,
    inventory,
    registered,
    renderer,
    reused_names=(),
) -> tuple[PlannedStage, ...]:
    selected = {identity for identity in selected_for_drop if identity.item == item}
    if not selected:
        return ()
    registered = dict(registered or {})
    graph = _drop_layers(repository, selected)
    identities = {str(identity): identity for identity in selected}
    stages = []
    for index, layer in enumerate(reversed(graph.layers())):
        payloads: dict[str, bytes] = {}
        changes = []
        actions = []
        for node in sorted(layer):
            identity = identities[node]
            installed = inventory.physical_type(identity)
            if installed is None:
                raise BuildError(
                    f"selected managed drop {identity} is absent from target inventory"
                )
            role = registered.get(identity)
            action = renderer(
                identity,
                installed,
                target,
                payloads,
                installed_role=None if role is None else role.object_role,
            )
            if identity in reused_names and action.kind == DROP_SHORTCUT:
                # This plan gives the name to an owned object, and OneLake
                # releases it after Fabric stops listing the shortcut.
                action = replace(action, awaits_name_release=True)
            actions.append(action)
            changes.append(
                removed(
                    _CHANGE_KIND[installed],
                    identity.object_id.qualified,
                    f"managed-drop-{_slug(identity)}",
                )
            )
        stages.append(
            PlannedStage(
                phase=DROP,
                index=index,
                slug="managed-drop",
                description="drop selected rebuild dependency layer",
                payloads=payloads,
                changes={target.id: tuple(changes)},
                batches=(
                    BuildBatch(
                        id=f"managed-drop-{_slug(item)}",
                        target_id=target.id,
                        actions=tuple(actions),
                    ),
                ),
            )
        )
    return tuple(stages)


def _drop_layers(repository, selected) -> Graph:
    chosen = {str(identity) for identity in selected}
    return Graph(
        chosen,
        [
            (edge.upstream, edge.downstream)
            for edge in repository.dependency_graph.edges
            if edge.upstream in chosen and edge.downstream in chosen
        ],
    )


def _installed_kind(identity, installed_type):
    try:
        return _DECLARATION_KIND[installed_type]
    except KeyError as exc:
        raise BuildError(
            f"registered document {identity} has unsupported type {installed_type!r}"
        ) from exc


def _lakehouse_drop_action(
    identity, installed_type, target, payloads, *, installed_role=None
) -> InstallAction:
    _refuse_protected(
        identity.object_id.schema, identity.object_id.object, str(identity)
    )
    if installed_role == ROLE_SHORTCUT:
        return _drop_shortcut_action(identity, payloads)
    installed_kind = _installed_kind(identity, installed_type)
    action_slug = _slug(identity)
    if installed_kind == FOLDER:
        return InstallAction(
            id=f"managed-drop-{action_slug}",
            kind=DROP_FOLDER,
            resource_node_id=str(identity),
            executor="folder",
            payload=None,
            payload_sha256=None,
        )
    template = "drop_view" if installed_kind == VIEW else "drop_table"
    content = render_sql_statement(
        "spark_sql",
        template,
        relation=target.spark_target.qualify(
            identity.object_id.schema, identity.object_id.object
        ),
    ).encode("utf-8")
    return _sql_drop_action(
        identity,
        installed_kind,
        content,
        payloads,
        executor="spark_sql",
        extension=".spark.sql",
    )


def _warehouse_drop_action(
    identity, installed_type, target, payloads, *, installed_role=None
) -> InstallAction:
    _refuse_protected(
        identity.object_id.schema, identity.object_id.object, str(identity)
    )
    installed_kind = _installed_kind(identity, installed_type)
    if installed_kind == FOLDER:
        raise BuildError(f"Warehouse object {identity} cannot be a folder")
    template = "drop_view" if installed_kind == VIEW else "drop_table"
    content = render_sql_statement(
        "tsql",
        template,
        relation=(
            f"{tsql_ident(identity.object_id.schema)}."
            f"{tsql_ident(identity.object_id.object)}"
        ),
    ).encode("utf-8")
    return _sql_drop_action(
        identity,
        installed_kind,
        content,
        payloads,
        executor="tsql",
        extension=".sql",
    )


def _sql_drop_action(
    identity, installed_kind, content, payloads, *, executor, extension
) -> InstallAction:
    action_slug = _slug(identity)
    filename = f"drop-{action_slug}{extension}"
    payloads[filename] = content
    return InstallAction(
        id=f"managed-drop-{action_slug}",
        kind=_DROP_KIND[installed_kind],
        resource_node_id=str(identity),
        executor=executor,
        payload=filename,
        payload_sha256=sha256_hex(content),
    )


def _drop_shortcut_action(identity, payloads) -> InstallAction:
    from .shortcuts import shortcut_removal_payload

    action_slug = _slug(identity)
    content = shortcut_removal_payload(identity)
    filename = f"drop-shortcut-{action_slug}.shortcut.json"
    payloads[filename] = content
    return InstallAction(
        id=f"managed-drop-{action_slug}",
        kind=DROP_SHORTCUT,
        resource_node_id=str(identity),
        executor="shortcut",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
