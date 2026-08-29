"""Render authored documents and arrange their dependency layers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..declaration.metadata import FOLDER, TABLE, VIEW
from ..declaration.model import WeaverItemId
from .changes import FOLDER as FOLDER_KIND
from .changes import TABLE as TABLE_KIND
from .changes import VIEW as VIEW_KIND
from .changes import added
from .models import (
    BUILD_FOLDER,
    BUILD_TABLE,
    BUILD_VIEW,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .stages import BUILD, PlannedStage

_OBJECT_KIND = {TABLE: BUILD_TABLE, VIEW: BUILD_VIEW}
_CHANGE_KIND = {FOLDER: FOLDER_KIND, TABLE: TABLE_KIND, VIEW: VIEW_KIND}


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-").replace(":", "-")


@dataclass(frozen=True)
class RenderedAction:
    """One authored document rendered as an action and frozen payload."""

    action: InstallAction
    payloads: Mapping[str, bytes]


def render_document_build_action(identity, source, *, destination) -> RenderedAction:
    """Render one document for a destination selected by the item planner."""

    action_slug = _slug(identity)
    if source.kind == FOLDER:
        return RenderedAction(
            action=InstallAction(
                id=f"folder-{action_slug}",
                kind=BUILD_FOLDER,
                resource_node_id=str(identity),
                executor="folder",
                payload=None,
                payload_sha256=None,
                source_path=getattr(source, "relative_path", None),
            ),
            payloads={},
        )
    ddl = source.create_ddl(destination=destination)
    filename = f"{action_slug}{ddl.extension}"
    content = ddl.content.encode("utf-8")
    return RenderedAction(
        action=InstallAction(
            id=f"object-{action_slug}",
            kind=_OBJECT_KIND[source.kind],
            resource_node_id=str(identity),
            executor=ddl.executor,
            payload=filename,
            payload_sha256=sha256_hex(content),
            source_path=getattr(source, "relative_path", None),
        ),
        payloads={filename: content},
    )


def render_lakehouse_document_build_action(
    identity, source, *, target
) -> RenderedAction:
    """Render one Lakehouse document with its Spark destination."""

    return render_document_build_action(
        identity, source, destination=target.spark_target
    )


def render_warehouse_document_build_action(
    identity, source, *, target
) -> RenderedAction:
    """Render one Warehouse document with its T-SQL destination."""

    return render_document_build_action(identity, source, destination=None)


def lakehouse_build_stages(
    repository, selected_for_build, *, item: WeaverItemId, target
) -> tuple[PlannedStage, ...]:
    """Render one Lakehouse item's document dependency layers."""

    return _item_build_stages(
        repository,
        selected_for_build,
        item=item,
        target=target,
        renderer=render_lakehouse_document_build_action,
    )


def warehouse_build_stages(
    repository, selected_for_build, *, item: WeaverItemId, target
) -> tuple[PlannedStage, ...]:
    """Render one Warehouse item's document dependency layers."""

    return _item_build_stages(
        repository,
        selected_for_build,
        item=item,
        target=target,
        renderer=render_warehouse_document_build_action,
    )


def _item_build_stages(
    repository,
    selected_for_build,
    *,
    item: WeaverItemId,
    target,
    renderer,
) -> tuple[PlannedStage, ...]:
    selected = {identity for identity in selected_for_build if identity.item == item}
    if not selected:
        return ()
    graph = repository.dependency_graph.subgraph({str(value) for value in selected})
    identities = {str(identity): identity for identity in selected}
    stages = []
    for index, layer in enumerate(graph.layers()):
        payloads: dict[str, bytes] = {}
        actions = []
        changes = []
        for node in sorted(layer):
            identity = identities[node]
            source = repository.source_documents[identity]
            rendered = renderer(identity, source, target=target)
            payloads.update(rendered.payloads)
            actions.append(rendered.action)
            changes.append(
                added(
                    _CHANGE_KIND[source.kind],
                    identity.object_id.qualified,
                    rendered.action.id,
                )
            )
        stages.append(
            PlannedStage(
                phase=BUILD,
                index=index,
                slug="build-objects",
                description="build dependency layer",
                payloads=payloads,
                changes={target.id: tuple(changes)},
                batches=(
                    BuildBatch(
                        id=f"{_slug(item)}",
                        target_id=target.id,
                        actions=tuple(actions),
                    ),
                ),
            )
        )
    return tuple(stages)
