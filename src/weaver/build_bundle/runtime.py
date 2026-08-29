"""Render runtime artefact installation and removal."""

from __future__ import annotations

from ..declaration.model import WeaverItemId
from ..etl import FILE_TYPE, PROCEDURE_TYPE
from .changes import FILE as FILE_KIND
from .changes import STORED_PROCEDURE as PROCEDURE_KIND
from .changes import added
from .changes import removed as change_removed
from .documents import RenderedAction
from .models import (
    BUILD_PROCEDURE,
    DELETE_FILE,
    DROP_PROCEDURE,
    WRITE_FILE,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .sql_templates import render_sql_statement, tsql_ident
from .stages import RUNTIME, PlannedStage


def _slug(value) -> str:
    return str(value).replace("/", "--").replace(" ", "-").replace(":", "-")


def render_runtime_build_action(artefact) -> RenderedAction:
    """Render one runtime artefact as an action and frozen payload."""

    action_slug = _slug(artefact.identity)
    if artefact.is_file:
        filename = f"{action_slug}.payload"
        executor, kind = "load_file", WRITE_FILE
    else:
        filename = f"{action_slug}.sql"
        executor, kind = "tsql", BUILD_PROCEDURE
    return RenderedAction(
        action=InstallAction(
            id=f"runtime-{action_slug}",
            kind=kind,
            resource_node_id=str(artefact.identity),
            executor=executor,
            payload=filename,
            payload_sha256=sha256_hex(artefact.installed_bytes),
            source_path=artefact.source_path,
        ),
        payloads={filename: artefact.installed_bytes},
    )


def item_runtime_stages(
    artefacts, selected_for_build, *, item: WeaverItemId, target
) -> tuple[PlannedStage, ...]:
    """Plan one item's selected runtime artefacts."""

    selected = [
        artefact
        for artefact in artefacts
        if artefact.identity.item == item and artefact.identity in selected_for_build
    ]
    if not selected:
        return ()
    payloads: dict[str, bytes] = {}
    actions = []
    changes = []
    for artefact in sorted(selected, key=lambda value: str(value.identity)):
        rendered = render_runtime_build_action(artefact)
        payloads.update(rendered.payloads)
        actions.append(rendered.action)
        changes.append(
            added(
                FILE_KIND if artefact.is_file else PROCEDURE_KIND,
                artefact.target_path
                if artefact.is_file
                else artefact.identity.object_id.qualified,
                rendered.action.id,
            )
        )
    return (
        PlannedStage(
            phase=RUNTIME,
            slug="runtime",
            description="install runtime artefacts",
            payloads=payloads,
            changes={target.id: tuple(changes)},
            batches=(
                BuildBatch(
                    id=f"{_slug(item)}", target_id=target.id, actions=tuple(actions)
                ),
            ),
        ),
    )


def item_runtime_removals(
    removed, *, item: WeaverItemId, target, registered
) -> tuple[PlannedStage, ...]:
    """Plan removals for runtime artefacts no longer claimed by the source."""

    selected = sorted(
        (
            identity
            for identity in removed
            if identity.item == item
            and registered[identity].object_type in (FILE_TYPE, PROCEDURE_TYPE)
        ),
        key=str,
    )
    payloads: dict[str, bytes] = {}
    actions = []
    changes = []
    for identity in selected:
        object_type = registered[identity].object_type
        action_slug = _slug(identity)
        if object_type == FILE_TYPE:
            actions.append(
                InstallAction(
                    id=f"runtime-remove-{action_slug}",
                    kind=DELETE_FILE,
                    resource_node_id=str(identity),
                    executor="load_file",
                    payload=None,
                    payload_sha256=None,
                )
            )
            changes.append(
                change_removed(
                    FILE_KIND,
                    f"{identity.object_id.schema}/{identity.object_id.object}",
                    f"runtime-remove-{action_slug}",
                )
            )
            continue
        content = render_sql_statement(
            "tsql",
            "drop_procedure",
            procedure=(
                f"{tsql_ident(identity.object_id.schema)}."
                f"{tsql_ident(identity.object_id.object)}"
            ),
        ).encode("utf-8")
        filename = f"drop-{action_slug}.sql"
        payloads[filename] = content
        actions.append(
            InstallAction(
                id=f"runtime-remove-{action_slug}",
                kind=DROP_PROCEDURE,
                resource_node_id=str(identity),
                executor="tsql",
                payload=filename,
                payload_sha256=sha256_hex(content),
            )
        )
        changes.append(
            change_removed(
                PROCEDURE_KIND,
                identity.object_id.qualified,
                f"runtime-remove-{action_slug}",
            )
        )
    if not actions:
        return ()
    return (
        PlannedStage(
            phase=RUNTIME,
            slug="runtime",
            description="install runtime artefacts",
            payloads=payloads,
            changes={target.id: tuple(changes)},
            batches=(
                BuildBatch(
                    id=f"remove-{_slug(item)}",
                    target_id=target.id,
                    actions=tuple(actions),
                ),
            ),
        ),
    )
