"""Plan preservation of protected contents borrowed through a mirror."""

from __future__ import annotations

import json

from ..declaration.metadata import FOLDER, TABLE
from ..declaration.model import LAKEHOUSE, WAREHOUSE
from .changes import added
from .dependencies import dropped_key, object_key
from .documents import document_requirements
from .models import BuildBatch, InstallAction
from .payloads import sha256_hex
from .stages import BUILD, PlannedStage

MATERIALISE_MIRROR = "materialise_mirror"


def protected_mirrors(repository, selected, mirrored):
    return {
        identity: mirrored[identity]
        for identity in selected
        if identity in mirrored
        and identity.item.item_type in (LAKEHOUSE, WAREHOUSE)
        and identity in repository.source_documents
        and repository.source_documents[identity].kind in (FOLDER, TABLE)
        and repository.source_documents[identity].document.prohibit_rebuild
    }


def materialisation_stage(repository, protected, *, item, target):
    actions, payloads, changes, provides, requires = [], {}, [], {}, {}
    for identity, mirror in sorted(protected.items(), key=lambda pair: str(pair[0])):
        if identity.item != item:
            continue
        kind = "folder" if identity.is_files else "table"
        slug = str(identity).replace("/", "--")
        filename = slug + ".materialise-mirror.json"
        content = json.dumps(
            {
                "identity": str(identity),
                "physical_type": kind,
                "area": "Files" if identity.is_files else "Tables",
                "schema": identity.object_id.schema,
                "name": identity.object_id.object,
                "source_workspace": mirror.source_workspace,
                "source_target": mirror.source_target,
                "source_schema": mirror.source_schema,
                "source_object": mirror.source_object,
            },
            sort_keys=True,
        ).encode()
        action = InstallAction(
            id="materialise-" + slug,
            kind=MATERIALISE_MIRROR,
            resource_node_id=str(identity),
            executor=MATERIALISE_MIRROR,
            payload=filename,
            payload_sha256=sha256_hex(content),
        )
        actions.append(action)
        payloads[filename] = content
        changes.append(added(kind, identity.object_id.qualified, action.id))
        provides[action.id] = (object_key(identity), dropped_key(identity))
        requires[action.id] = tuple(
            key
            for key in document_requirements(repository, identity, target_id=target.id)
            if key != dropped_key(identity)
        )
    if not actions:
        return None
    return PlannedStage(
        phase=BUILD,
        slug="materialise-protected-mirrors",
        description=f"Materialising {item.item_type}/{target.item_name} from {item.item_type}/{mirror.source_target}",
        batches=(
            BuildBatch(
                id="materialise-" + str(item).replace("/", "--"),
                target_id=target.id,
                actions=tuple(actions),
            ),
        ),
        payloads=payloads,
        changes={target.id: tuple(changes)},
        provides=provides,
        requires=requires,
    )


def materialisation_stages(repository, protected, *, item, target):
    groups = {}
    for identity, mirror in protected.items():
        if identity.item == item:
            groups.setdefault((mirror.source_workspace, mirror.source_target), {})[
                identity
            ] = mirror
    return tuple(
        materialisation_stage(repository, group, item=item, target=target)
        for _key, group in sorted(groups.items(), key=lambda pair: str(pair[0]))
    )
