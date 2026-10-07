from dataclasses import replace

from ..catalogue.semantic import json_text
from ..catalogue.tables import INSTALLATION
from ..declaration.model import WeaverDocumentId
from ..errors import BuildError
from ..report_definition import encode_report
from .dependencies import object_key
from .models import BuildBatch, InstallAction
from .payloads import sha256_hex
from .stages import BUILD, PlannedStage


def verified_model_key(item):
    return f"verified-model:{item}"


def bind_reports(repository, targets, catalogue):
    reports = dict(repository.reports)
    for item, contribution in reports.items():
        if item not in targets or contribution.model is None:
            continue
        model = targets.get(contribution.model)
        if model is not None:
            binding = {"workspace_id": model.workspace_id, "item_id": model.item_id}
        else:
            if (
                WeaverDocumentId.model_root(contribution.model)
                not in catalogue.registered
            ):
                raise BuildError(
                    f"{item}: {contribution.model} is not certified; build the model first"
                )
            rows = catalogue.rows.get(contribution.model, {}).get(INSTALLATION.name, ())
            if len(rows) != 1:
                raise BuildError(
                    f"{item}: build {contribution.model} first or select it with the Report"
                )
            binding = {
                field: rows[0].get(field) for field in ("workspace_id", "item_id")
            }
        bound = replace(contribution, binding=binding)
        encode_report(bound)
        reports[item] = bound
    return replace(repository, reports=reports)


def report_stages(repository, item, target):
    contribution = repository.reports[item]
    stages = []
    previous = verified_model_key(contribution.model) if contribution.model else None
    for executor in ("report_definition", "report_readback"):
        filename = f"{target.id}.{executor}.json"
        content = (
            json_text(
                {
                    "definition": encode_report(contribution),
                    "target_id": target.id,
                    "item": str(item),
                }
            )
            + "\n"
        ).encode()
        action = InstallAction(
            id=f"{executor}-{target.id}",
            kind=executor,
            executor=executor,
            resource_node_id=str(WeaverDocumentId.report_root(item)),
            payload=filename,
            payload_sha256=sha256_hex(content),
        )
        stages.append(
            PlannedStage(
                phase=BUILD,
                slug="build-objects",
                description="Build item documents",
                payloads={filename: content},
                provides={action.id: (object_key(WeaverDocumentId.report_root(item)),)}
                if executor == "report_readback"
                else {action.id: (f"report-submitted:{item}",)},
                requires={action.id: (previous,) if previous else ()},
                batches=(
                    BuildBatch(id=action.id, target_id=target.id, actions=(action,)),
                ),
            )
        )
        previous = f"report-submitted:{item}"
    return tuple(stages)
