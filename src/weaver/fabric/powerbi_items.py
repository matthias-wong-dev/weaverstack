"""Create the SemanticModel and Report items a Build deploys to.

Build creates missing Power BI items and no other kind of Fabric item. Fabric
creates each with a definition, which the Build then replaces. A model starts
as the scaffold model in its project's culture, which Fabric fixes at creation.
A linked Report starts bound to its model, so models are created first.
"""

from __future__ import annotations

from ..declaration.model import REPORT, SEMANTIC_MODEL
from ..errors import BuildError, WeaverError
from ..targets import SEMANTIC_MODEL_TARGET
from .resources import create_report, create_semantic_model, list_items

_ITEM_TYPES = {SEMANTIC_MODEL_TARGET: SEMANTIC_MODEL, "report": REPORT}


def create_powerbi_items(
    bindings,
    repository,
    *,
    session,
    workspace,
    physical=None,
    inventory=None,
    bundle_only: bool = False,
) -> tuple[str, ...]:
    """Create each selected SemanticModel and Report target the workspace lacks.

    ``physical`` and ``inventory`` are the workspace and its items when the
    caller has already listed them. Returns ``Type/Name`` of each created item.
    """

    selected = [b for b in bindings.entries if b.target.kind in _ITEM_TYPES]
    if not selected:
        return ()
    resolver = session.resolver(workspace)
    if physical is None or inventory is None:
        physical = resolver.workspace
        inventory = list_items(physical, client=resolver.client)
    held = {(item.type, item.name): item for item in inventory}
    missing = sorted(
        (b for b in selected if (_type(b), b.target.item.name) not in held),
        key=lambda b: (_type(b) == REPORT, str(b.item)),
    )
    if not missing:
        return ()
    if bundle_only:
        named = ", ".join(f"{_type(b)} {b.target.item.name!r}" for b in missing)
        raise BuildError(
            f"{named} {'does' if len(missing) == 1 else 'do'} not exist in "
            f"{physical.name!r} yet. weaver build creates "
            f"{'it' if len(missing) == 1 else 'them'}; a bundle names existing items."
        )
    _refuse_reports_without_their_model(missing, bindings, repository)

    client = resolver.client
    created = []
    with session.step("Create Power BI items", physical.name):
        for binding in missing:
            kind, name = _type(binding), binding.target.item.name
            with session.substep(f"Create {kind}/{name}"):
                try:
                    if kind == SEMANTIC_MODEL:
                        item = create_semantic_model(
                            physical,
                            name,
                            definition=_seed_model(
                                binding.item, repository.semantic_models[binding.item]
                            ),
                            client=client,
                        )
                    else:
                        item = create_report(
                            physical,
                            name,
                            definition=_report_definition(
                                repository.reports[binding.item], bindings, held
                            ),
                            client=client,
                        )
                except BuildError:
                    raise
                except WeaverError as exc:
                    raise BuildError(
                        f"{kind} {name!r} could not be created in "
                        f"{physical.name!r}: {exc}"
                    ) from exc
            held[(kind, name)] = item
            created.append(f"{kind}/{name}")
    return tuple(created)


def _type(binding) -> str:
    return _ITEM_TYPES[binding.target.kind]


def _refuse_reports_without_their_model(missing, bindings, repository) -> None:
    for binding in missing:
        if _type(binding) != REPORT:
            continue
        model = repository.reports[binding.item].model
        if model is not None and model not in bindings.by_item:
            raise BuildError(
                f"Report {binding.target.item.name!r} does not exist yet. Select "
                f"{model} in the same Build to create it."
            )


def _seed_model(item, contribution) -> dict:
    from ..onboarding.project import semantic_extension
    from ..semantic_models.definition import encode_parts
    from ..semantic_models.extensions import apply_extensions
    from ..semantic_models.objects import TmdlDefinition
    from ..semantic_models.source import SemanticContribution

    seed = apply_extensions(
        SemanticContribution({}, {}, {}),
        item.item_name,
        (
            (
                semantic_extension().encode("utf-8"),
                f"PowerBI/{item.item_name}/{item.item_name}.tmdl",
            ),
        ),
    )
    parts = dict(seed.parts)
    culture = TmdlDefinition(contribution.parts).model.culture
    if isinstance(culture, str) and culture:
        edited = TmdlDefinition(parts)
        edited.model.culture = culture
        parts = edited.parts
    return encode_parts(parts)


def _report_definition(contribution, bindings, held) -> dict:
    from dataclasses import replace

    from ..report_definition import encode_report

    if contribution.model is not None:
        target = bindings.by_item[contribution.model].target.item.name
        model = held[(SEMANTIC_MODEL, target)]
        contribution = replace(
            contribution,
            binding={"workspace_id": model.workspace_id, "item_id": model.id},
        )
    return encode_report(contribution)
