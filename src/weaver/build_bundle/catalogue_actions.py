"""Collect catalogue claims and render the three ordered catalogue barriers."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Iterable, Mapping

from ..catalogue.claims import CatalogueClaim, claim_rules_for_object_type
from ..catalogue.reconcile import publish
from ..catalogue.render import InstallationScope
from ..catalogue.state import Catalogue, for_targets, retaining
from ..catalogue.tables import (
    DICTIONARY_TABLES,
    INSTALLATION,
    REGISTRY,
    CatalogueTable,
)
from ..catalogue.tsql import identifier, literal, qualified_name
from ..declaration.model import WeaverDocumentId
from .models import (
    DELETE_CATALOGUE_CLAIMS,
    PUBLISH_CATALOGUE,
    PUBLISH_REGISTRY,
    BuildBatch,
    InstallAction,
)
from .payloads import sha256_hex
from .stages import CATALOGUE, PlannedStage


def collect_claims(
    catalogue: Catalogue,
    identities: Iterable[WeaverDocumentId],
    *,
    stale_claims: Iterable[CatalogueClaim] = (),
) -> tuple[CatalogueClaim, ...]:
    """Every catalogue claim this build must delete before it does physical work.

    Two sources, and they are symmetric: claims reconciliation already disproved
    against the inventory, and claims held by the objects this build is about to
    drop or remove. Both are passed in rather than one being read off the
    catalogue, because a catalogue describes what is claimed — not which of those
    claims some earlier step decided were wrong.
    """

    claims = list(stale_claims)
    for identity in sorted(set(identities), key=str):
        document = catalogue.registered.get(identity)
        if document is None:
            continue
        tables = catalogue.rows.get(identity.item, {})
        for rule in claim_rules_for_object_type(document.object_type):
            if any(rule.owns(row, identity) for row in tables.get(rule.table.name, ())):
                claims.append(CatalogueClaim(identity, rule))
    return tuple(dict.fromkeys(claims))


def _claim_statements(claims: Iterable[CatalogueClaim]) -> tuple[str, ...]:
    grouped: dict[tuple[CatalogueTable, object], set[WeaverDocumentId]] = defaultdict(
        set
    )
    for claim in claims:
        grouped[(claim.rule.table, claim.identity.item)].add(claim.identity)

    table_order = (REGISTRY, *reversed(DICTIONARY_TABLES))
    statements = []
    for table in table_order:
        groups = sorted(
            (
                (item, identities)
                for (claim_table, item), identities in grouped.items()
                if claim_table == table
            ),
            key=lambda pair: str(pair[0]),
        )
        for item, identities in groups:
            scope = InstallationScope(item.item_type, item.item_name)
            predicates = []
            for identity in sorted(identities, key=str):
                rule = next(
                    claim.rule
                    for claim in claims
                    if claim.identity == identity and claim.rule.table == table
                )
                values = rule.values(identity)
                predicates.append(
                    "("
                    + " AND ".join(
                        f"{identifier(table.public_name_of(column))} = {literal(value)}"
                        for column, value in zip(
                            rule.predicate_columns, values, strict=True
                        )
                    )
                    + ")"
                )
            statements.append(
                f"DELETE FROM {qualified_name(table)}\n"
                f"WHERE {scope.predicate}\n  AND (" + "\n    OR ".join(predicates) + ")"
            )
    return tuple(statements)


def _batch_payload(statements: Iterable[str]) -> bytes:
    return (json.dumps(list(statements), indent=2, ensure_ascii=False) + "\n").encode(
        "utf-8"
    )


def _stage(
    *,
    index: int,
    slug: str,
    description: str,
    kind: str,
    statements: Iterable[str],
    catalogue_target,
) -> PlannedStage | None:
    statements = tuple(statements)
    if not statements:
        return None
    content = _batch_payload(statements)
    filename = f"{slug}.tsql-batch.json"
    action = InstallAction(
        id=slug,
        kind=kind,
        resource_node_id=None,
        executor="tsql_batch",
        payload=filename,
        payload_sha256=sha256_hex(content),
    )
    return PlannedStage(
        phase=CATALOGUE,
        index=index,
        slug=slug,
        description=description,
        payloads={filename: content},
        batches=(
            BuildBatch(id=slug, target_id=catalogue_target.id, actions=(action,)),
        ),
    )


def render_catalogue_before_build(
    catalogue: Catalogue,
    identities: Iterable[WeaverDocumentId],
    *,
    catalogue_target,
    stale_claims: Iterable[CatalogueClaim] = (),
) -> PlannedStage | None:
    claims = collect_claims(catalogue, identities, stale_claims=stale_claims)
    return _stage(
        index=0,
        slug="catalogue-before-build",
        description="reconcile and remove catalogue claims before physical work",
        kind=DELETE_CATALOGUE_CLAIMS,
        statements=_claim_statements(claims),
        catalogue_target=catalogue_target,
    )


def _item_signature(repository, item) -> str:
    """The item's own signature, which is what an Installation row records."""

    return next(model.signature for model in repository.items if model.identity == item)


def _with_installation_rows(desired: Catalogue, installation) -> Catalogue:
    """Put each item's binding facts into the desired catalogue.

    A repository-derived catalogue cannot know them and must not invent them:
    which physical target an item is bound to, and which Weaver published it, are
    facts about *this build*, not about the source. They are folded in here so
    the diff compares complete rows against complete rows.
    """

    from types import MappingProxyType

    rows = {}
    for item, tables in desired.rows.items():
        merged = dict(tables)
        binding = installation.get(item)
        if binding is not None:
            merged[INSTALLATION.name] = tuple(binding)
        rows[item] = MappingProxyType(merged)
    return Catalogue(rows=MappingProxyType(rows))


def desired_catalogue(
    repository,
    selected_ids: Iterable[WeaverDocumentId],
    target_by_item: Mapping,
) -> Catalogue:
    """The catalogue state a successful build of ``selected_ids`` would leave.

    Logical, then narrowed, then bound — in that order and visibly so. The
    narrowing is what keeps a Registry row meaning "this succeeded"; the binding
    is what lets a shortcut be certified as the thing it physically is, and what
    supplies the Installation facts a repository cannot know.

    Named and separate because it is *both* halves of the fixed point. It is what
    publication compares the persisted catalogue against, and it is therefore
    exactly what the catalogue should already contain when nothing has changed —
    so a test can feed it back as the current state and hold the build to
    producing nothing, without restating any of this arithmetic itself.
    """

    from .. import __version__

    selected_ids = set(selected_ids)
    logical = Catalogue.from_repository(repository)
    certified = retaining(logical, repository, selected_ids)
    bound = for_targets(
        certified,
        repository,
        selected_ids,
        {item: target.kind for item, target in target_by_item.items()},
    )
    binding_rows = {
        item: (
            {
                "item_type": item.item_type,
                "item_name": item.item_name,
                "target_name": target_by_item[item].name,
                "weaver_version": __version__,
                "signature": _item_signature(repository, item),
            },
        )
        for item in target_by_item
    }
    return _with_installation_rows(bound, binding_rows)


def render_catalogue_after_build(
    repository,
    selected_ids: Iterable[WeaverDocumentId],
    target_by_item: Mapping,
    *,
    catalogue_target,
    current: Catalogue | None = None,
) -> tuple[PlannedStage, ...]:
    """Publish dictionaries and Installation in one batch, Registry last.

    Nothing closes the build. Catalogue rows are written over TDS into the
    Warehouse that holds them, so they are readable as soon as they are
    committed and there is no endpoint standing between the write and the next
    reader.
    """

    desired = desired_catalogue(repository, selected_ids, target_by_item)

    # The publication is a genuine diff against what is persisted: a table whose
    # rows are all unchanged produces no statement, so an identical second build
    # appends nothing here.
    publication = publish(current or Catalogue(rows={}), desired)

    # Registry last, in its own barrier — taken from the structure rather than
    # recovered from the SQL, so the ordering invariant is carried by the type
    # instead of by a string match.
    catalogue_statements: list[str] = [
        statement
        for table_plan in (*publication.dictionaries, publication.installation)
        for statement in table_plan.statements
    ]
    registry_statements: list[str] = list(publication.registry.statements)

    rendered = (
        _stage(
            index=1,
            slug="publish-catalogue",
            description="publish catalogue dictionaries and installations",
            kind=PUBLISH_CATALOGUE,
            statements=catalogue_statements,
            catalogue_target=catalogue_target,
        ),
        _stage(
            index=2,
            slug="publish-registry",
            description="publish item registry last",
            kind=PUBLISH_REGISTRY,
            statements=registry_statements,
            catalogue_target=catalogue_target,
        ),
    )
    return tuple(stage for stage in rendered if stage is not None)
