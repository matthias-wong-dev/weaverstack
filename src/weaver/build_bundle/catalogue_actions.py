"""Collect catalogue claims and render the three ordered catalogue barriers."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Iterable, Mapping

from ..catalogue.claims import CatalogueClaim, claim_rules_for_object_type
from ..catalogue.state import Catalogue, for_targets, retaining
from ..catalogue.reconcile import reconcile
from ..catalogue.render import InstallationScope, identifier, literal
from ..catalogue.state import Catalogue, for_targets, retaining
from ..catalogue.tables import DICTIONARY_TABLES, REGISTRY, CatalogueTable
from ..declaration.model import WeaverDocumentId
from ..spark.tokens import object_token
from .models import (
    DELETE_CATALOGUE_CLAIMS,
    PUBLISH_CATALOGUE,
    PUBLISH_REGISTRY,
    REFRESH_SQL_ENDPOINT,
    BuildAction,
    BuildBatch,
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
    grouped: dict[tuple[CatalogueTable, object], set[WeaverDocumentId]] = defaultdict(set)
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
                        f"{identifier(column)} = {literal(value)}"
                        for column, value in zip(
                            rule.predicate_columns, values, strict=True
                        )
                    )
                    + ")"
                )
            statements.append(
                f"DELETE FROM {object_token('_', table.name)}\n"
                f"WHERE {scope.predicate}\n  AND ("
                + "\n    OR ".join(predicates)
                + ")"
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
    control_target,
) -> PlannedStage | None:
    statements = tuple(statements)
    if not statements:
        return None
    content = _batch_payload(statements)
    filename = f"{slug}.spark-sql-batch.json"
    action = BuildAction(
        id=slug,
        kind=kind,
        resource_node_id=None,
        executor="spark_sql_batch",
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
            BuildBatch(id=slug, target_id=control_target.id, actions=(action,)),
        ),
    )


def render_catalogue_before_build(
    catalogue: Catalogue,
    identities: Iterable[WeaverDocumentId],
    *,
    control_target,
    stale_claims: Iterable[CatalogueClaim] = (),
) -> PlannedStage | None:
    claims = collect_claims(catalogue, identities, stale_claims=stale_claims)
    return _stage(
        index=0,
        slug="catalogue-before-build",
        description="reconcile and remove catalogue claims before physical work",
        kind=DELETE_CATALOGUE_CLAIMS,
        statements=_claim_statements(claims),
        control_target=control_target,
    )


def _item_signature(repository, item) -> str:
    """The item's own signature, which is what an Installation row records."""

    return next(
        model.signature for model in repository.items if model.identity == item
    )


def render_catalogue_after_build(
    repository,
    selected_ids: Iterable[WeaverDocumentId],
    target_by_item: Mapping,
    *,
    control_target,
    current: Catalogue | None = None,
) -> tuple[PlannedStage, ...]:
    """Publish dictionaries and Installation in one batch, Registry last.

    A final refresh of the Weaver Lakehouse's own SQL analytics endpoint closes
    the build: the catalogue is a set of Delta tables like any other, and the next
    reader of it — a report, a GUI, the next build — reaches it through that
    endpoint.
    """

    from .. import __version__

    selected_ids = set(selected_ids)

    # The publication is a diff: what the repository describes, against what is
    # persisted. Only the desired side drives the statements — see
    # `CatalogueChanges` — but routing production through the same call the
    # reporting uses is what stops the two drifting apart.
    # Logical, then narrowed, then bound — in that order and visibly so. The
    # narrowing is what keeps a Registry row meaning "this succeeded"; the
    # binding is what lets an alias be certified as the thing it physically is.
    logical = Catalogue.from_repository(repository)
    certified = retaining(logical, repository, selected_ids)
    desired = for_targets(
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
    by_item = (current or Catalogue(rows={})).diff(desired).render_dml(
        installation=binding_rows
    )

    catalogue_statements: list[str] = []
    registry_statements: list[str] = []
    for item in sorted(target_by_item, key=str):
        result = by_item[item]
        # Registry last, in its own barrier — taken from the structure rather
        # than recovered from the SQL, so the ordering invariant is carried by
        # the type instead of by a string match.
        for table_plan in (*result.dictionaries, result.installation):
            catalogue_statements.extend(table_plan.statements)
        registry_statements.extend(result.registry.statements)

    rendered = (
        _stage(
            index=1,
            slug="publish-catalogue",
            description="publish catalogue dictionaries and installations",
            kind=PUBLISH_CATALOGUE,
            statements=catalogue_statements,
            control_target=control_target,
        ),
        _stage(
            index=2,
            slug="publish-registry",
            description="publish item registry last",
            kind=PUBLISH_REGISTRY,
            statements=registry_statements,
            control_target=control_target,
        ),
    )
    published = tuple(stage for stage in rendered if stage is not None)
    if not published:
        return ()
    return published + (_control_refresh_stage(control_target),)


def _control_refresh_stage(control_target) -> PlannedStage:
    return PlannedStage(
        phase=CATALOGUE,
        index=3,
        slug="refresh-control-endpoint",
        description="refresh the Weaver Lakehouse SQL endpoint after catalogue DML",
        batches=(
            BuildBatch(
                id="refresh-control-endpoint",
                target_id=control_target.id,
                actions=(
                    BuildAction(
                        id="refresh-sql-endpoint-control",
                        kind=REFRESH_SQL_ENDPOINT,
                        resource_node_id=None,
                        executor="sql_endpoint_refresh",
                        payload=None,
                        payload_sha256=None,
                    ),
                ),
            ),
        ),
    )
