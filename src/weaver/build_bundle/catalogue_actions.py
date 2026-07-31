"""Collect catalogue claims and render the three ordered catalogue barriers."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Iterable, Mapping

from ..catalogue.claims import CatalogueClaim, claim_rules_for_object_type
from ..catalogue.projection import project_item_installation
from ..catalogue.reconcile import reconcile
from ..catalogue.render import InstallationScope, identifier, literal
from ..catalogue.state import ReconciledCatalogue
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
    catalogue: ReconciledCatalogue,
    identities: Iterable[WeaverDocumentId],
) -> tuple[CatalogueClaim, ...]:
    """Collect existing claims using installed types and explicit ownership."""

    claims = list(catalogue.stale_claims)
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
    catalogue: ReconciledCatalogue,
    identities: Iterable[WeaverDocumentId],
    *,
    control_target,
) -> PlannedStage | None:
    claims = collect_claims(catalogue, identities)
    return _stage(
        index=0,
        slug="catalogue-before-build",
        description="reconcile and remove catalogue claims before physical work",
        kind=DELETE_CATALOGUE_CLAIMS,
        statements=_claim_statements(claims),
        control_target=control_target,
    )


def render_catalogue_after_build(
    repository,
    selected_ids: Iterable[WeaverDocumentId],
    target_by_item: Mapping,
    *,
    control_target,
) -> tuple[PlannedStage, ...]:
    """Publish dictionaries and Installation in one batch, Registry last.

    A final refresh of the Weaver Lakehouse's own SQL analytics endpoint closes
    the build: the catalogue is a set of Delta tables like any other, and the next
    reader of it — a report, a GUI, the next build — reaches it through that
    endpoint.
    """

    from .. import __version__

    selected_ids = set(selected_ids)
    catalogue_statements: list[str] = []
    registry_statements: list[str] = []
    for item in sorted(target_by_item, key=str):
        projection = project_item_installation(
            repository,
            item=item,
            retained=(identity for identity in selected_ids if identity.item == item),
            target_name=target_by_item[item].name,
            weaver_version=__version__,
            target_kind=target_by_item[item].kind,
        )
        result = reconcile(projection)
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
