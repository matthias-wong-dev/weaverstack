"""Read and reconcile catalogue claims before bundle generation."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from ..declaration.model import WeaverDocumentId, WeaverItemId
from ..errors import BuildError
from ..spark.tokens import object_token
from .claims import CatalogueClaim, claim_rules_for_object_type
from .reader import _is_absent, read_installation
from .render import InstallationScope
from .tables import BUILD_EPOCH, CATALOGUE_TABLES, OBJECT_TYPES, REGISTRY


@dataclass(frozen=True)
class CatalogueState:
    status: str
    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
    present_tables: frozenset[str]
    missing_tables: frozenset[str]


@dataclass(frozen=True, init=False)
class ReconciledCatalogue:
    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
    registered: Mapping[WeaverDocumentId, "RegisteredDocument"]
    stale_claims: tuple[CatalogueClaim, ...]
    stale_objects: tuple[str, ...]

    def __init__(
        self,
        rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]],
        *,
        registered: Mapping[WeaverDocumentId, "RegisteredDocument"] | None = None,
        stale_claims: tuple[CatalogueClaim, ...] = (),
        stale_objects: tuple[str, ...] = (),
    ) -> None:
        frozen_rows = MappingProxyType(dict(rows))
        object.__setattr__(self, "rows", frozen_rows)
        object.__setattr__(
            self,
            "registered",
            _registered_documents(frozen_rows)
            if registered is None
            else MappingProxyType(dict(registered)),
        )
        object.__setattr__(self, "stale_claims", tuple(stale_claims))
        object.__setattr__(self, "stale_objects", tuple(stale_objects))


@dataclass(frozen=True)
class RegisteredDocument:
    """One validated Registry row, parsed once at the catalogue boundary."""

    identity: WeaverDocumentId
    object_type: str
    signature: str
    #: When the build that last certified this object published it. ``None`` for
    #: a row written before epochs existed, which orders as older than any epoch.
    build_epoch: object = None


def _registered_documents(
    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
) -> Mapping[WeaverDocumentId, RegisteredDocument]:
    registered: dict[WeaverDocumentId, RegisteredDocument] = {}
    for item, tables in rows.items():
        for row in tables.get(REGISTRY.name, ()):
            identity = _row_identity(item, row)
            object_type = str(row.get("object_type") or "")
            if object_type not in OBJECT_TYPES:
                expected = ", ".join(OBJECT_TYPES)
                raise BuildError(
                    f"Registry row for {identity} has unsupported object_type "
                    f"{object_type!r}; expected one of {expected}"
                )
            signature = str(row.get("signature") or "")
            if not signature:
                raise BuildError(f"Registry row for {identity} has no signature")
            document = RegisteredDocument(
                identity, object_type, signature, row.get(BUILD_EPOCH)
            )
            prior = registered.get(identity)
            if prior is not None and prior != document:
                raise BuildError(f"Registry contains conflicting rows for {identity}")
            registered[identity] = document
    return MappingProxyType(registered)


def read_catalogue_state(catalogue: Any, items) -> CatalogueState:
    """Read selected scopes and distinguish absent, partial and invalid shape."""

    present: set[str] = set()
    missing: set[str] = set()
    incompatible: list[str] = []
    for table in CATALOGUE_TABLES:
        name = catalogue.expand(object_token("_", table.name))
        try:
            columns = catalogue.spark.table(name).columns
        except Exception as exc:
            if _is_absent(exc):
                missing.add(table.name)
                continue
            raise
        present.add(table.name)
        folded = {column.casefold() for column in columns}
        # Business columns only. A published column is written by the installer
        # and never compared, so a catalogue built before one existed is an
        # *older shape* rather than an incompatible one — the reader already
        # gives this Weaver the column it expects as a typed null, and the next
        # build repairs it. Requiring it here would turn a routine upgrade into
        # a hard failure.
        required = {name.casefold() for name in table.column_names}
        absent_columns = sorted(required - folded)
        if absent_columns:
            incompatible.append(f"{table.name}.{absent_columns[0]}")
    if incompatible:
        raise BuildError(
            "catalogue schema is incompatible; missing required column(s): "
            + ", ".join(incompatible)
        )
    status = "valid"
    if not present:
        status = "absent"
    elif missing:
        status = "partial"
    rows = {
        item: MappingProxyType(
            read_installation(
                catalogue,
                scope=InstallationScope(item.item_type, item.item_name),
            )
        )
        for item in items
    }
    return CatalogueState(
        status=status,
        rows=MappingProxyType(rows),
        present_tables=frozenset(present),
        missing_tables=frozenset(missing),
    )


def reconcile_catalogue_state(
    state: CatalogueState, *, inventories: Mapping[WeaverItemId, Any]
) -> ReconciledCatalogue:
    """Discard catalogue roots disproved by prepared target inventories."""

    registered = _registered_documents(state.rows)
    reconciled = {}
    stale_claims: list[CatalogueClaim] = []
    stale_labels: list[str] = []
    for item, tables in state.rows.items():
        inventory = inventories.get(item)
        stale: dict[WeaverDocumentId, RegisteredDocument] = {}
        if inventory is not None:
            for identity, document in registered.items():
                if identity.item != item:
                    continue
                schema = (
                    f"Files/{identity.object_id.schema}"
                    if identity.is_files
                    else identity.object_id.schema
                )
                if not inventory.has_object(
                    schema, identity.object_id.object, document.object_type
                ):
                    stale[identity] = document
        filtered = {}
        for table in CATALOGUE_TABLES:
            rows = tables.get(table.name, ())
            rules = {
                rule
                for document in stale.values()
                for rule in claim_rules_for_object_type(document.object_type)
                if rule.table == table
            }
            if not rules:
                filtered[table.name] = tuple(rows)
                continue
            filtered[table.name] = tuple(
                row
                for row in rows
                if not any(
                    rule.owns(row, identity)
                    for identity, document in stale.items()
                    for rule in claim_rules_for_object_type(document.object_type)
                    if rule.table == table
                )
            )
            if table.name in state.present_tables:
                stale_claims.extend(
                    CatalogueClaim(identity, rule)
                    for identity, document in stale.items()
                    for rule in claim_rules_for_object_type(document.object_type)
                    if rule.table == table
                )
        reconciled[item] = MappingProxyType(filtered)
        stale_labels.extend(str(identity) for identity in stale)
    retained = {
        identity: document
        for identity, document in registered.items()
        if identity not in {claim.identity for claim in stale_claims}
    }
    return ReconciledCatalogue(
        rows=MappingProxyType(reconciled),
        registered=retained,
        stale_claims=tuple(dict.fromkeys(stale_claims)),
        stale_objects=tuple(sorted(stale_labels)),
    )


def _row_identity(item: WeaverItemId, row: Mapping[str, object]) -> WeaverDocumentId:
    schema = str(row.get("schema_name") or "")
    is_files = schema.startswith("Files/")
    logical_schema = schema[len("Files/") :] if is_files else schema
    prefix = "Files/" if is_files else ""
    return WeaverDocumentId.parse(
        f"{item}/{prefix}{logical_schema}.{row.get('object_name')}"
    )
