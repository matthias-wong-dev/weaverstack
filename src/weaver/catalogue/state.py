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


@dataclass(frozen=True, init=False)
class Catalogue:
    """The catalogue the build reads and reasons about.

    One class, whatever produced it. In production it is read from the Weaver
    Lakehouse over Spark; a test may build one directly from Registry rows, or
    from a repository — the state a successful build of that repository would
    have left. That is not a fake: it is the same class the build consumes, begun
    further along, exactly as installing a frozen bundle begins further along
    than building one from a repository.

    Everything the build's own logic needs is here and nothing else. Incremental
    selection, alias staleness and claim collection all work from ``registered``
    and ``rows``, so they are pure Python and can be proven without standing up a
    Lakehouse to seed a signature.

    ``rows`` is the catalogue's own row data, by item and table. ``registered`` is
    the certified documents derived from the Registry rows — identity, type,
    signature and publication epoch, and no audit columns, because none of the
    build's decisions depend on who wrote a row or when it was touched.

    ``present_tables`` records which catalogue tables physically exist. One line
    of the reconciler needs it and the rule it encodes is not optional: a claim
    can only be raised against a table that is actually there, or reconciliation
    would emit deletes against tables that are not.
    """

    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
    registered: Mapping[WeaverDocumentId, "RegisteredDocument"]
    present_tables: frozenset[str]

    def __init__(
        self,
        rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]],
        *,
        registered: Mapping[WeaverDocumentId, "RegisteredDocument"] | None = None,
        present_tables: frozenset[str] | None = None,
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
        # Defaulting to "every table this catalogue carries rows for" keeps a
        # hand-built catalogue honest without making every caller state it: a
        # claim is raisable against exactly the tables that are represented.
        object.__setattr__(
            self,
            "present_tables",
            frozenset(
                present_tables
                if present_tables is not None
                else {
                    table
                    for tables in frozen_rows.values()
                    for table in tables
                }
            ),
        )


def catalogue_from_repository(
    repository,
    *,
    retained: Mapping[WeaverItemId, Any],
    target_kinds: Mapping[WeaverItemId, str] | None = None,
) -> Catalogue:
    """The catalogue a repository *describes* — the desired state, from source.

    The logical twin of :func:`read_catalogue_state`. One reads what is
    persisted; this derives what the declarations say should be. Both produce the
    same class, so the two are directly comparable, which is what a diff needs.

    It exists in production rather than in a fixture for a reason worth stating:
    a projection the build itself uses cannot drift. A test fixture listing the
    rows a repository ought to produce has to be updated by hand every time an
    artefact is added, and will be wrong the first time someone forgets.

    Nothing about a *binding* is here. No target name, no Weaver version, no
    Installation row, no publication epoch: those are facts a build knows and a
    repository does not, and they are supplied when the publication is rendered.
    The single exception is ``target_kinds``, because an alias destination is
    registered as the thing it physically is — a view in a Warehouse, a table in
    a Lakehouse — and the catalogue's readers need the real type.
    """

    from .projection import project_item_installation

    target_kinds = dict(target_kinds or {})
    rows: dict[WeaverItemId, Mapping[str, tuple]] = {}
    for item, identities in retained.items():
        projection = project_item_installation(
            repository,
            item=item,
            retained=identities,
            **(
                {"target_kind": target_kinds[item]} if item in target_kinds else {}
            ),
        )
        rows[item] = MappingProxyType(dict(projection.rows))
    return Catalogue(rows=MappingProxyType(rows))


@dataclass(frozen=True)
class Reconciliation:
    """What reconciling a catalogue against prepared inventories produced.

    Two things, and they are genuinely two: the catalogue with disproved claims
    removed, and the claims that were removed — which the build turns into delete
    DML. Carrying the second on the catalogue itself made it look like catalogue
    state, when it is a *finding about* the catalogue, and left claim collection
    reading one of its two claim sources off an object and computing the other.
    """

    catalogue: Catalogue
    #: Claims disproved by the inventory, to be deleted before physical work.
    stale_claims: tuple[CatalogueClaim, ...]
    #: The disproved objects, as readable labels. Nothing in the build consumes
    #: this — it exists so a reconciliation decision can be seen and asserted
    #: rather than inferred from the DML it eventually produces.
    stale_objects: tuple[str, ...] = ()


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


def read_catalogue_state(catalogue: Any, items) -> Catalogue:
    """Read the catalogue over Spark — the production way to populate one.

    The one place a catalogue meets a session. Everything downstream of it is
    pure, so this is the boundary whose *fidelity* is worth a Spark test: does a
    real catalogue read back into the same class a fixture builds directly.

    The shape check is deliberately strict and must stay so. A physically
    incomplete catalogue is rejected here rather than tolerated, because tests
    wanting a Registry-only catalogue can construct one directly — weakening this
    to accommodate them would trade a real production guarantee for a fixture's
    convenience.
    """

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
        # Published columns are required too, and deliberately: the merge writes
        # one on every insert, so a catalogue without it can be *read* but not
        # *written*. Exempting it here would let planning succeed and push the
        # failure into the install, where it surfaces as an engine complaint
        # about an unknown column rather than as a statement about the
        # catalogue's shape. The reader's null tolerance answers a different
        # question — a column that exists but predates some rows — and both hold
        # at once: require the column, tolerate the value.
        required = {
            name.casefold()
            for name in table.column_names + table.published_column_names
        }
        absent_columns = sorted(required - folded)
        if absent_columns:
            incompatible.append(f"{table.name}.{absent_columns[0]}")
    if incompatible:
        raise BuildError(
            "catalogue schema is incompatible; missing required column(s): "
            + ", ".join(incompatible)
        )
    rows = {
        item: MappingProxyType(
            read_installation(
                catalogue,
                scope=InstallationScope(item.item_type, item.item_name),
            )
        )
        for item in items
    }
    return Catalogue(
        rows=MappingProxyType(rows),
        present_tables=frozenset(present),
    )


def reconcile_catalogue_state(
    state: Catalogue, *, inventories: Mapping[WeaverItemId, Any]
) -> Reconciliation:
    """Discard catalogue claims the prepared inventories physically disprove.

    Pure: a catalogue in, an inventory in, a catalogue and its stale claims out.
    Both inputs can be built directly, so what a registered object does when the
    thing it claims is *not there* needs no Lakehouse to demonstrate.
    """

    registered = state.registered
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
    return Reconciliation(
        catalogue=Catalogue(
            rows=MappingProxyType(reconciled),
            registered=retained,
            present_tables=state.present_tables,
        ),
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
