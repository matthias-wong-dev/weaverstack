"""Read and reconcile catalogue claims before bundle generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping

from ..declaration.metadata import ObjectId
from ..declaration.model import (
    FILE_SHAPE,
    PROCEDURE_SHAPE,
    WeaverDocumentId,
    WeaverItemId,
)
from ..errors import BuildError
from ..spark.tokens import object_token
from .claims import CatalogueClaim, catalogue_schema, claim_rules_for_object_type
from .reader import _is_absent, read_installation
from .render import InstallationScope
from .tables import BUILD_EPOCH, CATALOGUE_TABLES, INSTALLATION, OBJECT_TYPES, REGISTRY

#: What a Folder's stored schema carries, so a table and a folder of the same
#: name stay apart. Only the object shape uses it — see :func:`catalogue_schema`.
_FILES_PREFIX = "Files/"


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
        # hand-built catalogue honest without making every caller state it.
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

    def to_mapping(self) -> dict[str, object]:
        """A versioned JSON-safe representation for a remote state boundary."""

        return {
            "format_version": 1,
            "items": [
                {
                    "item": str(item),
                    "tables": {
                        table: [
                            {key: _encode_json_value(value) for key, value in row.items()}
                            for row in rows
                        ]
                        for table, rows in sorted(tables.items())
                    },
                }
                for item, tables in sorted(self.rows.items(), key=lambda pair: str(pair[0]))
            ],
            "present_tables": sorted(self.present_tables),
        }

    @classmethod
    def from_mapping(cls, mapping) -> "Catalogue":
        """Reconstruct catalogue state without querying a target locally."""

        version = mapping.get("format_version")
        if version != 1:
            raise BuildError(
                f"unsupported catalogue format_version {version!r}; expected 1"
            )
        rows = {
            WeaverItemId.parse(entry["item"]): MappingProxyType(
                {
                    table: tuple(
                        {
                            key: _decode_json_value(value)
                            for key, value in row.items()
                        }
                        for row in table_rows
                    )
                    for table, table_rows in entry.get("tables", {}).items()
                }
            )
            for entry in mapping.get("items", ())
        }
        return cls(
            rows=MappingProxyType(rows),
            present_tables=frozenset(mapping.get("present_tables", ())),
        )

    # --- constructors ---------------------------------------------------------
    #
    # Symmetrical on purpose. One reads what is persisted, the other derives what
    # the source says should be; both produce this class, which is what lets the
    # two be compared at all.

    @classmethod
    def from_weaver_lakehouse(cls, catalogue: Any, items) -> "Catalogue":
        """The persisted catalogue, read over Spark from the Weaver Lakehouse."""

        return read_catalogue_state(catalogue, items)

    @classmethod
    def from_repository(cls, repository) -> "Catalogue":
        """Everything the source declares — the whole logical catalogue.

        *All* of it, deliberately. Not the subset some build is ready to certify:
        making selection an input would mean the caller had to know what was
        certifiable before the desired state could be described, and the desired
        state would then be a statement about a build rather than about the
        repository. Selection and materialisation *transform* this later — see
        :meth:`retaining` and :meth:`for_targets`.

        It carries no binding: no target name, no Weaver version, no Installation
        row, no publication epoch, and no Registry certification for an alias
        destination, because an alias is a view in a Warehouse and a table in a
        Lakehouse and this does not know which.
        """

        from ..etl import item_load_artefacts
        from .projection import project_item_catalogue

        rows = {}
        for model in repository.items:
            item = model.identity
            declared = {
                identity
                for identity in repository.source_documents
                if identity.item == item
            }
            # A load artefact is declared by the source exactly as a document is
            # — derived from it, but a target in its own right — so it belongs in
            # what the repository says should exist.
            declared.update(
                artefact.identity
                for artefact in item_load_artefacts(repository, item=item)
            )
            projection = project_item_catalogue(
                repository, item=item, retained=declared
            )
            rows[item] = MappingProxyType(dict(projection.rows))
        return cls(rows=MappingProxyType(rows))

    # --- transformations ------------------------------------------------------

    def diff(self, desired: "Catalogue") -> "CatalogueChanges":
        """How this catalogue would move toward the one ``desired`` describes.

        Read it as: *persisted* `.diff(` *derived from source* `)`. The result
        reports from both sides and renders statements from ``desired`` alone —
        see :class:`CatalogueChanges` for why that asymmetry is deliberate.
        """

        return CatalogueChanges(current=self, desired=desired)


@dataclass(frozen=True)
class CatalogueChanges:
    """How a persisted catalogue would move toward the one a repository describes.

    Carries both sides, and uses them for different things — which is the whole
    of the design and the part worth not getting wrong.

    ``current`` informs *reporting*: how many rows are new, changed, unchanged
    and removed, so a reviewer can see what a bundle will do before it runs.

    ``desired`` alone drives the *statements*. The delete keeps exactly the keys
    the desired catalogue claims and the merge is idempotent, so the pair is
    correct against any prior state — including one the reader never saw. Deriving
    the delete from the row-level difference instead would look equivalent and
    would not be: a partial or scoped-wrong read returns fewer rows in
    ``current``, so the diff would emit fewer deletes and obsolete claims would
    survive indefinitely, with nothing to notice. As it stands a bad read costs a
    misleading *report* and cannot corrupt the catalogue.
    """

    current: "Catalogue"
    desired: "Catalogue"

    def per_table(self):
        """``{item: (TableChanges, ...)}`` — reporting only, never statements."""

        from .reconcile import compare
        from .tables import DICTIONARY_TABLES, INSTALLATION, REGISTRY

        tables = (*DICTIONARY_TABLES, INSTALLATION, REGISTRY)
        report = {}
        for item, wanted in self.desired.rows.items():
            found = self.current.rows.get(item, {})
            report[item] = tuple(
                compare(table, wanted.get(table.name, ()), found.get(table.name, ()))
                for table in tables
            )
        return report

    @property
    def is_noop(self) -> bool:
        return all(
            change.is_noop
            for changes in self.per_table().values()
            for change in changes
        )

    def render_dml(self, *, installation=None):
        """``{item: CatalogueReconciliation}`` making the catalogue match ``desired``.

        A structured result rather than flat statements, and deliberately: the
        caller needs the dictionaries, the Installation row and the Registry
        separated, because Registry is written last in its own barrier so a row
        certifying an object cannot outrun the work it attests to. Handing back
        one list would leave the caller to recover that ordering by inspecting
        the SQL, which is a guess dressed as a grouping.

        ``installation`` supplies the binding facts per item — which target, which
        Weaver — because a repository-derived catalogue does not know them and
        must not invent them.
        """

        from .projection import CatalogueProjection
        from .reconcile import reconcile

        installation = dict(installation or {})
        rendered = {}
        for item, wanted in self.desired.rows.items():
            rows = dict(wanted)
            binding = installation.get(item)
            if binding is not None:
                rows[INSTALLATION.name] = tuple(binding)
            projection = CatalogueProjection(
                scope=InstallationScope(item.item_type, item.item_name),
                rows=rows,
            )
            rendered[item] = reconcile(projection)
        return rendered


def retaining(catalogue: Catalogue, repository, identities) -> Catalogue:
    """Narrow a desired catalogue to what a build actually certified.

    The step that keeps a Registry row meaning *this succeeded*. Publishing the
    whole logical catalogue would claim every declared object as installed,
    including the ones a build omitted or failed to materialise — which is what
    the planner's uncertified set exists to prevent.

    A function rather than a method, and ``repository`` passed rather than
    remembered, because a catalogue is rows: making it carry the repository it
    came from would give a *persisted* one two fields that mean nothing and two
    methods that refuse. The dependency is real, so it is visible.
    """

    from .projection import project_item_catalogue

    wanted = set(identities)
    rows = {}
    for item in catalogue.rows:
        kept = {identity for identity in wanted if identity.item == item}
        if not kept:
            # An item this build retains nothing of is out of scope, not empty.
            # Keeping it would publish a scope that deletes everything the item
            # has, and would demand a binding for an item that has none.
            continue
        projection = project_item_catalogue(repository, item=item, retained=kept)
        rows[item] = MappingProxyType(dict(projection.rows))
    return Catalogue(rows=MappingProxyType(rows))


def for_targets(
    catalogue: Catalogue,
    repository,
    identities,
    target_kinds: Mapping[WeaverItemId, str],
) -> Catalogue:
    """Bind to targets: certify alias destinations, and scope to what is bound.

    ``target_kinds`` names the items being published *and* what each is bound to,
    and those are one decision rather than two. An item not named is not
    published — so an alias can never be certified against a guessed kind,
    because there is no path that reaches the certification without stating the
    binding. A default would have written a Warehouse alias into the Registry as
    a table, quietly, in the authoritative record.

    ``identities`` is what the build certified, and it is passed rather than read
    off the rows because the two differ on purpose: an alias whose source item is
    unbound still has its *declaration* published — the name does point there —
    while a Registry row would claim work that never happened.

    An item named here but retaining nothing is still published, and must be: its
    scope's rows are all obsolete and the publication is what says so.
    """

    from .projection import project_alias_registry

    certified = set(identities)
    rows = {}
    for item, kind in target_kinds.items():
        tables = dict(catalogue.rows.get(item, {}))
        certifiable = {
            alias.destination
            for alias in repository.aliases
            if alias.destination.item == item and alias.destination in certified
        }
        if certifiable:
            tables[REGISTRY.name] = tuple(
                tables.get(REGISTRY.name, ())
            ) + project_alias_registry(
                repository, item=item, retained=certifiable, target_kind=kind
            )
        rows[item] = MappingProxyType(tables)
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


def _encode_json_value(value):
    if isinstance(value, datetime):
        return {"$weaver_type": "datetime", "value": value.isoformat()}
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise BuildError(
        f"catalogue state contains a non-JSON value: {type(value).__name__}"
    )


def _decode_json_value(value):
    if isinstance(value, dict) and value.get("$weaver_type") == "datetime":
        return datetime.fromisoformat(value["value"])
    return value


def _registered_documents(
    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
) -> Mapping[WeaverDocumentId, RegisteredDocument]:
    registered: dict[WeaverDocumentId, RegisteredDocument] = {}
    for item, tables in rows.items():
        for row in tables.get(REGISTRY.name, ()):
            # The type is read first because it is what says how the other two
            # columns are shaped: ``_/Load/lib`` and ``dates.py`` are a schema
            # and an object only once the row has said it describes a file.
            object_type = str(row.get("object_type") or "")
            if object_type not in OBJECT_TYPES:
                expected = ", ".join(OBJECT_TYPES)
                raise BuildError(
                    f"Registry row for {item}/{row.get('schema_name')}."
                    f"{row.get('object_name')} has unsupported object_type "
                    f"{object_type!r}; expected one of {expected}"
                )
            identity = _row_identity(item, row, object_type)
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
                if not inventory.has_object(
                    catalogue_schema(identity),
                    identity.object_id.object,
                    document.object_type,
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


def _row_identity(
    item: WeaverItemId, row: Mapping[str, object], object_type: str
) -> WeaverDocumentId:
    """One Registry row's identity, built from its own columns.

    The row already *is* the identity: item, schema, object and what kind of
    object it is are four stored fields. Composing them into a line and handing
    it to a parser was a round trip through a grammar built for table-style
    ``Schema.Object`` names, and it could not express the two the load layer
    installs — a path and a filename, or a procedure named for what it loads.
    Constructing directly means the stored name is the real name, in both
    directions.
    """

    schema = str(row.get("schema_name") or "")
    name = str(row.get("object_name") or "")
    if object_type == "file":
        return WeaverDocumentId(item, ObjectId(schema, name), shape=FILE_SHAPE)
    if object_type == "stored_procedure":
        return WeaverDocumentId(item, ObjectId(schema, name), shape=PROCEDURE_SHAPE)
    is_files = schema.startswith(_FILES_PREFIX)
    return WeaverDocumentId(
        item,
        ObjectId(schema[len(_FILES_PREFIX) :] if is_files else schema, name),
        is_files=is_files,
    )
