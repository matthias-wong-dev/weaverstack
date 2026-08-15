"""Read and validate central catalogue state.

The reader accepts bootstrap and compatible-upgrade absences and reports other
storage or schema errors.
"""

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
from ..spark.catalogue import is_absent
from .claims import CatalogueClaim, catalogue_schema, claim_rules_for_object_type
from .reader import read_installations, read_table
from .render import InstallationScope, InstallationScopes
from .tables import (
    BUILD_DATETIME,
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    INSTALLATION,
    OBJECT_ROLES,
    OBJECT_TYPES,
    REGISTRY,
    ROLE_DATA,
    RUNTIME_ROLES,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
    TEST_DICTIONARY,
    VALIDATION_ROLES,
)

#: What a Folder's stored schema carries, so a table and a folder of the same
#: name stay apart. Only the object shape uses it — see :func:`catalogue_schema`.
_FILES_PREFIX = "Files/"


@dataclass(frozen=True, init=False)
class Catalogue:
    """The catalogue the build reads and reasons about.

    One class whatever produced it: read from the Weaver Lakehouse in
    production, or built directly from Registry rows or a repository in a test.
    Incremental selection, alias staleness and claim collection all work from
    ``registered`` and ``rows``, so they are pure Python.

    ``rows`` is the row data by item and table. ``registered`` is the certified
    documents derived from the Registry rows — identity, type, signature and
    publication build_datetime, without the audit columns, which no build decision reads.

    ``present_tables`` records which catalogue tables physically exist: a claim
    can only be raised against a table that is there, or reconciliation would
    emit deletes against tables that are not.
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
                else {table for tables in frozen_rows.values() for table in tables}
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
                            {
                                key: _encode_json_value(value)
                                for key, value in row.items()
                            }
                            for row in rows
                        ]
                        for table, rows in sorted(tables.items())
                    },
                }
                for item, tables in sorted(
                    self.rows.items(), key=lambda pair: str(pair[0])
                )
            ],
            "present_tables": sorted(self.present_tables),
        }

    @classmethod
    def from_mapping(cls, mapping) -> "Catalogue":
        """Reconstruct catalogue state from a payload, querying nothing."""

        version = mapping.get("format_version")
        if version != 1:
            raise BuildError(
                f"unsupported catalogue format_version {version!r}; expected 1"
            )
        rows = {
            WeaverItemId.parse(entry["item"]): MappingProxyType(
                {
                    table: tuple(
                        {key: _decode_json_value(value) for key, value in row.items()}
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
    # One reads what is persisted, the other derives what the source says should
    # be. Both produce this class, which is what lets the two be compared.

    @classmethod
    def from_catalogue(cls, catalogue: Any, items) -> "Catalogue":
        """The persisted catalogue, read over Spark from the Weaver Lakehouse."""

        return read_catalogue_state(catalogue, items)

    @classmethod
    def from_repository(cls, repository) -> "Catalogue":
        """Everything the source declares — the whole logical catalogue.

        All of it, not the subset some build is ready to certify: with selection
        as an input, the desired state would be a statement about a build rather
        than about the repository. :meth:`retaining` and :meth:`for_targets`
        transform it later.

        It carries no binding — no target name, Weaver version, Installation
        row, publication build_datetime, or Registry certification for an alias
        destination, because an alias is a view in a Warehouse and a table in a
        Lakehouse and this does not know which.
        """

        from ..etl import item_runtime_artefacts
        from .projection import project_item_catalogue

        rows = {}
        for model in repository.items:
            item = model.identity
            declared = {
                identity
                for identity in repository.source_documents
                if identity.item == item
            }
            # A runtime artefact is declared by the source exactly as a
            # document is — derived from it, but a target in its own right — so
            # it belongs in what the repository says should exist.
            declared.update(
                artefact.identity
                for artefact in item_runtime_artefacts(repository, item=item)
            )
            projection = project_item_catalogue(
                repository, item=item, retained=declared
            )
            rows[item] = MappingProxyType(dict(projection.rows))
        return cls(rows=MappingProxyType(rows))

    # --- transformations ------------------------------------------------------

    def diff(self, desired: "Catalogue") -> "CatalogueChanges":
        """How this catalogue would move toward the one ``desired`` describes.

        Read it as *persisted* ``.diff(`` *derived from source* ``)``. This is
        the report a reviewer sees before a bundle runs; the statements come
        from :func:`weaver.catalogue.reconcile.publish`, which compares the same
        two sides.
        """

        return CatalogueChanges(current=self, desired=desired)


@dataclass(frozen=True)
class CatalogueChanges:
    """What moving a persisted catalogue to a desired one would change.

    Reporting only. How many rows are new, changed, unchanged and removed, per
    item and per table, so a bundle can be reviewed before it is installed.

    A row is unchanged when every non-key column matches, which is the same
    condition publication tests before it emits anything — so a reported no-op
    and a silent build are one fact rather than two that agree.
    """

    current: "Catalogue"
    desired: "Catalogue"

    def per_table(self):
        """``{item: (TableChanges, ...)}`` — reporting only, never statements."""

        from .reconcile import compare
        from .tables import DICTIONARY_TABLES, REGISTRY

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


def retaining(catalogue: Catalogue, repository, identities) -> Catalogue:
    """Narrow a desired catalogue to what a build actually certified.

    What keeps a Registry row meaning "this succeeded": publishing the whole
    logical catalogue would claim every declared object as installed, including
    those a build omitted or failed to materialise.

    A function rather than a method, with ``repository`` passed rather than
    remembered, because a persisted catalogue has no repository and would carry
    two fields meaning nothing.
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

    ``target_kinds`` names the items being published and what each is bound to,
    as one decision: an item not named is not published, so an alias can never
    be certified against a guessed kind. A default would write a Warehouse alias
    into the Registry as a table.

    ``identities`` is what the build certified, passed rather than read off the
    rows because the two differ: an alias whose source item is unbound still has
    its declaration published, while a Registry row would claim work that never
    happened.

    An item named here but retaining nothing is still published: its scope's
    rows are all obsolete, and the publication is what says so.
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

    Two things: the catalogue with disproved claims removed, and the claims that
    were removed, which the build turns into delete DML. The second is a finding
    about the catalogue rather than catalogue state, so it is not carried on it.
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
    #: What the installed object is *for* — data, load, test or assumption.
    #:
    #: Kept after parsing, because it is the only place the answer survives. A
    #: physical shape once implied it — a file or a procedure could only be a
    #: load artefact — but a Test compiles to a module and a procedure too, so
    #: planning has to read what the row says.
    object_role: str = ROLE_DATA
    #: When the build that last certified this object published it. ``None`` for
    #: a row written before build datetimes existed, which orders as older than any build_datetime.
    build_datetime: object = None

    @property
    def is_runtime_artefact(self) -> bool:
        """Whether this is something installed to be run rather than to hold rows."""

        return self.object_role in RUNTIME_ROLES

    @property
    def is_validation(self) -> bool:
        return self.object_role in VALIDATION_ROLES


#: Catalogue tables introduced after the first release of the control plane.
#:
#: An estate built by an older Weaver has every other table and not these: an
#: upgrade rather than damage, and indistinguishable from the physical state
#: alone. The partial-catalogue refusal protects rows a scoped build would lose,
#: and a table that never existed has none to lose.
#:
#: Add a name here in the same change that adds the table, and only then: a table
#: listed here that *was* in an older release would turn a repair case into a
#: silent partial rebuild.
INTRODUCED_TABLES = frozenset({TEST_DICTIONARY.name})


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
    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]],
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
            object_role = str(row.get("object_role") or "")
            if object_role not in OBJECT_ROLES:
                expected = ", ".join(OBJECT_ROLES)
                raise BuildError(
                    f"Registry row for {identity} has unsupported object_role "
                    f"{object_role!r}; expected one of {expected}"
                )
            document = RegisteredDocument(
                identity,
                object_type,
                signature,
                object_role,
                row.get(BUILD_DATETIME),
            )
            prior = registered.get(identity)
            if prior is not None and prior != document:
                raise BuildError(f"Registry contains conflicting rows for {identity}")
            registered[identity] = document
    return MappingProxyType(registered)


def read_catalogue_state(catalogue: Any, items) -> Catalogue:
    """Read the catalogue from the Weaver Lakehouse, for the items named.

    A missing table is either a first run or damage, and what tells them apart
    is whether anything else is there: every table missing is bootstrap and
    reads as an empty catalogue, while some missing and some present stops the
    build.

    That holds for dictionary tables too, tempting as the exception is. A build
    is scoped to the items it was pointed at, so recreating a dictionary would
    republish rows for *those* items and leave every other item's Registry and
    Installation rows claiming objects the dictionaries no longer describe.
    Repairing a partial catalogue needs authority over every installation, which
    a scoped build does not have.
    """

    present: set[str] = set()
    missing: set[str] = set()
    incompatible: list[str] = []
    for table in CATALOGUE_TABLES:
        name = catalogue.qualify(CATALOGUE_SCHEMA, table.name)
        try:
            columns = catalogue.columns_of(name)
        except Exception as exc:
            if is_absent(exc):
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
    # A table a *later* Weaver introduced is not a damaged catalogue. It holds no
    # rows for anyone — nothing could ever have written to it — so creating it
    # under a scoped build loses nothing, and the items this build was not
    # pointed at are correctly represented by having no rows in it yet. Refusing
    # here instead would mean that adding a dictionary table stopped every
    # existing estate from building until somebody repaired a catalogue that was
    # never broken.
    unexpected = missing - INTRODUCED_TABLES
    if present and unexpected:
        raise BuildError(
            "catalogue is incomplete: "
            + ", ".join(sorted(unexpected))
            + " missing while "
            + ", ".join(sorted(present))
            + " remain. An ordinary build is scoped to the items it was pointed "
            "at, so it can only republish those; rows belonging to other "
            "installed items would be lost when the table was recreated, while "
            "their Registry and Installation rows survived to claim them. "
            "Restoring a partial catalogue needs a repair with authority over "
            "every installation, not a scoped build."
        )

    wanted = tuple(items)
    scopes = InstallationScopes(
        tuple(InstallationScope(item.item_type, item.item_name) for item in wanted)
    )
    by_table = read_installations(catalogue, scopes=scopes)

    # Seeded before grouping, and that is not tidiness. An item with no rows yet
    # is an ordinary state — it has never been built — and it must still appear,
    # because everything downstream iterates the catalogue's items to decide what
    # to compare, reconcile and publish. An item that fell out here would look
    # like an item the build was never pointed at.
    grouped: dict[WeaverItemId, dict[str, list[Mapping[str, object]]]] = {
        item: {table.name: [] for table in CATALOGUE_TABLES} for item in wanted
    }
    for table_name, table_rows in by_table.items():
        for row in table_rows:
            item = WeaverItemId(
                str(row.get(SCOPE_ITEM_TYPE) or ""),
                str(row.get(SCOPE_ITEM_NAME) or ""),
            )
            scoped = grouped.get(item)
            if scoped is None:
                # The predicate asked for these scopes and no others, so this is
                # a read that did not do what it was told rather than a row worth
                # keeping. Dropping it silently would let a widened predicate
                # pull an unrelated installation into a build's state.
                raise BuildError(
                    f"{table_name} returned a row for {item}, which this build "
                    "did not ask for; the catalogue read was not scoped correctly"
                )
            scoped[table_name].append(row)

    rows = {
        item: MappingProxyType(
            {name: tuple(table_rows) for name, table_rows in tables.items()}
        )
        for item, tables in grouped.items()
    }
    return Catalogue(
        rows=MappingProxyType(rows),
        present_tables=frozenset(present),
    )


def read_installed_catalogue(catalogue: Any) -> Catalogue:
    """Read the whole installed catalogue, without being told what is in it.

    The sibling of :func:`read_catalogue_state`, for an operation that runs
    after a build. A build knows its items and reads each installation's scope;
    load orchestration knows only physical targets and has to discover which
    logical items are installed and where they are bound. So this reads unscoped
    and groups rows by the scope they carry.

    The shape check is weaker than the build's: a missing table reads as no rows
    rather than a fault, because an estate with no aliases has never had an
    Alias table written. Nothing here writes, so nothing needs the guarantee
    that the catalogue can be written.
    """

    rows: dict[WeaverItemId, dict[str, list[Mapping[str, object]]]] = {}
    present: set[str] = set()
    for table in CATALOGUE_TABLES:
        table_rows = read_table(catalogue, table)
        if table_rows:
            present.add(table.name)
        for row in table_rows:
            item_type = str(row.get(SCOPE_ITEM_TYPE) or "")
            item_name = str(row.get(SCOPE_ITEM_NAME) or "")
            if not item_type or not item_name:
                raise BuildError(
                    f"{table.qualified} holds a row with no installation scope; "
                    "every catalogue row names the logical item it belongs to"
                )
            item = WeaverItemId(item_type, item_name)
            rows.setdefault(item, {}).setdefault(table.name, []).append(row)
    return Catalogue(
        rows=MappingProxyType(
            {
                item: MappingProxyType(
                    {name: tuple(table_rows) for name, table_rows in tables.items()}
                )
                for item, tables in rows.items()
            }
        ),
        present_tables=frozenset(present),
    )


def reconcile_catalogue_state(
    state: Catalogue, *, inventories: Mapping[WeaverItemId, Any]
) -> Reconciliation:
    """Discard catalogue claims the prepared inventories physically disprove.

    Pure: a catalogue and an inventory in, a catalogue and its stale claims out.
    Both inputs can be built directly, so no Lakehouse is needed to demonstrate
    what happens when a registered object is not there.
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

    The row is the identity: item, schema, object and object type are four
    stored fields. Composing them into a line for the ``Schema.Object`` parser
    could not express what the load layer installs — a path and a filename, or a
    procedure named for what it loads — so it is constructed directly and the
    stored name stays the real name.
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
