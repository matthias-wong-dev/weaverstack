"""Read and validate central catalogue state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from ..declaration.metadata import ObjectId
from ..declaration.model import (
    FILE_SHAPE,
    FILES,
    PROCEDURE_SHAPE,
    TABLES,
    WeaverDocumentId,
    WeaverItemId,
    WeaverSchemaId,
)
from ..errors import BuildError, ConfigError
from .claims import (
    CatalogueClaim,
    catalogue_columns,
    catalogue_schema,
    claim_rules_for_object_type,
    stored_area,
)
from .reader import read_installations, read_table
from .render import InstallationScope, InstallationScopes
from .tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL,
    BORROWED_TABLES,
    BUILD_DATETIME,
    CURRENT_STATE_TABLES,
    INSTALLATION,
    LOAD_STATUS,
    MIRROR,
    OBJECT_ROLES,
    OBJECT_TYPES,
    PROJECTED_TABLES,
    READABLE_TABLES,
    REGISTRY,
    ROLE_DATA,
    RUNTIME_ROLES,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
    TEST_DICTIONARY,
    TEST_STATUS,
    VALIDATION_ROLES,
)


@dataclass(init=False)
class Catalogue:
    """Materialised catalogue rows and runtime writes.

    ``rows`` are grouped by item and table; ``registered`` contains certified
    documents derived from Registry. ``materialised`` records which tables were
    read, independently of their physical existence.

    Updates are visible in memory immediately. :meth:`flush` is the durability
    barrier. A borrowed Session remains open; :meth:`close` closes only a Session
    opened by the catalogue.
    """

    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
    registered: Mapping[WeaverDocumentId, "RegisteredDocument"]
    #: Installed objects whose data is borrowed, from ``_.Mirror``. Empty where
    #: the catalogue has no such table.
    mirrors: Mapping[WeaverDocumentId, "InstalledMirror"]
    materialised: frozenset[str]

    def __init__(
        self,
        rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]],
        *,
        registered: Mapping[WeaverDocumentId, "RegisteredDocument"] | None = None,
        materialised: frozenset[str] | None = None,
        load_history: Any = None,
        writer: Any = None,
        session: Any = None,
        owns_session: bool = False,
    ) -> None:
        self.rows = MappingProxyType(dict(rows))
        self.registered = (
            _registered_documents(self.rows)
            if registered is None
            else MappingProxyType(dict(registered))
        )
        self.mirrors = _installed_mirrors(self.rows)
        # Hand-built catalogues materialise every table they carry by default.
        carried = {table for tables in self.rows.values() for table in tables}
        self.materialised = frozenset(
            materialised if materialised is not None else carried
        )
        self._load_history = load_history
        self._writer = writer
        self._session = session
        self._owns_session = owns_session
        # Consulted before persisted rows so callers see their writes immediately.
        self._written: dict[str, dict[tuple, dict]] = {}

    @property
    def load_history(self):
        """Current load state and its selected statistics, if requested."""

        return self._load_history

    # --- the Session it reaches its Warehouse through -----------------------

    @property
    def session(self):
        return self._session

    def close(self) -> None:
        """Close only a Session opened by this catalogue."""

        if self._owns_session and self._session is not None:
            self._session.close()
            self._session = None
            self._owns_session = False

    def __enter__(self) -> "Catalogue":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    # --- writing ------------------------------------------------------------

    @property
    def writer(self):
        if self._writer is None:
            from .writer import RefusingWriter

            self._writer = RefusingWriter(
                "it was built without a connection to write through"
            )
        return self._writer

    def submit(self, table, row: Mapping[str, object]) -> None:
        self.writer.submit(table, row)

    def update(self, table, row: Mapping[str, object]) -> None:
        """Record a keyed row in memory now and in the Warehouse on flush."""

        self._written.setdefault(table.name, {})[
            tuple(row.get(name) for name in table.key)
        ] = dict(row)
        self.writer.update(table, row)

    def remove(self, table, rows: Sequence[Mapping[str, object]]) -> None:
        """Remove keyed rows from the Warehouse and this in-memory view."""

        keys = {tuple(row.get(name) for name in table.key) for row in rows}
        self._written.setdefault(table.name, {})
        for key in keys:
            self._written[table.name].pop(key, None)
        self.rows = MappingProxyType(
            {
                item: MappingProxyType(
                    {
                        name: tuple(
                            row
                            for row in table_rows
                            if name != table.name
                            or tuple(row.get(column) for column in table.key)
                            not in keys
                        )
                        for name, table_rows in tables.items()
                    }
                )
                for item, tables in self.rows.items()
            }
        )
        self.writer.delete(table, rows)

    def flush(self) -> None:
        """Wait for pending writes and raise any write failure."""
        self.writer.flush()

    # --- reading ------------------------------------------------------------

    def table_rows(self, table) -> tuple[Mapping[str, object], ...]:
        """Rows across materialised items, including unflushed keyed writes."""

        written = self._written.get(table.name, {})
        found: dict[tuple, Mapping[str, object]] = {}
        for tables in self.rows.values():
            for row in tables.get(table.name, ()):
                found[tuple(row.get(name) for name in table.key)] = row
        found.update(written)
        return tuple(found.values())

    def bookmark(self, identity: WeaverDocumentId) -> datetime:
        """Return the bookmark, or the sentinel before any clean load."""

        from .claims import bookmark_row

        wanted = bookmark_row(identity)
        for row in self.table_rows(BOOKMARK):
            if all(row.get(name) == value for name, value in wanted.items()):
                return _aware(row.get("bookmark_datetime")) or BOOKMARK_SENTINEL
        return BOOKMARK_SENTINEL

    def installed_object(
        self,
        *,
        target_kind: str,
        target_name: str,
        schema: str,
        object: str,
        is_files: bool,
    ) -> WeaverDocumentId:
        """Resolve a physical address to exactly one installed logical object."""

        stored = f"{FILES}/{schema}" if is_files else f"{TABLES}/{schema}"
        bound = self.bound_to(kind=target_kind, name=target_name)
        found = [
            identity
            for identity, document in self.registered.items()
            if identity.item in bound
            and document.object_role == ROLE_DATA
            and catalogue_schema(identity).casefold() == stored.casefold()
            and identity.object_id.object.casefold() == object.casefold()
        ]
        if len(found) == 1:
            return found[0]
        where = f"{stored}.{object} in {target_kind}/{target_name}"
        if not found:
            raise ConfigError(
                f"{where} is not recorded as an installed object. Build it first "
                "or name the target where it was installed."
            )
        raise ConfigError(
            f"{where} matches more than one installed object: "
            + ", ".join(sorted(str(identity) for identity in found))
            + ". The catalogue identity is ambiguous because two logical items "
            "are bound to this target."
        )

    def installed_validation(
        self, *, target_kind: str, target_name: str, schema: str, object: str
    ) -> WeaverDocumentId:
        """Resolve a physical address to one declared validation.

        TestDictionary carries the validation identity; Registry carries the
        module or procedure it compiles to.
        """

        bound = self.bound_to(kind=target_kind, name=target_name)
        found = [
            identity
            for identity in self._validations()
            if identity.item in bound
            and identity.object_id.schema.casefold() == schema.casefold()
            and identity.object_id.object.casefold() == object.casefold()
        ]
        if len(found) == 1:
            return found[0]
        where = f"{schema}.{object} in {target_kind}/{target_name}"
        if not found:
            raise ConfigError(
                f"{where} is not recorded as an installed validation. Build it "
                "first or name the target where it was installed."
            )
        raise ConfigError(
            f"{where} matches more than one installed validation: "
            + ", ".join(sorted(str(identity) for identity in found))
            + ". The catalogue identity is ambiguous because two logical items "
            "are bound to this target."
        )

    def _validations(self):
        from .tables import TEST_DICTIONARY

        return tuple(
            WeaverDocumentId.validation(
                _item_of(row),
                ObjectId(
                    str(row.get("schema_name") or ""),
                    str(row.get("object_name") or ""),
                ),
            )
            for row in self.table_rows(TEST_DICTIONARY)
        )

    def is_mirrored(self, identity: WeaverDocumentId) -> bool:
        return identity in self.mirrors

    def effective_physical_type(self, identity: WeaverDocumentId) -> str | None:
        """Return Mirror's physical type when borrowed, otherwise Registry's."""

        borrowed = self.mirrors.get(identity)
        if borrowed is not None:
            return borrowed.physical_type
        document = self.registered.get(identity)
        return None if document is None else document.object_type

    def bound_to(self, *, kind: str, name: str) -> set:
        """Return items bound to a physical ``(kind, name)`` in the read scope.

        Use :func:`read_target_occupancy` to inspect bindings outside that scope.
        """

        return {
            item
            for item, target in _installed_targets(self.table_rows(INSTALLATION))
            if target == (kind.casefold(), name.casefold())
        }

    def dag(self):
        """Derive the immutable installed graph from these rows."""

        from ..installed import installed_dag

        return installed_dag(self)

    def to_mapping(self) -> dict[str, object]:
        """A versioned JSON-safe representation for remote callers."""

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
            "materialised": sorted(self.materialised),
        }

    @classmethod
    def from_mapping(cls, mapping) -> "Catalogue":
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
            materialised=frozenset(mapping.get("materialised", ())),
        )

    # --- constructors ---------------------------------------------------------
    #
    # Persisted and desired state share this representation for comparison.

    @classmethod
    def from_catalogue(cls, catalogue: Any, items) -> "Catalogue":
        return read_catalogue_state(catalogue, items)

    @classmethod
    def from_repository(cls, repository) -> "Catalogue":
        """Derive the complete unbound logical catalogue from a repository.

        Selection and target binding happen later. Logical shortcut rows are
        included so installed operations can reconstruct the graph from catalogue
        state alone.
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
            # Runtime artefacts are derived source declarations and target objects.
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

    def update_using(self, plan) -> "Catalogue":
        """Apply a plan's declared current-state effects without parsing its DML.

        Historical tables are unchanged because rebuilds do not invalidate them.
        """

        from .runtime_state import with_established, without_invalidated

        invalidation = tuple(getattr(plan, "runtime_state", ()))
        establishment = tuple(getattr(plan, "runtime_state_established", ()))
        if not invalidation and not establishment:
            return self
        return Catalogue(
            rows=with_established(
                without_invalidated(self.rows, invalidation), establishment
            ),
            materialised=self.materialised,
            load_history=self._load_history,
            writer=self._writer,
            session=self._session,
        )

    def diff(self, desired: "Catalogue") -> "CatalogueChanges":
        """Report how persisted state would move toward desired state."""

        return CatalogueChanges(current=self, desired=desired)


@dataclass(frozen=True)
class CatalogueChanges:
    """Per-item, per-table row changes for reporting.

    Its unchanged condition matches publication: every non-key column is equal.
    """

    current: "Catalogue"
    desired: "Catalogue"

    def per_table(self):
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
    """Narrow desired state to the identities a build certified.

    This keeps Registry from claiming omitted or failed objects as installed.
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
    """Bind desired state to target kinds and certify shortcut destinations.

    Only named items are published, so shortcut physical types are never guessed.
    A named item with no retained identities is still published to remove obsolete
    rows in its scope.
    """

    from .projection import project_shortcut_registry

    certified = set(identities)
    rows = {}
    for item, kind in target_kinds.items():
        tables = dict(catalogue.rows.get(item, {}))
        # Every declaration, not only the logical ones: a physical shortcut is
        # installed here exactly as a logical one is, and an uncertified
        # destination would be pruned on the next build.
        certifiable = {
            declaration.destination
            for declaration in repository.shortcuts
            if declaration.owner == item and declaration.destination in certified
        } | {
            shortcut.destination
            for shortcut in repository.logical_shortcuts
            if shortcut.destination.item == item and shortcut.destination in certified
        }
        if certifiable:
            tables[REGISTRY.name] = tuple(
                tables.get(REGISTRY.name, ())
            ) + project_shortcut_registry(
                repository, item=item, retained=certifiable, target_kind=kind
            )
        rows[item] = MappingProxyType(tables)
    return Catalogue(rows=MappingProxyType(rows))


@dataclass(frozen=True)
class Reconciliation:
    """Catalogue state and claims removed after inventory reconciliation."""

    catalogue: Catalogue
    #: Claims disproved by the inventory, to be deleted before physical work.
    stale_claims: tuple[CatalogueClaim, ...]
    #: The disproved objects, as readable labels. Nothing in the build consumes
    #: this. It exists so a reconciliation decision can be seen and asserted
    #: rather than inferred from the DML it eventually produces.
    stale_objects: tuple[str, ...] = ()


@dataclass(frozen=True)
class RegisteredDocument:
    """A validated Registry row."""

    identity: WeaverDocumentId
    object_type: str
    signature: str
    #: Explicit because files and procedures may be load or validation artefacts.
    object_role: str = ROLE_DATA
    #: When the build that last certified this object published it. ``None`` for
    #: a row written before build datetimes existed, which orders as older than
    #: any build_datetime.
    build_datetime: object = None

    @property
    def is_runtime_artefact(self) -> bool:
        return self.object_role in RUNTIME_ROLES

    @property
    def is_validation(self) -> bool:
        return self.object_role in VALIDATION_ROLES


@dataclass(frozen=True)
class InstalledMirror:
    """A validated ``_.Mirror`` row and its source."""

    identity: WeaverDocumentId
    source_workspace: str
    source_target: str
    source_schema: str
    source_object: str
    #: What physically stands at the address while the data is borrowed.
    physical_type: str

    @property
    def source(self) -> str:
        return f"{self.source_target}.{self.source_schema}.{self.source_object}"


#: Tables absent from older catalogues. Add a table only when it is introduced;
#: misclassifying an existing table would hide damage during a scoped rebuild.
INTRODUCED_TABLES = frozenset(
    {TEST_DICTIONARY.name, BOOKMARK.name, LOAD_STATUS.name, TEST_STATUS.name}
)

#: Projected, borrowed and current state needed by a build. History is excluded
#: because it does not affect decisions and grows with the estate's age.
READ_FOR_BUILD = PROJECTED_TABLES + BORROWED_TABLES + CURRENT_STATE_TABLES

#: The tables whose presence a build has to know about, being the ones it reads:
#: a claim may only be raised against a table that is there. ``_.Mirror`` is not
#: among them: nothing declares it, so absence is nothing borrowed.
CHECKED_TABLES = PROJECTED_TABLES + CURRENT_STATE_TABLES


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


def _aware(at) -> datetime | None:
    """Interpret timezone-free catalogue ``datetime2`` values as UTC."""

    if not isinstance(at, datetime):
        return None
    return at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)


def _item_of(row: Mapping[str, object]) -> WeaverItemId:
    return WeaverItemId(
        str(row.get(SCOPE_ITEM_TYPE) or ""), str(row.get(SCOPE_ITEM_NAME) or "")
    )


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
                identity = f"{item}/{row.get('schema_name')}.{row.get('object_name')}"
                raise BuildError(
                    f"The Weaver catalogue has unsupported object type "
                    f"{object_type!r} for {identity}; expected one of {expected}. "
                    "Use the Weaver version that created this catalogue or repair "
                    "the catalogue before building."
                )
            identity = _row_identity(item, row, object_type)
            signature = str(row.get("signature") or "")
            if not signature:
                raise BuildError(
                    f"The Weaver catalogue is missing build state for {identity}. "
                    f"Build {identity.item} before retrying."
                )
            object_role = str(row.get("object_role") or "")
            if object_role not in OBJECT_ROLES:
                expected = ", ".join(OBJECT_ROLES)
                raise BuildError(
                    f"The Weaver catalogue has unsupported object role "
                    f"{object_role!r} for {identity}; expected one of {expected}. "
                    "Use the Weaver version that created this catalogue or repair "
                    "the catalogue before building."
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
                raise BuildError(
                    f"The Weaver catalogue has conflicting entries for {identity}. "
                    "Repair the catalogue before building."
                )
            registered[identity] = document
    return MappingProxyType(registered)


def _installed_mirrors(
    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]],
) -> Mapping[WeaverDocumentId, InstalledMirror]:
    mirrors: dict[WeaverDocumentId, InstalledMirror] = {}
    for item, tables in rows.items():
        for row in tables.get(MIRROR.name, ()):
            physical_type = str(row.get("physical_type") or "")
            if physical_type not in OBJECT_TYPES:
                expected = ", ".join(OBJECT_TYPES)
                identity = f"{item}/{row.get('schema_name')}.{row.get('object_name')}"
                raise BuildError(
                    f"The mirrored catalogue has unsupported physical type "
                    f"{physical_type!r} for {identity}; expected one of {expected}. "
                    "Recreate the mirror with this Weaver version."
                )
            identity = _row_identity(item, row, physical_type)
            mirrors[identity] = InstalledMirror(
                identity=identity,
                source_workspace=str(row.get("source_workspace_name") or ""),
                source_target=str(row.get("source_target_name") or ""),
                source_schema=str(row.get("source_schema_name") or ""),
                source_object=str(row.get("source_object_name") or ""),
                physical_type=physical_type,
            )
    return MappingProxyType(mirrors)


def read_target_occupancy(catalogue: Any) -> dict[tuple[str, str], frozenset]:
    """Read whole-estate target occupancy from ``_.Installation``.

    Keys are casefolded ``(kind, name)`` pairs. This read is deliberately
    unscoped so it detects bindings outside the build scope.
    """

    occupied: dict[tuple[str, str], set] = {}
    for item, target in _installed_targets(read_table(catalogue, INSTALLATION)):
        occupied.setdefault(target, set()).add(item)
    return {target: frozenset(items) for target, items in occupied.items()}


def _installed_targets(rows):
    """Yield each installation and its casefolded ``(item type, target name)``."""

    for row in rows:
        name = str(row.get("target_name") or "").strip()
        if name:
            item = _item_of(row)
            yield item, (item.item_type.casefold(), name.casefold())


def read_catalogue_state(catalogue: Any, items) -> Catalogue:
    """Read and validate catalogue state for the named items.

    No tables is bootstrap. A partial catalogue is damage unless every missing
    table was introduced after the existing catalogue. Scoped builds cannot
    repair damage because they do not own other installations' rows.
    """

    present: set[str] = set()
    missing: set[str] = set()
    incompatible: list[str] = []
    for table in CHECKED_TABLES:
        columns = catalogue.columns_of(table)
        if columns is None:
            missing.add(table.name)
            continue
        present.add(table.name)
        folded = set(columns)
        # Merges require published columns, though values may be null on older
        # rows. Compare public spellings because internal keys are never stored.
        required = {
            table.public_name_of(name).casefold(): table.public_name_of(name)
            for name in table.column_names + table.published_column_names
        }
        absent_columns = sorted(
            public
            for folded_name, public in required.items()
            if folded_name not in folded
        )
        if absent_columns:
            incompatible.append(f"{table.name}.{absent_columns[0]}")
    if incompatible:
        raise BuildError(
            "Catalogue schema is incompatible; missing required columns: "
            + ", ".join(incompatible)
        )
    # A newly introduced table has no pre-existing rows, so a scoped build may
    # create it without losing another installation's state.
    unexpected = missing - INTRODUCED_TABLES
    if present and unexpected:
        raise BuildError(
            "Catalogue is incomplete: "
            + ", ".join(sorted(unexpected))
            + " missing while "
            + ", ".join(sorted(present))
            + " remain. A scoped build cannot recreate missing tables without "
            "losing rows for other installations. Repair the catalogue with "
            "authority over every installation."
        )

    wanted = tuple(items)
    scopes = InstallationScopes(
        tuple(InstallationScope(item.item_type, item.item_name) for item in wanted)
    )
    # Current-state rows are read for invalidation but never projected.
    by_table = read_installations(catalogue, scopes=scopes, tables=READ_FOR_BUILD)

    # Seed empty requested items so downstream code distinguishes unbuilt from
    # out of scope.
    grouped: dict[WeaverItemId, dict[str, list[Mapping[str, object]]]] = {
        item: {table.name: [] for table in READ_FOR_BUILD} for item in wanted
    }
    for table_name, table_rows in by_table.items():
        for row in table_rows:
            item = WeaverItemId(
                str(row.get(SCOPE_ITEM_TYPE) or ""),
                str(row.get(SCOPE_ITEM_NAME) or ""),
            )
            scoped = grouped.get(item)
            if scoped is None:
                # A widened predicate must not pull another installation into scope.
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
        materialised=frozenset(
            table.name for table in READ_FOR_BUILD if table.name in present
        ),
    )


def read_installed_catalogue(
    catalogue: Any,
    *,
    tables=READABLE_TABLES,
    load_history: bool = False,
    writer=None,
    session=None,
) -> Catalogue:
    """Read installed state unscoped and group rows by logical item.

    ``tables`` selects materialised tables and excludes growing history by
    default. ``load_history`` reads a matching status/statistics window separately.
    Missing optional runtime tables read as empty.
    """

    from .history import read_load_history

    rows: dict[WeaverItemId, dict[str, list[Mapping[str, object]]]] = {}
    for table in tables:
        table_rows = read_table(catalogue, table)
        for row in table_rows:
            item = _item_of(row)
            if not item.item_type or not item.item_name:
                raise BuildError(
                    "The Weaver catalogue contains an entry that is not assigned "
                    "to an item. Repair the catalogue before retrying."
                )
            rows.setdefault(item, {}).setdefault(table.name, []).append(row)
    return Catalogue(
        rows=MappingProxyType(
            {
                item: MappingProxyType(
                    {name: tuple(table_rows) for name, table_rows in tables_of.items()}
                )
                for item, tables_of in rows.items()
            }
        ),
        materialised=frozenset(table.name for table in tables),
        load_history=read_load_history(catalogue) if load_history else None,
        writer=writer,
        session=session,
    )


def catalogue_for(
    session, workspace=None, *, tables=READABLE_TABLES, load_history: bool = False
) -> Catalogue:
    """Read a writable catalogue through a borrowed Session."""

    from .connection import catalogue_connection
    from .writer import writer_for

    resolved = session.workspace_or_default(workspace)
    return read_installed_catalogue(
        catalogue_connection(session, resolved),
        tables=tables,
        load_history=load_history,
        writer=writer_for(session, resolved),
        session=session,
    )


def catalogue_in(workspace, *, tables=READABLE_TABLES) -> Catalogue:
    """Read a workspace catalogue through a Session the catalogue owns."""

    from ..sessions.host import session_for

    session = session_for(workspace)
    try:
        catalogue = catalogue_for(session, workspace, tables=tables)
    except BaseException:
        session.close()
        raise
    catalogue._owns_session = True
    return catalogue


def reconcile_catalogue_state(
    state: Catalogue, *, inventories: Mapping[WeaverItemId, Any]
) -> Reconciliation:
    """Remove claims disproved by inventories using each effective physical type."""

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
                schema_name, object_name = catalogue_columns(identity)
                expected = state.effective_physical_type(identity)
                if not inventory.has_object(schema_name, object_name, expected):
                    stale[identity] = document
        # Reconciliation removes disproved declaration claims. Current runtime
        # state is not a claim and must survive into build planning, where the
        # selected physical lifecycle decides whether to invalidate it.
        filtered = {name: tuple(rows) for name, rows in tables.items()}
        for table in PROJECTED_TABLES:
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
            if table.name in state.materialised:
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
            materialised=state.materialised,
        ),
        stale_claims=tuple(dict.fromkeys(stale_claims)),
        stale_objects=tuple(sorted(stale_labels)),
    )


def _row_identity(
    item: WeaverItemId, row: Mapping[str, object], object_type: str
) -> WeaverDocumentId | WeaverSchemaId:
    """Construct a Registry identity directly from its stored fields."""

    schema = str(row.get("schema_name") or "")
    name = str(row.get("object_name") or "")
    if object_type == "schema":
        # A schema shortcut names the schema in both columns, because the Registry
        # keys on both (see `weaver.catalogue.projection._identity`). Reading it
        # back as a two-part object identity would key it differently from the
        # declaration, so classification would never find its signature and would
        # rebuild the shortcut on every build.
        return WeaverSchemaId(item, schema)
    if object_type == "file":
        return WeaverDocumentId(item, ObjectId(schema, name), shape=FILE_SHAPE)
    if object_type == "stored_procedure":
        return WeaverDocumentId(item, ObjectId(schema, name), shape=PROCEDURE_SHAPE)
    area, relational = stored_area(schema)
    return WeaverDocumentId(item, ObjectId(relational, name), is_files=area == FILES)
