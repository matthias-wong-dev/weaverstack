"""Read and validate central catalogue state.

The reader accepts bootstrap and compatible-upgrade absences and reports other
storage or schema errors.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Mapping

from ..declaration.metadata import ObjectId
from ..declaration.model import (
    FILE_SHAPE,
    PROCEDURE_SHAPE,
    WeaverDocumentId,
    WeaverItemId,
)
from ..errors import BuildError, ConfigError
from .claims import CatalogueClaim, catalogue_schema, claim_rules_for_object_type
from .reader import read_installations, read_table
from .render import InstallationScope, InstallationScopes
from .tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL,
    BUILD_DATETIME,
    INSTALLATION,
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
    VALIDATION_ROLES,
)

#: What a Folder's stored schema carries, so a table and a folder of the same
#: name stay apart. Only the object shape uses it — see :func:`catalogue_schema`.
_FILES_PREFIX = "Files/"


@dataclass(init=False)
class Catalogue:
    """The catalogue an operation reads, reasons about, and records into.

    Live runtime state rather than a snapshot: rows are read into it, written
    through it, and read back from it, so it is mutable and says so.

    One class whatever produced it: read from the catalogue Warehouse in
    production, or built directly from Registry rows or a repository in a test.
    Incremental selection, shortcut staleness and claim collection all work from
    ``registered`` and ``rows``, so they are pure Python.

    ``rows`` is the row data by item and table. ``registered`` is the certified
    documents derived from the Registry rows — identity, type, signature and
    publication build_datetime, without the audit columns, which no build decision reads.

    A catalogue is *selectively materialised*. It holds the rows of the tables it
    was asked for and nothing else, because reading everything is not free:
    ``_.Log`` is history and grows with the estate's age, and nothing consults
    it. What was read is :attr:`materialised`. Whether a table physically exists
    is a different question, and a target's inventory answers it.

    Runtime rows are written through it — :meth:`submit` appends, :meth:`update`
    merges on the table's key — and an updated row is visible to a reader of this
    catalogue at once, before it has reached the Warehouse. :meth:`flush` is the
    durability barrier and the only place a write failure surfaces.

    A catalogue reaches its Warehouse through a Session. One it was handed is
    borrowed and left open; one it opened for itself is closed by :meth:`close`.
    """

    rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]]
    registered: Mapping[WeaverDocumentId, "RegisteredDocument"]
    materialised: frozenset[str]

    def __init__(
        self,
        rows: Mapping[WeaverItemId, Mapping[str, tuple[Mapping[str, object], ...]]],
        *,
        registered: Mapping[WeaverDocumentId, "RegisteredDocument"] | None = None,
        materialised: frozenset[str] | None = None,
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
        # Defaulting to "every table this catalogue carries rows for" keeps a
        # hand-built catalogue honest without making every caller state it.
        carried = {table for tables in self.rows.values() for table in tables}
        self.materialised = frozenset(
            materialised if materialised is not None else carried
        )
        self._writer = writer
        self._session = session
        self._owns_session = owns_session
        # Rows this catalogue has written, by table and key. Consulted ahead of
        # what was read, so a caller sees its own update immediately.
        self._written: dict[str, dict[tuple, dict]] = {}

    # --- the Session it reaches its Warehouse through -----------------------

    @property
    def session(self):
        """The Session this catalogue reads and writes through, if it has one."""

        return self._session

    def close(self) -> None:
        """Close the Session this catalogue opened, if it opened one.

        A borrowed Session belongs to whoever opened it and is left alone. One
        this catalogue opened for itself is closed here, so the caller that
        named a catalogue by name has one thing to close.
        """

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
        """Where this catalogue's runtime writes go, or a refusal if nowhere."""

        if self._writer is None:
            from .writer import RefusingWriter

            self._writer = RefusingWriter(
                "it was built without a connection to write through"
            )
        return self._writer

    def submit(self, table, row: Mapping[str, object]) -> None:
        """Record one appended row — a settled unit of work."""

        self.writer.submit(table, row)

    def update(self, table, row: Mapping[str, object]) -> None:
        """Record one keyed row, in memory now and in the Warehouse on flush.

        Both, because a caller that just recorded something must not read back
        what it replaced: a run advancing a bookmark and then asking for it is
        asking about the load it just did.
        """

        self._written.setdefault(table.name, {})[
            tuple(row.get(name) for name in table.key)
        ] = dict(row)
        self.writer.update(table, row)

    def flush(self) -> None:
        """Wait for every row this catalogue wrote. Raises what did not land."""

        self.writer.flush()

    # --- reading ------------------------------------------------------------

    def table_rows(self, table) -> tuple[Mapping[str, object], ...]:
        """Every row of one table, across the items this catalogue holds.

        Rows this catalogue wrote replace the ones it read, keyed on the table's
        own key, so a reader sees the catalogue as it now stands.
        """

        written = self._written.get(table.name, {})
        found: dict[tuple, Mapping[str, object]] = {}
        for tables in self.rows.values():
            for row in tables.get(table.name, ()):
                found[tuple(row.get(name) for name in table.key)] = row
        found.update(written)
        return tuple(found.values())

    def bookmark(self, identity: WeaverDocumentId) -> datetime:
        """How far ``identity`` has been loaded.

        A missing row is not a failure and not an absence to handle: it means no
        clean load has run since this object's current physical incarnation, so
        it reads as the sentinel and an incremental read asks for everything.
        """

        from .claims import bookmark_row

        wanted = bookmark_row(identity)
        for row in self.table_rows(BOOKMARK):
            if all(row.get(name) == value for name, value in wanted.items()):
                return _aware(row.get("bookmark_datetime")) or BOOKMARK_SENTINEL
        return BOOKMARK_SENTINEL

    def installed_object(
        self, *, target_name: str, schema: str, object: str, is_files: bool
    ) -> WeaverDocumentId:
        """Which installed object a physical target's ``Schema.Object`` is.

        The catalogue answers it, never the target's name: ``Installation`` says
        which logical item is bound to a physical one and ``Registry`` says what
        that item installed.

        Exactly one match, or a failure saying which. Two items may be bound to
        one physical target, so a name that resolves twice is genuinely ambiguous
        and anything that guessed would act on the wrong object.
        """

        stored = f"{_FILES_PREFIX}{schema}" if is_files else schema
        bound = {
            _item_of(row)
            for row in self.table_rows(INSTALLATION)
            if str(row.get("target_name") or "").casefold() == target_name.casefold()
        }
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
        where = f"{stored}.{object} in {target_name}"
        if not found:
            raise ConfigError(
                f"{where} is not an object the Weaver catalogue records as "
                "installed, so it has no catalogue identity. Build it first, or "
                "name the target it was built into."
            )
        raise ConfigError(
            f"{where} matches more than one installed object — "
            + ", ".join(sorted(str(identity) for identity in found))
            + ". Two logical items are bound to this target, so which one is "
            "meant cannot be settled here."
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
            "materialised": sorted(self.materialised),
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
            materialised=frozenset(mapping.get("materialised", ())),
        )

    # --- constructors ---------------------------------------------------------
    #
    # One reads what is persisted, the other derives what the source says should
    # be. Both produce this class, which is what lets the two be compared.

    @classmethod
    def from_catalogue(cls, catalogue: Any, items) -> "Catalogue":
        """The persisted catalogue, read over TDS from its Warehouse."""

        return read_catalogue_state(catalogue, items)

    @classmethod
    def from_repository(cls, repository) -> "Catalogue":
        """Everything the source declares — the whole logical catalogue.

        All of it, not the subset some build is ready to certify: with selection
        as an input, the desired state would be a statement about a build rather
        than about the repository. :meth:`retaining` and :meth:`for_targets`
        transform it later.

        It carries no binding — no target name, Weaver version, Installation
        row, publication build_datetime, or Registry certification for a shortcut
        destination, because a shortcut is a view in a Warehouse and a table in a
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
    """Bind to targets: certify shortcut destinations, and scope to what is bound.

    ``target_kinds`` names the items being published and what each is bound to,
    as one decision: an item not named is not published, so a shortcut can never
    be certified against a guessed kind. A default would write a Warehouse shortcut
    into the Registry as a table.

    ``identities`` is what the build certified, passed rather than read off the
    rows because the two differ: a shortcut whose source item is unbound still has
    its declaration published, while a Registry row would claim work that never
    happened.

    An item named here but retaining nothing is still published: its scope's
    rows are all obsolete, and the publication is what says so.
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


#: Catalogue tables introduced after the first release of the catalogue.
#:
#: An estate built by an older Weaver has every other table and not these: an
#: upgrade rather than damage, and indistinguishable from the physical state
#: alone. The partial-catalogue refusal protects rows a scoped build would lose,
#: and a table that never existed has none to lose.
#:
#: Add a name here in the same change that adds the table, and only then: a table
#: listed here that *was* in an older release would turn a repair case into a
#: silent partial rebuild.
INTRODUCED_TABLES = frozenset({TEST_DICTIONARY.name, BOOKMARK.name})

#: What a build reads. The projected tables, which it compares and republishes,
#: and ``_.Bookmark``, whose rows it decides the obsolete ones from. ``_.Log`` is
#: absent: it is history, nothing reads it to decide anything, and reading it
#: would grow with the estate's age.
READ_FOR_BUILD = PROJECTED_TABLES + (BOOKMARK,)

#: The tables whose presence a build has to know about — the ones it reads, since
#: a claim may only be raised against a table that is there.
CHECKED_TABLES = READ_FOR_BUILD


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
    """One stored instant, always aware and always UTC.

    The ``_`` schema holds ``datetime2``, which carries no zone, and every
    instant Weaver writes there is UTC.
    """

    if not isinstance(at, datetime):
        return None
    return at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)


def _item_of(row: Mapping[str, object]) -> WeaverItemId:
    """The logical item one catalogue row belongs to."""

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
    """Read the catalogue from its Warehouse, for the items named.

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
    for table in CHECKED_TABLES:
        columns = catalogue.columns_of(table)
        if columns is None:
            missing.add(table.name)
            continue
        present.add(table.name)
        folded = set(columns)
        # Published columns are required too, and deliberately: the merge writes
        # one on every insert, so a catalogue without it can be *read* but not
        # *written*. Exempting it here would let planning succeed and push the
        # failure into the install, where it surfaces as an engine complaint
        # about an unknown column rather than as a statement about the
        # catalogue's shape. The reader's null tolerance answers a different
        # question — a column that exists but predates some rows — and both hold
        # at once: require the column, tolerate the value.
        # Compared in the public spelling, because that is what the Warehouse
        # holds; the internal keys never reach a physical schema.
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
    # `_.Bookmark` as well as the projected tables. Nothing projects it, so it
    # takes no part in publication — but a build decides which rows are obsolete,
    # and deciding from rows it has read is the same arithmetic every other
    # catalogue decision uses. An absent table reads as no rows.
    by_table = read_installations(catalogue, scopes=scopes, tables=READ_FOR_BUILD)

    # Seeded before grouping, and that is not tidiness. An item with no rows yet
    # is an ordinary state — it has never been built — and it must still appear,
    # because everything downstream iterates the catalogue's items to decide what
    # to compare, reconcile and publish. An item that fell out here would look
    # like an item the build was never pointed at.
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
        materialised=frozenset(
            table.name for table in READ_FOR_BUILD if table.name in present
        ),
    )


def read_installed_catalogue(
    catalogue: Any, *, tables=READABLE_TABLES, writer=None, session=None
) -> Catalogue:
    """Read the installed catalogue, without being told what is in it.

    The sibling of :func:`read_catalogue_state`, for an operation that runs
    after a build. A build knows its items and reads each installation's scope;
    load orchestration knows only physical targets and has to discover which
    logical items are installed and where they are bound. So this reads unscoped
    and groups rows by the scope they carry.

    ``tables`` is what to materialise, and it defaults to everything an operation
    reads — which is not everything the catalogue owns. ``_.Log`` is left out:
    it is history, and reading it would grow with the estate's age for an answer
    nothing asks.

    The shape check is weaker than the build's: a missing table reads as no rows
    rather than a fault, because an estate with no shortcuts has never had a
    Shortcut table written.
    """

    rows: dict[WeaverItemId, dict[str, list[Mapping[str, object]]]] = {}
    for table in tables:
        table_rows = read_table(catalogue, table)
        for row in table_rows:
            item = _item_of(row)
            if not item.item_type or not item.item_name:
                raise BuildError(
                    f"{table.qualified} holds a row with no installation scope; "
                    "every catalogue row names the logical item it belongs to"
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
        writer=writer,
        session=session,
    )


def catalogue_for(session, workspace=None, *, tables=READABLE_TABLES) -> Catalogue:
    """The installed catalogue, read and writable, through a Session it borrows.

    The one construction an operation needs: it reads what it asked for and
    carries the way back, so a caller records a settled unit of work or a moved
    bookmark by telling this catalogue rather than by opening a stream of its own.

    The Session belongs to the caller and is left open. For one the catalogue
    opens for itself, see :func:`catalogue_in`.
    """

    from .connection import catalogue_connection
    from .writer import writer_for

    resolved = session.workspace_or_default(workspace)
    return read_installed_catalogue(
        catalogue_connection(session, resolved),
        tables=tables,
        writer=writer_for(session, resolved),
        session=session,
    )


def catalogue_in(workspace, *, tables=READABLE_TABLES) -> Catalogue:
    """The catalogue in one workspace, through a Session it opens for itself.

    For a caller that names a catalogue rather than holding a Session — authored
    code anchoring an object by name. The Session is the catalogue's, so
    :meth:`Catalogue.close` closes it and nothing else has to know it exists.
    """

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
