# The central catalogue

## Purpose

This document defines the authoritative central catalogue: its ownership,
records, reconciliation, and certification behaviour.

The catalogue scopes installations by `(item_type, item_name)`, projects each
item's own `alias.yml`, and builds generated `Warehouse/_weaver` through the
ordinary planner.

For every successfully built object, the catalogue records the metadata declared
by its Weaver document. It lives in schema `_` of the Weaver Lakehouse and
provides the control-plane records used by later operations.

Weaver documents remain authoritative for descriptive metadata, keys, lineage,
dependencies, and behavioural flags. The catalogue contains their installed
projection; it is not populated by physical discovery or manual editing.

A row in `_.Registry` means:

> Weaver currently certifies that this object was built successfully.

A physical table may exist with no Registry row. Weaver then does not treat it as
valid.

## Installation scope is identity

The workspace declaration owns several exact logical items, and the same
`Schema.Object` may legitimately exist in several of them. Every catalogue row is
therefore keyed on `item_type` and `item_name` before anything else:

```text
Lakehouse | Raw       | Sales       | Customer
Lakehouse | Raw       | Files/Sales | Customer
Warehouse | Reporting | Sales       | Customer
```

Those are three rows and all are real. The first two share one Lakehouse item but
remain distinct because `Files/` is part of the Folder's schema.

A runtime artefact — a deployed load module, a generated load procedure, or the
procedure or module a validation compiles to — is keyed the same way, with the
two halves spelled as the target itself spells them — a containing path and a complete filename, or a schema and a
procedure named for what it loads:

```text
Lakehouse | Sales     | _/Load/lib  | dates.py
Warehouse | Reporting | _           | Load Sales.Customer
```

Nothing is encoded to fit table-style naming, so the identity a build reasons
about and the name the target holds are the same string. Load artefacts claim the
Registry and nothing else: they declare no columns, keys, relationships or
dependencies, so no dictionary row describes them. A build cannot
touch another item because every key begins with its exact item identity.

An object whose owning item is not bound is outside the build scope. The build
does not delete or update its catalogue rows.

The bound item's name is an attribute rather than part of its identity. Rebinding
an item to a different Lakehouse updates its `_.Installation` row instead of
adding a second installation.

## The ten tables

Every table carries `signature` — the content hash of whatever the row projects —
plus Weaver's audit columns (`row_insert_datetime`, `row_update_datetime`,
`row_delete_datetime`), which the ordinary build appends to any Delta table.

| Table | One row per | Notes |
|---|---|---|
| `_.Installation` | logical item | The physical target currently bound, the installed item's signature, and the Weaver version that last reconciled it. |
| `_.Registry` | installed object | What Weaver certifies. `object_type` is folder, table, view, file or stored_procedure; `object_role` is `data` for something that holds or shapes rows, `load` for something that does the work of filling one, and `test` or `assumption` for the runnable form of a validation. `build_datetime` dates the build that published the row. |
| `_.SchemaDictionary` | schema in use | Only schemas the installation actually uses. |
| `_.TableDictionary` | table or view | Tables and views together — they are described the same way. Keys, behavioural flags, description and lineage. |
| `_.FolderDictionary` | managed folder | Keeps the folder's two-part identity, and its file key — the scope of what Weaver manages inside it. |
| `_.ColumnDictionary` | described column | Purely descriptive: the columns an author wrote a note about, plus Weaver's surrogate. Not every column. |
| `_.KeyDictionary` | logical key | The primary key and any alternate keys. Nothing is built. |
| `_.ForeignKeyDictionary` | declared relationship | An ER model, not constraints. |
| `_.TestDictionary` | Test or Assumption | The **logical** authored validation — `test_type`, description and the declared `primary_key`. The procedure or module it compiles to is a physical artefact and is certified in `_.Registry`; there is no Registry row under the logical validation ID. See [validation](validation.md). |
| `_.Dependency` | consumer-owned edge | The two-/three-/four-part spelling the consumer authored, plus `is_within_item`. |
| `_.Alias` | destination-keyed declaration | The canonical destination/source pair reproduced from the consuming item's `alias.yml`. |

An alias destination also gets a `_.Registry` row, typed as what it physically is
— a folder under `Files`, a view in a Warehouse, a table in a Lakehouse. There is
no `shortcut` type: to a reader of the catalogue a Lakehouse alias *is* a table,
and that it is implemented as a OneLake shortcut is execution detail. Its
alias-ness is recorded in `_.Alias` and nowhere else, which keeps installed object
state and cross-item relationship separate. It describes nothing further: no
dictionary, column, key or dependency rows, because it declares none of them.

### Why some tables look sparse

`_.ColumnDictionary` contains described columns rather than a physical column
list. A SQL-backed table can infer its shape from its query, so those columns are
not known until installation ([How Weaver Build Works](how-does-build-work.md),
section 2). Omitting ordinals, types, and nullability allows the complete
catalogue to be projected during bundle generation. Whether every column has a
description is a quality question, not a condition of publication.

Logical keys and relationships are identified without names because no physical
object is created from them. A key is identified by its type and its
columns; a relationship by the whole edge — which is why several relationships may
run between one pair of objects, and why an object may reference itself.

`_.Dependency` retains the author's spelling and resolves it within the
consuming item. Cross-item and cross-engine references use aliases, which are
stored separately in `_.Alias`. Combining `_.Dependency`, `_.Alias`, and
`_.Registry` produces the full estate graph.

A dependency may leave its item. A two-part logical name is recorded with
`is_within_item=true`; a canonical cross-item name or an authored physical name is
recorded exactly as declared with `is_within_item=false`.

## Weaver builds its own catalogue

Weaver composes `Warehouse/_weaver` in memory from the authoritative table
definitions and parses the generated schema and source files through the same
static readers used for authored content. The ordinary item planner and installer
build the result; authored source is unchanged.

Every build implicitly binds `Warehouse/_weaver` to the control Lakehouse.
Missing tables are classified as new and created before the same bundle reaches
the catalogue tail. Certified unchanged tables emit no physical action, so their
existing rows remain in place.

One bundle does the whole bootstrap, because the barriers already order it:

```text
first physical phase   create schema `_` when inventory says it is absent
dependency layers      create the ten new catalogue tables
sequence 9000          publish dictionaries and Installation as one batch
sequence 9010          certify them in their own Registry
```

The catalogue's own DML runs after the tables it writes to exist, so no first-run
mode is needed. Generation reads nothing, so an absent catalogue is not a special
case — the statements are correct against it either way.

Initialisation uses the ordinary authoritative prune, but the built-in
`_weaver` inventory is restricted to the reserved `_` schema. Application
schemas and Files areas in the same control Lakehouse are therefore outside its
scope and cannot be treated as orphans.

## How a build writes it

Catalogue work concludes every build and is appended during bundle generation.
Each table receives two statements: a scoped delete for rows absent from the
desired catalogue, followed by an idempotent merge for desired rows.

The desired catalogue is derived in three steps, each one idea:

```python
logical = Catalogue.from_repository(repository)  # everything the source declares
certified = retaining(logical, repository, ids)  # what this build actually proved
publishable = for_targets(certified, repository, ids, kinds)
```

`from_repository` takes no selection or binding and represents the declared
source. `retaining` limits Registry rows to objects the build successfully
materialised. `for_targets` adds target bindings for alias destinations, whose
physical type differs between Warehouses and Lakehouses.

Publication is then a diff:

```python
changes = current.diff(publishable)
dml = changes.render_dml(installation=...)
```

`current` produces the report of new, changed, unchanged, and removed rows.
`desired` alone produces the statements.

The statements use `desired` rather than `current` because a partial or
wrongly scoped read can omit rows. A delete derived from that read would retain
obsolete claims. Statements scoped to the desired catalogue are independent of
the reader's prior state; the targeted diff tests assert that this produces
byte-identical statements for different persisted catalogues.

Nothing about a binding reaches the projection. Target name, Weaver version, the
Installation row and the publication build_datetime are supplied at render time, because
they are things a *build* knows and a repository does not.

Generation does not let the catalogue's *state* reach the plan. It briefly did, to
put a row count in each sequence description — but a description is part of the
hashed plan, so two runs of the same repository produced *different bundle
identities* purely because the catalogue had changed. Counting rows is a report
about state, not part of a frozen contract.
`weaver.catalogue.reconcile.summarise` and the tolerant reader remain the API for
asking what a build would change.

Ordering is the one strict invariant:

1. **dictionaries** describe what was built;
2. **`_.Installation`** records which physical item the logical item is bound to;
3. **`_.Registry`** certifies.

Registry is last and is its own barrier, so a row in it cannot outrun the work it
attests to. Any earlier failure — physical, dictionary, or the installation
record — stops the install before anything is certified. Dictionaries need no
all-or-nothing transaction: partial state is repaired by the next successful
build's ordinary row comparison.

An unchanged row is a genuine no-op. The merge's `MATCHED` branch is guarded by a
comparison of every non-key column, so rebuilding unchanged Weaver document writes nothing and
does not move `row_update_datetime`.

`build_datetime` is supplied by the installer, excluded from the merge
comparison, and written only on insert. Including it in the comparison would
update every row on every build because the value changes for each publication.

The Installation signature is item-scoped. The repository signature represents
the complete source used for planning, while object rows retain individual source
signatures. This allows an incremental planner to distinguish a change to
`Lakehouse/Raw` from the state of `Warehouse/Reporting`. An alias contributes to
the signature of its consuming item's `alias.yml`.

## Removing things

Three removal scopes:

| Scope | What it removes | Reached from |
|---|---|---|
| object | rows no longer projected, within one installation | every build |
| installation | one `(item_type, item_name)` entirely | decommissioning a logical item, explicitly |

Only object removal is part of a build. An unbound item's installation rows are
outside the build path.

### Reconciliation scope

Reconciliation repairs catalogue rows only for the items read by the build.

`catalogue_items_for_build` returns the bound items plus the source items of any
alias they consume. `read_build_state` reads the catalogue for exactly those, so
`reconcile_catalogue_state` only ever walks rows belonging to them. An item the
build did not bind is never read, never compared against an inventory, and never
healed — because its claims may be perfectly true about a Lakehouse this build
cannot see, and deleting them would destroy the record of a real installation.

A physical target can retain Registry rows for every logical item bound to it.
For an intentional shared target, reset the control plane with
`weaver.wipe(..., unbind_from=...)` rather than unbinding individual residue;
the next build bootstraps the catalogue from the built-in item.

Load orchestration is where this becomes visible, because it reads the *whole*
installed catalogue rather than one build's scope — so it is the first operation
that can see two items claiming one physical object. It refuses such a request
(`load_dag`), and deliberately only when the ambiguity touches what was asked
for.

Schema `_` in the Weaver Lakehouse's `Tables` area is reserved from ordinary
prune. An application build normally cannot see it — prune is scoped to the bound
destination's own storage, and the catalogue lives in the Weaver Lakehouse — but a
repository built *into* the Weaver Lakehouse would, and a prune that dropped `_`
would take the record of every installation with it.

The load layer's `_` is a different thing wearing the same name: a folder
`Files/_/Load` in a bound Lakehouse, and a schema `_` in a bound Warehouse. Both
are *generated and managed* rather than reserved, so ordinary prune is exactly
what removes them once an item stops declaring load code. They never meet the
catalogue's `_`, which is a `Tables` schema in a different item.

## Reading it tolerantly

Two absences are ordinary and read as data:

- **a missing table** — bootstrap, since the build that writes the catalogue is the
  build that creates it;
- **a missing column** — upgrade, where a newer Weaver compares against a table an
  older one created. It reads as a typed null and the next build repairs it.

An unexpected extra column is ignored, so a newer catalogue does not break an older
Weaver.

Everything else propagates. A permission error, a corrupt Delta log or a broken
session read as "no rows" would tell the next build that nothing is catalogued —
and once drop policy lands, that is a licence to remove an estate. So absence is
recognised only by Spark's own `TABLE_OR_VIEW_NOT_FOUND` error class, never by
message text.

## Target addressing during catalogue work

**The Spark session is attached to the Weaver Lakehouse.** That is the fixed
control-plane context: it is where the session lives, and it is a useful execution
attachment. It is not what makes an operation land in the right place.

Correctness does not depend on it. Every statement names the Lakehouse it is
about, including the catalogue's own — `` `_`.`Registry` `` would mean "the
Registry of whatever the session is attached to", which is true today and would
stop being true the moment anything changed the attachment. Destination Lakehouses
are the **variable data plane**, and one session addresses all of them:

```text
Spark session                      attached to Weaver, for execution
Weaver Lakehouse                   control plane, named explicitly
├── _.Installation
├── _.Registry
└── …
Build targets                      data plane, named explicitly
├── Lakehouse A
├── Lakehouse B
└── Lakehouse C
```

One invocation can therefore build several Lakehouses without conflating objects
with the same schema and name.

### A Lakehouse has two addresses, and a build needs both

```python
location = resolver.lakehouse_spark_location(target)  # where the bytes are
location.table_path("Sales", "Customer")
location.folder_path("Sales", "Export")

destination = resolver.spark_destination(target)  # what it is called
destination.qualify("Sales", "Customer")
```

On Fabric the second is the native four-part name:

```sql
CREATE TABLE `Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer` …
--            ^workspace ^lakehouse       ^schema ^object
```

One session can create, read and drop through that name in any Lakehouse in the
workspace, and can build a view in one over a table in another. Two destinations
declaring a schema of the same name therefore stay apart without either naming
the other.

Both are needed and neither substitutes for the other: a folder is created at a
path and has no catalogue name, while a view exists only as a name and has no path
of its own.

`SparkCatalogue` binds a session to one destination and is how every catalogue
operation is performed — execute, create a schema, list views, ask whether an
object exists. Enumerating a destination's *schemas* is deliberately not among
them: Fabric refuses `SHOW SCHEMAS IN `workspace`.`lakehouse``, and a bare
`SHOW SCHEMAS` answers for the attached Lakehouse only, so schema discovery reads
the destination's `Tables/` area through the store instead.

`ItemRef` identifies the logical item. The workspace adapter resolves both
addresses, the plan carries the item, and the executor uses the resolved target.

Two things worth knowing:

- On Fabric a Lakehouse has two *storage* addresses as well — the DFS location the
  store lists through, and the `abfss://` root Spark reads and writes through.
  `LakehouseSparkLocation` carries the second; target *inspection* lists through
  the first.
- Neither address is in the bundle. Both embed workspace and item ids on Fabric,
  and a temporary directory locally, so a bundle carrying one would not be
  comparable between environments ([How Weaver Build Works](how-does-build-work.md), section 15). The bundle names the
  item; the installer resolves it. That is why a payload says
  `{{object:_.Registry}}` and not the qualified name.

## Incremental installation

The reconciled Registry supplies the certified effective signatures used for
incremental selection. New objects are created; changed objects and their
same-item descendants are uncertified, dropped, and rebuilt; unchanged objects
receive no physical action. `Prohibit Rebuild` suppresses only the physical
replacement of an existing object, while the incoming catalogue projection still
advances. Planned creates and managed drops are strict, so an unexpected physical
collision fails rather than being hidden.

## The catalogue lives in a Lakehouse

The control plane is a Lakehouse, and every table above is Delta read and
written through Spark SQL. Moving it to a Warehouse is separate future work and
is not part of the Fabric-only refactor: it changes the transport every
catalogue read and write uses, the concurrency the Registry can assume, and what
a build needs before it can plan. Nothing in this document should be read as
preparing for that move.

## See also

- [How Weaver Build Works](how-does-build-work.md) — build lifecycle and
  catalogue reconciliation.
- [Build philosophy](build-philosophy.md) — planning and installation invariants.
- [Weaver repository sources](weaver-repository.md) — repository declarations.
