# The central catalogue

## Purpose

This document defines the authoritative central catalogue: its ownership,
records, reconciliation, and certification behaviour.

The catalogue scopes installations by `(item_type, item_name)`, projects each
item's own `alias.yml`, and builds generated `Warehouse/_weaver` through the
ordinary planner.

For every successfully built object, the catalogue records the metadata declared
by its Weaver document. It lives in schema `_` of a configured Fabric
Warehouse and provides the records later operations read.

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

## The eleven tables

Every table carries `signature` — the content hash of whatever the row projects —
plus Weaver's audit columns (`row_insert_datetime`, `row_update_datetime`,
`row_delete_datetime`), which the ordinary build appends to any table it
creates. Every physical name is the public sentence-case spelling — `[Item
type]`, `[Build datetime]` — and the internal Python keys stay snake case; the
persistence boundary maps between them and nothing above it sees SQL.

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
| `_.Dependency` | referencing-owned edge | The spelling the author wrote, kept as `dependency_reference`, plus the edge Weaver resolved it to. |
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

Every build implicitly binds `Warehouse/_weaver` to the catalogue Warehouse.
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
schemas in the same Warehouse are therefore outside its scope and cannot be
treated as orphans. That is what lets the catalogue share a Warehouse with a
user's own schemas: Weaver owns `_` there and nothing else.

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

Schema `_` is reserved from ordinary prune. A build into the Warehouse holding
the catalogue would otherwise see it, and a prune that dropped `_` would take
the record of every installation with it. This is what makes a shared catalogue
host safe: a user's own schemas in that Warehouse are pruned by their own
items' rules, and `_` is claimed by neither.

The load layer's `_` is a different thing wearing the same name: a folder
`Files/_/Load` in a bound Lakehouse, and a schema `_` in a bound Warehouse. Both
are *generated and managed* rather than reserved, so ordinary prune is exactly
what removes them once an item stops declaring load code.

## Reading it tolerantly

Two absences are ordinary and read as data:

- **a missing table** — bootstrap, since the build that writes the catalogue is the
  build that creates it;
- **a missing column** — upgrade, where a newer Weaver compares against a table an
  older one created. It reads as a typed null and the next build repairs it.

An unexpected extra column is ignored, so a newer catalogue does not break an older
Weaver.

Everything else propagates. A permission error or a broken connection read as
"no rows" would tell the next build that nothing is catalogued — and once drop
policy lands, that is a licence to remove an estate. So absence is not read off a
failure at all: the reader asks `INFORMATION_SCHEMA` what the `_` schema holds,
once per connection, and a table that is not in the answer is absent. A failure
is a failure.

## Target addressing during catalogue work

**The catalogue names itself in two parts.** `[_].[Registry]` means the Registry
of the Warehouse the connection is open against, and a Warehouse connection
reaches one database — so there is no ambiguity to resolve and nothing for the
statement to inherit.

Destination Lakehouses and Warehouses are a different matter. They are the
variable data plane, one build may write to several, and each is named
explicitly:

```text
Weaver catalogue                   a Warehouse, reached over TDS
├── _.Installation
├── _.Registry
└── …
Build targets                      data plane, named explicitly
├── Lakehouse A
├── Lakehouse B
└── Warehouse C
```

One invocation can therefore build several targets without conflating objects
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
path and has no catalogue name, while a view exists only as a name and has no
path of its own. A Warehouse has neither — its objects are named over the
connection the statement runs on.

A Fabric Spark session is still created *against* a Lakehouse, because its id is
in the Livy URL. Which Lakehouse carries no meaning: it is a home, not a
destination, and it is taken from the workspace's own configured Lakehouses.

Neither address is in the bundle. Both embed workspace and item ids on Fabric,
so a bundle carrying one would not be comparable between environments ([How
Weaver Build Works](how-does-build-work.md), section 15). The bundle names the
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

## `_.Log` is evidence, not state

One more table lives under `_`, and it is not a catalogue table. `_.Log` holds
one row per settled unit of Weaver work: what ran, against which physical
target, how it ended and how long it took. Nothing reconciles it and nothing
projects it from a declaration — a build creates the table and a run appends to
it.

```text
[Log SK]                 a meaningless immutable surrogate
[Workflow ID]            correlates every row one workflow produced
[Task type]              load, test
[Target type]            Lakehouse, Warehouse
[Target name]            the physical item
[Schema name]            the object, where the work was about one
[Object name]
[Result]                 Succeeded, Failed, Skipped, Blocked
[Started datetime]
[Completed datetime]
[Duration milliseconds]
[Message]                one concise line
[Details]                the node's own record, as JSON
```

There is no run row and no completion row: a workflow *is* its rows. A reader
asking what a run did selects on `[Workflow ID]`.

Rows are appended asynchronously, through a flusher the Session owns and caches
per write stream. `submit` does not wait for the Warehouse, writes batch, and
ordering is preserved; a failure is remembered and raised by `flush` or `close`
rather than leaving an empty table to read as an empty run. Session close is the
durability barrier, and the trade is stated rather than hidden:

> `_.Log` is operational evidence, not transactional authority for installed
> catalogue state.

A dry run writes nothing. A row for work nobody did would be evidence of a load
that never happened.

## The catalogue lives in a Warehouse

Every table above is a Warehouse table under `_`, read and written over TDS.
Nothing about the catalogue needs Spark: a Warehouse-only estate builds, loads
and tests without a Spark session ever starting.

The Warehouse need not be Weaver's. `catalogue="Warehouse/Curated"` puts `_`
alongside a user's own schemas, and Weaver owns `_` there and nothing else —
initialisation creates only `_`, prune never claims a non-`_` object, and
resetting the catalogue never touches the Warehouse containing it.

## See also

- [How Weaver Build Works](how-does-build-work.md) — build lifecycle and
  catalogue reconciliation.
- [Build philosophy](build-philosophy.md) — planning and installation invariants.
- [Weaver repository sources](weaver-repository.md) — repository declarations.
