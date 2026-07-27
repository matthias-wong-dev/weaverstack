# The central catalogue

The item-oriented catalogue scopes installations by `(item_type, item_name)`,
projects each item's own `alias.yml`, and
builds generated `Lakehouse/_weaver` through the ordinary planner. The earlier
flat planner is retained only for isolated compatibility tests and is not part of
the advertised public or CLI model.

Weaver's catalogue records, for every object it has successfully built, what SES
declared about it. It lives in schema `_` of the Weaver Lakehouse, and it is the
control plane the rest of the system reads from.

It is **not** a second authoring model. Nothing in it is discovered from a
physical table and nothing in it can be written by hand. SES remains authoritative
for descriptive metadata, keys, lineage, dependencies and behavioural flags; the
catalogue is where that information lands once an object exists, so later
operations are driven from one place instead of by re-reading a repository.

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
remain distinct because `Files/` is part of the Folder's schema. A build cannot
touch another item because every key begins with its exact item identity.

An object left out because its owning item was not bound is **out of scope**, not
deleted. A build has no opinion about unbound items.

The bound item's name is an attribute, never identity. Rebinding an item to a
different Lakehouse **updates** its `_.Installation` row; it does not add a second
installation.

## The ten tables

Every table carries `signature` — the content hash of whatever the row projects —
plus Weaver's audit columns (`row_insert_datetime`, `row_update_datetime`,
`row_delete_datetime`), which the ordinary build appends to any Delta table.

| Table | One row per | Notes |
|---|---|---|
| `_.Installation` | logical item | The physical target currently bound, the installed item's signature, and the Weaver version that last reconciled it. |
| `_.Registry` | installed object | What Weaver certifies. `object_type` is folder, table or view; `object_role` is `data` today and `load` when stored procedures arrive. |
| `_.SchemaDictionary` | schema in use | Only schemas the installation actually uses. |
| `_.TableDictionary` | table or view | Tables and views together — they are described the same way. Keys, behavioural flags, description and lineage. |
| `_.FolderDictionary` | managed folder | Keeps the folder's two-part identity, and its file key — the scope of what Weaver manages inside it. |
| `_.ColumnDictionary` | described column | Purely descriptive: the columns an author wrote a note about, plus Weaver's surrogate. Not every column. |
| `_.IndexDictionary` | logical key | The primary key and any alternate keys. Nothing is built. |
| `_.ForeignKeyDictionary` | declared relationship | An ER model, not constraints. |
| `_.Dependency` | consumer-owned edge | The two-/three-/four-part spelling the consumer authored, plus `is_within_item`. |
| `_.Alias` | destination-keyed declaration | The canonical destination/source pair reproduced from the consuming item's `alias.yml`. |

### Why some tables look sparse

**`_.ColumnDictionary` holds only described columns.** It is documentation, not a
physical column list. That matters architecturally: a SQL-backed table may infer
its shape from its query, and those columns are not known until install time
(build-philosophy §7.3). Keeping ordinals, types and nullability out is what
allows the *whole* catalogue to be projected when the bundle is generated rather
than half of it waiting on the engine. Whether every column *should* carry a note
is a quality question to ask of this table, not a precondition for filling it.

**Logical keys and relationships have no names.** Nothing physical is created from
them, so a name would have to be invented. A key is identified by its type and its
columns; a relationship by the whole edge — which is why several relationships may
run between one pair of objects, and why an object may reference itself.

**`_.Dependency` says nothing about targets.** The reference is logical and
inherits the owner's target type: a Warehouse object resolves its dependencies in
the Warehouse. Recording a target on the reference would let a Warehouse object
appear to depend directly on a Delta table. Crossing engines is an *alias*, and
aliases are a separate table — composing `_.Dependency`, `_.Alias` and
`_.Registry` is what yields the estate's whole graph, and only that composition
may cross.

A dependency may leave its item. A two-part logical name is recorded with
`is_within_item=true`; a canonical cross-item name or an authored physical name is
recorded exactly as declared with `is_within_item=false`.

## Weaver builds its own catalogue

Weaver materialises `Lakehouse/_weaver` beneath `Files/weaver_items` from the
authoritative table definitions and parses those generated schema and source files through the same
static readers as authored content. The **ordinary item planner and installer**
then build it. There is no second "create the control tables" path, and that
recursion is the point.

On a new control plane, bind `Lakehouse/_weaver` to the control Lakehouse in the
first coordinated item build. Its physical actions create the tables before the
same bundle reaches the catalogue tail. Ordinary later builds leave `_weaver`
unbound and reconcile rows in those existing tables. Rebinding `_weaver` is the
explicit destructive catalogue-evolution operation while no migration promise
exists.

One bundle does the whole bootstrap, because the barriers already order it:

```text
sequence   20   create schema `_`
sequence   40   create the ten catalogue tables
sequence 9000   describe them in their own dictionaries
sequence 9010   record the installation
sequence 9020   certify them in their own registry
```

The catalogue's own DML runs after the tables it writes to exist, so no first-run
mode is needed. Generation reads nothing, so an absent catalogue is not a special
case — the statements are correct against it either way.

Setup never prunes. The Weaver Lakehouse belongs to the installation, not to the
built-in repository, so a reconciling build would treat anything else there as an
orphan.

## How a build writes it

Catalogue work concludes every build and is appended at bundle **generation**, not
decided at install time. Its statements are two per table — a scoped delete of
everything the projection does not claim, then an idempotent merge of everything it
does.

Those statements deliberately **do not depend on reading the catalogue first**. The
pair is correct against any prior state, including one the planner could not see. A
build that derived its deletes from an inventory would have its deletion scope
widened by a failed read, which is what build-philosophy §6 exists to prevent.

Generation therefore does not read the catalogue at all. It briefly did, to put a
row count in each sequence description — but a description is part of the hashed
plan, so two runs of the same repository produced *different bundle identities*
purely because the catalogue's state had changed. Counting rows is a report about
state, not part of a frozen contract.
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
comparison of every non-key column, so rebuilding unchanged SES writes nothing and
does not move `row_update_datetime`.

The Installation signature is deliberately item-scoped. The repository signature
still certifies the complete coordinated source and bundle snapshot, while object
rows retain their individual source signatures. This separation lets a future
incremental planner see that changing `Lakehouse/Raw` does not by itself make an
installed `Warehouse/Reporting` stale. An alias lives in the consuming item's own
`alias.yml`, so it contributes to that item's signature and not the producer's.

## Removing things

Three scopes, and they are deliberately different operations:

| Scope | What it removes | Reached from |
|---|---|---|
| object | rows no longer projected, within one installation | every build |
| installation | one `(item_type, item_name)` entirely | decommissioning a logical item, explicitly |

Only the first is part of a build. A build that did not bind an item has no opinion
about it, so nothing in the build path can reach its installation rows.

Schema `_` is reserved from ordinary prune. An application build normally cannot
see it — prune is scoped to the bound destination's own storage, and the catalogue
lives in the Weaver Lakehouse — but a repository built *into* the Weaver Lakehouse
would, and a prune that dropped `_` would take the record of every installation
with it.

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

## The execution model

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

That is what makes one invocation building several Lakehouses possible. Relying on
the current catalogue would make `Sales` in Lakehouse A indistinguishable from
`Sales` in Lakehouse B.

### A Lakehouse has two addresses, and a build needs both

```python
location = resolver.lakehouse_spark_location(target)   # where the bytes are
location.table_path("Sales", "Customer")
location.folder_path("Sales", "Export")

destination = resolver.spark_destination(target)       # what it is called
destination.qualify("Sales", "Customer")
```

On Fabric the second is the native four-part name:

```sql
CREATE TABLE `Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer` …
--            ^workspace ^lakehouse       ^schema ^object
```

One session can create, read and drop through that name in any Lakehouse in the
workspace, and can build a view in one over a table in another. The local
emulator has one namespace level and cannot be given another, so it folds the
Lakehouse into that level — `` `sales_lh__sales`.`Customer` `` — which is not
Fabric syntax and is not meant to be. What it reproduces is the property: two
destinations declaring a schema of the same name stay apart. Storage is untouched
by the folding. The folded schema uses the local catalogue's canonical lower-case
spelling; the emulator keeps declared object names exact-case and therefore uses
case-sensitive analysis for its Spark session.

Both are needed and neither substitutes for the other: a folder is created at a
path and has no catalogue name, while a view exists only as a name and has no path
of its own.

`SparkCatalogue` binds a session to one destination and is how every catalogue
operation is performed — execute, create a schema, list views, ask whether an
object exists. Enumerating a destination's *schemas* is deliberately not among
them: Fabric refuses `SHOW SCHEMAS IN `workspace`.`lakehouse``, and a bare
`SHOW SCHEMAS` answers for the attached Lakehouse only, so schema discovery reads
the destination's `Tables/` area through the store instead.

Responsibilities stay separated. `ItemRef` identifies the logical item; the host
adapter resolves both addresses; the plan carries the item; the installation
context resolves it once per target; the executor uses it. An executor deriving
either for itself would be re-deciding where an action lands, which is a planning
decision.

Two things worth knowing:

- On Fabric a Lakehouse has two *storage* addresses as well — the DFS location the
  store lists through, and the `abfss://` root Spark reads and writes through.
  `LakehouseSparkLocation` carries the second; target *inspection* lists through
  the first.
- Neither address is in the bundle. Both embed workspace and item ids on Fabric,
  and a temporary directory locally, so a bundle carrying one would not be
  comparable between environments (build-philosophy §10). The bundle names the
  item; the installer resolves it. That is why a payload says
  `{{object:_.Registry}}` and not the qualified name.

## What this branch does not do yet

Build still emits `CREATE OR REPLACE TABLE`, so an explicit `_weaver` rebuild
empties the catalogue tables before the same coordinated build repopulates the
workspace declaration's rows. Ordinary builds leave `_weaver` unbound.

Dropping only what changed is the next branch, and it is what the signatures in
this catalogue exist for: compare the recorded signature with the source, drop the
changed objects and their descendants, honour `Prohibit rebuild`, and dirty the
catalogue before mutating anything. Certification is then per object rather than
per build — a rebuild of `A` in `A → B → C` uncertifies all three, and each
returns only after it builds.

## See also

- [build-philosophy.md](build-philosophy.md) — the governing properties every
  build implementation must preserve.
- [ses-repository.md](ses-repository.md) — where a repository lives and how it is
  installed.
- [journal.md](journal.md) — why each of these decisions was taken.
