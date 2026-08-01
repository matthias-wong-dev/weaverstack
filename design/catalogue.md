# The central catalogue

The catalogue scopes installations by `(item_type, item_name)`, projects each
item's own `alias.yml`, and builds generated `Lakehouse/_weaver` through the
ordinary planner.

Weaver's catalogue records, for every object it has successfully built, what Weaver document
declared about it. It lives in schema `_` of the Weaver Lakehouse, and it is the
control plane the rest of the system reads from.

It is **not** a second authoring model. Nothing in it is discovered from a
physical table and nothing in it can be written by hand. Weaver document remains authoritative
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
| `_.Registry` | installed object | What Weaver certifies. `object_type` is folder, table or view; `object_role` is `data` today and `load` when stored procedures arrive. `build_epoch` dates the build that published the row. |
| `_.SchemaDictionary` | schema in use | Only schemas the installation actually uses. |
| `_.TableDictionary` | table or view | Tables and views together — they are described the same way. Keys, behavioural flags, description and lineage. |
| `_.FolderDictionary` | managed folder | Keeps the folder's two-part identity, and its file key — the scope of what Weaver manages inside it. |
| `_.ColumnDictionary` | described column | Purely descriptive: the columns an author wrote a note about, plus Weaver's surrogate. Not every column. |
| `_.IndexDictionary` | logical key | The primary key and any alternate keys. Nothing is built. |
| `_.ForeignKeyDictionary` | declared relationship | An ER model, not constraints. |
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

**`_.ColumnDictionary` holds only described columns.** It is documentation, not a
physical column list. That matters architecturally: a SQL-backed table may infer
its shape from its query, and those columns are not known until install time
([How Weaver Build Works](how-does-build-work.md), section 2). Keeping ordinals, types and nullability out is what
allows the *whole* catalogue to be projected when the bundle is generated rather
than half of it waiting on the engine. Whether every column *should* carry a note
is a quality question to ask of this table, not a precondition for filling it.

**Logical keys and relationships have no names.** Nothing physical is created from
them, so a name would have to be invented. A key is identified by its type and its
columns; a relationship by the whole edge — which is why several relationships may
run between one pair of objects, and why an object may reference itself.

**`_.Dependency` keeps the author's spelling.** The reference is recorded exactly
as written and resolves within the consuming item. Recording a resolved physical
name would let one item appear to depend directly on another's storage. Crossing
items or engines is an *alias*, and aliases are a separate table — composing
`_.Dependency`, `_.Alias` and `_.Registry` is what yields the estate's whole
graph, and only that composition may cross.

A dependency may leave its item. A two-part logical name is recorded with
`is_within_item=true`; a canonical cross-item name or an authored physical name is
recorded exactly as declared with `is_within_item=false`.

## Weaver builds its own catalogue

Weaver composes `Lakehouse/_weaver` in memory from the authoritative table
definitions and parses those generated schema and source files through the same
static readers as authored content. It never mutates authored source to make the
built-in visible. The **ordinary item planner and installer** then build it.
There is no second "create the control tables" path.

Every build implicitly binds `Lakehouse/_weaver` to the control Lakehouse.
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

Catalogue work concludes every build and is appended at bundle **generation**, not
decided at install time. Its statements are two per table — a scoped delete of
everything the desired catalogue does not claim, then an idempotent merge of
everything it does.

The desired catalogue is derived in three steps, each one idea:

```python
logical     = Catalogue.from_repository(repository)   # everything the source declares
certified   = retaining(logical, repository, ids)     # what this build actually proved
publishable = for_targets(certified, repository, ids, kinds)
```

`from_repository` takes no selection and no binding: it is what the *source* says,
so a developer keeps it correct by adding a declaration rather than by remembering
a fixture. `retaining` is what keeps a Registry row meaning *this succeeded* —
publishing the whole declaration would claim objects a build omitted or failed to
materialise. `for_targets` certifies alias destinations, which cannot be done
earlier: an alias is a view in a Warehouse and a table in a Lakehouse, so the
Registry row needs the binding. It scopes as well as binds, so there is no path
that certifies an alias against a guessed kind.

Publication is then a diff:

```python
changes = current.diff(publishable)
dml     = changes.render_dml(installation=...)
```

**The two sides are used for different things, and that asymmetry is the design.**
`current` informs the *report* — new, changed, unchanged, removed — so a reviewer
can see what a bundle will do before it runs. `desired` alone drives the
*statements*.

That matters because a row-level delete would look equivalent and is not. A
partial or wrongly-scoped read returns *fewer* rows in `current`, so it would emit
*fewer* deletes — and obsolete claims would survive indefinitely, silently, in the
authoritative record. Scoped against what the desired catalogue claims, the pair
is correct against any prior state, including one the reader never saw. Three
catalogues that disagree completely about what is persisted render byte-identical
statements, which `tests/targeted/test_catalogue_diff.py` asserts rather than
describes.

Nothing about a binding reaches the projection. Target name, Weaver version, the
Installation row and the publication epoch are supplied at render time, because
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

`build_epoch` is a **published** column: declared and created like any other, but
supplied by the installer rather than projected, and excluded from that
comparison. One that compared would differ on every build by construction — its
value is new each time — and every row would update every build, which would
destroy the no-op above. It is written on insert only; see
[how-does-build-work §7a](how-does-build-work.md#7a-cross-item-freshness) for why
that is what makes it true rather than merely cheap.

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

Responsibilities stay separated. `ItemRef` identifies the logical item; the workspace
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

## See also

- [How Weaver Build Works](how-does-build-work.md) — the mechanics and governing properties every
  build implementation must preserve.
- [weaver-repository.md](weaver-repository.md) — where a repository lives and how it is
  installed.
- [weaver_master_cli_plan.md](weaver_master_cli_plan.md) — the authoritative lifecycle plan.
