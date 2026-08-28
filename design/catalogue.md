# The central catalogue

## Purpose

This document defines the authoritative central catalogue: its ownership,
records, reconciliation, and certification behaviour.

The catalogue scopes installations by `(item_type, item_name)`, projects each
item's own shortcut declarations, and builds generated `Warehouse/_weaver` through the
ordinary planner.

For every successfully built object, the catalogue records the metadata declared
by its Weaver document. It lives in schema `_` of a configured Fabric
Warehouse and provides the records later operations read.

Weaver documents remain authoritative for descriptive metadata, keys, lineage,
dependencies, and behavioural flags. The catalogue contains their installed
projection; it is not populated by physical discovery or manual editing.

A row in `_.Registry` means:

> Weaver currently certifies that this object was built successfully.

A physical table may exist with no Registry row. Weaver does not treat it as
certified, but target inventory still proves that it exists and what kind it is.
The next build classifies that desired object as changed and converges it before
publishing a new certification.

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

## The eleven projected tables

Eleven of the catalogue's sixteen tables are projected from repository state and
reconciled against it. The other five are [maintained at
runtime](#runtime-catalogue-tables).

Every table here carries `signature` — the content hash of whatever the row projects —
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
| `_.Shortcut` | one installed relation | Every shortcut an item declares, plus Weaver's package-owned runtime references. Authored rows reproduce `shortcuts.py` or `shortcuts.yml`; runtime rows carry the same logical producer pair without pretending they were authored. |

A shortcut destination, including a package-owned runtime reference, also gets a
`_.Registry` row, typed as what it physically is: a folder under `Files`, a view
in a Warehouse, a table in a Lakehouse, and
`schema` for a schema shortcut. There is no `shortcut` object *type*, because to a
reader of the catalogue a Lakehouse table shortcut is a table. What it is for is
the object *role*, recorded as `shortcut`, and where it points is `_.Shortcut`. That
keeps installed object state and cross-item relationship separate. It describes
nothing further: no dictionary, column, key or dependency rows, because it
declares none of them.

The contents of a schema shortcut get no rows at all. They belong to the item the
shortcut points at, and they can change without a build.

`_.Shortcut` answers two questions, and keeps them apart.

**What the shortcut is, in the item that declares it.** `Shortcut ID` is the
declaration as its author wrote it and is the row's key, because a schema
shortcut names no object and a merge key cannot be null. `Schema` and `Object`
are the same decomposed pair `_.Registry` names an object by, so the two tables
join without anything having to split an id.

| | Shortcut ID | Schema | Object |
|---|---|---|---|
| table | `Sales.Customer` | `Sales` | `Customer` |
| folder | `Sales.Incoming` | `Sales` | `Incoming` |
| schema | `Reference` | `Reference` | NULL |

**What it points at.** `Target type` is `Logical` or `Physical`. For a logical
target the four target columns give the producer's identity whole:

| Target type | Target item type | Target item name | Target schema | Target object |
|---|---|---|---|---|
| Logical | Lakehouse | Sales | Sales | Customer |

so a reader rebuilds `Lakehouse/Sales/Sales.Customer` from the row alone, with no
join to Installation, no parsing, and no knowledge of a Fabric workspace or item
id. That is what lets the estate DAG be reconstructed from the catalogue.

A physical target names the Fabric item itself, and `Target workspace` is set
when it is in another workspace. It has no logical producer, so nothing in the
row pretends otherwise.

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
consuming item. Cross-item and cross-engine references use shortcuts and external
views, which are stored separately in `_.Shortcut`. Combining `_.Dependency`,
`_.Shortcut`, and `_.Registry` produces the full estate graph.

A dependency may leave its item. A two-part logical name is recorded with
`is_within_item=true`; a canonical cross-item name or an authored physical name is
recorded exactly as declared with `is_within_item=false`.

## Weaver builds its own catalogue

`Warehouse/_weaver` is checked-in repository content, held under
`src/weaver/fragments/catalogue/` and composed into every parsed repository
through the same static readers used for authored content. What a build composes
is what is reviewed. The ordinary item planner and installer build the result;
authored source is unchanged.

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
materialised. `for_targets` adds target bindings for shortcut destinations, whose
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
`Lakehouse/Raw` from the state of `Warehouse/Reporting`. A shortcut contributes
to the signature of the item that declares it.

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
shortcut they consume. `read_build_state` reads the catalogue for exactly those, so
`reconcile_catalogue_state` only ever walks rows belonging to them. An item the
build did not bind is never read, never compared against an inventory, and never
healed — because its claims may be perfectly true about a Lakehouse this build
cannot see, and deleting them would destroy the record of a real installation.

A physical target can retain Registry rows for every item ever bound to it. Reset
the catalogue with `weaver.wipe(...)` against a resolved catalogue rather than
unbinding individual residue; the next build bootstraps the catalogue from the
built-in item.

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

## Runtime catalogue tables

The `_` schema is the Weaver catalogue. Its tables are either **projected** from
repository state during build and reconciled against it, or **maintained at
runtime**. Five are runtime tables. All of them are declared as ordinary Weaver
documents in `Warehouse/_weaver` and built like any other, so a first build of the
control item creates sixteen tables.

Nothing projects them and nothing reconciles them.

They divide once, and everything else follows from the division:

| | current state | history |
|---|---|---|
| tables | `_.Bookmark`, `_.LoadStatus`, `_.TestStatus` | `_.Log`, `_.LoadStatistic` |
| holds | what is true of an object *now* | what happened |
| written | merged on the object's identity | appended |
| a rebuild | ends the row | leaves it alone |
| read by | a build, deciding what is obsolete | nothing |

A current-state row describes one object's **current physical incarnation**. A
build that drops and rebuilds the object, or stops installing it, ends that
incarnation, and the row goes with it — deleted rather than reset, because absence
already means what a sentinel would. History records that something happened, and
a rebuild does not unhappen it.

`RuntimeTable` carries which of the two it is, and which population's rebuild ends
a current-state row. Everything reads that declaration: the writer refuses a merge
into history, the build's reconciliation stage derives its intent from every
current-state table at once, and one stage per target presents whichever runtime
tables the target is missing.

A `Catalogue` is *selectively materialised*: it holds the tables it was asked for,
and `materialised` says which those are. The history tables are never among them —
nothing consults them and reading one would grow with the estate's age. The
current-state tables are, because a build decides which of their rows its own work
has made obsolete and cannot prune what it cannot see.

What is loaded and what physically exists are different questions. A catalogue
answers the first; a target's inventory answers the second, the way it does for
every other object in that target.

### The result vocabulary

One vocabulary wherever an outcome is recorded, so a reader comparing a load's
result with a validation's is comparing the same words.

```text
Succeeded   completed, and what it produced was acceptable
Failed      ran under Weaver's control and produced an unacceptable result
Error       could not be evaluated at all
Blocked     did not happen, because something upstream prevented it
Skipped     deliberately not done
Pending     no outcome established for this incarnation
Rejected    a load completed with rejected rows — loads only
```

The distinctions are the ones an operator acts on, and two of them are load-bearing.

**Failed is not Error.** A validation that found discrepancies has told you
something about the data; one whose procedure threw has told you nothing. Reading
the second as the first is the one answer a validation must never give, so a
`failure_count` is left null rather than defaulted to zero. The line is drawn the
same way on both engines: in Python by `isinstance(exc, WeaverError)`, in T-SQL by
`error_number()` falling inside the range Weaver's own generated code throws with.

**Rejected is neither.** Valid rows landed and some did not, and only a load can
be one.

### Who writes what

There is no flag. The **interface** decides:

```text
lower primitive        never records                the run's path, and an author's
run                    records centrally, async     one queue, one flush at the end
standalone wrapper     records synchronously        flushes before returning
```

In Python that is `_load()` against `load()` for a Table and a Folder, and
`read()` against `run()` for a Test and an Assumption. In T-SQL it is
`_.[Load X.Y]`, `_.[Test X.Y]` and `_.[Assumption X.Y]` against `_.Load` and
`_.Test`. Nothing takes a parameter about it, so one row has one writer without a
caller having to get a boolean right.

`_.Load` and `_.Test` are the two generic entry points, checked in under
`src/weaver/fragments/standard/Warehouse/programmables/` and composed into every
normal Warehouse item. They carry the role `programmable`: Weaver installs,
signs, selects and prunes them, and nothing schedules them. `_.Test` dispatches
to a Test or an Assumption, so a person asking about a validation by name need
not know which it was declared as; there is no `_.Assumption`.

Dispatch reads the physical estate. `_.Load` validates `@object_name` as exactly
`Schema.Object`, builds `_.[Load Schema.Object]` and checks it with
`object_id(…, 'P')`; the implementation procedure is in `_`, and the source
object's schema is part of its name. `_.Test` checks for `_.[Test X.Y]` and
`_.[Assumption X.Y]`, runs whichever is there, and refuses an installation
holding both. An unknown name is refused before any outcome row is written, and
the Registry is not consulted.

Each takes an optional `@item_name`. Supplied, it is used as given, so a caller
that has it pays for no lookup. Omitted, the procedure recovers it from
`_.Installation`: exactly one logical Warehouse item bound to this database, and
any other answer is refused rather than guessed. Because these are Warehouse
procedures, the item type needs no parameter.

Generated load procedures own the `@weaver_*` variable namespace. Their physical
outputs are `@weaver_succeeded`, `@weaver_rows_read`,
`@weaver_bookmark_datetime`, and the corresponding names for the rest of the
logical result. Direct dispatch and `_.Load` map those parameters back to the
stable `succeeded`, `rows_read`, `bookmark_datetime`, and other `LoadResult`
fields. Authored SQL can therefore declare natural variables such as
`@bookmark_datetime` without colliding with the generated procedure signature.

### `_.Bookmark`

```text
[Item type]              the Registry's identity, exactly
[Item name]
[Schema name]            `Files/<schema>` for a Folder, as Registry spells it
[Object name]
[Bookmark datetime]      datetime2(6), UTC, never null
```

One row per Weaver-loadable Table or Folder, holding the UTC instant immediately
before its most recent clean load began.

**A row means a clean load has run for this object's current physical
incarnation.** Absence means none has, and readers coalesce it to
`1900-01-01T00:00:00Z`. There is no stored sentinel.

```text
new object                     no row
first clean load               MERGE inserts the instant
later clean load               MERGE updates it
dropped and rebuilt            DELETE, before the physical work
no longer declared or loaded   DELETE
unchanged object               left alone
```

The governing rule: too old means replay, too advanced can omit data, so prefer
replay. That is why the invalidation runs **before** any physical work — an absent
bookmark makes the next load read the whole source, while one left in place over a
recreated table makes it read almost nothing.

Two things read it, and neither reads the target's contents. An incremental read
asks its source for changes after it. A `Static` object is skipped once the row is
there — `Static` means "load this once", and the row is the record of whether that
has happened, so a table somebody populated by hand is still loaded and a table a
clean load emptied is still skipped.

**What advances it** is a clean success and nothing else. Every load primitive
reports `bookmark_datetime`, the instant *it* began, taken by the engine that ran
it — and reports none unless the load was clean.

```text
succeeded, zero rejects   advance to the instant the load reported
succeeded with rejects    unchanged: it has not read its window
failed, error, blocked    unchanged
Static skip               unchanged: a clean success reporting no instant
endpoint refresh          not an object
test, assumption          never
```

A clean load that moved no rows still advances: the row records the window that
was read, not whether rows moved.

### `_.LoadStatus`

```text
[Item type]              the Registry's identity, exactly
[Item name]
[Schema name]
[Object name]
[Workflow ID]            the workflow whose load produced this state
[Result]
[Started datetime]
[Completed datetime]
[Duration milliseconds]
```

How each loadable object's most recent load ended. Current state, on the same key
as the bookmark: an absent row means no load has settled since the object was last
built.

Logical identity only. Where the object is physically installed is the
Installation's to say, and a status row that duplicated it would give a reader two
places to disagree.

### `_.LoadStatistic`

```text
[Load statistic SK]      a meaningless immutable surrogate
[Workflow ID]
[Item type]              the Registry's identity, exactly
[Item name]
[Schema name]
[Object name]
[Started datetime]
[Completed datetime]
[Duration milliseconds]
[Rows read]              what the source produced
[Rows inserted]          what the load did with it
[Rows updated]
[Rows deleted]
[Rows rejected]
[Is reload]              false until reload is available
[Is static skip]
```

What each load did, as counts, appended so an object's load history accumulates
across rebuilds. The counts describe the *target*, not the source: `Rows read` need
not equal the sum of the others, because an unchanged row is read and neither
inserted nor updated.

A row is written for a load that **executed**. A blocked node did nothing, and a
row of zeroes for it would read as a load that moved nothing.

`Is static skip` is reported by the engine that skipped rather than inferred from
the counts: a skip and a load that read an empty window both move no rows, and only
the engine knows which happened.

There is no target row count. `Rows read` plus the mutation counts are already
useful, and counting the target would be an expensive engine-specific read taken
for telemetry alone.

### `_.TestStatus`

```text
[Item type]              the validation's own identity
[Item name]
[Schema name]
[Object name]
[Test type]              Test or Assumption
[Workflow ID]
[Result]
[Started datetime]
[Completed datetime]
[Duration milliseconds]
[Failure count]          discrepancy rows, or contradicting rows
```

How each validation's most recent run ended, keyed the way `_.LoadStatus` is.
Rebuilding the validation ends the incarnation and the row goes with it;
rebuilding a loadable object says nothing about it, and the reverse holds too.

`Failure count` is meaningful only for a validation that was evaluated. One that
could not run found nothing, and its count is null.

### `_.Log`

One row per settled unit of Weaver work.

```text
[Log SK]                 a meaningless immutable surrogate
[Workflow ID]            correlates every row one workflow produced
[Task type]              load, test
[Target type]            Lakehouse, Warehouse
[Target name]            the physical item
[Schema name]            the object, where the work was about one
[Object name]
[Result]
[Started datetime]
[Completed datetime]
[Duration milliseconds]
[Message]                one concise line
[Details]                the node's own record, as JSON
```

There is no run row and no completion row: a workflow *is* its rows, and a reader
asking what a run did selects on `[Workflow ID]`. A call by hand is a workflow of
one.

> `_.Log` is operational evidence, not transactional authority for installed
> catalogue state.

A dry run writes nothing at all. A row for work nobody did would be evidence of a
load that never happened, and a bookmark it moved would make the next real load
skip a window nothing had read.

### Reaching them from where work runs

A generated procedure says `[_].[Bookmark]` and writes `[_].[LoadStatus]`, and
authored Spark SQL may read either, so every target holding runnable objects
presents them: the tables themselves in the catalogue Warehouse, a view over the
catalogue's three-part name in any other Warehouse, and read-only OneLake shortcuts
at `Tables/_` in a Lakehouse. Both are gated on what the target physically holds, so
an unchanged repository plans neither and one somebody removed comes back. One
action per target carries every reference the target is missing: they are one
decision about one item, and a round trip per table would be five where one does.

The references read documents the built-in item owns, so the item graph puts that
item first — the same edge a declared shortcut's source item gets. One build creates
the tables and then points at them, in that order.

Ordering is necessary and not sufficient: a Warehouse creates a table in its own
catalogue and publishes the Delta directory behind it a moment later, so a shortcut
can arrive before there is anything to point at. Fabric validates a shortcut's
target, and `weaver.fabric.shortcuts.create_shortcut` waits for a source it has just
been asked to point at — bounded, so a source that will never appear still fails.
They are Weaver infrastructure rather than authored declarations, but publish the
same `_.Shortcut` producer pair and `_.Registry` certification. That is what lets
load planning reconstruct the relation after the source repository is gone. Their
physical views and OneLake shortcuts are rendered by the same code as declared
shortcuts.

**Every write through a view is a MERGE**, including the appends. Fabric accepts
`SELECT`, `UPDATE`, `DELETE` and a `MERGE`'s insert through a cross-database view
and refuses a plain `INSERT`, so an appended row merges on a surrogate generated a
moment ago and is never matched.

### Authored Python

An object is freestanding or catalogue-anchored:

```python
My__Table(spark)  # freestanding
My__Table(spark, catalogue="Warehouse/Weaver")  # anchored
```

Every authored object takes it, Tests and Assumptions included. Anchoring resolves
the catalogue and the object's installed identity once, at construction; a name that
resolves to none or to more than one is a `ConfigError` there. A loadable object is
resolved through `_.Installation` and `_.Registry`; a validation through
`_.Installation` and `_.TestDictionary`, because a validation materialises nothing
and what Registry certifies is the module or procedure it compiles to.

Anchoring **adds** the operational interface and takes nothing away. A freestanding
object still reads, which is what lets an author call `read()` and look at what came
back. `load()` and `run()` need the catalogue and say so.

`catalogue=` takes the name of the Warehouse, or a `Catalogue` already read. Named,
it opens the Session it reads and writes through and owns it — closing the catalogue
closes that Session. Handed one, it reuses whatever that one already has and closes
nothing.

### They are never dropped

Every catalogue table holds state no declaration reproduces, so all of them declare
`Prohibit rebuild`, the managed-drop renderer refuses one, and prune spares them —
asked of a *table* and not of a view, since a runtime table is the catalogue's own in
one Warehouse and a reference to it everywhere else.

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
