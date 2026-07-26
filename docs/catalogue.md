# The central catalogue

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

A repository is installed independently into its Lakehouse and its Warehouse, and
the same `Schema.Object` legitimately exists in both — a Delta table and a
Warehouse table of one name are two objects in two places. So every catalogue row
is keyed on `repository` and `target_type` before anything else:

```text
SalesRepo | lakehouse | Sales | Customer
SalesRepo | warehouse | Sales | Customer
```

Those are two rows and both are real. A Lakehouse-only build reconciles only the
first; it must not touch the second, and cannot, because there is no way to name a
row without naming the installation it belongs to.

An object left out of a build because its target was not bound is **out of
scope**, not deleted. A Lakehouse build has no opinion about the Warehouse
installation — which is a different thing from having removed it.

The bound item's name is an attribute, never identity. Rebinding a repository to a
different Lakehouse **updates** its `_.Installation` row; it does not add a second
installation.

## The ten tables

Every table carries `signature` — the content hash of whatever the row projects —
plus Weaver's audit columns (`row_insert_datetime`, `row_update_datetime`,
`row_delete_datetime`), which the ordinary build appends to any Delta table.

| Table | One row per | Notes |
|---|---|---|
| `_.Installation` | repository + target type | The item currently bound, and the Weaver version that last reconciled it. |
| `_.Registry` | installed object | What Weaver certifies. `object_type` is folder, table or view; `object_role` is `data` today and `load` when stored procedures arrive. |
| `_.SchemaDictionary` | schema in use | Only schemas the installation actually uses. |
| `_.TableDictionary` | table or view | Tables and views together — they are described the same way. Keys, behavioural flags, description and lineage. |
| `_.FolderDictionary` | managed folder | Keeps the folder's two-part identity, and its file key — the scope of what Weaver manages inside it. |
| `_.ColumnDictionary` | described column | Purely descriptive: the columns an author wrote a note about, plus Weaver's surrogate. Not every column. |
| `_.IndexDictionary` | logical key | The primary key and any alternate keys. Nothing is built. |
| `_.ForeignKeyDictionary` | declared relationship | An ER model, not constraints. |
| `_.Dependency` | resolved edge | Three-part logical reference, inheriting the owner's target type. |
| `_.Alias` | cross-engine publication | Where the graph crosses engines. |

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

A dependency may leave the repository. A three-part reference names a physical
item deliberately, and is recorded with `is_within_repository` false — the first
part is an item, not a repository, so nothing here resolves it.

## Weaver builds its own catalogue

`weaver/builtin/catalogue/` is an SES repository shipped as package resources.
Setup materialises it into the Weaver Lakehouse and the **ordinary** planner and
installer build it. There is no second "create the control tables" path, and that
recursion is the point: if the catalogue needed privileged machinery to exist, the
claim that a catalogue table is an ordinary Weaver object would be false.

```python
from weaver import ItemRef, initialise_weaver_lakehouse

result = initialise_weaver_lakehouse(
    weaver_lakehouse=ItemRef("Weaver"),
    host=host,
    store=store,
    spark=spark,          # a Fabric notebook already has one
)
```

One bundle does the whole bootstrap, because the barriers already order it:

```text
sequence   20   create schema `_`
sequence   40   create the ten catalogue tables
sequence 9000   describe them in their own dictionaries
sequence 9010   record the installation
sequence 9020   certify them in their own registry
```

The catalogue's own DML runs after the tables it writes to exist, so no first-run
mode is needed. What *is* needed is a reader that tolerates absence, since planning
reads a catalogue that is not there yet.

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
widened by a failed read, which is what build-philosophy §6 exists to prevent. The
catalogue *is* read at plan time — to report what will change, so a reviewer can
see the effect before it runs — and a session that cannot read it degrades the
report rather than the correctness.

Ordering is the one strict invariant:

1. **dictionaries** describe what was built;
2. **`_.Installation`** records which item the repository is bound to;
3. **`_.Registry`** certifies.

Registry is last and is its own barrier, so a row in it cannot outrun the work it
attests to. Any earlier failure — physical, dictionary, or the installation
record — stops the install before anything is certified. Dictionaries need no
all-or-nothing transaction: partial state is repaired by the next successful
build's ordinary row comparison.

An unchanged row is a genuine no-op. The merge's `MATCHED` branch is guarded by a
comparison of every non-key column, so rebuilding unchanged SES writes nothing and
does not move `row_update_datetime`.

## Removing things

Three scopes, and they are deliberately different operations:

| Scope | What it removes | Reached from |
|---|---|---|
| object | rows no longer projected, within one installation | every build |
| installation | one `(repository, target_type)` entirely | decommissioning a target, explicitly |
| repository | every installation of one repository | repository lifecycle, explicitly |

Only the first is part of a build. A build that did not include a target type has
no opinion about it, so nothing in the build path can reach installation prune.

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

## What this branch does not do yet

Build still emits `CREATE OR REPLACE TABLE`, so a re-run of **setup** empties the
catalogue tables before repopulating the built-in repository's own rows — and any
other repository's rows go with them. Only setup rebuilds schema `_`; an ordinary
build never does.

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
