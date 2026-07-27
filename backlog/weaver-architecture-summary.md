# Weaver Workspace, Item and Document Architecture

## Status

This is the accepted and implemented architecture for the
repository/item/document re-architecture. The item model is the public and CLI
surface; the earlier flat planner remains only as isolated compatibility code.
Final end-to-end consolidation is tracked in
[the re-architecture checkpoint plan](weaver-repositories-items-documents-checkpoints.md).

[`docs/journal.md`](../docs/journal.md) remains the record of implementation
decisions and empirical Fabric behavior. Where this summary and the journal
differ, the journal is authoritative.

---

## 1. The mental model

Weaver is a control plane for declaring the logical contents of a Microsoft
Fabric workspace:

```text
Fabric workspace declaration
└── Weaver items
    └── Weaver documents
```

A **Weaver source repository** is the development and certification unit. Once
uploaded, its directory name is not logical identity: one control plane exposes
one fixed workspace declaration at `Files/weaver_items`.

A **Weaver item** is a logical Fabric item owned by that repository. The initial
item types are:

```text
Lakehouse
Warehouse
```

A **Weaver document** is an authored declaration owned by one item. It declares a
Delta table, Spark view, SQL table, SQL view or Lakehouse folder. A schema
declaration is item-owned too, but remains its own document type because it has a
different contract and is expected to carry security policy later.

Folder, Delta, Spark SQL and T-SQL describe materialisation or execution. They are
not deployment targets and they are not architectural tiers.

Semantic Models are outside this re-architecture.

---

## 2. Control-plane cardinality

One Weaver Lakehouse is one control-plane environment and contains one workspace
declaration. Weaver's own catalogue is the built-in `Lakehouse/_weaver` item of
that same declaration.

One Fabric workspace may hold several Weaver Lakehouses. A team may use one per
developer or deployment environment, with each control plane binding the same
logical items to shared or individual physical items. A Fabric Environment may
likewise carry a different Weaver version where necessary; that does not create a
second declaration inside one control plane.

Within one control plane:

- a logical Weaver item has at most one current physical binding;
- at least one item must be bound for a build;
- unbound items are ordinary and usually outnumber bound items;
- physical binding is environment state, never logical identity.

Weaver does not need a special guard against two logical items binding the same
physical item. With prune enabled, each declaration will reconcile that item
against itself and the two will consume one another's objects. `prune=False` is an
explicit escape hatch for someone deliberately sharing a physical item, but this
is not a recommended deployment shape and Weaver does not make it safe.

---

## 3. Repository structure

The first directory level is the singular item type. The second is the logical
item name:

```text
Files/weaver_items/
├── Lakehouse/
│   ├── Raw/
│   │   ├── schemas/
│   │   │   ├── Ref.yml
│   │   │   └── Sales.yml
│   │   ├── Ref__Agency.py
│   │   ├── Files/
│   │   │   ├── Sales__Landing.py
│   │   │   └── Sales__Order.py
│   │   └── lib/
│   │       └── csv_helpers.py
│   ├── Curated/
│   │   ├── schemas/
│   │   │   └── Sales.yml
│   │   └── Sales__Customer.py
│   └── _weaver/                  generated and Weaver-managed
├── Warehouse/
│   └── Reporting/
│       ├── schemas/
│       │   └── Sales.yml
│       ├── alias.yml
│       └── Sales.Customer.sql
└── _ignore/
    └── unfinished authored work
```

The ordinary authored directories are:

| Directory | Meaning |
|---|---|
| item root | Delta, Spark SQL or Warehouse documents |
| `Files/` | Folder documents belonging to a Lakehouse item |
| `schemas/` | One schema declaration per file, owned by the item |
| `lib/` | Python support modules, not Weaver documents |

`_ignore/` is the only ignored directory. Its complete subtree is absent from
discovery, installation and the repository signature. It is a keyword for
parking work that is not ready to become part of the repository contract.

No other leading-underscore convention exists. A path such as `_draft` or
`_helpers` passes through ordinary discovery and validation. Known editor,
virtual-environment and bytecode exclusions are not a second authoring
convention; repository content should not depend on them.

Repository authors do not create `__init__.py`. Weaver supplies package-aware
loading internally. A user-authored `__init__.py` is ordinary input and is
rejected by the repository contract rather than executed as hidden package code.

---

## 4. Logical identity

An item is identified by exact-case:

```text
ItemType/ItemName
```

An object or schema is identified by exact-case beneath that item:

```text
ItemType/ItemName/Schema
ItemType/ItemName/Schema.Object
ItemType/ItemName/Files/Schema.Object
```

Examples:

```text
Lakehouse/Raw/Sales
Lakehouse/Raw/Sales.Customer
Lakehouse/Raw/Files/Sales.CustomerCsv
Warehouse/Reporting/Sales.Customer
```

Within the current item, the short forms are:

```text
Sales
Sales.Customer
Files/Sales.CustomerCsv
```

Short names resolve from the owning item root, never from the referring file's
filesystem directory. A Folder document therefore still writes
`Files/Ref.Lookup` to name another Folder and `Ref.Lookup` to name a table-style
document.

Logical identity and every logical reference are case-sensitive. A casing mismatch
is an unresolved reference and is an error. Declarations that differ only by case
are nevertheless rejected, because some physical targets collapse their names and
could not materialise both safely.

The source repository's local directory name is not logical identity and is not
stored in the catalogue. The complete source still has a repository signature for
bundle certification.

The physical binding is separate:

```text
logical:  Lakehouse/Curated
physical: Curated_LH
```

Physical Fabric item names never appear in logical identity or `alias.yml`.

---

## 5. Item ownership

A Lakehouse item owns both of the Fabric areas it presents:

```text
Lakehouse item
├── Tables
│   ├── Delta tables
│   └── Spark views
└── Files
    └── Folder objects
```

Folder is therefore a Weaver document and managed object kind, not a target.
`FolderTarget`, a separate folder binding and folder-specific installation scope
do not exist in the target architecture.

Because `Files/` is a distinct logical namespace, these may coexist:

```text
Lakehouse/Raw/Sales.Order
Lakehouse/Raw/Files/Sales.Order
```

They have different source paths, logical identities and Python module identities,
while sharing the one `Lakehouse/Raw` physical binding.

Each item owns its own `schemas/` declarations. The same schema spelling in two
items denotes two declarations:

```text
Lakehouse/Raw/Sales
Lakehouse/Curated/Sales
Warehouse/Reporting/Sales
```

There is no repository-global schema declaration.

---

## 6. Bindings and partial builds

A build supplies logical-item bindings:

```text
Lakehouse/Raw       -> Raw_LH
Lakehouse/Curated   -> Curated_LH
Warehouse/Reporting -> Reporting_WH
```

The precise CLI syntax is an adapter concern. The core contract is a mapping from
one declared logical item to one typed physical Fabric item.

Bindings are sparse by default. A developer working on one domain may bind only
that domain and assume the rest of the repository is static. At least one binding
is required.

Projection is by exact owning item, not merely by `lakehouse` or `warehouse` kind:

- documents owned by bound items are eligible for the build;
- documents owned by unbound items are not built or recertified;
- a dependency on an unbound item does not by itself omit the bound consumer;
- the catalogue records the dependency as the consumer declared it;
- a declared-schema Python object can have its structure built without running or
  locating its producer, so operational failure is deferred to load;
- an action that genuinely requires an existing producer at build time may still
  fail honestly at the target engine.

A multi-item build is one coordinated unit. Physical actions follow the global
dependency graph; catalogue dictionaries, installation records and registry
publication form one tail after all retained physical work. A failure before that
tail leaves the prior certification in place and is visible as partial physical
execution in the installation report.

---

## 7. References and dependencies

Two-part references are logical and item-relative:

```text
Sales.Customer
Files/Sales.CustomerCsv
```

Three- and four-part SQL names are physical declarations written by the author.
Weaver preserves them. This is the low-ceremony route: a coordinated build may
work because an authored three-part name matches a physical item binding, at the
cost of being locked to that environment.

Portable cross-item lookup uses a repository alias. A dependency row belongs to
the consumer item and records the reference exactly as the consumer declared it:

- two-part logical names remain two-part;
- three- and four-part physical names remain as authored;
- `is_within_repository` becomes `is_within_item`.

An unbound producer is treated as static for projection. Weaver does not pretend to
have built or certified it in that build.

### Metadata references

The canonical identity grammar is accepted by:

- descriptions;
- lineage;
- column notes;
- foreign keys;
- generated documentation hyperlinks.

It is not added to `Dependencies` in this re-architecture; portable dependency
names are expressed through the consumer item's alias namespace.

All logical metadata references must resolve exactly, including case. An
unresolved reference is a repository error. Description copying remains metadata
reuse only: it creates no dependency, alias, inheritance or physical relationship.

Column references retain the existing bracket suffix and literal dollars retain
the existing escape:

```text
$Lakehouse/Raw/Sales.Customer[CustomerId]
$$not-a-reference
```

---

## 8. Item aliases

An alias is a name one item wants for a document another item owns, so it is
declared in the consuming item's own `alias.yml`. The file's location names the
destination item; nothing in the file repeats it.

Inside `Warehouse/Reporting`:

```yaml
aliases:
  Sales.Customer: Lakehouse/Curated/Sales.Customer
```

The mapping is deliberately **destination keyed**:

```text
this item's local Schema.Object -> canonical four-part source
```

That is declarative: the consumer states what must exist in its own namespace.
Every destination has exactly one source, while one source may appear at several
destinations, in as many items as want it. Both sides use canonical exact-case
logical identity. Aliases may cross any item types and never contain physical
Fabric names.

An item's `alias.yml` certifies with that item's other source files, so adding an
alias changes exactly one item signature.

Aliases participate in repository validation, dependency resolution, the logical
graph and catalogue projection. `_.Alias` reproduces the `alias.yml` declarations.
Python imports may resolve to an alias destination even when no source document
exists at that path, because declared-schema Python structure does not use the
producer until load.

Physical alias behaviour is **not implemented by this re-architecture**. A bundle
whose retained physical work uses an alias fails explicitly with:

```text
NotImplementedError: Alias usage is not yet supported
```

It must never silently bind the two-part name to the consumer's physical item.
Materialising shortcuts, rewriting cross-item physical names and establishing
cross-engine refresh barriers are later work.

---

## 9. Python package semantics

Python object dependencies remain imports, analysed statically without executing
the module:

```python
# Delta document -> Folder document in the same Lakehouse item
from .Files.Sales__Landing import Sales__Landing

# Folder document -> Delta document in the same Lakehouse item
from ..Sales__Customer import Sales__Customer

# Helper import, not a graph edge
from .lib.csv_helpers import read_csv
```

For a Folder document, the helper import uses `..lib`. Imports that resolve under
`lib/` are implementation imports, not Weaver dependencies.

Weaver supplies a repository-qualified package context internally, so two
repositories or two execution contexts cannot collide in `sys.modules`. The
private root name is not part of the authored contract; relative imports are.
Documents are never executed as standalone files.

Cross-item portable imports resolve through destination entries in `alias.yml`.
Their runtime accessor/module behaviour lands with physical alias support; this
checkpoint establishes only static identity, graph and catalogue behaviour.

---

## 10. The central catalogue

The catalogue remains in schema `_` of the Weaver Lakehouse and remains the one
authority for the control plane. The ten-table machinery, generated DML,
signatures, registry-last ordering and tolerant reading are preserved.

Installation scope is exactly:

```text
item_type, item_name
```

The physical target name remains an installation attribute. Rebinding an item
updates its current installation row after a successful build. The old physical
item is left untouched and is later wiped only by naming that physical item
explicitly; the catalogue does not retain installation history merely to find it.

Certification separates at three levels. The repository signature covers the
complete source and certifies the coordinated plan and snapshot. Each item has a
signature over its own identity, schemas, documents, support files and aliases it
consumes; `_.Installation` records that item signature, so an unrelated item edit
does not make every installation appear stale. Registry and dictionary signatures
remain document- or declaration-specific.

Object dictionaries add `schema_name, object_name` beneath the item scope. Folder
and Delta rows share the same Lakehouse installation scope; a Folder's schema is
stored as `Files/<declared-schema>`, so the four catalogue identity columns remain
enough without an `object_namespace` dimension.

`_.Alias` reproduces the destination-keyed entries of `alias.yml`. `_.Dependency`
is scoped to the consumer item, keeps two-part logical references two-part and
keeps authored physical names as written. Cross-item composition is obtained by
joining the dependency's consumer-facing name through `_.Alias`.

The built-in `Lakehouse/_weaver` item declares the catalogue itself and is built
through the ordinary repository, planner and installer path. It is bound to the
control-plane Weaver Lakehouse.

Catalogue schema migration is deliberately deferred. During this early stage the
catalogue is destructively rebuilt from the repository when its representation
changes. It becomes durable only when incremental build starts depending on its
history.

`weaver.catalogue` exposes this item-scoped representation. The earlier flat
planner's repository/target catalogue is isolated in `weaver.catalogue.legacy`
only as a temporary compatibility seam and is not part of the public architecture.

---

## 11. Build, prune, wipe and rebind

Build retains the governing contract in
[`docs/build-philosophy.md`](../docs/build-philosophy.md): interpret once, freeze a
complete bundle, keep installation mechanical, create structure rather than load
data, and make every destructive action reviewable.

Prune is part of a build. For each bound item it compares the physical item's
visible structure with the documents retained for that item and freezes explicit
drop/delete actions for everything not declared. A Lakehouse prune reconciles both
Tables and Files as parts of one item. `prune=False` emits no such actions.

Wipe is a separate, deliberately blunt physical operation. Wiping a Lakehouse
clears both Tables and Files; wiping a Warehouse clears its supported user
objects. It does not mean "remove what this logical item catalogued", and rebinding
does not cause an automatic wipe of the old target.

The Weaver Lakehouse is not implicitly in either operation. It is reached only
when explicitly selected as the physical target; setup continues to protect its
catalogue schema and never prunes application content merely because it shares the
control-plane item.

---

## 12. Execution model

The Spark session is attached to the Weaver Lakehouse. That is the fixed control
plane context. Destination items are a variable data plane and every action names
its bound physical item explicitly.

Generation and installation both run in the target environment:

| resources | code runs | path |
|---|---|---|
| local emulator | local process | development and most tests |
| Fabric | Fabric session | product path; notebook or Livy submission |

Desktop CLI and Fabric tests may cross the boundary using REST, DFS and Livy, but
core never silently substitutes a desktop client for the session-native path.

Inside Fabric, the session makes one recursive copy of `Files/weaver_items` into
a driver-local temporary directory. Discovery, validation,
signatures, planning, snapshot generation and direct installation then use local
files. No persisted bundle is required for the normal development path. A caller
may subsequently preserve the complete bundle as one deterministic
`<timestamp>.weaver.zip` for audit or handover; a receiving session copies and
extracts that archive locally before validation and installation.

One bundle may contain batches for several Lakehouses, Warehouses and the Weaver
control-plane Lakehouse. Each batch is bound to exactly one physical target and
each action remains mechanical.

---

## 13. Explicitly deferred work

This re-architecture does not implement:

- physical alias materialisation or relation rewriting;
- cross-engine refresh barriers required by physical aliases;
- load execution, merge policy or dependency accessors at runtime;
- Semantic Models;
- catalogue migration or multi-version history;
- making unsafe shared physical-item prune safe;
- multiple workspace declarations in one Weaver control plane.

The immediate outcome is a truthful logical model: one workspace declaration
contains many items, every document is owned by one item, bindings are
item-specific, the graph and catalogue carry that identity, and unsupported alias
execution fails before mutation.
