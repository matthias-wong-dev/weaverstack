# Weaver repository sources

## Purpose

This document defines the authored repository layout and the relationship
between logical items, source files, and bound targets.

A Weaver repository has no mandatory workspace location. A build receives its
source explicitly. Supported sources are a local checkout, a Fabric Notebook's
Resources directory, or an accessible OneLake location. Remote trees are copied
to driver-local temporary storage before static parsing.

The catalogue Warehouse holds catalogue state and nothing else — it has no
Files area to hold a repository in. A tree can be copied into any Lakehouse's
Files through `weaver.push.push_item_repository`, which the Fabric suite uses to
stage one; build does not read that location implicitly.

## Item-owned layout

The first directory is the item type and the second is the logical item name:

```text
repository/
├── Lakehouse/
│   ├── Raw/
│   │   ├── schemas/
│   │   │   └── Sales.yml
│   │   ├── Sales__Customer.py
│   │   ├── Files/
│   │   │   └── Sales__Customer.py
│   │   └── lib/
│   │       └── csv_helpers.py
│   ├── Curated/
│   │   ├── schemas/
│   │   │   └── Sales.yml
│   │   ├── shortcuts.py                Fabric shortcuts this item declares
│   │   └── Sales.Rollup.sql            Spark SQL — it is in a Lakehouse
│   └── _weaver/                        refused: this item is Weaver's own
├── Warehouse/
│   └── Reporting/
│       ├── schemas/
│       │   └── Sales.yml
│       ├── shortcuts.yml               shortcuts this Warehouse declares
│       ├── programmables/
│       │   └── dbo.RefreshSummary.sql  a stored procedure this item manages
│       └── Sales.Customer.sql          T-SQL — it is in a Warehouse
├── Notebooks/                          unrelated project content is ignored
├── README.md
└── _ignore/
    └── unfinished.py                   explicitly excluded content
```

Each schema and source document belongs to exactly one item. `Files/` contains
Folder documents owned by a Lakehouse; it is not a separate deployment target.
`lib/` contains Python helpers for that Lakehouse item. Discovery selects the
`Lakehouse/` and `Warehouse/` trees and the recognised surfaces inside each
item. Unrelated siblings and unrelated item content are absent from discovery
and the repository signature. `_ignore/` explicitly excludes content that would
otherwise be recognised. A misplaced Weaver declaration and a malformed
recognised surface are refused. Authors do not add `__init__.py`; Weaver supplies
package loading.

## The item chooses the SQL dialect

A SQL document is `Schema.Object.sql`. There is no dialect suffix, because the
containing item already decides: a Lakehouse materialises Delta through Spark,
a Warehouse materialises through T-SQL.

```text
Lakehouse/Raw/DWG.Customer.sql          Spark SQL
Warehouse/Reporting/DWG.Customer.sql    T-SQL
```

## Shortcuts

An item reaches outside itself by declaring what it wants to see. A Lakehouse
declares its shortcuts in `shortcuts.py`, a Warehouse in `shortcuts.yml`. Both
files sit at the item root, and both are declarations rather than programs. Only
the physical form differs: a Lakehouse shortcut is a OneLake shortcut, a
Warehouse one is a view.

### Lakehouse: `shortcuts.py`

```python
from weaver import Shortcut

Sales__Customer = Shortcut(
    shortcut_type="table",
    target_type="logical",
    target="Lakehouse/Sales/Sales.Customer",
)

Reference = Shortcut(
    shortcut_type="schema",
    target_type="physical",
    target="Lakehouse/Reference/Sales",
    workspace="Shared Data",
)

Landing__Incoming = Shortcut(
    shortcut_type="folder",
    target_type="physical",
    target="Lakehouse/Landing/Files/Incoming",
    workspace="Shared Data",
)
```

The variable name is the destination, spelled as every other document in the item
is: `Schema__Object` for a table or a folder, and the schema name itself for a
schema shortcut. There is no separate destination field.

`shortcut_type` is `table`, `schema` or `folder`, and it decides both paths: a
table sits under `Tables/<schema>`, a schema directly under `Tables`, and a folder
under `Files/<schema>`.

`workspace` names a Fabric workspace, and omitting it means the current one.

`target_type` decides what the item half of `target` means, and is exactly
`logical` or `physical`. A logical target is a Weaver-managed object—an authored
document or another declared shortcut destination—and Weaver follows its current
binding before creating the shortcut; it must be in this repository, it orders
the two items, and it cannot also name a workspace. This permits a physical
cross-workspace shortcut to be projected onward through a same-workspace logical
shortcut. A physical target
is the Fabric item itself, and may name a workspace. A schema shortcut is
physical only: a schema's contents belong to the item it points at, and Weaver
binds objects rather than namespaces.

The file is parsed, never executed, so it holds `Shortcut` declarations, imports
and comments only. A computed argument, a loop or a conditional is refused rather
than ignored.

The same names are importable from the item's own programs, because a build
deploys a generated module of the same name beside them:

```python
from shortcuts import Sales__Customer, Reference, Landing__Incoming

Sales__Customer(self).dataframe()
Reference(self).Product.dataframe()
Reference(self).table("Product Detail").dataframe()
Landing__Incoming(self).path()
Landing__Incoming(self).spark_path()
```

A schema shortcut names its tables when they are read rather than generating a
symbol for each, because what is inside one can change without a build.
Attribute access is the ordinary form and `table(name)` stays for names that are
not Python identifiers. If you want a table named at build time, declare a table
shortcut.

### What a program reads through a shortcut

Data comes through the shortcut in the declaring item, whatever the target type.
A logical declaration also carries the Weaver document it named, so the same
object answers what Weaver records about the source:

| method | table | folder | needs a logical target |
|---|---|---|---|
| `dataframe()` | yes | | no |
| `empty_dataframe()` | yes | | no |
| `path()` | | yes | no |
| `spark_path()` | | yes | no |
| `bookmark()` | yes | yes | yes |
| `files_since(bookmark)` | | yes | yes |
| `latest_files()` | | yes | yes |
| `deleted_since(bookmark)` | | yes | yes |

`bookmark()` is the source object's, so a consumer can ask how far the producer
got. The window an incremental read measures from is its own `self.bookmark()`,
because the boundary is the consumer's last clean load:

```python
class Cur__Event(Table):
    def read(self):
        return shaped(Src__Events(self).files_since(self.bookmark()))
```

The Folder history comes from the source's `_changes` directory, read through
the consuming item's own shortcut path, so the paths returned are ones this item
can open. Only what `_changes` records takes part; see
[Folder loads and changes](weaver-architecture.md#folder-loads-and-changes).

A physical shortcut names a Fabric location. Weaver records nothing about what
is on the far side, so the four methods that need a Weaver document fail and say
which target type answers them.

A shortcut reads. It exposes no `load()`, no `staging_folder()` and no authored
`read()`, and OneLake permitting a write beneath one does not make it a place to
write.

### Warehouse: `shortcuts.yml`

```yaml
logical:
  Warehouse/Reporting/Sales.Customer: Lakehouse/Sales/Sales.Customer

physical:
  Warehouse/Reporting/Sales.ReferenceCustomer: Warehouse/Reference/Sales.Customer
```

Two sections, named for how the target is read, each mapping a destination to
what it points at. The destination is the item's own four-part identity. A
logical target may be an authored document or another item's declared shortcut
destination. A Warehouse shortcut is always same-workspace and always
materialised as a local view, so it carries neither a workspace nor a shortcut
type.

### What a declaration may not do

The destination schema must be declared by the owning item, and a declaration may
not shadow a document that item already declares. Two items may each reference the
same source under their own local names.

**Weaver owns the shortcut and nothing reachable through it.** A Fabric shortcut
is a read-write window into the item it points at: writing beneath one writes into
that item, in that item's workspace, and so does deleting. So nothing may be
declared inside a schema or folder shortcut, and a repository that does is
refused. Removing the shortcut itself is safe and is the only thing Weaver ever
does to one.

Build materialises a Lakehouse declaration as a OneLake shortcut and a Warehouse
one as a view. See [how build works](how-does-build-work.md#4a-shortcuts).

The built-in `Warehouse/_weaver` item is Weaver-owned repository content,
checked into the package and composed into every parsed repository. It declares
the catalogue tables through the ordinary repository readers, exactly as an
authored item would be read; an authored `Warehouse/_weaver` is rejected.

## Build binds logical items

The workspace declaration is immutable build input. A build selects physical
targets and resolves each to one logical item:

```text
Lakehouse/Raw       -> Raw_Dev
Lakehouse/Curated   -> Curated_Dev
Warehouse/Reporting -> Reporting_Dev
```

Unbound items remain out of scope. One coordinated bundle orders retained work
through the repository graph, reconciles each bound physical item, publishes its
item-scoped catalogue rows and certifies Registry last.

From the CLI:

```bash
weaver build \
  ./estate \
  --item Lakehouse/Raw=Lakehouse/Raw_Dev \
  --item Warehouse/Reporting=Warehouse/Reporting_Dev \
  --workspace Analytics --environment Runtime \
  --catalogue Warehouse/Control
```

From Python inside the target environment, `weaver.build(source, items=...)` is
the ordinary source-neutral operation. It copies
a remote source once to a session-local temporary directory when required, parses it,
ensures the catalogue, reads target and catalogue state, reconciles, and
then calls internal planner and installer seams. The generated bundle contains
only frozen outputs — every statement, every deployed file, every hash — and no
copy of the source, so installation cannot reopen or reinterpret the repository
even in principle. `repository_signature` still records which authored state the
bundle was planned from.

## Migrating a flat repository

Flat repositories are not inferred. Discovery fails with concrete instructions:

- move Delta and Spark documents to `Lakehouse/<item>/`;
- move Warehouse documents to `Warehouse/<item>/`;
- move Folder documents to `Lakehouse/<item>/Files/`;
- replace `_schemas/` with each owning item's `schemas/`;
- move Python helpers to `Lakehouse/<item>/lib/`;
- replace document-local alias headers with `shortcuts.py` in a Lakehouse item
  and `shortcuts.yml` in a Warehouse one;
- drop the `.spark.sql` suffix — a document in a Lakehouse item is already
  Spark SQL.

This is intentionally explicit. Guessing item ownership from an old target kind
would make physical deployment history part of logical identity.

## See also

- [CLI usage](cli-usage.md) — workspaces, build, wipe and capacity
- [Weaver architecture](weaver-architecture.md) — product behaviour and command lifecycle
- [Agent guide](../AGENTS.md) — implementation invariants

## What Weaver composes in

A parsed repository carries more than what was authored, and composition has one
path. The authored tree, Weaver's own fragments and generated content are each
read into a repository part, combined through `merge_repository`, and only then
validated, signed and resolved into one repository. The rules: a unique identity
is added, a duplicate identity is refused, identities differing only by case are
refused, and there is no precedence.

**Fragments** are checked-in files under `src/weaver/fragments/`, read through
the same declaration readers as an authored tree:

- `catalogue/` gives the `Warehouse/_weaver` item and its tables, always;
- `standard/Warehouse/` gives schema `_`, and the `_.Load` and `_.Test` entry
  points, for every normal Warehouse item;
- `standard/Lakehouse/` gives schema `_`, and, only while the item has load code,
  `Files/___Load.py`, the folder that code is deployed into.

**Generated content** is what follows an item's own declarations: one
implementation procedure per Warehouse table Weaver loads, `_.[Load X.Y]`, and
one per Warehouse validation, `_.[Test X.Y]` or `_.[Assumption X.Y]`.

**The standard catalogue surface** is a set of logical shortcut declarations.
Every normal item presents `_.Installation` and the operational tables under its
own names, so dependency resolution and physical planning see them like any other
shortcut. When an item is bound to the Warehouse holding the catalogue, planning
creates no views back over tables already there; that is physical planning, not a
different logical surface.

`___Load.py` is `_.Load`: a schema of `_` plus the `__` separator. A run of
leading underscores is read as the schema it is, which is why the file can be
named at all.

`_` is therefore the one schema an ordinary item may not author into. Every other
underscore schema is free. `_weaver` declares its own catalogue in `_`, because it
is the item that owns it.

### Programmables

A Warehouse item authors stored procedures under
`programmables/<Schema>.<Procedure>.sql`. Each file becomes a Programmable: a
managed declaration carrying its procedure identity, its text, a signature and a
role. Authored content, generated implementation procedures and the two entry
points are all Programmables behind one lifecycle: discover, validate, sign,
select, install through the ordinary T-SQL executor, register under their own
role, prune when the source stops declaring them.

An authored file's SQL must contain exactly one `create or alter procedure`
statement, creating the procedure its filename names. Exactly one, so what Weaver
installs is what it registers and prunes; `create or alter`, so replacing what is
installed works. The `_` schema stays Weaver's: an authored programmable may not
create into it.
