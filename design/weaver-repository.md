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
│   │   └── Sales.Rollup.sql            Spark SQL — it is in a Lakehouse
│   └── _weaver/
│       └── … generated catalogue sources …
├── Warehouse/
│   └── Reporting/
│       ├── schemas/
│       │   └── Sales.yml
│       ├── alias.yml
│       └── Sales.Customer.sql          T-SQL — it is in a Warehouse
└── _ignore/
    └── unfinished.py
```

Each schema and source document belongs to exactly one item. `Files/` contains
Folder documents owned by a Lakehouse; it is not a separate deployment target.
`lib/` contains Python helpers for that Lakehouse item. `_ignore/` is the only
directory absent from discovery and the repository signature. Authors do not add
`__init__.py`; Weaver supplies package loading.

## The item chooses the SQL dialect

A SQL document is `Schema.Object.sql`. There is no dialect suffix, because the
containing item already decides: a Lakehouse materialises Delta through Spark,
a Warehouse materialises through T-SQL.

```text
Lakehouse/Raw/DWG.Customer.sql          Spark SQL
Warehouse/Reporting/DWG.Customer.sql    T-SQL
```

## Aliases are item-local

An alias is a name one item wants for a document another item owns, so it is
declared in the consuming item's own `alias.yml`. The file's location names that
item, so the declaration only maps the item's local `Schema.Object` to the full
four-part source.

Inside `Warehouse/Reporting/alias.yml`:

```yaml
aliases:
  DWG.Customer: Lakehouse/Raw/DWG.Customer
```

The destination schema must be declared by the owning item, and an alias may not
shadow a document that item already declares natively. Two items may each alias
the same source under their own local names.

An alias also orders the two items: the consuming item is built after the item
that produces the source, and a cycle between items is a repository error. Build
materialises the alias as a OneLake shortcut for a Lakehouse destination and as a
view for a Warehouse one — see
[how build works](how-does-build-work.md#4a-aliases).

The built-in `Warehouse/_weaver` item is generated and managed by Weaver inside
the parsed repository in memory. It declares the catalogue tables and is never
written into authored source; an authored `Warehouse/_weaver` is rejected.

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
  --bind Lakehouse/Raw_Dev=Raw \
  --bind Warehouse/Reporting_Dev=Reporting \
  --workspace Analytics --environment Runtime \
  --catalogue Warehouse/Control
```

From Python inside the target environment, `weaver.build(source, bind=...)` is
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
- replace document-local aliases with the consuming item's own `alias.yml`;
- drop the `.spark.sql` suffix — a document in a Lakehouse item is already
  Spark SQL.

This is intentionally explicit. Guessing item ownership from an old target kind
would make physical deployment history part of logical identity.

## See also

- [CLI usage](cli-usage.md) — workspaces, build, wipe and capacity
- [Weaver architecture](weaver-architecture.md) — product behaviour and command lifecycle
- [Agent guide](../AGENTS.md) — implementation invariants

## Generated declarations

A parsed repository carries more than what was authored. Weaver composes two
kinds of generated document into it and reads them through the same static
readers as authored content, so there is no second parsing path:

- `Warehouse/_weaver` — the catalogue's own tables, always;
- an item's `schemas/_.yml`, and for a Lakehouse `Files/___Load.py` — the schema
  its generated load procedures live in, and the folder its load code is deployed
  into, present only while the item has load code.

`___Load.py` is `_.Load`: a schema of `_` plus the `__` separator. A run of
leading underscores is read as the schema it is, which is why the file can be
named at all.

Because those are generated, `_` is the one schema an ordinary item may not
author into. Every other underscore schema is free — `_weaver` declares its own
catalogue in `_`, because it is the item that owns it.
