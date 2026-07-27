# Where the Weaver workspace declaration lives

A control-plane Lakehouse exposes one fixed declaration root:

```text
Control/Files/weaver_items/
```

One control plane holds one declaration. A Fabric workspace may hold several
control-plane Lakehouses—for example, one per developer—and each may bind the
same logical items differently.

## Item-owned layout

The first directory is the item type and the second is the logical item name:

```text
Files/weaver_items/
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

The built-in `Lakehouse/_weaver` item is generated and managed by Weaver inside
this same tree. It declares the ten catalogue tables; authored changes to it are
replaced or rejected.

## Build binds logical items

The workspace declaration is immutable build input. A build supplies any non-empty
set of logical-to-physical bindings:

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
  --bind Lakehouse/Raw=Raw_Dev \
  --bind Warehouse/Reporting=Reporting_Dev \
  --host Development --hosts env.yml
```

From Python inside the target environment, use `build_item_repository()` for the
ordinary coordinated path. It copies `Files/weaver_items` once to a
session-local temporary directory, reads and plans there, installs from a local
bundle and removes the working files. The generated bundle contains a certified
repository snapshot, so installation never reopens or reinterprets the source
repository. Persisting a `.weaver.zip` archive is optional and intended for a
record or handover rather than the normal development build.

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

- [CLI usage](cli-usage.md) — hosts, build, wipe and capacity
- [Architecture summary](../backlog/weaver-architecture-summary.md) — the full model
- [Agent guide](../AGENTS.md) — implementation invariants
