# Weaver Architecture

## Overview

This document defines Weaver's product model and durable architecture. It is
authoritative when it differs from implementation detail elsewhere in the
repository.

Weaver is a declarative framework for authoring, building, and deploying
Microsoft Fabric workspaces. Developers describe a workspace's logical
structure in a repository; Weaver manages dependencies, metadata, orchestration,
deployment, and lifecycle operations.

A fundamental design goal is that developers continue to write ordinary
executable SQL and Python. Weaver adds structure around those assets without
introducing template languages or domain-specific scripting.

---

# Design Principles

Weaver is built around several core principles.

- **Logical before physical.** Source code describes the logical workspace rather than physical deployment targets.
- **Repository as source of truth.** The repository completely describes the intended Fabric workspace.
- **Natural development.** Developers write ordinary SQL and Python.
- **Deterministic deployment.** Every deployment is generated into an explicit execution plan before installation.
- **Ownership.** Every deployed object has a known logical owner.
- **Metadata first.** Deployment metadata is continuously maintained, enabling dependency analysis, orchestration and incremental build.

---

# Repository Structure

A Weaver repository represents an entire Microsoft Fabric workspace.

The repository is organised around **Weaver Items**, which are the logical declaration of Fabric Items.

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
│   │   └── Sales.Rollup.sql
│
├── Warehouse/
│   └── Reporting/
│       ├── schemas/
│       │   └── Sales.yml
│       ├── shortcuts.yml
│       └── Sales.Customer.sql
│
└── _ignore/
    └── unfinished.py
```

Each directory beneath an item type declares a single Weaver Item.

For example,

```
Lakehouse/Raw
```

declares a logical Lakehouse named **Raw**.

Likewise,

```
Warehouse/Reporting
```

declares a logical Warehouse named **Reporting**.

These declarations are logical. Deployment binds them to physical Fabric Items.

---

# Weaver Documents

Within each Weaver Item are one or more **Weaver Documents**.

A Weaver Document is the logical declaration of a Fabric object authored within that item.

Examples include:

| Document | Represents |
|----------|------------|
| `Sales__Customer.py` | Python-authored Delta table |
| `Sales.Rollup.sql` | Spark SQL object |
| `Sales.Customer.sql` | T-SQL object |
| `schemas/*.yml` | Schema declarations |
| `shortcuts.py` | Fabric shortcuts a Lakehouse declares |
| `shortcuts.yml` | Shortcuts a Warehouse declares, materialised as views |
| `Files/...` | Lakehouse Files content |

Directories such as `lib/` contain ordinary Python modules shared by authored documents.

They are helper code rather than Weaver Documents.

Directories under `_ignore/` are excluded from Weaver.

Every recognised Weaver Document has a defined contract understood by the framework.

---

# Logical and Physical Models

The repository describes a logical workspace. Deployment binds its items to
physical Fabric resources, so one logical Warehouse can use different physical
items in different environments.

```
Logical Warehouse

↓

Development Workspace
    Sales_DEV

↓

Test Workspace
    Sales_TEST

↓

Production Workspace
    Sales
```

The logical declaration remains unchanged while its binding varies. The same
repository can therefore deploy to development, test, and production workspaces
without modification.

---

# Build

The build process transforms logical Weaver Documents into deployment artefacts.

Each Weaver Document has a defined contract describing the artefacts it generates.

For example,

```
Sales__Customer.py

↓

Generates

• Delta DDL
• Runtime metadata
• Catalogue metadata
• Endpoint refresh operations
```

A SQL document may generate:

```
Sales.Customer.sql

↓

Generates

• CREATE VIEW
• CREATE TABLE
• CREATE PROCEDURE
• Dependency metadata
• Catalogue metadata
```

Developers author Weaver Documents. Weaver generates deployment artefacts from
those documents.

---

# Build Bundle

Generated artefacts are collected into a **Build Bundle**.

The Build Bundle describes the work required to realise the logical repository.

```
Repository

↓

Build

↓

Build Bundle

↓

Installation
```

The Build Bundle may contain:

- SQL DDL
- Generated files
- Stored procedures
- Metadata updates
- Catalogue updates
- Endpoint refresh operations
- Other deployment actions

Build does not deploy resources. The Build Bundle is its deployment plan.

Because every deployment action exists explicitly within the bundle:

- builds are deterministic
- installations are repeatable
- execution can be inspected
- deployment is independent of source generation

---

# Installation

Installation executes the Build Bundle.

Unlike the build stage, installation performs no generation or discovery.

Its responsibility is to apply the generated artefacts to the target Fabric workspace.

```
Repository

↓

Build

↓

Build Bundle

↓

Install

↓

Fabric Workspace
```

Separating build from installation keeps generation and physical execution
separate.

---

# Weaver catalogue

Each workspace names a Warehouse whose `_` schema stores the catalogue
describing its deployed workspace. It is Weaver's operational metadata store.

The catalogue records information such as:

- object identity
- ownership
- signatures
- dependencies
- bindings
- deployment timestamps

The catalogue supports questions such as:

- What objects exist?
- What owns this object?
- What changed?
- What depends on this?
- What requires rebuilding?

without rediscovering the entire Fabric workspace.

---

# Incremental Build

Incremental build is possible because every deployed object has:

- identity
- owner
- signature
- dependency information

When source code changes, Weaver determines the affected objects and constructs a new Build Bundle.

```
Changed Documents

↓

Dependency Analysis

↓

Build Bundle

↓

Installation
```

Weaver rebuilds the required artefacts without requiring developers to maintain
dependency graphs or incremental-build configuration.

---

# Runtime

Build handles deployment and runtime executes the installed code. Developers
continue to write ordinary Python and SQL.

For example,

```python
Sales__Customer(self).current_dataframe()
```

```python
self.delta_history()
```

```sql
SELECT *
FROM Sales.Customer
```

Weaver provides the execution context without a template language.

## Folder loads and changes

A Folder manages only files claimed by its `File key`. `Incremental` is optional
and defaults to `true`, so omission does not make an absent source file a delete.
`Incremental: false` declares that staging is the whole incoming state and lets
ordinary reconciliation remove claimed files omitted from it. `Prohibit rebuild`
is also optional and defaults to `false` for a Folder.

Files staged outside the key are rejected before publication. Weaver preserves
them beneath the sibling `<Folder>_Reject` Folder, retaining their relative
paths. A clean later load removes stale reject evidence. Files placed manually
in the destination outside the declared key are not part of the load and remain
untouched.

Each successful mutation appends one JSON document beneath the managed Folder's
`_changes/` directory. Its UTC filename identifies when the change was committed,
and its insert, update and delete entries are paths relative to the Folder root.
No-op loads append nothing. `_changes/` is Weaver-owned: authored code cannot
stage or delete its contents, and Folder reconciliation never inventories it as
business data.

Three Folder methods report that history. Each returns
`dict[Path, datetime]`: keys are full paths ordinary Python can open, and values
are the UTC datetime Weaver recorded the change, which is a file's update
datetime.

| method | what it returns |
|---|---|
| `files_since(bookmark)` | current files changed after the bookmark |
| `latest_files()` | current files from the newest change that left files in place |
| `deleted_since(bookmark)` | files deleted after the bookmark, and when |

Iterating a result gives the files themselves, so a consumer that needs no
metadata writes `for path in folder.files_since(bookmark)`.

The bookmark an incremental read passes is its own — `self.bookmark()` — and the
two compose into "what has arrived since":

```python
class Sales__Order(Table):
    def read(self):
        export = Sales__OrderExport(self)
        arrived = export.files_since(self.bookmark())
        if not arrived:
            return self.empty_dataframe(), None
        ...
```

`files_since` and `deleted_since` take a timezone-aware bookmark, open only
documents strictly newer than it, and collapse the event stream by logical file
path. A file whose latest event is a deletion appears in `deleted_since` and not
in `files_since`, and its path no longer exists. `latest_files` reads documents
newest first and stops at the first one with a file still in place, so a load
that wrote several files reports all of them and a later deletion-only change
does not become the latest delivery.

These methods report the lifecycle Weaver observed, and there is no filesystem
timestamp fallback. A Folder with no `_changes` history and no file matching its
`File key` returns an empty result. One that holds managed files without a
history raises, because Weaver never saw those files arrive and cannot say when
they changed.

---

# Developer Experience

Weaver supplies orchestration without requiring a separate authoring model.

Developers author:

- Python
- Spark SQL
- T-SQL
- Files
- Metadata

Weaver provides:

- dependency analysis
- orchestration
- metadata management
- incremental build
- deployment
- catalogue
- logging

without requiring additional configuration.

Developers continue to write ordinary code while Weaver provides deterministic
deployment and lifecycle management.

---

# Architectural Layers

Weaver uses three progressively more concrete representations of a solution.

```
Logical

    Weaver Items
    Weaver Documents

↓

Build

    Build Bundle
    Generated Artefacts

↓

Physical

    Fabric Workspace
```

Each layer has a distinct responsibility.

| Layer | Responsibility |
|--------|----------------|
| Repository | Describes the desired logical workspace |
| Build Bundle | Describes the work required to realise that workspace |
| Fabric Workspace | Contains the deployed physical objects |
| Weaver Catalogue | Records deployed state, ownership, dependencies and history |

This separation supports deterministic deployment, dependency management,
incremental build, and multi-environment deployment for ordinary SQL and Python.


# Command-Line Lifecycle

This section describes command responsibilities. Command syntax, options, and
interactive behaviour are documented in [CLI usage](cli-usage.md).

The Weaver CLI exposes the lifecycle of a declared workspace. Each command
operates at a different architectural layer.

```text
wipe

    Remove physical objects from one or more targets.

↓

build

    Bind logical Weaver Items to physical Fabric Items.
    Generate a Build Bundle.
    Install the bundle.

↓

load

    Execute the installed ETL and data movement.
```

| Command | Concern |
|---------|---------|
| `wipe` | Physical state |
| `build` | Deployment and installation |
| `load` | Runtime execution |

---

# Workspace Resolution

Every command executes against a resolved Workspace.

A Workspace is one Microsoft Fabric workspace, and identifies:

- the workspace, by name
- the Warehouse holding the catalogue
- the Fabric Environment carrying the published Weaver, where one is named

It says where the resources are. Where Weaver's own code runs is a Session
question, not a property of the Workspace.

For example:

```bash
weaver build \
    ./estate \
    --workspace-config workspace.yml \
    --bind Lakehouse/Raw
```

or

```bash
weaver build \
    ./estate \
    --workspace MyWorkspace \
    --catalogue Warehouse/Weaver \
    --bind Lakehouse/Raw
```

The repository does not contain deployment-specific information. The Workspace
determines where logical declarations are deployed.

`--environment` names the Fabric Environment `weaver fabric environment publish`
published to, and is needed by the commands that run Weaver-authored Python
inside Fabric: `load` and `test`. A build and bundle installation execute
frozen SQL and bundle payloads, so neither needs it.

---

# wipe

`wipe` removes one or more physical deployment targets.

```bash
weaver wipe \
    Lakehouse/Raw \
    --workspace-config workspace.yml \
```

The command accepts multiple targets.

```bash
weaver wipe \
    Lakehouse/Raw \
    Lakehouse/Curated \
    Warehouse/Reporting \
    --workspace-config workspace.yml \
```

Unlike incremental build, a wipe removes all user-created objects from the
selected target, including objects not created by Weaver.

```text
Physical Target

↓

Preview

↓

Confirmation

↓

Remove Objects

↓

Remove Catalogue Bindings
```

By default, Weaver displays the proposed changes before asking for confirmation.
A dry run previews the operation without modifying the workspace.

```bash
weaver wipe \
    Lakehouse/Raw \
    --workspace-config workspace.yml \
    --dry-run
```

For unattended execution, confirmation can be skipped.

```bash
weaver wipe \
    Lakehouse/Raw \
    --workspace-config workspace.yml \
    --yes
```

Physical wipe is independent of catalogue cleanup. Add `--unbind-from Weaver`
when stale claims should be removed immediately; otherwise the next build
reconciles them against physical inventory.

`wipe` does not modify the authored repository.

---

# build

`build` reconciles the declared workspace with the physical workspace.

Each target is supplied using one or more bindings.

```bash
weaver build \
    ./estate \
    --workspace-config workspace.yml \
    --bind Lakehouse/Raw \
    --bind Lakehouse/Curated \
    --bind Warehouse/Reporting
```

Where the logical and physical names differ, an explicit mapping may be supplied.

```bash
weaver build \
    ./estate \
    --workspace-config workspace.yml \
    --bind Lakehouse/Raw_DEV=Raw \
    --bind Warehouse/Reporting_DEV=Reporting
```

The build process performs the following steps.

```text
Repository

↓

Resolve Bindings

↓

Read Weaver Catalogue

↓

Determine Changes

↓

Generate Artefacts

↓

Assemble Build Bundle

↓

Install

↓

Update Catalogue
```

The Build Bundle contains every deployment action required to realise the logical repository.

Typical artefacts include:

- SQL DDL
- Delta table definitions
- Stored procedures
- Files
- Metadata
- Endpoint refresh operations
- Catalogue updates

Although build and installation are architecturally distinct, the CLI performs them as a single operation.

The developer therefore uses a single command.

```bash
weaver build \
    ./estate \
    --workspace-config workspace.yml \
    --bind Lakehouse/Raw
```

---

## Deployment bundles

A Build Bundle may be explicitly retained for a split build/deploy workflow.

```bash
weaver build \
    ./estate \
    --workspace-config workspace.yml \
    --bind Lakehouse/Raw \
    --bundle-only \
    --bundle-path ./dist/raw-bundle
```

The generated directory is the canonical bundle and can later be installed
without rereading source or replanning:

```bash
weaver install ./dist/raw-bundle --workspace-config workspace.yml
```

Normal `weaver build` plans and installs immediately; it does not retain a
bundle as a side effect.

The repository remains the source of truth.

---

# load

Build creates the deployed structures required by the solution.

Load executes the declared data movement.

```text
Build

    Create tables
    Create views
    Create folders
    Install stored procedures
    Install Python runtime
    Update metadata

↓

Load

    Execute ETL
    Transform data
    Advance bookmarks
    Record execution metadata
```

A bookmark is how far an object has been loaded, held in `_.Bookmark` under the
Registry's own four-part identity. Only a clean success advances one, to the
instant the primitive itself reported.

Beside it, a run records how each unit of work ended — `_.Log` and `_.LoadStatus`
for a load, `_.LoadStatistic` for what it moved, `_.TestStatus` for what a
validation found. Which interface was called decides who records: an orchestrated
run records centrally, and `_.Load`, `_.Test`, `Table.load()` and
`Validation.run()` record synchronously for a developer running one by hand. See
[the central catalogue](catalogue.md).

A target-scoped load names the physical Lakehouses and Warehouses it may touch:

```bash
weaver load Lakehouse/Raw Warehouse/Curated \
    --workspace-config workspace.yml
```

Dependencies order loadables only within that explicit target set. Naming one
target is therefore a hard boundary: a dependency does not implicitly add
another Lakehouse or Warehouse. Naming multiple targets permits dependency
edges, including endpoint-refresh barriers, between exactly those targets.

An exact operator-selected load repeats ``--name`` for each installed
``Schema.Object``:

```bash
weaver load Warehouse/Curated \
    --workspace-config workspace.yml \
    --name DWG.Customer \
    --name DWG.Order
```

Name selection runs only those objects. It does not expand or order them through
declared dependencies.

Conceptually, load operates on logical Weaver Documents rather than requiring developers to execute generated stored procedures or notebooks directly.

```text
Logical Load Request

↓

Resolve Binding

↓

Resolve Dependencies

↓

Execute Runtime Artefacts

↓

Record the operational outcome
```

This preserves the same architectural separation used throughout Weaver.

- Developers author logical declarations.
- Build generates deployment artefacts.
- Load executes those artefacts.

## Cross-engine boundaries in a load

A load graph orders two kinds of edge. Most are ordinary dependencies: the
producer finishes and the consumer runs. The rest cross from one Fabric engine to
another, where the producer finishing is not yet the same thing as the consumer
being able to see what it produced. Weaver waits at both of them.

```text
Lakehouse producer  →  Warehouse consumer   the Lakehouse's SQL analytics
                                            endpoint has to catch up
Warehouse producer  →  Lakehouse consumer   the Warehouse's OneLake Delta
                                            publication has to catch up
```

The first is the endpoint refresh barrier: a node of its own in the load graph,
placed behind every selected load in the refreshed Lakehouse.

The second is the OneLake publication wait, in `weaver.run.publication`. Fabric
publishes a Warehouse table's Delta log in the background after the transaction
commits, so a Lakehouse consumer reading that table through a shortcut can see
the previous snapshot, or a new snapshot whose Parquet files it cannot open yet.
The wait proves both: a commit that was not published before the load ran, and an
opened read of each Parquet file that commit added, through the consuming
shortcut's own path. It counts no rows, because Delta answers `count(*)` from the
commit's statistics and would report a snapshot readable before its files are.

The wait is selective. It happens only where the planner found a real Warehouse
to Lakehouse shortcut crossing, and only when the load reported changed rows, so
a Warehouse-only load still submits nothing to Spark.

---

# Typical Development Cycle

A complete rebuild of a development workspace might be:

```bash
weaver wipe \
    Lakehouse/Raw \
    Lakehouse/Curated \
    Warehouse/Reporting \
    --workspace-config workspace-dev.yml \
    --unbind-from Weaver \
    --yes

weaver build \
    ./estate \
    --workspace-config workspace-dev.yml \
    --bind Lakehouse/Raw \
    --bind Lakehouse/Curated \
    --bind Warehouse/Reporting

weaver load \
    --workspace-config workspace-dev.yml
```

For day-to-day development, wipe is usually unnecessary.

The normal workflow is:

```bash
weaver build \
    ./estate \
    --workspace-config workspace-dev.yml \
    --bind Lakehouse/Raw \
    --bind Lakehouse/Curated \
    --bind Warehouse/Reporting

weaver load \
    --workspace-config workspace-dev.yml
```

Because Weaver maintains a complete catalogue of the deployed workspace, each build determines the required changes automatically.

Developers declare the desired state of the workspace.

Weaver determines how to reconcile the physical deployment with that declaration.
