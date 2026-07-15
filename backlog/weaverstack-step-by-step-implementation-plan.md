# Weaverstack Architecture and Step-by-Step Implementation Plan

**Purpose:** Guide a coding agent with no prior context through the construction of `weaverstack`, using the existing sibling `weaver` repository as reference material.

**Reference repository:** `matthias-wong-dev/weaver`  
**Reference snapshot reviewed:** `main` at commit `a97ba8a0b00dd66dff1b2c5e818403694562fd30`

---

# How this plan is intended to be used

The repositories should exist side by side:

```text
workspace/
├── weaver/
└── weaverstack/
```

`weaver` is the existing implementation. It remains available as:

- a behavioural reference;
- a source of proven algorithms;
- a source of existing SES examples and fixtures;
- a guide to Fabric, OneLake, Spark and Warehouse edge cases.

`weaverstack` is a new implementation of the architecture described here.

The coding agent should not treat this document as an instruction to implement everything in one pass. Each numbered checkpoint is a separate unit of work.

For every checkpoint:

1. Read only the relevant section and listed reference files.
2. Discuss the unresolved design choices with Matthias.
3. Implement only the agreed checkpoint.
4. Present the resulting code and observable behaviour.
5. Allow Matthias to examine and revise the design.
6. Do not begin the next checkpoint until explicitly asked.

The reference files identify where existing logic can be found so the agent does not need to search the old repository. They are not an instruction to preserve the old module structure.

---

# Part I — Architecture preface

## 1. Product shape

The distribution is called:

```text
weaverstack
```

The intended Python import remains concise:

```python
import weaver
```

The core framework is usable:

- inside a Microsoft Fabric notebook;
- through Fabric Livy submission;
- from a local Python process;
- through an optional desktop CLI.

The Fabric-native path is primary. Once `weaverstack` is published to PyPI, it can be installed into a Fabric Environment and used without desktop Python.

The optional CLI remains in the same repository and distribution initially, but it must remain an adapter over the core:

```text
CLI ───────────┐
Fabric notebook ├── Weaver core
Livy submission┘
```

The core must never import the CLI.

---

## 2. Host model

A host describes the environment in which named physical targets exist.

There are two initial host types.

### Fabric host

A Fabric host represents one workspace.

It must provide enough information to resolve:

- the workspace;
- Lakehouses by unique workspace item name;
- Warehouses by unique workspace item name;
- Lakehouse item IDs;
- Warehouse SQL endpoints;
- Fabric Environment IDs where required;
- OneLake and ABFSS paths;
- Livy endpoints.

Conceptually:

```yaml
type: Fabric
workspace: I Love Government
environment: Weaver
```

The exact configuration shape should be discussed at its checkpoint. The invariant is that physical item names are unique within the workspace and are the normal names used by Weaver APIs.

### Local host

A local host is a root directory containing filesystem representations of Lakehouses:

```text
.local/
├── Weaver/
│   ├── Files/
│   └── Tables/
├── T0_DWG/
│   ├── Files/
│   └── Tables/
└── T1_DWG/
    ├── Files/
    └── Tables/
```

Local execution does not initially require a local SQL implementation. Folder and Delta behaviour should work locally; Warehouse behaviour is Fabric-only until a separate SQL host is deliberately added.

---

## 3. Physical target model

Weaverstack does not need the old system of separately named database-representation aliases merely to identify Fabric items that already have unique names.

The three build destinations are physical targets.

### Folder target

A Folder target identifies a directory inside a Lakehouse Files area.

Examples:

```text
T0_DWG/Files
T0_DWG/Files/Extracts
Model_Lakehouse/Files/Predictions
```

A Folder object declared as:

```text
Folder ID: Budget.BudgetPaper
```

under a target root of:

```text
T0_DWG/Files
```

materialises at:

```text
T0_DWG/Files/Budget/BudgetPaper
```

Its object-local staging sibling is:

```text
T0_DWG/Files/Budget/BudgetPaper_Staging
```

There is no shared staging area.

### Delta target

A Delta target identifies a Lakehouse:

```text
T1_DWG
```

A Python object declared as:

```text
Table ID: Budget.Expense
```

materialises as the Delta table:

```text
T1_DWG/Tables/Budget/Expense
```

### SQL target

A SQL target identifies a Warehouse:

```text
T2_DWG
```

A SQL object declared as:

```text
Table ID: DWG.AgencyExpense
```

materialises in that Warehouse and receives its generated per-object load stored procedure.

---

## 4. SES repository model

An SES repository is a directory whose Weaver documents are at its root:

```text
SES/
├── Agor__OrganisationCsv.py
├── Agor__Organisation.py
├── Budget__BudgetPaper.py
├── Budget__Expense.py
├── Budget.AgencyExpense.sql
├── DWG.AgencyBudget.sql
└── _helpers/
    ├── organisations.py
    └── budget.py
```

Top-level `.py` and `.sql` files are candidate Weaver documents.

Subdirectories may contain arbitrary helper modules, packages, templates or supporting files. They are installed with the repository but are not themselves discovered as Weaver objects.

Object kind is inferred from the document and metadata:

| Document | Meaning |
|---|---|
| Python with `Folder ID` | Folder materialisation |
| Python with `Table ID` | Delta table materialisation |
| SQL with `Table ID` | Warehouse table plus load procedure |
| SQL with `View ID` | Warehouse view |

Metadata remains YAML embedded in:

- the module docstring for Python;
- the opening `/* ... */` block for SQL.

The repository itself does not impose T0/T1/T2 or any other data architecture. Those are user choices. Folder, Delta and SQL are materialisation forms.

---

## 5. Weaver Lakehouse

A mandatory Weaver Lakehouse is the control plane for one installation.

In Fabric, the Weaver package should normally come from the attached Fabric Environment. The Lakehouse therefore does not need a copied Weaver runtime once PyPI installation is in use.

Its high-level shape is:

```text
Weaver Lakehouse/
├── Files/
│   └── repos/
│       ├── repository-a/
│       │   └── SES/
│       └── repository-b/
│           └── SES/
└── Tables/
    ├── Repository
    ├── RepositoryInstallation
    ├── Catalogue
    ├── Dependency
    ├── TableDictionary
    ├── ColumnDictionary
    ├── IndexDictionary
    ├── ForeignKeyDictionary
    ├── Build
    ├── BuildStep
    ├── Workflow
    └── WorkflowStep
```

The exact table names and schemas should be discussed when the catalogue is implemented.

### Installed source

A repository uploaded or synchronised into:

```text
Files/repos/<repository-name>/...
```

is the source used for build and load.

The full repository tree is retained together so Python objects can share arbitrary helper modules.

### Catalogue meaning

A catalogue row is not merely an inventory entry.

A catalogue row means:

> Weaver currently certifies that this object was built successfully and is safe to use against the currently certified versions of its dependencies.

A physical table or folder may exist without a catalogue row. In that case Weaver does not treat it as valid.

### Build safety

Before rebuilding an object, Weaver invalidates or removes catalogue rows for:

- the objects being rebuilt;
- their managed descendants.

Each object is restored to the catalogue only after that object has built successfully.

For a dependency chain:

```text
A → B → C
```

a rebuild of `A` begins by uncertifying all three.

If `A` and `B` rebuild successfully but `C` fails, the final certified state is:

```text
A certified
B certified
C not certified
```

An optional build record may retain audit history, but it is not the mechanism that determines which catalogue is active.

---

## 6. Build package architecture

Generation and installation are deliberately separate.

### Generate

```python
generate_build_package(
    ses_repository,
    weaver_lakehouse,
    folder_target=None,
    delta_target=None,
    sql_target=None,
    output=None,
)
```

produces a target-resolved package.

By default it may use a temporary directory. A caller may request a persistent local or Lakehouse location.

The package conceptually contains ordered operations for:

1. recording or identifying the build attempt;
2. invalidating catalogue rows for selected objects and descendants;
3. installing the repository snapshot in the Weaver Lakehouse;
4. creating managed Folder destinations;
5. building Delta structures;
6. refreshing affected Lakehouse SQL endpoints in bulk after Delta build work;
7. building Warehouse tables, views and stored procedures;
8. restoring catalogue and dictionary rows for successful objects;
9. recording completion or failure information.

The final package format should be agreed at the relevant checkpoint. It must be inspectable and target-resolved.

### Install

```python
install_build_package(package_directory, host)
```

executes the package in its declared order.

The package should normally contain its own resolved target identities. Requiring callers to supply a second, potentially conflicting set of targets at install time should be avoided.

### Build

```python
build(...)
```

is composition:

```python
package = generate_build_package(...)
return install_build_package(package, host=host)
```

Build logic should not be duplicated outside those two operations.

---

## 7. Initialisation

```python
initialise_weaver_lakehouse(...)
```

uses a built-in SES repository that describes Weaver’s own control-plane tables.

It should use the normal build-package generator and installer as far as practical.

The bootstrap difference is that:

- there is no existing catalogue to invalidate;
- the first control-plane tables must be created before normal catalogue DML can run.

This recursive use of Weaver’s own build machinery is an architectural checkpoint: the same primitives used for user repositories should establish the Weaver Lakehouse itself.

---

## 8. Load architecture

```python
load(
    weaver_lakehouse="Weaver",
    targets=[
        "T0_DWG/Files",
        "T1_DWG",
        "T2_DWG",
    ],
)
```

uses physical target names as a selection.

Weaver:

1. consults the central catalogue;
2. validates that requested targets and objects are certified;
3. expands managed dependencies;
4. orders one global dependency graph;
5. imports Python object code from the installed repository in the Weaver Lakehouse;
6. executes Folder and Delta objects using explicit destination roots;
7. invokes generated Warehouse load procedures through `mssql-python`;
8. records workflow, step, error and CRUD information centrally.

The graph is global rather than target-by-target. Execution may return to the same target in several waves.

### Explicit Lakehouse roots

A target Lakehouse does not need to be attached to the notebook.

The validated Fabric root pattern is:

```python
spark_root = (
    f"abfss://{workspace_id}"
    f"@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}"
)

files_root = f"{spark_root}/Files"
```

This has been proven to list, write and read multiple non-default Lakehouses from one Fabric notebook.

### SQL execution

The validated Fabric Warehouse connection-string pattern is:

```python
connection_string = (
    f"Server={SERVER},1433;"
    f"Database={DATABASE};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
```

`mssql-python` has been proven:

- on macOS with `DefaultAzureCredential`;
- from a Fabric Lakehouse notebook to a Warehouse endpoint.

---

## 9. SQL endpoint refresh during load

Refresh state is workflow-local and tracked per Delta table.

The stable identity is its SQL three-part name:

```text
<Lakehouse>.<Schema>.<Table>
```

Each Delta table has:

```text
loaded_generation
refreshed_generation
```

Whenever a Delta load performs a physical write:

```text
loaded_generation += 1
```

Before a SQL object runs, Weaver examines its Delta SQL dependencies.

If any relevant dependency has:

```text
loaded_generation > refreshed_generation
```

Weaver:

1. refreshes the associated Lakehouse SQL endpoint once;
2. waits for metadata visibility;
3. marks all currently dirty Delta tables exposed by that endpoint as refreshed;
4. executes the Warehouse procedure.

Several Delta writes before the next SQL dependency therefore collapse into one refresh.

---

## 10. Logging

Execution logging belongs centrally in the Weaver Lakehouse.

The required information includes:

- workflow identity;
- object identity;
- source module;
- target;
- start and completion times;
- duration;
- status;
- file or row CRUD counts;
- physical-write indicator;
- structured exception details.

Logging should not synchronously append or merge a central Delta table after every object if that materially delays execution.

The intended model is:

- workflow record allocated synchronously;
- step events queued;
- batched central persistence;
- durable outbox fallback;
- idempotent event IDs.

The exact mechanism should be discussed at its checkpoint.

---

## 11. Desktop CLI

The CLI is optional and remains thin.

Initial desktop utilities are:

```text
Fabric capacity status/resume/suspend
Fabric Files push/synchronise
Fabric Livy submit
```

After the corresponding core APIs exist, CLI commands may also wrap:

```text
build
load
wipe
workflow
```

The CLI should not own build, load or catalogue semantics.

---

## 12. Workflow configuration

A workflow configuration is an ordered list of normal Weaver commands:

```yaml
commands:
  - wipe:
      targets:
        - T1_DWG
        - T2_DWG

  - build:
      repository: Weaver/Files/repos/ilovegov-etl/SES
      folder_target: T0_DWG/Files
      delta_target: T1_DWG
      sql_target: T2_DWG

  - load:
      targets:
        - T0_DWG/Files
        - T1_DWG
        - T2_DWG
```

It is a repeatable command runner, not a second dependency or orchestration language.

---

## 13. First-run experience

Installation into a Fabric Environment:

```bash
pip install weaverstack
```

Then:

```python
import weaver

weaver.initialise_weaver_lakehouse("Weaver")
```

An example installation should eventually be:

```python
weaver.setup_example(
    weaver_lakehouse="Weaver",
    lakehouse="Example",
    warehouse="Example_Warehouse",
    exists_ok=True,
)
```

This allows a Fabric user to see an end-to-end example without desktop Python or manually uploading an initial repository.

---

# Part II — Implementation checkpoints

# Checkpoint 0 — Establish the new repository and working protocol

## Discuss

Agree:

- distribution name and import name;
- minimum Python version;
- initial package boundaries;
- whether CLI dependencies are an optional extra;
- which old repository commit is the reference baseline;
- that `weaver` remains unchanged unless explicitly requested.

Do not make detailed architectural commitments beyond the package boundary yet.

## Implement

Create the initial `weaverstack` repository with:

- package metadata;
- source package;
- a minimal importable public API;
- an isolated CLI package or module that depends on core;
- repository guidance for coding agents;
- a statement that the sibling `weaver` repository is reference-only.

A possible shape is:

```text
weaverstack/
├── pyproject.toml
├── AGENTS.md
├── src/
│   ├── weaver/
│   └── weaver_cli/
└── tests/
```

This layout is illustrative. Final module names should be agreed during discussion.

## Examine

Matthias should be able to:

- install the package in editable mode;
- import `weaver`;
- invoke the empty CLI help;
- inspect and understand dependency direction between core and CLI.

No domain logic should be ported yet.

## Existing Weaver reference

| File | What to inspect |
|---|---|
| `setup.cfg` | Current package metadata, extras and console entry point |
| `src/weaver_runtime/cli.py` | Current top-level command routing |
| `AGENTS.md` | Current repository role, invariants and operational guidance |
| `src/weaver_runtime/errors.py` | Current top-level error hierarchy |
| `src/weaver_runtime/dbrep/errors.py` | Domain-specific error types |

### Carry forward

- Python 3.11 baseline unless there is a reason to change it;
- one clear error hierarchy;
- core remaining importable without Spark.

### Do not carry forward automatically

- package name `weaver-runtime`;
- `pyodbc`;
- target-local runtime assumptions;
- old command shape.

---

# Checkpoint 1 — Define the host and physical-target vocabulary

## Discuss

Agree the smallest public model for:

- `FabricHost`;
- `LocalHost`;
- `FolderTarget`;
- `DeltaTarget`;
- `WarehouseTarget`;
- Weaver Lakehouse identity.

Resolve questions such as:

- whether workspace is supplied by name, ID or either;
- whether target strings are parsed directly or represented by dataclasses first;
- how a local target mirrors a Fabric Lakehouse;
- whether host configuration is YAML, Python or both;
- how a Fabric Environment is associated with a host.

Avoid implementing item discovery until the vocabulary is agreed.

## Implement

Introduce pure, non-networked domain objects and parsing.

They should be able to represent examples such as:

```text
Fabric workspace: I Love Government
Weaver Lakehouse: Weaver
Folder target: T0_DWG/Files/Extracts
Delta target: T1_DWG
Warehouse target: T2_DWG
```

and local equivalents beneath a configured root.

The result of this checkpoint should be target identities, not resolved IDs or open connections.

## Examine

Matthias should be able to construct or parse the target values and inspect an unambiguous normalised representation.

The representation should not require old SES/Files/Delta/SQL database aliases.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/config/environment.py` | Host parsing, allowed-key validation and environment-neutral defaults |
| `src/weaver_runtime/dbrep/config/databases.py` | Representation type validation patterns |
| `src/weaver_runtime/dbrep/config/resolution.py` | `ResolvedDatabase`, Fabric workspace/Lakehouse split and path vocabulary |
| `src/weaver_runtime/dbrep/targets/local_lakehouse.py` | Local Files/Tables Lakehouse shape |
| `AGENTS.md` | Rule against embedded product/workspace defaults |

### Port or adapt

- strict config validation;
- immutable domain records;
- separation between unresolved names and resolved physical IDs.

### Replace

- paired database-representation aliases;
- `server.database.type.schema.object` as the public conceptual stack;
- `Files/_weaver/runtime` as a target-relative invariant.

---

# Checkpoint 2 — Implement local host resolution

## Discuss

Agree the exact local directory convention.

A likely model is:

```text
<local-root>/<lakehouse>/Files
<local-root>/<lakehouse>/Tables
```

Resolve:

- whether the supplied local root contains Lakehouses directly;
- how Folder target subpaths are normalised;
- what local Warehouse behaviour should be when SQL is unavailable;
- which paths are returned as `Path` and which remain logical target records.

## Implement

Add local resolution functions for:

- Lakehouse root;
- Files root;
- configured Folder root;
- Tables root;
- Delta object path;
- Weaver Lakehouse repository root;
- Weaver Lakehouse control-table root where needed by the local implementation.

Do not yet implement object build or load.

## Examine

Given a local host root and physical target names, Matthias should be able to inspect every resolved local path before any filesystem mutation occurs.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/config/resolution.py` | `filesystem_host`, `lakehouse_root`, `files_root`, `tables_root`, path joining |
| `src/weaver_runtime/dbrep/targets/local_lakehouse.py` | Current local host wrapper |
| `src/weaver_runtime/dbrep/runtime/load.py` | `_join_root`, which distinguishes URL roots from filesystem paths |

### Port or adapt

- path-safety discipline;
- explicit separation between Files and Tables roots;
- URL-aware joining as a general helper.

### Replace

- current local co-location of multiple logical databases beneath one Lakehouse’s `Files/<database>` and `Tables/<database>`;
- derivation of Lakehouse root from a target-local runtime path.

---

# Checkpoint 3 — Port SES metadata policy

## Discuss

Review the current metadata keys and decide which are part of the initial `weaverstack` contract.

At minimum, discuss:

- `Folder ID`, `Table ID`, `View ID`;
- `Description`;
- `Lineage`;
- `Primary key`;
- `File key`;
- `Incremental`;
- `Static`;
- `Load mode`;
- `Schema`;
- column notes and semantic dictionary metadata.

The goal is to agree policy before altering parser code.

## Implement

Port the static metadata extraction and validation into a clean SES-policy layer.

The parser should:

- read a Python module docstring without importing the module;
- read the opening SQL metadata block;
- reject duplicate YAML keys;
- require exactly one object ID;
- normalise `Schema.Object`;
- validate object-kind-specific metadata;
- return an immutable metadata document.

## Examine

Matthias should be able to point the parser at one Python or SQL document and inspect its normalised metadata and any high-quality validation errors.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/ses/metadata.py` | Primary implementation to port |
| `docs/authoring.md` | Current author-facing semantics |
| `tests/test_metadata.py` | Existing examples of accepted and rejected metadata |
| `tests/fixtures/ses/Schema.Name.sql` | Small SQL metadata example |
| `tests/fixtures/generic_ses/SES/T1/Stage__Record.py` | Python Table example |

### Strong port candidate

Most of `ses/metadata.py` is independent of the old physical architecture and can be transplanted with naming and policy changes agreed by Matthias.

---

# Checkpoint 4 — Port the authoring object API

## Discuss

Agree the initial public authoring surface:

```python
from weaver import Folder, Table, View
```

Review and agree the intended semantics of:

- `self.repo`;
- `self.path`;
- `self.spark`;
- `self.schema`;
- `self.primary_key`;
- `self.is_incremental`;
- `self.current_dataframe`;
- `self.empty_frame()`;
- `self.staging_folder()`.

Decide whether `View` remains a Python authoring class if SQL views are represented only by SQL files.

## Implement

Port the lightweight base classes and context-facing accessors without importing PySpark at module import time.

No physical execution is needed yet. The classes should define the user contract and delegate runtime behaviour to an injected context.

## Examine

A developer should be able to author representative Folder and Table classes against the new import path without any Fabric or Spark setup.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/objects.py` | Primary authoring class implementation |
| `src/weaver_runtime/dbrep/runtime/context.py` | Context properties used by the object API |
| `docs/authoring.md` | Existing author-facing examples |

### Port

- lightweight imports;
- context delegation;
- ergonomic `self.*` surface;
- `read()` returning a pair.

### Leave open

- exact runtime-context class shape;
- whether every existing property is part of the first public release.

---

# Checkpoint 5 — Build the SES repository reader

## Discuss

Agree the repository discovery rules:

- object files live at the repository root;
- top-level `.py` and `.sql` files are inspected;
- `_`-prefixed or otherwise reserved files may be ignored;
- arbitrary subdirectories are retained as helper/support content;
- Python object filename and class naming rules;
- SQL filename and declared ID naming rules;
- how repository identity is supplied or derived.

This checkpoint replaces the current database-folder discovery convention.

## Implement

Create an `SESRepository` representation that:

1. accepts a repository root;
2. discovers top-level Python and SQL object documents;
3. applies SES policy;
4. statically validates Python object classes;
5. records helper and support content without treating it as objects;
6. rejects duplicate object IDs;
7. exposes the complete repository document model.

Do not bind objects to physical targets yet.

## Examine

Matthias should be able to point the reader at a mixed root and inspect:

- repository identity;
- all discovered objects;
- language;
- kind;
- metadata;
- source path;
- retained helper paths.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/ses/discovery.py` | Static file loading, filename rules and AST class validation |
| `src/weaver_runtime/dbrep/ses/metadata.py` | Metadata model |
| `src/weaver_runtime/dbrep/build/runtime_bundle.py` | Existing source-tree copying with helpers preserved |
| `src/weaver_runtime/dbrep/fabric/transfer.py` | Existing ignored-directory and ignored-file rules |
| `tests/test_structural_discovery.py` | Existing discovery behaviour |
| `tests/dbrep_helpers.py` | Existing fixture construction helpers |

### Port

- static, no-import discovery;
- Python AST class validation;
- source text retention;
- ignore rules for caches and transient files.

### Replace

- immediate child folders as “databases”;
- one installed helper tree per old database folder;
- object identity prefixed by the containing source database.

---

# Checkpoint 6 — Port dependency extraction and graph primitives

## Discuss

Agree dependency identity within the new single-repository model.

Discuss:

- two-part references within the repository;
- three-part references visible to SQL;
- which references are managed versus external;
- how dependencies on already-certified catalogue objects are represented;
- whether Python dependencies remain only literal `self.repo["..."]`;
- when unresolved external references are acceptable;
- how descendants are calculated for catalogue invalidation.

No physical build should happen in this checkpoint.

## Implement

Port or adapt:

- static Python dependency extraction;
- static SQL relation extraction;
- dependency normalisation;
- topological order;
- topological layers;
- cycle reporting;
- descendant traversal.

The result should be a repository dependency document that is independent of targets.

## Examine

Matthias should be able to inspect:

- each object’s raw references;
- resolved managed dependencies;
- unresolved external references;
- topological order;
- descendants of a selected object.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/ses/python_discovery.py` | AST extraction of literal `self.repo[...]` references |
| `src/weaver_runtime/dbrep/ses/sql_discovery.py` | FROM/JOIN/APPLY/USING relation extraction |
| `src/weaver_runtime/dbrep/ses/dependencies.py` | Current dependency records and classification |
| `src/weaver_runtime/dbrep/ses/graph.py` | Topological order and layers |
| `src/weaver_runtime/dbrep/build/planner.py` | Building edges and validating missing managed dependencies |
| `tests/test_python_discovery.py` | Existing Python examples |
| `tests/test_sql_discovery.py` | Existing SQL examples |
| `tests/test_dependency_classification.py` | Existing classification cases |
| `tests/fixtures/ses_dag/` | Existing SQL DAG fixture |

### Port

- static extractors;
- graph algorithms;
- clear cycle and missing-dependency diagnostics.

### Redesign

- dependency scope based on the old set of supplied database representations;
- object identity derived from a containing database folder.

---

# Checkpoint 7 — Implement Fabric resource discovery and root helpers

## Discuss

Agree the public Fabric utility API for:

- workspace resolution;
- Lakehouse resolution;
- Warehouse resolution;
- Environment resolution;
- ABFSS root;
- Lakehouse Files root;
- SQL endpoint connection string;
- notebook/bootstrap path insertion.

Resolve whether functions accept names, IDs or both and what they return.

## Implement

Create Fabric helpers that, given a host and physical item name, return stable resolved records.

Required resolved values include:

```text
workspace_id
workspace_name
lakehouse_id
lakehouse_name
spark_root
files_root
warehouse_id
warehouse_name
sql_endpoint
connection_string
environment_id
```

Do not implement build or load orchestration yet.

## Examine

From a Fabric notebook or authenticated desktop session, Matthias should be able to resolve and print roots and connection details for named workspace items.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/fabric/resources.py` | Workspace/item/Lakehouse/Environment lookup |
| `src/weaver_runtime/fabric/context.py` | Combined Lakehouse target resolution |
| `src/weaver_runtime/fabric/auth.py` | `DefaultAzureCredential` and token acquisition |
| `src/weaver_runtime/fabric/client.py` | Fabric REST request helpers |
| `src/weaver_runtime/fabric/settings.py` | Technical default resolution |
| `src/weaver_runtime/fabric/onelake.py` | `LakehouseTarget`, path construction and DFS operations |
| `src/weaver_runtime/dbrep/fabric/onelake.py` | Current convenience wrapper around Lakehouse resolution |
| `scripts/sparksession.py` | Legacy Spark-root/bootstrap reference |

### Proven examples to retain

```python
spark_root = (
    f"abfss://{workspace_id}"
    f"@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}"
)
files_root = f"{spark_root}/Files"
```

### Replace

- target-local runtime-root derivation;
- assumption that the target Lakehouse is attached to the notebook.

---

# Checkpoint 8 — Establish generic local and Livy program execution

## Discuss

Agree the standard inputs made available to a generated program.

The old standard globals were:

```text
spark
WEAVER_RUNTIME_ROOT
WEAVER_SPARK_ROOT
```

The new model should instead pass explicit resolved context such as:

- Weaver Lakehouse root;
- repository root;
- Folder target root;
- Delta target root;
- catalogue connection/access;
- SQL target connection details.

Agree whether the generated package uses Python programs, structured operations or both.

## Implement

Port the generic principle that:

- one generated program is executed verbatim locally or through Livy;
- transport is operation-agnostic;
- result is JSON-serialisable;
- Fabric session lifecycle is isolated from build/load logic.

At this checkpoint a trivial generated program is sufficient.

## Examine

Matthias should be able to execute the same generated program:

- locally;
- through Fabric Livy;

and inspect the same structured result.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/execution.py` | Generic local `exec` substrate and result validation |
| `src/weaver_runtime/fabric/livy.py` | Livy lifecycle, Environment attachment and generic runtime execution |
| `src/weaver_runtime/dbrep/lakehouse/programs.py` | Deterministic generated program pattern |
| `src/weaver_runtime/dbrep/fabric/lakehouse.py` | Current handoff from build/load to generic Livy |
| `scripts/run_fabric_notebook_job.py` | Notebook-job reference where useful |

### Port

- generic executor;
- JSON result contract;
- Livy session cleanup;
- Environment selection.

### Replace

- bootstrap that mounts the target Lakehouse specifically to import a copied runtime;
- `WEAVER_RUNTIME_ROOT` as a necessary target-local concept.

---

# Checkpoint 9 — Port the independent desktop Fabric utilities

## Discuss

Agree exact CLI commands for:

- capacity status;
- capacity resume;
- capacity suspend;
- local-folder-to-Lakehouse-Files synchronisation;
- generic Livy submission.

Clarify ownership and delete semantics for Files synchronisation, particularly for repository deployment into the Weaver Lakehouse.

## Implement

Port these utilities behind reusable core functions, then expose thin CLI commands.

The Files synchroniser should:

- skip transient development files;
- compare content signatures;
- upload changed files;
- optionally delete files only within an explicitly owned destination;
- remove empty directories where appropriate;
- report uploaded, unchanged and deleted files.

## Examine

Matthias should be able to use the CLI independently of build/load:

```text
capacity status/resume/suspend
sync repository to Weaver Lakehouse Files
submit a small generated program through Livy
```

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/capacity.py` | Azure CLI capacity command construction and invocation |
| `src/weaver_runtime/cli.py` | Current Fabric command routing |
| `src/weaver_runtime/dbrep/fabric/transfer.py` | Signature snapshot, diff, upload, scoped delete and empty-directory cleanup |
| `src/weaver_runtime/fabric/onelake.py` | Low-level upload/list/delete operations |
| `src/weaver_runtime/fabric/livy.py` | Generic Livy submission |
| `src/weaver_runtime/fabric/resources.py` | Target item discovery |

### Port

- transfer diff algorithm;
- ignored cache/tool directories;
- bounded parallel upload;
- capacity utility.

### Replace or omit

- old workspace-item push semantics where unrelated to Lakehouse Files;
- DBRep-specific names in generic synchronisation output.

---

# Checkpoint 10 — Replace the SQL connection layer with `mssql-python`

## Discuss

Agree the SQL adapter interface:

- connection creation;
- parameterised execution;
- script execution;
- result-set draining;
- query-to-dictionaries;
- transaction ownership;
- stored-procedure execution;
- authentication injection.

Resolve whether Weaver passes access tokens explicitly or allows `mssql-python` to use `DefaultAzureCredential` through its supported authentication mode.

## Implement

Create a driver-independent SQL module backed by `mssql-python`.

Use the validated connection-string shape:

```python
connection_string = (
    f"Server={SERVER},1433;"
    f"Database={DATABASE};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)
```

Preserve reliable handling of:

- multiple result sets;
- commit;
- rollback;
- query rows;
- controlled error translation.

## Examine

Matthias should be able to use the same adapter:

- on macOS;
- in a Fabric notebook;
- against a Fabric Warehouse;
- to execute a simple stored procedure.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/sql/connection.py` | Existing abstraction, execution, query and cursor-draining behaviour |
| `src/weaver_runtime/fabric/sql.py` | Existing Fabric SQL/token helper |
| `src/weaver_runtime/fabric/auth.py` | Credential acquisition |
| `setup.cfg` | Current `pyodbc` dependency to remove |
| `tests/fabric/test_sql_target.py` | Existing Warehouse integration behaviour |

### Port

- public abstraction shape;
- result draining;
- dictionary row conversion;
- domain-specific connection errors.

### Replace

- ODBC Driver 18 requirement;
- packed ODBC access-token attributes;
- `pyodbc`.

---

# Checkpoint 11 — Define the build-package document model

## Discuss

This checkpoint is intentionally design-heavy.

Agree:

- package directory structure;
- manifest schema;
- ordered step representation;
- how source files are included;
- how target identities are embedded;
- how generated code and declarative operations coexist;
- where pre-build and post-object catalogue DML lives;
- how partial installation results are represented;
- what makes a package immutable or reproducible.

Do not generate real DDL yet.

## Implement

Create the package model and serializer.

It should be able to describe, without executing:

- source repository identity and signature;
- Weaver Lakehouse;
- optional Folder target;
- optional Delta target;
- optional SQL target;
- ordered build operations;
- object IDs;
- dependencies;
- generated artifact paths;
- expected catalogue invalidation and certification operations.

A possible illustrative structure is:

```text
package/
├── manifest.yml
├── repository/
├── pre_build/
├── folders/
├── delta/
├── endpoint/
├── sql/
└── catalogue/
```

The exact structure is a discussion outcome, not a requirement.

## Examine

Matthias should be able to open a generated placeholder package and understand:

- what it intends to change;
- in what order;
- against which physical targets;
- which objects will be uncertified and later certified.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/build/planner.py` | Existing immutable plan records |
| `src/weaver_runtime/dbrep/build/manifest.py` | Source hashing, catalogue and dictionary documents |
| `src/weaver_runtime/dbrep/lakehouse/artifacts.py` | Existing generated host artifact and build-plan document |
| `src/weaver_runtime/dbrep/lakehouse/programs.py` | Generated executable program convention |
| `src/weaver_runtime/dbrep/cli/commands.py` | Current `generate` versus `build` separation |
| `src/weaver_runtime/dbrep/build/runtime_bundle.py` | Existing staging of runtime/source/metadata |
| `src/weaver_runtime/dbrep/targets/base.py` | Existing install-action abstraction |

### Port conceptually

- generation separate from application;
- deterministic ordered plan;
- source signature;
- inspectable artifacts.

### Do not port architecturally

- one artifact per target-local runtime host;
- copied Weaver orchestrator;
- catalogue JSON under each target;
- old positional `from → to` build pairs.

---

# Checkpoint 12 — Bind a repository to Folder, Delta and SQL destinations

## Discuss

Agree routing rules for one mixed SES repository:

```text
Python Folder ID  → folder target
Python Table ID   → delta target
SQL Table/View    → SQL target
```

Decide behaviour when:

- a required destination is absent;
- an unused destination is supplied;
- a repository contains only one representation;
- the same repository is built narrowly in separate invocations;
- object IDs collide;
- dependencies reference certified objects outside the package.

## Implement

Create the target-binding phase that turns an `SESRepository` plus optional physical targets into planned objects.

Each planned object should have:

- source document;
- logical object ID;
- kind and language;
- physical target;
- physical materialisation;
- managed dependencies;
- source hash;
- intended build operation category.

No DDL generation is required yet.

## Examine

Matthias should be able to inspect a dry, target-resolved object list from:

```python
generate_build_package(
    ses_repository=...,
    weaver_lakehouse="Weaver",
    folder_target="T0_DWG/Files",
    delta_target="T1_DWG",
    sql_target="T2_DWG",
)
```

without any side effects.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/build/planner.py` | `PlannedObject`, dependency validation, materialisation binding |
| `src/weaver_runtime/dbrep/build/compatibility.py` | Current object-kind/target compatibility |
| `src/weaver_runtime/dbrep/config/resolution.py` | Current materialisation helpers |
| `src/weaver_runtime/dbrep/targets/files.py` | Folder target operation |
| `src/weaver_runtime/dbrep/targets/sql.py` | SQL target operation description |
| `src/weaver_runtime/dbrep/runtime/initialise.py` | Delta object-spec extraction |

### Port

- planned-object concept;
- target compatibility validation;
- topological order.

### Replace

- `BuildPair`;
- positional source/target zipping;
- source database as part of every object ID.

---

# Checkpoint 13 — Generate Folder build operations

## Discuss

Agree what “building” a Folder object means.

Possible responsibilities include:

- ensuring schema/object directories exist;
- recording managed ownership;
- preserving existing materialised files;
- handling a changed Folder target;
- deciding whether a marker file is useful.

The sibling `_Staging` directory is a load-time concern and should not be created permanently during build.

## Implement

Generate ordered Folder operations into the package.

The operations should use the target root plus `Schema/Object` and should be executable both locally and against OneLake.

## Examine

Matthias should be able to inspect the package and then install it to see only the expected managed directories created.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/targets/files.py` | Current mkdir and managed-marker behaviour |
| `src/weaver_runtime/dbrep/config/resolution.py` | Existing Folder materialisation path |
| `src/weaver_runtime/dbrep/runtime/folders.py` | Destination/staging relationship that build must not violate |
| `src/weaver_runtime/fabric/onelake.py` | OneLake directory creation |
| `src/weaver_runtime/dbrep/fabric/transfer.py` | Safe path handling |

### Discuss before porting

The current `_weaver.json` marker is an implementation choice, not a required part of the new architecture.

---

# Checkpoint 14 — Generate Delta build operations

## Discuss

Agree initial Delta build semantics:

- create only when missing;
- schema validation;
- schema reshape behaviour;
- preservation versus recomputation decisions;
- whether build emits Spark SQL, Python specs or both;
- what qualifies as a Delta schema change for endpoint refresh;
- when a table becomes certified.

Keep later incremental-build-by-signature logic out of this checkpoint.

## Implement

Generate Delta build artifacts/specs from declared SES schema.

The package should include:

- target ABFSS root identity;
- `Tables/<schema>/<object>` materialisation;
- declared schema;
- ordered table initialisation/reshape operations;
- a bulk SQL endpoint refresh step after all affected Delta operations for the Lakehouse;
- post-object catalogue certification operations.

## Examine

Matthias should be able to inspect the exact generated Delta work before installation and then confirm the expected zero-row or reshaped tables appear at the explicit target root.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/runtime/initialise.py` | Delta specs, schema validation and zero-row creation |
| `src/weaver_runtime/dbrep/lakehouse/programs.py` | Generated Spark build program |
| `src/weaver_runtime/dbrep/lakehouse/artifacts.py` | Build program and plan packaging |
| `src/weaver_runtime/dbrep/runtime/spark_io.py` | Delta existence and schema helpers |
| `src/weaver_runtime/dbrep/config/resolution.py` | Delta materialisation path |
| `tests/test_delta_materialisation.py` | Existing materialisation examples |

### Port

- schema-only build rather than calling `read()`;
- explicit Spark root;
- lazy Spark imports;
- generated-program parity.

### Extend

- schema-change handling agreed during discussion;
- endpoint refresh package step;
- central certification DML.

---

# Checkpoint 15 — Port Warehouse DDL and stored-procedure generation

## Discuss

Review and agree which current SQL architecture is intentionally retained:

- backing current/history tables;
- public view;
- table-shape inference;
- staging/upsert artefacts;
- per-object load stored procedure;
- Incremental semantics;
- naming conventions;
- generated metadata columns;
- primary-key behaviour in Fabric Warehouse.

This checkpoint should not blindly preserve every legacy detail.

## Implement

Port the agreed SQL generation algorithms into build-package generation.

For each SQL Table object, generate:

- schema DDL;
- table/backing-table/view DDL;
- per-object load stored procedure.

For each SQL View object, generate:

- schema DDL;
- create-or-alter view DDL.

Store scripts in package execution order.

## Examine

Matthias should be able to open the generated SQL and compare it with the old implementation before it is executed.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/sql/ddl.py` | Main Warehouse DDL and table-shape inference |
| `src/weaver_runtime/dbrep/sql/etl.py` | Per-object load procedure generation |
| `src/weaver_runtime/dbrep/sql/wrangle.py` | SQL rewriting and template rendering |
| `src/weaver_runtime/dbrep/sql/warehouse_type_mapping.yml` | Warehouse type mapping |
| `src/weaver_runtime/dbrep/sql/templates/ddl/infer_create_table.sql` | DDL template |
| `src/weaver_runtime/dbrep/sql/templates/ddl/metadata_column_validation.sql` | Column validation |
| `src/weaver_runtime/dbrep/sql/templates/etl/load_proc.sql` | Procedure template |
| `src/weaver_runtime/dbrep/sql/templates/etl/create_etl_proc_installer.sql` | Procedure installer |
| `src/weaver_runtime/dbrep/sql/templates/etl/column_metadata.sql` | Runtime column metadata |
| `src/weaver_runtime/dbrep/sql/templates/etl/full_refresh_body.sql` | Full-refresh load |
| `src/weaver_runtime/dbrep/sql/templates/etl/primary_key_body.sql` | Primary-key load |
| `src/weaver_runtime/dbrep/sql/backend.py` | Current object installation sequence |

### Strong port candidates

The DDL and ETL generators contain years of proven SQL behaviour and should be reused selectively rather than re-derived.

### Do not make authoritative

The existing Warehouse-local `_weaver.objects` table. The central Weaver catalogue is authoritative in the new architecture.

---

# Checkpoint 16 — Define and generate central catalogue documents

## Discuss

Agree the initial control-plane tables and their semantics.

At minimum discuss:

- Repository;
- RepositoryInstallation;
- Catalogue;
- Dependency;
- TableDictionary;
- ColumnDictionary;
- IndexDictionary;
- ForeignKeyDictionary;
- optional Build and BuildStep audit;
- Workflow and WorkflowStep.

For each table, agree:

- stable key;
- whether rows are current state or history;
- relationship to object certification;
- how physical targets are stored;
- how source hashes are stored;
- how descendants are queried.

## Implement

Create pure generation functions that turn the planned repository into candidate catalogue and dictionary rows.

Do not yet write them physically.

The generated catalogue object should include enough information to load later:

- repository;
- installed source path;
- object ID;
- kind;
- language;
- source hash;
- target kind;
- target physical name;
- materialisation;
- SQL procedure where relevant;
- schema/load policy;
- dependencies.

## Examine

Matthias should be able to inspect the generated rows beside the generated physical build operations and verify that they describe the same objects.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/build/manifest.py` | Existing catalogue, dependency and dictionary projections |
| `src/weaver_runtime/dbrep/build/runtime_bundle.py` | Existing metadata merge logic |
| `src/weaver_runtime/dbrep/runtime/orchestrator.py` | Fields currently required at load time |
| `src/weaver_runtime/dbrep/sql/backend.py` | Existing SQL managed-object metadata |
| `docs/sql-tables-and-central-metadata-plan.md` | Existing design thinking |

### Port

- source hashing;
- table/column/index/foreign-key dictionary projection;
- explicit installed-source path.

### Replace

- JSON files as authoritative storage;
- per-target metadata merging;
- target-local SQL manifest as the load source of truth.

---

# Checkpoint 17 — Implement catalogue invalidation and recertification planning

## Discuss

Agree exact safety behaviour.

Questions include:

- whether catalogue rows are deleted or marked invalid;
- whether dictionary rows are removed at the same time;
- how descendants are calculated when the candidate graph changes;
- what happens when a physical build succeeds but certification DML fails;
- whether one successful object can be certified before all peers complete;
- how an interrupted package can be rerun.

The invariant is:

> No object or descendant remains certified while its upstream physical definition is being rebuilt.

## Implement

Generate package operations for:

### Pre-build

- identify selected objects;
- identify current certified descendants;
- invalidate/remove affected catalogue and dictionary rows.

### Post-object

- after each physical object build succeeds, restore that object’s catalogue and relevant dictionary rows.

### Failure

- leave all uncompleted objects and descendants uncertified;
- record build failure information if the optional audit tables are present.

## Examine

Matthias should be able to inspect a simple dependency chain and see the exact catalogue DML order before running it.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/build/prune.py` | Existing stale/removed object planning |
| `src/weaver_runtime/dbrep/build/runtime_bundle.py` | Existing row merge/removal logic |
| `src/weaver_runtime/dbrep/build/manifest.py` | Current metadata row shapes |
| `src/weaver_runtime/dbrep/ses/graph.py` | Descendant/topological graph basis |
| `src/weaver_runtime/dbrep/sql/backend.py` | Existing delete-then-insert metadata update pattern |

### Deliberate change

Do not implement whole-build catalogue promotion as the primary safety model. Certification is restored object by object.

---

# Checkpoint 18 — Implement `install_build_package` locally

## Discuss

Agree local package execution semantics:

- package validation before side effects;
- operation ordering;
- failure reporting;
- resumability or rerun behaviour;
- filesystem mutation boundaries;
- how local catalogue tables are represented;
- handling of SQL steps when no local SQL host is available.

## Implement

Create:

```python
install_build_package(package_directory, host=local_host)
```

It should execute the declared operations in order:

- pre-build catalogue changes;
- repository installation;
- Folder build;
- Delta build;
- catalogue recertification;
- completion/failure recording.

SQL steps may be explicitly unsupported locally at this stage rather than silently skipped.

## Examine

Matthias should be able to inspect:

- the package before execution;
- each reported operation;
- the resulting local Lakehouse structure;
- final certified catalogue state;
- partial state after a deliberately interrupted build.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/build/runtime_bundle.py` | Current local install sequencing |
| `src/weaver_runtime/dbrep/lakehouse/artifacts.py` | Current staging and completion record |
| `src/weaver_runtime/dbrep/execution.py` | Local generated-program executor |
| `src/weaver_runtime/dbrep/cli/commands.py` | Current local build program execution |
| `src/weaver_runtime/dbrep/runtime/initialise.py` | Local Delta initialisation |
| `src/weaver_runtime/dbrep/targets/files.py` | Folder creation |

### Do not port

- installation of a complete Weaver runtime beneath every target;
- target-local catalogue JSON.

---

# Checkpoint 19 — Implement Fabric build-package installation

## Discuss

Agree how the installer accesses:

- Weaver Lakehouse Files;
- Weaver catalogue Delta tables;
- Folder targets;
- Delta targets;
- Warehouse targets;
- Fabric Environment;
- SQL endpoint metadata refresh API.

Agree whether one generated Fabric program performs all Lakehouse work or whether the installer sequences structured operations around Spark and SQL executors.

## Implement

Add Fabric installation for the same package.

It must:

1. resolve all physical items;
2. install the repository snapshot into the Weaver Lakehouse;
3. execute pre-build catalogue invalidation;
4. create Folder destinations;
5. execute Delta build work against explicit ABFSS roots;
6. bulk-refresh each affected Lakehouse SQL endpoint after its Delta build work;
7. execute Warehouse DDL and stored-procedure scripts with `mssql-python`;
8. recertify each successful object;
9. record structured results.

No destination Lakehouse should need to be attached as the notebook default.

## Examine

Matthias should be able to build into multiple explicit Lakehouses and a Warehouse from a notebook attached only to the Weaver Lakehouse, or through Livy.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/fabric/lakehouse.py` | Current Fabric staging, resolution and Livy execution flow |
| `src/weaver_runtime/fabric/livy.py` | Generic Livy execution |
| `src/weaver_runtime/fabric/resources.py` | Item resolution |
| `src/weaver_runtime/fabric/context.py` | Lakehouse target resolution |
| `src/weaver_runtime/fabric/onelake.py` | OneLake Files operations |
| `src/weaver_runtime/dbrep/fabric/transfer.py` | Repository tree synchronisation |
| `src/weaver_runtime/dbrep/sql/backend.py` | SQL build execution |
| `src/weaver_runtime/dbrep/sql/connection.py` | Connection abstraction to replace with the new driver |
| `src/weaver_runtime/dbrep/lakehouse/programs.py` | Generated Spark program pattern |

### Proven platform assumptions

- explicit ABFSS access to non-default Lakehouses works;
- `mssql-python` works from Fabric to Warehouse;
- a single central Spark session can address multiple target Lakehouses.

---

# Checkpoint 20 — Compose `generate_build_package` and `build`

## Discuss

Review the public signatures and result objects.

Agree:

- input repository forms: local path versus Weaver Lakehouse path;
- default temporary-package behaviour;
- persistent output option;
- overwrite/existence behaviour;
- public build report;
- dry generation versus immediate installation;
- narrow one-target builds versus mixed builds.

## Implement

Expose:

```python
generate_build_package(...)
install_build_package(...)
build(...)
```

with `build()` containing no additional physical logic beyond composition.

## Examine

Matthias should be able to perform the same build in two ways:

```python
package = generate_build_package(...)
install_build_package(package, host=...)
```

and:

```python
build(...)
```

and inspect equivalent results.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/cli/commands.py` | Current `run_generate` and `run_build` relationship |
| `src/weaver_runtime/dbrep/lakehouse/artifacts.py` | Shared artifact generator used by generate and build |
| `src/weaver_runtime/dbrep/build/planner.py` | Current build request/plan boundary |

---

# Checkpoint 21 — Implement Weaver Lakehouse initialisation

## Discuss

Agree the built-in catalogue repository:

- which control tables exist initially;
- their schemas and keys;
- how local and Fabric initialisation differ;
- whether initialisation is idempotent;
- upgrade behaviour;
- what `exists_ok` means;
- how the bootstrap crosses from “no catalogue” to normal package installation.

## Implement

Create:

```python
initialise_weaver_lakehouse(...)
```

using a built-in repository definition and the standard package machinery wherever possible.

The bootstrap should leave:

- required control-plane Delta tables;
- `Files/repos`;
- a self-describing certified catalogue where appropriate;
- an inspectable initialisation result.

## Examine

Matthias should be able to initialise an empty local Weaver Lakehouse and a Fabric Weaver Lakehouse and inspect the same logical control-plane structure.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/runtime/initialise.py` | Current Delta table initialisation from declared schemas |
| `src/weaver_runtime/dbrep/lakehouse/programs.py` | Current initialisation program |
| `src/weaver_runtime/dbrep/build/manifest.py` | Metadata table concepts |
| `docs/sql-tables-and-central-metadata-plan.md` | Central metadata design reference |

### New work

The existing repository does not currently bootstrap a central Weaver Lakehouse through a built-in SES repository. This checkpoint should use existing primitives but is architecturally new.

---

# Checkpoint 22 — Install and import repository source centrally

## Discuss

Agree repository installation identity:

- repository name;
- source path;
- content signature;
- replacement versus retained historical snapshots;
- exact `Files/repos` layout;
- module/package import strategy;
- how arbitrary helper folders are added to Python import resolution;
- whether installed SQL source is retained beside Python source.

## Implement

Add repository installation into the Weaver Lakehouse and runtime import helpers.

The loader must be able to:

- locate a certified object’s installed source;
- create a unique synthetic package namespace per repository;
- preserve relative imports;
- avoid collisions between repositories;
- import only when the object executes;
- verify the source hash against the catalogue.

## Examine

Matthias should be able to load two repositories with similarly named helper modules and see them remain isolated.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/build/runtime_bundle.py` | Existing source tree copying and ignore rules |
| `src/weaver_runtime/dbrep/runtime/load.py` | `_import_object_module`, synthetic package creation and class lookup |
| `src/weaver_runtime/dbrep/runtime/orchestrator.py` | Installed source hash validation |
| `src/weaver_runtime/dbrep/build/manifest.py` | Source hashing and installed-source path |
| `src/weaver_runtime/dbrep/fabric/transfer.py` | Repository tree synchronisation |

### Port

- synthetic package import;
- relative-import support;
- source-hash validation.

### Replace

- one package per old database folder;
- source stored under each target runtime;
- runtime root added to `sys.path` as the source of the Weaver package.

---

# Checkpoint 23 — Port Folder load execution

## Discuss

Review and agree the existing Folder contract:

```python
def read(self):
    ...
    return staging_folder, files_to_delete
```

Agree:

- File key enforcement;
- Incremental semantics;
- explicit deletes;
- complete reconciliation;
- file comparison;
- staging lifecycle;
- failure retention;
- target root validation;
- CRUD counts;
- absence of a shared rejects or staging area.

## Implement

Port Folder execution into the new central orchestrator.

The object source is imported from the Weaver Lakehouse, while the destination and staging sibling belong to the explicit Folder target.

The path model must support:

- local `Path`;
- Fabric Files paths accessible to ordinary Python/OneLake operations.

## Examine

Matthias should be able to run one Folder object and inspect:

- resolved destination;
- sibling staging path;
- staged files;
- final files;
- retained staging after failure;
- cleanup after success;
- CRUD report.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/objects.py` | Folder authoring contract |
| `src/weaver_runtime/dbrep/runtime/context.py` | Staging issuance and cleanup |
| `src/weaver_runtime/dbrep/runtime/folders.py` | Core staging validation and reconciliation |
| `src/weaver_runtime/dbrep/runtime/load.py` | `_execute_folder_step` |
| `src/weaver_runtime/dbrep/runtime/logging.py` | CRUD model |
| `tests/fixtures/generic_ses/` | Existing Folder/Table dependency examples where applicable |

### Strong port candidate

Most of `runtime/folders.py` is pure filesystem logic and should be retained, but its old requirement that destinations sit beneath `Files/database/schema/object` must be adapted to the new configurable Folder root.

---

# Checkpoint 24 — Port Delta load semantics and execution

## Discuss

Review and agree:

- no-primary-key replacement;
- primary-key upsert;
- Incremental meaning;
- missing-row reconciliation;
- explicit deletes;
- blank-key rejects;
- duplicate-key handling;
- schema projection;
- null-safe change comparison;
- empty-input/no-op behaviour;
- CRUD metric source;
- physical-write indicator.

## Implement

Port:

- pure load policy;
- Spark DataFrame validation;
- Delta initialisation;
- append/replace/merge paths;
- explicit delete handling;
- CRUD outcomes;
- `wrote` flag;
- `self.current_dataframe`;
- schema alignment.

All reads and writes must use the object’s explicit target ABFSS root.

## Examine

Matthias should be able to inspect a Delta load report that distinguishes:

- input;
- accepted;
- rejected;
- inserted;
- updated;
- deleted;
- whether physical write occurred;
- whether reconciliation ran.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/runtime/load_policy.py` | Pure semantic source of truth |
| `src/weaver_runtime/dbrep/runtime/delta_table_load.py` | Spark-native physical executor |
| `src/weaver_runtime/dbrep/runtime/spark_io.py` | Delta existence/read helpers |
| `src/weaver_runtime/dbrep/runtime/load.py` | `_execute_table_step`, schema alignment and target path |
| `src/weaver_runtime/dbrep/runtime/context.py` | `current_dataframe` and `empty_frame` |
| `src/weaver_runtime/dbrep/objects.py` | Table accessors |
| `tests/spark/test_load_behaviour.py` | Existing behavioural reference |
| `tests/spark/test_local_lakehouse_load.py` | Existing end-to-end local reference |

### Strong port candidate

`load_policy.py` and `delta_table_load.py` should be treated as proven algorithms, with changes only where the new architecture or an explicitly agreed semantic change requires them.

---

# Checkpoint 25 — Port Warehouse load execution

## Discuss

Agree the runtime contract for SQL objects:

- one generated load stored procedure per Table;
- how procedure names are stored in the catalogue;
- result-set handling;
- CRUD result reporting;
- transaction boundaries;
- errors;
- parallelism within dependency layers;
- whether Views have load steps.

## Implement

Use the central catalogue to resolve:

- Warehouse endpoint;
- database;
- procedure name;
- object metadata.

Invoke each procedure through the new `mssql-python` adapter.

Do not read a Warehouse-local manifest as the authoritative load plan.

## Examine

Matthias should be able to select one Warehouse object and see the exact procedure invoked and its structured outcome.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/sql/backend.py` | Current procedure execution, layer parallelism and SQL load result |
| `src/weaver_runtime/dbrep/sql/etl.py` | Generated procedure contract |
| `src/weaver_runtime/dbrep/sql/connection.py` | Execution and result draining to port to the new driver |
| `src/weaver_runtime/dbrep/cli/commands.py` | Current SQL load branch |

### Replace

- `_weaver.objects` as the source of load order;
- target-by-target isolated SQL load orchestration.

---

# Checkpoint 26 — Build the global load orchestrator

## Discuss

Agree selection semantics:

- targets;
- repositories;
- explicit objects;
- dependency expansion;
- static objects;
- already-loaded dependencies;
- parallelism;
- failure stopping;
- global order across Folder, Delta and SQL;
- return to the same target in multiple waves.

## Implement

Create the central orchestrator that:

1. reads certified catalogue rows;
2. validates installed source hashes;
3. selects requested target/object scope;
4. expands managed dependencies;
5. topologically orders the global graph;
6. dispatches each object to Folder, Delta or SQL execution;
7. caches dependency representations;
8. stops and reports on failure.

## Examine

Matthias should be able to request a Warehouse target and inspect the full prerequisite graph Weaver intends to run, including upstream Folder and Delta objects where required.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/runtime/orchestrator.py` | Catalogue validation, target/object selection and ordering |
| `src/weaver_runtime/dbrep/runtime/load.py` | Step dispatch, context creation, dynamic imports and `Repo` |
| `src/weaver_runtime/dbrep/runtime/context.py` | Dependency resolver/cache |
| `src/weaver_runtime/dbrep/ses/graph.py` | Graph ordering |
| `src/weaver_runtime/dbrep/sql/backend.py` | Existing SQL layer execution |

### Port

- selection and source-hash validation;
- cached dependency resolver;
- per-step context.

### Replace

- target-internal edges only;
- target-local catalogue files;
- separate SQL and Lakehouse orchestrators.

---

# Checkpoint 27 — Add SQL endpoint refresh barriers to load

## Discuss

Confirm:

- how Delta SQL dependencies are recorded in the catalogue;
- how Lakehouse SQL endpoint identity is resolved;
- what “metadata visible” check is required;
- behaviour when a Delta load is a no-op;
- whether a failed refresh retries;
- how multiple dirty tables share one endpoint refresh.

## Implement

Maintain workflow-local state keyed by Delta SQL three-part name:

```text
Lakehouse.Schema.Table
```

When a Delta load outcome reports `wrote=True`:

```text
loaded_generation += 1
```

Before a SQL object executes:

- inspect its Delta SQL dependencies;
- if any are dirty, refresh the endpoint once;
- wait for visibility;
- mark all dirty tables exposed by that endpoint refreshed;
- invoke the SQL procedure.

## Examine

Matthias should be able to inspect an execution trace such as:

```text
load Delta A
load Delta B
refresh T1_DWG SQL endpoint
load SQL C
load SQL D
```

with no second refresh unless another relevant Delta write occurs.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/runtime/delta_table_load.py` | Reliable `wrote` indicator |
| `src/weaver_runtime/dbrep/runtime/load.py` | Step dispatch point |
| `src/weaver_runtime/fabric/client.py` | REST calls |
| `src/weaver_runtime/fabric/resources.py` | Lakehouse and endpoint discovery |
| `src/weaver_runtime/dbrep/fabric/lakehouse.py` | Fabric execution sequencing |
| Existing ILG notebook logic | Current proven endpoint refresh API call and polling behaviour |

This refresh-state mechanism is new and should be implemented centrally rather than hidden in individual object code.

---

# Checkpoint 28 — Implement central workflow and step logging

## Discuss

Agree:

- Workflow and WorkflowStep schemas;
- CRUD schema;
- error payload;
- event IDs;
- asynchronous batching boundary;
- durable outbox location and format;
- recovery/replay;
- when a workflow is considered complete;
- how build and load logging differ.

## Implement

Port the current identifiers, timing, CRUD and structured exception capture into central event records.

Add:

- event queue;
- batched Delta persistence;
- durable fallback;
- idempotent replay.

Keep logging outside object-authored code.

## Examine

Matthias should be able to inspect a successful and failed workflow in the Weaver Lakehouse and see all completed steps even when a later object failed.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/runtime/workflow_logging.py` | Workflow IDs, timing and detailed exception capture |
| `src/weaver_runtime/dbrep/runtime/logging.py` | `CrudCounts`, step/report records and pair validation |
| `src/weaver_runtime/dbrep/runtime/load.py` | Current per-step log lifecycle |
| `tests/test_runtime_logging.py` | Existing structured output examples |

### Port

- high-signal structured exception payload;
- one workflow ID per invocation;
- one step outcome per object;
- common CRUD shape.

### Replace

- synchronous JSON write under each target’s `Files/_logs`;
- target-local logging authority.

---

# Checkpoint 29 — Implement `load`

## Discuss

Review the public API and defaults:

```python
load(
    weaver_lakehouse,
    targets=None,
    objects=None,
    repositories=None,
    include_static=False,
)
```

Agree whether target selection includes descendants, dependencies or both, and what a dry plan should display.

## Implement

Expose the public `load()` composition around:

- catalogue selection;
- global orchestrator;
- execution contexts;
- refresh barriers;
- central logging.

## Examine

Matthias should be able to:

- plan without execution;
- load one object;
- load one target;
- load several physical targets;
- load an entire repository;
- inspect a single coherent report.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/cli/commands.py` | Current `run_load` options and local/Fabric branches |
| `src/weaver_runtime/dbrep/runtime/orchestrator.py` | Current plan-versus-execute split |
| `src/weaver_runtime/dbrep/lakehouse/programs.py` | Generated load program |
| `src/weaver_runtime/dbrep/fabric/lakehouse.py` | Current Fabric load submission |

---

# Checkpoint 30 — Implement `wipe`

## Discuss

Agree:

- target/object/repository selection;
- dependency safety;
- catalogue invalidation before physical deletion;
- whether descendants are wiped or merely uncertified;
- managed-only versus destructive wipe;
- Folder, Delta and SQL deletion semantics;
- local and Fabric differences.

## Implement

Create catalogue-aware wipe operations for:

- managed Folder materialisations;
- Delta tables;
- Warehouse tables/views/procedures;
- catalogue and dictionary rows.

Default behaviour must remove only Weaver-managed objects.

## Examine

Matthias should be able to inspect a wipe plan before execution and verify exactly which physical and catalogue objects will be affected.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/cli/commands.py` | `run_wipe` and `_wipe_lakehouse` |
| `src/weaver_runtime/dbrep/sql/backend.py` | `wipe_sql_target` |
| `src/weaver_runtime/dbrep/sql/templates/admin/wipe.sql` | Existing SQL wipe script |
| `src/weaver_runtime/dbrep/build/prune.py` | Managed-object removal planning |
| `src/weaver_runtime/fabric/onelake.py` | Files deletion |
| `src/weaver_runtime/dbrep/fabric/onelake.py` | Current recursive Lakehouse delete wrapper |

### Deliberate change

The central catalogue must be invalidated consistently with physical deletion. Wipe must not rely solely on physical discovery.

---

# Checkpoint 31 — Implement workflow command files

## Discuss

Agree the minimal YAML syntax and command parameters.

The file should remain an ordered command list and should not acquire object dependency syntax.

Discuss:

- failure stopping;
- variable substitution;
- host selection;
- command result aggregation;
- whether commands may reference previous command outputs.

## Implement

Create a command runner that invokes the same public Python APIs used directly:

```text
wipe
generate
install
build
load
```

No command-specific implementation should live in the workflow runner.

## Examine

Matthias should be able to repeatedly execute the same ordinary development loop from one readable YAML file.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/dbrep/cli/commands.py` | Current command function separation |
| `src/weaver_runtime/cli.py` | Current command routing |
| Existing ILG workflow notebooks | Practical sequence and parameter examples |

This is primarily new composition code.

---

# Checkpoint 32 — Implement `setup_example`

## Discuss

Agree the example’s teaching purpose and minimal physical requirements.

The example should demonstrate:

```text
Folder → Delta → Warehouse
```

without requiring a pre-existing user repository.

Agree:

- built-in example source;
- default target use;
- behaviour with no Warehouse supplied;
- `exists_ok`;
- whether the first load runs automatically;
- printed next steps.

## Implement

Create:

```python
weaver.setup_example(
    weaver_lakehouse,
    lakehouse=None,
    warehouse=None,
    exists_ok=True,
)
```

It should:

1. initialise Weaver if needed;
2. install a built-in example repository;
3. generate and install its build package;
4. optionally run its first load;
5. return object and target locations.

## Examine

A new Fabric user should be able to install the PyPI package, run one notebook cell and inspect a working example.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `tests/fixtures/generic_ses/` | Existing generic multi-layer object examples |
| `tests/fixtures/ses_dag/` | Existing SQL dependency example |
| `tests/fabric/test_end_to_end.py` | Existing end-to-end Fabric sequence |
| `docs/authoring.md` | Existing object-authoring language |

Use these as source material, not necessarily as the final public example.

---

# Checkpoint 33 — Add final thin CLI adapters

## Discuss

Agree which core operations are useful from desktop after Fabric-native APIs work:

- generate;
- install;
- build;
- load;
- wipe;
- workflow.

Decide whether desktop build/load executes locally, submits through Livy or exposes explicit modes.

## Implement

Add CLI argument parsing and presentation only.

The CLI should call the same public APIs and return the same result structures as notebooks.

## Examine

Matthias should be able to compare a notebook invocation and CLI invocation and see equivalent underlying behaviour.

## Existing Weaver reference

| File | Reusable reference |
|---|---|
| `src/weaver_runtime/cli.py` | Top-level parser |
| `src/weaver_runtime/dbrep/cli/parser.py` | Existing dbrep subcommands |
| `src/weaver_runtime/dbrep/cli/commands.py` | Existing plain-dict command functions |
| `setup.cfg` | Current console entry point |

### Preserve

The useful existing convention that command functions return plain serialisable structures and the CLI merely prints them.

---

# Part III — Existing Weaver reference map

The following map allows an agent to locate relevant code without searching.

## A. Package, errors and CLI

| Existing path | Role in current Weaver | Disposition |
|---|---|---|
| `setup.cfg` | Package, dependencies, extras, console command | Reference and replace |
| `AGENTS.md` | Current system overview and invariants | Read first |
| `src/weaver_runtime/errors.py` | Top-level command errors | Adapt |
| `src/weaver_runtime/dbrep/errors.py` | Build/load/config errors | Adapt |
| `src/weaver_runtime/cli.py` | Main argparse routing | Reference for final CLI |
| `src/weaver_runtime/dbrep/cli/parser.py` | dbrep parser definitions | Reference for final CLI |
| `src/weaver_runtime/dbrep/cli/commands.py` | Build/generate/load/wipe composition | Major reference, architecture replaced |

## B. Configuration and resolution

| Existing path | Role | Disposition |
|---|---|---|
| `src/weaver_runtime/dbrep/config/environment.py` | Host declarations | Adapt |
| `src/weaver_runtime/dbrep/config/databases.py` | SES/Files/Delta/SQL aliases | Parsing patterns only |
| `src/weaver_runtime/dbrep/config/resolution.py` | Physical path and identity resolution | Adapt heavily |
| `src/weaver_runtime/dbrep/targets/local_lakehouse.py` | Local Lakehouse wrapper | Adapt |
| `src/weaver_runtime/dbrep/targets/fabric_lakehouse.py` | Old placeholder interface | Little reusable logic |

## C. SES policy and repository parsing

| Existing path | Role | Disposition |
|---|---|---|
| `src/weaver_runtime/dbrep/ses/metadata.py` | Metadata extraction/validation | Strong port |
| `src/weaver_runtime/dbrep/ses/discovery.py` | Static file and class discovery | Port internals, replace folder model |
| `src/weaver_runtime/dbrep/ses/python_discovery.py` | Python dependency extraction | Strong port |
| `src/weaver_runtime/dbrep/ses/sql_discovery.py` | SQL dependency extraction | Strong port |
| `src/weaver_runtime/dbrep/ses/dependencies.py` | Dependency classification | Adapt |
| `src/weaver_runtime/dbrep/ses/graph.py` | DAG algorithms | Strong port |
| `src/weaver_runtime/dbrep/objects.py` | Folder/Table/View authoring API | Strong port |
| `docs/authoring.md` | Current authoring contract | Behavioural reference |

## D. Build planning and artifacts

| Existing path | Role | Disposition |
|---|---|---|
| `src/weaver_runtime/dbrep/build/planner.py` | Current paired build plan | Concepts ported, request model replaced |
| `src/weaver_runtime/dbrep/build/manifest.py` | Hash/catalogue/dictionaries | Strong projection reference |
| `src/weaver_runtime/dbrep/build/runtime_bundle.py` | Target-local runtime install | Algorithms only; architecture not ported |
| `src/weaver_runtime/dbrep/build/prune.py` | Stale managed object planning | Adapt |
| `src/weaver_runtime/dbrep/lakehouse/artifacts.py` | Generated build artifacts | Strong package reference |
| `src/weaver_runtime/dbrep/lakehouse/programs.py` | Deterministic generated programs | Strong port pattern |
| `src/weaver_runtime/dbrep/runtime/initialise.py` | Delta schema specs and creation | Strong port |
| `src/weaver_runtime/dbrep/targets/base.py` | Install action abstraction | Reference |
| `src/weaver_runtime/dbrep/targets/files.py` | Folder build operation | Adapt |
| `src/weaver_runtime/dbrep/targets/sql.py` | SQL plan operations | Reference |

## E. Fabric and OneLake

| Existing path | Role | Disposition |
|---|---|---|
| `src/weaver_runtime/fabric/auth.py` | Credential and tokens | Port |
| `src/weaver_runtime/fabric/client.py` | Fabric REST transport | Port |
| `src/weaver_runtime/fabric/settings.py` | Technical settings | Port |
| `src/weaver_runtime/fabric/resources.py` | Workspace/item resolution | Port and extend for Warehouse |
| `src/weaver_runtime/fabric/context.py` | Lakehouse target resolution | Adapt |
| `src/weaver_runtime/fabric/onelake.py` | Low-level DFS operations | Strong port |
| `src/weaver_runtime/fabric/livy.py` | Generic Livy execution | Strong port |
| `src/weaver_runtime/dbrep/fabric/transfer.py` | Signature-based tree sync | Strong port into generic Fabric utility |
| `src/weaver_runtime/dbrep/fabric/onelake.py` | Runtime-specific OneLake wrapper | Lower-level ideas only |
| `src/weaver_runtime/dbrep/fabric/lakehouse.py` | Old build/load Fabric orchestration | Reference for transport sequence |
| `src/weaver_runtime/capacity.py` | Capacity CLI utility | Port |
| `src/weaver_runtime/workspace.py` | Workspace-item push | Reference only where relevant |

## F. Runtime and loading

| Existing path | Role | Disposition |
|---|---|---|
| `src/weaver_runtime/dbrep/runtime/orchestrator.py` | Target-local selection/orchestration | Adapt heavily to central/global |
| `src/weaver_runtime/dbrep/runtime/load.py` | Folder/Delta execution and dynamic imports | Major port source |
| `src/weaver_runtime/dbrep/runtime/context.py` | Object context and dependency cache | Strong port |
| `src/weaver_runtime/dbrep/runtime/folders.py` | Folder staging/reconciliation | Strong port with target-root changes |
| `src/weaver_runtime/dbrep/runtime/load_policy.py` | Pure Delta load semantics | Strong port |
| `src/weaver_runtime/dbrep/runtime/delta_table_load.py` | Spark Delta writes and metrics | Strong port |
| `src/weaver_runtime/dbrep/runtime/spark_io.py` | Delta read/existence/schema helpers | Port |
| `src/weaver_runtime/dbrep/runtime/logging.py` | CRUD/report records | Port |
| `src/weaver_runtime/dbrep/runtime/workflow_logging.py` | IDs, timing and exception detail | Port internals; storage replaced |
| `src/weaver_runtime/dbrep/runtime/rejects.py` | Existing target-local reject storage | Do not assume current location model |

## G. SQL generation and execution

| Existing path | Role | Disposition |
|---|---|---|
| `src/weaver_runtime/dbrep/sql/connection.py` | pyodbc SQL abstraction | Replace driver, retain interface ideas |
| `src/weaver_runtime/dbrep/sql/backend.py` | SQL build/load/wipe | Major port source |
| `src/weaver_runtime/dbrep/sql/ddl.py` | Warehouse DDL generation | Strong port |
| `src/weaver_runtime/dbrep/sql/etl.py` | Load stored-procedure generation | Strong port |
| `src/weaver_runtime/dbrep/sql/wrangle.py` | SQL transformation/template utilities | Strong port |
| `src/weaver_runtime/dbrep/sql/warehouse_type_mapping.yml` | Type mapping | Port after review |
| `src/weaver_runtime/dbrep/sql/templates/ddl/*` | DDL templates | Port selectively |
| `src/weaver_runtime/dbrep/sql/templates/etl/*` | ETL templates | Port selectively |
| `src/weaver_runtime/dbrep/sql/templates/admin/wipe.sql` | Warehouse wipe | Reference |
| `src/weaver_runtime/dbrep/sql/_ses_compat.py` | Legacy compatibility bridge | Do not port as architecture |

---

# Part IV — Existing fixtures and behavioural references

These files are reference material for examining intended behaviour. This plan does not prescribe a testing strategy.

## Generic Python SES references

```text
tests/fixtures/generic_ses/SES/T1/Stage__Record.py
tests/fixtures/generic_ses/SES/T1/Mart__RecordAudit.py
tests/fixtures/generic_ses/SES/T1/Mart__RecordSnapshot.py
tests/fixtures/generic_ses/SES/T1/Mart__RecordCurrentAuto.py
tests/fixtures/generic_ses/SES/T1/Mart__RecordCurrentKeep.py
tests/fixtures/generic_ses/SES/T2/Mart__RecordAggregate.py
tests/fixtures/generic_ses/SES/T3/Report__RecordSummary.py
```

These demonstrate:

- Python Table definitions;
- `self.repo` references;
- multiple load policies;
- multi-level dependencies.

They use the old folder-per-database structure and therefore need flattening or adaptation for the new repository model.

## SQL SES references

```text
tests/fixtures/ses/Schema.Name.sql
tests/fixtures/ses/mart.Customer.sql
tests/fixtures/ses/report.CustomerView.sql
tests/fixtures/ses_dag/raw.Order.sql
tests/fixtures/ses_dag/raw.Customer.sql
tests/fixtures/ses_dag/dim.Product.sql
tests/fixtures/ses_dag/dim.Customer.sql
tests/fixtures/ses_dag/fact.Order.sql
```

These demonstrate:

- SQL metadata headers;
- Table and View documents;
- dependency extraction;
- DAG construction;
- SQL generation.

## Existing behavioural reference files

```text
tests/test_metadata.py
tests/test_structural_discovery.py
tests/test_python_discovery.py
tests/test_sql_discovery.py
tests/test_dependency_classification.py
tests/test_delta_materialisation.py
tests/test_runtime_logging.py
tests/test_ddlhelper.py
tests/test_sql_and_fabric.py
tests/spark/test_load_behaviour.py
tests/spark/test_local_lakehouse_load.py
tests/fabric/test_sql_target.py
tests/fabric/test_end_to_end.py
```

The coding agent should consult these when trying to understand why current logic behaves in a particular way. They are not automatically the required structure of the new project.

---

# Part V — Explicit architectural replacements

The coding agent must not accidentally reproduce these current `weaver` assumptions.

## Replace target-local runtimes

Current Weaver installs:

```text
Files/_weaver/runtime
```

in target Lakehouses.

Weaverstack instead uses:

- PyPI/Fabric Environment for Weaver code;
- central Weaver Lakehouse `Files/repos` for SES source;
- clean destination Lakehouses containing only materialisations.

## Replace per-target catalogues

Current Weaver writes JSON catalogue, dependency and dictionary files into each target runtime.

Weaverstack stores authoritative control-plane state centrally in the Weaver Lakehouse.

## Replace positional source-target pairs

Current build pairs each SES database representation with a target alias.

Weaverstack accepts one repository and independently optional:

```text
folder_target
delta_target
sql_target
```

Routing is inferred from object language and kind.

## Replace target-scoped orchestration

Current Lakehouse and SQL loads are separate target operations.

Weaverstack builds one global graph from the central catalogue and dispatches across all representations.

## Replace SQL driver

Current SQL execution uses `pyodbc`.

Weaverstack uses `mssql-python`.

## Replace attachment dependence

No destination Lakehouse needs to be attached to the notebook.

Every target root is explicit.

## Preserve catalogue invalidation safety

Before physical rebuild begins, affected objects and descendants cease to be certified. They return to the catalogue only after successful object build.

---

# Part VI — Deferred mature capabilities

After the checkpoints above are complete and the core system is stable, later work may include:

- incremental builds driven by catalogue signatures;
- a formal test framework;
- mirror/branch environments created from existing targets;
- richer deployment and promotion tooling using saved build packages;
- semantic-model generation;
- a read-only Fabric App for dependency and operational visualisation.

These are intentionally outside the initial step-by-step implementation sequence.

---

# Final instruction to the coding agent

Do not attempt to “finish Weaverstack” from this document.

Begin at Checkpoint 0.

At each checkpoint:

- inspect only the listed reference files;
- explain what you believe should be ported and what should be replaced;
- identify decisions that require Matthias’s judgement;
- wait for those decisions;
- implement the bounded checkpoint;
- present the resulting structure and observable behaviour;
- wait for approval before continuing.

The existing `weaver` code is reference material. The architecture in this document is authoritative where the two differ.
