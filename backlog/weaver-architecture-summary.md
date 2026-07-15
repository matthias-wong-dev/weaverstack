# Weaver Central Lakehouse Architecture

## 1. Architectural intent

Weaver separates four concerns:

| Concern | Responsibility |
|---|---|
| SES repository | Defines Folder, Delta and SQL objects, shared helpers and dependencies |
| Weaver Lakehouse | Holds the Weaver runtime, installed SES repositories, catalogue and orchestration state |
| Physical targets | Hold only the materialised Folder, Delta and SQL outputs |
| Weaver orchestrator | Executes one dependency graph across all configured targets |

Folder, Delta and SQL are **materialisation forms**, not architectural tiers. A repository may use T0/T1/T2, domains, products or any other structure, but Weaver assigns no semantics to those names.

---

## 2. Existing dbrep configuration remains authoritative

No new deployment-mapping file is introduced.

The existing dbrep configuration continues to define:

- SES source aliases;
- Files targets;
- Delta targets;
- Warehouse targets;
- server details;
- Fabric environments;
- SQL endpoints;
- local and Fabric variants.

The only additional configuration is a normal target entry for the mandatory Weaver Lakehouse, using the same existing dbrep configuration mechanism. A conventional alias such as `WEAVER` can identify it.

Conceptually:

```yaml
servers:
  Weaver_LH:
    type: Fabric Lakehouse
    server: I Love Government/Weaver
    environment: ilg

databases:
  WEAVER:
    type: Delta
    server: Weaver_LH
    database: Weaver
```

The exact alias is configurable, but it should be stable and resolved from the existing dbrep configuration. It does not need to be repeated on every build command.

---

## 3. SES repository structure

An SES source is a single top-level directory containing any mixture of supported objects:

```text
SES/
├── Gazette__VacancyNoticePdf.py
├── Gazette__Vacancy.py
├── Gazette.Vacancy.sql
├── Budget__BudgetPaper.py
├── Budget__Expense.py
├── DWG.AgencyExpense.sql
└── _helpers/
    ├── gazette.py
    └── common.py
```

Classification is inferred from the source:

| Source | Materialisation |
|---|---|
| Python file with `Folder ID` | Lakehouse Files |
| Python file with `Table ID` | Lakehouse Delta table |
| SQL file with Table/View declaration | Warehouse object |
| Helper Python files and packages | Installed with the SES source but not materialised directly |

The complete source tree is installed intact, including SQL and helper folders.

A repository may alternatively retain several SES folders and build each separately. The physical result can be identical. A merged SES folder is an organisational simplification, not a hard requirement.

---

## 4. Weaver Lakehouse structure

The Weaver Lakehouse is mandatory and wholly controlled by Weaver.

```text
Weaver Lakehouse/
├── Files/
│   ├── runtime/
│   │   └── weaver_runtime/
│   └── repos/
│       ├── ilovegov-etl/
│       │   └── SES/
│       │       ├── *.py
│       │       ├── *.sql
│       │       └── _helpers/
│       └── another-repository/
│           └── SES/
└── Tables/
    ├── Repository
    ├── RepositoryInstallation
    ├── Target
    ├── Catalogue
    ├── Dependency
    ├── TableDictionary
    ├── ColumnDictionary
    ├── Build
    ├── BuildStep
    ├── Workflow
    └── WorkflowStep
```

### `Files/runtime`

For now, this contains the Weaver Python code because Weaver is not installed into a Fabric Environment.

Later, moving Weaver into a Fabric Environment removes only this directory. The repository installation and central catalogue design remain unchanged.

### `Files/repos`

Each installed SES repository is copied intact into one central location. There is no target-local copy of the SES source and no target-local Weaver runtime.

### `Tables`

These are the authoritative control-plane tables for target bindings, catalogue state, dependency resolution, builds, workflows and logs.

---

## 5. Physical targets

### Folder target

For:

```text
Folder ID: Budget.BudgetPaper
```

and a configured Files root:

```text
T0_DWG/Files
```

Weaver materialises:

```text
T0_DWG/Files/Budget/BudgetPaper
T0_DWG/Files/Budget/BudgetPaper_Staging
```

Staging remains beside the managed Folder object. There is no shared staging or rejects directory because that would weaken the object-level security boundary.

The Folder target contains only materialised output.

### Delta target

For:

```text
Table ID: Budget.Expense
```

Weaver materialises:

```text
T1_DWG/Tables/Budget/Expense
```

The Python source remains centrally installed in the Weaver Lakehouse.

### Warehouse target

For:

```text
Table ID: DWG.AgencyExpense
```

Weaver installs the Warehouse table or view and the generated per-object load stored procedure.

The central catalogue records the Warehouse target and procedure name used during orchestration.

---

## 6. Build command model

The build command maps an SES source alias to one or more existing target aliases:

```bash
weaver build \
  --from DWG_SES \
  --to-folders T0_DWG_FABRIC \
  --to-delta T1_DWG_FABRIC \
  --to-sql T2_DWG
```

The aliases are resolved from the existing dbrep configuration.

Destination arguments are optional according to the objects discovered in the source. A narrow build is valid:

```bash
weaver build \
  --from T0_DWG_SES \
  --to-folders T0_DWG_FABRIC
```

```bash
weaver build \
  --from T1_DWG_SES \
  --to-delta T1_DWG_FABRIC
```

```bash
weaver build \
  --from T2_DWG_SES \
  --to-sql T2_DWG
```

Those three commands can produce the same physical outcome as one mixed-source build. The differences are:

- build scope;
- catalogue promotion scope;
- atomicity;
- whether cross-representation changes are validated together.

A single mixed build provides one coordinated catalogue promotion. Separate builds provide independent deployment units.

---

## 7. Build flow

### Step 1 — Resolve configuration

Weaver resolves from dbrep:

- the SES source;
- the central Weaver Lakehouse;
- the Files destination, when supplied;
- the Delta destination, when supplied;
- the Warehouse destination, when supplied.

### Step 2 — Install the SES source centrally

The complete SES tree is copied to:

```text
Weaver/Files/repos/<repository>/<source>
```

The installation includes Python, SQL, helpers and other repository-relative resources.

### Step 3 — Discover and classify objects

Weaver scans the installed source and classifies each object:

```text
Folder ID       → Files destination
Python Table ID → Delta destination
SQL object      → Warehouse destination
```

Helpers remain available for imports but do not become catalogue objects.

### Step 4 — Resolve target bindings

For each discovered object, Weaver records:

- source repository and source path;
- object ID and representation;
- destination target alias;
- physical materialisation path or SQL procedure;
- source hash and build version;
- schema, key and load policy;
- dependency references.

### Step 5 — Compile the global dependency graph

Weaver validates:

- missing dependencies;
- cycles;
- incompatible target assignments;
- unresolved source references.

The graph may cross materialisation forms in any order:

```text
Delta → Folder → SQL → Delta
```

### Step 6 — Build physical targets

Weaver performs the required target-specific work:

- Folder: validate and prepare managed destinations;
- Delta: create or reshape tables and properties;
- SQL: create tables/views and generated load stored procedures.

### Step 7 — Promote the catalogue

After all required target work succeeds, Weaver promotes the new repository installation, target bindings, catalogue and dependency graph as the active build.

A failed coordinated build remains recorded but does not replace the previously active catalogue.

---

## 8. Orchestration model

The orchestrator runs from the Weaver Lakehouse.

It loads:

```text
Files/runtime
```

onto the Python path, imports SES modules from:

```text
Files/repos/<repository>/<source>
```

and reads the active central catalogue and dependency graph.

The orchestrator may select:

- one object;
- one target;
- one repository;
- several targets;
- the complete active graph.

Execution follows global dependency order rather than target order. The graph may return to the same physical target in several waves.

### Folder execution

Weaver passes:

- installed repository root;
- target Files root;
- resolved destination path;
- sibling staging path;
- workflow context.

The object uses the normal Weaver interface such as `self.path`, `self.staging_folder()` and `self.repo`.

### Delta execution

Weaver passes:

- installed repository root;
- Spark session;
- target Lakehouse ABFSS root;
- resolved Delta table path;
- schema, primary key and load policy;
- workflow context.

The object uses `self.spark`, `self.path`, `self.current_dataframe`, `self.schema`, `self.primary_key` and `self.repo`.

### SQL execution

Weaver resolves:

- Warehouse endpoint;
- database;
- generated stored procedure;
- invocation parameters;
- workflow context.

The orchestrator invokes the procedure through `mssql-python`.

Where Warehouse SQL depends on a recently created or changed Delta table, the endpoint metadata refresh is an orchestration barrier before the dependent SQL step runs.

---

## 9. Parameters passed through the system

### Build command parameters

```text
--from
--to-folders   optional
--to-delta     optional
--to-sql       optional
```

All values are existing dbrep aliases.

### Resolved central parameters

```text
Weaver Lakehouse identity
runtime root
installed repository root
active build ID
```

### Folder step parameters

```text
object ID
source module
target Files root
destination path
staging path
workflow ID
```

### Delta step parameters

```text
object ID
source module
Spark session
target ABFSS root
resolved table path
schema
primary key
load policy
workflow ID
```

### SQL step parameters

```text
object ID
Warehouse endpoint
database
stored procedure
procedure parameters
workflow ID
```

Raw workspace IDs, Lakehouse IDs, ABFSS construction and SQL connection details are resolved by Weaver from the target aliases. SES code should not construct or manage them.

---

## 10. Resulting mental model

> Install an SES source into Weaver, point its Folder, Delta and SQL objects at existing dbrep targets, and let the central Weaver Lakehouse build, catalogue and orchestrate those objects into clean destination Lakehouses and Warehouses.
