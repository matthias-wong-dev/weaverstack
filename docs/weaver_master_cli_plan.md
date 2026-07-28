# Weaver Master CLI, Workspace, Repository Parsing and Catalogue Reconciliation Plan

## 1. Purpose

This document is the authoritative implementation plan for the current Weaver CLI and build refactor.

It supersedes:

- the earlier CLI and Workspace specification;
- the earlier non-destructive Delta, catalogue and repository parsing plan;
- the earlier use of `--weaver-repository`;
- the earlier Host/Hosts terminology;
- the earlier optional `catalogue=False` build path.

It incorporates the subsequent architectural decisions made during review.

This plan is intentionally strict about:

- architecture;
- lifecycle;
- command interfaces;
- ownership of responsibilities;
- state transitions;
- invariants;
- required behaviour;
- testable boundaries.

It is intentionally not prescriptive about low-level Python mechanics such as:

- exact module names;
- exact dataclass names beyond the architectural concepts identified here;
- whether internal immutable values use dataclasses, named tuples or another representation;
- whether state readers are free functions, services or Workspace-owned adapters;
- exact Spark, SQL or Fabric SDK calls where multiple valid implementations exist.

The principal outcomes are:

1. `parse_item_repository` becomes the canonical pure-Python repository boundary.
2. `weaver push` validates a complete local repository and uploads it to the Weaver Lakehouse.
3. Workspace replaces Host throughout the public CLI and code model.
4. `Lakehouse/_weaver` is logically included in every repository and build.
5. Every ordinary build is catalogue-aware and catalogue-certified.
6. `catalogue=False` is removed.
7. Delta table creation becomes non-destructive through `CREATE TABLE IF NOT EXISTS`.
8. Catalogue reconciliation produces a trustworthy `ReconciledCatalogue` and associated delete DML before bundle generation.
9. Bundle generation consumes prepared repository, catalogue and target state and performs no remote discovery.
10. `weaver unbind` removes catalogue state for named physical targets.
11. Catalogue reconciliation remains distinct from unbind.
12. Incremental build selection and full schema-evolution logic remain separate future work.

---

# 2. Scope

## 2.1 In scope

This branch includes:

- Host-to-Workspace terminology migration;
- revised Workspace configuration model;
- revised CLI command surface;
- revised binding grammar;
- pure repository parsing;
- repository validation before push;
- full-repository push;
- canonical remote repository location;
- generated `_weaver` composition;
- implicit `_weaver` binding;
- removal of `catalogue=False`;
- prepared catalogue and target-state reading;
- catalogue reconciliation;
- `ReconciledCatalogue`;
- reconciliation delete DML;
- non-destructive Delta table creation;
- narrowing of build orchestration;
- narrowing of bundle generation;
- explicit unbind behaviour;
- wipe integration;
- catalogue foreign-key ownership;
- examples, documentation and notebook parity;
- migration of tests, fixtures, errors and help text.

## 2.2 Out of scope

This branch does not implement the complete future incremental-build design.

Specifically out of scope:

- selecting changed Weaver documents by signature;
- rebuilding dependent descendants based on changed signatures;
- general Delta schema evolution;
- `ALTER TABLE` planning;
- compatibility classification for schema changes;
- automatic data migration;
- remote parse-after-push verification;
- selective push;
- local desktop bundle generation as a separate workflow;
- multi-workspace configuration files;
- multiple simultaneous physical installations of one logical item in one build;
- automatic creation of bound Fabric Lakehouse and Warehouse items.

Some interfaces introduced here must support future incremental build, but this branch must not broaden into implementing that later feature.

---

# 3. Architectural invariants

The following rules are mandatory.

## 3.1 Repository parsing is pure

`parse_item_repository` reads and validates repository files and generated built-in declarations only.

It must not:

- connect to Fabric;
- inspect a Workspace;
- read the catalogue;
- inspect physical targets;
- execute Spark;
- execute T-SQL;
- resolve physical bindings;
- generate prune actions;
- generate a build bundle;
- install anything.

## 3.2 One Weaver repository per Weaver Lakehouse

The Weaver Lakehouse represents the declaration and control state of the whole Workspace.

There is not a collection of named Weaver repositories within one Weaver Lakehouse.

The canonical uploaded repository location is:

```text
Files/
├── Lakehouse/
└── Warehouse/
```

The local repository name, such as `ilg`, is only the local folder name. Its contents are uploaded directly into `Files/`.

## 3.3 `_weaver` is always present logically

Every parsed `WeaverRepository` includes the generated logical item:

```text
Lakehouse/_weaver
```

It is composed in memory with authored repository files.

The parser must not need to write generated `_weaver` source files into the authored local repository.

## 3.4 `_weaver` is always bound implicitly

Every ordinary build contains the implicit binding:

```text
Lakehouse/_weaver
→ configured Weaver Lakehouse
```

The effective binding set is:

```text
implicit _weaver binding
+ explicitly selected physical target bindings
```

## 3.5 Physical Fabric items already exist

Before build:

- the Weaver Lakehouse must exist;
- all physical Lakehouses selected for binding must exist;
- all physical Warehouses selected for binding must exist.

`weaver initialise` creates or prepares the Weaver Lakehouse.

Ordinary build does not create Fabric items.

If a selected physical target does not exist, the Fabric access path may raise the underlying error. Weaver should preserve enough context to show which logical item and physical target were involved, but a separate target-existence management subsystem is not required.

## 3.6 Every build is catalogue-aware

There is no ordinary uncertified physical build mode.

Every ordinary build:

```text
builds or preserves physical objects
→ publishes catalogue rows
→ certifies registry state last
```

`catalogue=False` is removed from all public and internal build APIs.

## 3.7 Delta creation is non-destructive

Generated Delta table DDL uses:

```sql
CREATE TABLE IF NOT EXISTS
```

It does not use:

```sql
CREATE OR REPLACE TABLE
```

Views may continue to use:

```sql
CREATE OR REPLACE VIEW
```

This is an interim safety change that gets the architecture through the current branch.

Future schema evolution is signature-driven:

```text
signature changed
→ object selected for replacement
→ existing object dropped before creation
```

This branch does not implement that incremental selection.

## 3.8 Planner performs no remote discovery

The bundle generator receives prepared state.

It does not:

- read the Workspace;
- inspect Lakehouses;
- inspect Warehouses;
- read the catalogue itself;
- open Spark or SQL connections;
- construct physical resolvers from CLI or YAML.

## 3.9 One logical item has one physical target per build

Within one invocation:

```text
one logical Weaver item
→ no more than one physical target
```

A later build may bind that same logical item to another physical target.

The new successful build replaces the catalogue rows that represented the prior physical installation for that logical item.

Multiple candidate physical mappings may exist in Workspace configuration, but one invocation resolves each logical item uniquely.

---

# 4. Domain terminology

## 4.1 Required terms

The user-facing and architectural language is:

- Workspace;
- Workspace configuration;
- Environment;
- Weaver Lakehouse;
- Weaver repository;
- Weaver item;
- Weaver document;
- logical item;
- physical item;
- binding;
- catalogue;
- target inventory;
- reconciled catalogue;
- build bundle.

## 4.2 Removed terms

Remove from the CLI, public APIs, documentation, tests and user-facing errors:

- Host;
- Hosts;
- Fabric Host;
- Local Host;
- host configuration;
- `--host`;
- `--hosts`;
- `--weaver-repository`.

Temporary internal aliases may exist during migration where required, but:

- new code must use Workspace terminology;
- help text and logs must use Workspace terminology;
- aliases must be treated as deprecated migration support only.

## 4.3 Workspace

Workspace is Weaver's abstraction over the part of a physical Workspace Weaver chooses to represent.

It is not intended to model every feature of Microsoft Fabric.

It may provide access to Weaver capabilities such as:

- workspace identity;
- authentication and connectivity;
- Weaver Lakehouse access;
- repository file upload;
- Lakehouse target inspection;
- Warehouse target inspection;
- catalogue reading;
- build-bundle execution;
- wipe execution;
- runtime installation.

The exact implementation may use:

```text
FabricWorkspace
LocalWorkspace
```

The distinction is how Weaver reaches the Workspace, not a different public domain concept.

## 4.4 Environment

`environment` identifies the Fabric Environment or equivalent Weaver execution environment used in the Workspace.

It does not mean dev, test, pre-production or production.

Those deployment contexts are represented through different Workspace configuration files.

## 4.5 Weaver Lakehouse

The Weaver Lakehouse is the control Lakehouse for one Workspace.

It stores:

- repository content directly under `Files/`;
- `_weaver` catalogue tables;
- registry state;
- object dictionaries;
- bindings and installation identity;
- dependencies;
- future workflow and execution metadata;
- other Weaver-owned persistent state.

---

# 5. Workspace configuration

## 5.1 One file per Workspace

A Workspace configuration file describes exactly one Workspace.

It is not:

- a registry of multiple Workspaces;
- a multi-environment switchboard;
- a separate bindings file;
- a repository declaration.

Example files:

```text
workspaces/
├── Matthias Workspace.yml
├── CI Workspace.yml
├── Preprod Workspace.yml
└── Production Workspace.yml
```

## 5.2 Minimal form

```yaml
workspace: Production Workspace
```

## 5.3 Typical desktop form

```yaml
workspace: Matthias Workspace
type: local
environment: weaver
weaver_lakehouse: Weaver

execution:
  parallel_workers: 8

lakehouses:
  Dev_Data: Lakehouse/Sales

warehouses:
  Dev_Data: Warehouse/Raw
  Dev_Reporting: Warehouse/Reporting
```

Fabric execution is the default when `type` is omitted.

## 5.4 Physical-centric declarations

The Workspace configuration is physical-centric.

The key is the physical Fabric item.

The value is the default logical Weaver item.

```yaml
lakehouses:
  Dev_Data: Lakehouse/Sales

warehouses:
  Dev_Warehouse: Warehouse/Raw
```

Meaning:

```text
physical Lakehouses/Dev_Data
→ default logical Lakehouse/Sales

physical Warehouses/Dev_Warehouse
→ default logical Warehouse/Raw
```

The command selects which configured physical targets participate.

## 5.5 Physical names can overlap across types

This is valid:

```yaml
lakehouses:
  Dev_Data: Lakehouse/Sales

warehouses:
  Dev_Data: Warehouse/Raw
```

The typed identities are:

```text
Lakehouses/Dev_Data
Warehouses/Dev_Data
```

A bare physical name is not globally unique.

## 5.6 Multiple candidate targets

This is valid:

```yaml
warehouses:
  Dev_Warehouse: Warehouse/Raw
  Prod_Warehouse: Warehouse/Raw
```

Both are candidate physical targets for the same logical item.

A build invocation selects one.

The same invocation must not select both when they resolve to the same logical item.

## 5.7 Expanded declarations

An expanded form may be supported:

```yaml
warehouses:
  Dev_Warehouse:
    item: Warehouse/Raw
    execution:
      parallel_workers: 4
```

The semantic direction remains:

```text
physical target
→ default logical item
```

## 5.8 No separate binding YAML

There is no separate:

```text
bindings.yml
--bind-file
```

Workspace configuration declares defaults and available targets.

The CLI selects and overrides bindings inline.

---

# 6. Common CLI resolution

## 6.1 Common Workspace options

Commands use the applicable subset of:

```text
--workspace
--workspace-config
--environment
--weaver-lakehouse
```

## 6.2 Precedence

General values resolve in this order:

```text
explicit CLI argument
→ Workspace configuration
→ Weaver default
```

Bindings resolve in this order:

```text
inline --bind override
→ configured physical-to-logical default
→ error if unresolved
```

## 6.3 Workspace resolution

At least one of the following must resolve:

```text
--workspace
--workspace-config containing workspace
```

When both are supplied, explicit CLI values override configuration values.

---

# 7. Binding grammar and semantics

## 7.1 Select a configured physical target

```bash
--bind Lakehouses/Dev_Data
```

```bash
--bind Warehouses/Dev_Warehouse
```

When no `=` is present:

1. resolve the typed physical item from the Workspace configuration;
2. read its configured default logical item;
3. create the effective binding for this invocation.

Example:

```yaml
warehouses:
  Dev_Warehouse: Warehouse/Raw
```

```bash
--bind Warehouses/Dev_Warehouse
```

Effective binding:

```text
Warehouse/Raw
→ Warehouses/Dev_Warehouse
```

The Workspace configuration remains physical-centric, while the resolved build model may represent the mapping in whichever internal direction is appropriate.

## 7.2 Override the logical item

```bash
--bind Warehouses/Dev_Warehouse=Warehouse/NotRaw
```

This uses physical target `Warehouses/Dev_Warehouse` for logical item `Warehouse/NotRaw` in this invocation only.

The Workspace configuration is not changed.

## 7.3 Grammar

```text
--bind <typed-physical-item>
--bind <typed-physical-item>=<logical-item>
```

Typed physical forms:

```text
Lakehouses/<physical-name>
Warehouses/<physical-name>
```

Logical forms:

```text
Lakehouse/<logical-name>
Warehouse/<logical-name>
```

## 7.4 Type compatibility

Valid:

```bash
--bind Warehouses/Dev_Warehouse=Warehouse/Raw
```

Invalid:

```bash
--bind Warehouses/Dev_Warehouse=Lakehouse/Sales
```

Reject mismatches before build execution.

## 7.5 Binding uniqueness

For a single invocation:

- the same physical item cannot be assigned inconsistently;
- one logical item cannot resolve to more than one selected physical target;
- one physical target participates with one resolved logical item.

The effective binding set also includes:

```text
Lakehouse/_weaver
→ configured Weaver Lakehouse
```

This implicit binding is not supplied through `--bind`.

---

# 8. Target command surface

```text
weaver install
weaver initialise
weaver push
weaver build
weaver wipe
weaver unbind
weaver test
weaver doctor
```

---

# 9. `weaver install`

## Purpose

Install or publish the Weaver runtime into the selected Workspace Environment.

## Syntax

```bash
weaver install \
  [--workspace <workspace-name> | --workspace-config <path>] \
  [--environment <environment-name>]
```

## Behaviour

`install`:

- resolves the Workspace;
- resolves the Weaver Environment;
- installs or updates the Weaver runtime;
- does not push repository files;
- does not bind logical items;
- does not build targets;
- does not create target Lakehouses or Warehouses.

The exact relationship between runtime installation and Weaver Lakehouse creation remains implementation-specific. The command must not silently perform a target build.

---

# 10. `weaver initialise`

## Purpose

Create or prepare the Weaver Lakehouse.

## Syntax

```bash
weaver initialise \
  [--workspace <workspace-name> | --workspace-config <path>] \
  [--environment <environment-name>] \
  [--weaver-lakehouse <lakehouse-name>] \
  [--exists-ok]
```

## Behaviour

`initialise`:

- creates the Weaver Lakehouse if it does not exist;
- prepares required Weaver-owned file locations;
- may initialise catalogue structures where appropriate;
- validates an existing Weaver Lakehouse;
- does not push a repository;
- does not build user target items.

## `--exists-ok`

`--exists-ok` means an already existing Weaver Lakehouse is not itself an error.

It does not mean:

- ignore invalid structure;
- ignore incompatible catalogue schema;
- suppress failed migrations;
- accept structural damage silently.

Ordinary build must also be capable of recreating missing `_weaver` tables inside an existing Weaver Lakehouse.

---

# 11. `parse_item_repository`

## 11.1 Canonical operation

Conceptual interface:

```python
parse_item_repository(
    root: Location,
    *,
    store: Store | None = None,
) -> WeaverRepository
```

The exact parameter types may follow the existing codebase.

## 11.2 Responsibilities

The parser must:

1. read the complete authored repository;
2. discover logical Lakehouse and Warehouse item directories;
3. reject invalid authored root entries;
4. parse item schemas;
5. parse Weaver documents;
6. validate document placement by item type;
7. parse aliases;
8. validate logical names;
9. validate case-exact uniqueness;
10. validate schema membership;
11. validate metadata contracts;
12. resolve aliases and logical references;
13. construct the dependency graph;
14. reject prohibited cycles;
15. calculate Weaver document signatures;
16. calculate Weaver item signatures;
17. calculate the repository signature;
18. compose generated `Lakehouse/_weaver` declarations in memory;
19. return the complete immutable repository representation.

## 11.3 Exclusions

It must not:

- inspect the Workspace;
- inspect target state;
- read catalogue state;
- resolve physical bindings;
- execute Spark;
- execute T-SQL;
- create bundle actions;
- create prune actions;
- install anything.

## 11.4 Rename

`parse_item_repository` becomes the public architectural name.

A temporary compatibility alias may exist:

```python
read_weaver_repository = parse_item_repository
```

All new code, examples and tests use `parse_item_repository`.

## 11.5 Generated built-ins

Generated `_weaver` declarations are package-owned logical declarations.

The parser composes:

```text
authored repository
+ generated _weaver declarations
→ WeaverRepository
```

It must not mutate the authored repository merely to make parsing possible.

Materialised generated source may still be produced for:

- build-bundle snapshots;
- remote inspection;
- debugging;
- export.

It is not the canonical parser input mechanism.

---

# 12. `weaver push`

## 12.1 CLI

```bash
weaver push ./ilg \
  --weaver-lakehouse Weaver \
  [--workspace <workspace-name> | --workspace-config <path>]
```

The first positional argument is the local repository root.

There is no `--weaver-repository`.

The old option came from the earlier architecture in which Weaver did not yet represent the whole Workspace and several named repositories could be imagined inside one control plane.

That model is removed.

## 12.2 Behaviour

Push performs:

```text
parse_item_repository(local root)
→ fail immediately when invalid
→ upload complete authored repository folder
→ destination Files/
```

For this branch:

- push is whole-repository only;
- no document selection is supported;
- no remote parse is required;
- no remote signature comparison is required;
- no physical build occurs;
- no catalogue rows are mutated;
- no target binding is performed.

Future work may bring more validation forward to desktop execution and may make push incremental or selective.

## 12.3 Canonical remote layout

Local:

```text
./ilg/
├── Lakehouse/
└── Warehouse/
```

Remote:

```text
<Weaver Lakehouse>/
└── Files/
    ├── Lakehouse/
    └── Warehouse/
```

The contents of `./ilg` are uploaded directly into `Files/`.

There is no additional `ilg/` remote layer.

## 12.4 Reusable operation

Push must be an application operation reusable by:

- `weaver push`;
- `weaver build ./ilg ...` or the final agreed local-source build form;
- tests;
- future programmatic clients.

It must not exist only as CLI adapter logic.

---

# 13. `_weaver` lifecycle

## 13.1 Logical inclusion

Every parsed repository contains:

```text
Lakehouse/_weaver
```

## 13.2 Physical binding

Every build adds:

```text
Lakehouse/_weaver
→ configured Weaver Lakehouse
```

## 13.3 Physical item requirement

The Weaver Lakehouse itself must already exist.

`weaver initialise` is responsible for creating it.

## 13.4 Catalogue structure

Catalogue table DDL uses:

```sql
CREATE SCHEMA IF NOT EXISTS _;

CREATE TABLE IF NOT EXISTS _.Registry (
    ...
)
USING DELTA;
```

and equivalent DDL for all other `_weaver` Delta tables.

## 13.5 Catalogue absent

When catalogue structures are absent in an existing Weaver Lakehouse:

```text
ordinary build
→ ensures catalogue tables exist
→ publishes rows for what the build successfully installs
```

If the catalogue was wiped and only selected targets are rebuilt, only those successful builds are registered.

This is not described as an “incomplete catalogue”.

The catalogue truth is:

> it records what Weaver has built and registered since the current catalogue state was created.

Physical targets that exist but have not been rebuilt are simply not registered.

Weaver must not pretend to recover them automatically.

## 13.6 Catalogue evolution

`CREATE TABLE IF NOT EXISTS` does not migrate an existing catalogue schema.

Future catalogue migrations require explicit migration logic.

For this branch:

- do not replace existing catalogue data;
- do not silently certify against an incompatible catalogue schema;
- existing validation behaviour should remain or be strengthened where already available.

---

# 14. Delta table creation

## 14.1 Required change

Replace generated Delta table DDL:

```sql
CREATE OR REPLACE TABLE ...
```

with:

```sql
CREATE TABLE IF NOT EXISTS ...
USING DELTA
```

Apply this to:

- ordinary Lakehouse Delta tables;
- generated `_weaver` tables;
- Python-declared Delta tables;
- Spark-SQL-declared Delta tables;
- test fixtures and examples that exercise generated DDL.

## 14.2 Views

Views retain replacement semantics:

```sql
CREATE OR REPLACE VIEW ...
```

Replacing a view definition is not equivalent to replacing stored table data.

## 14.3 Interim semantics

This branch does not need to compare physical Delta schema to declared schema as a new general planning feature.

The intended future schema behaviour is signature-based:

```text
catalogue signature unchanged
→ existing managed table is treated as current

catalogue signature changed
→ future incremental planner selects it
→ existing table is dropped upfront
→ table is recreated
```

`CREATE TABLE IF NOT EXISTS` is an interim non-destructive primitive. It prevents the current general build path from replacing table data merely because a create statement is emitted.

## 14.4 Shared rendering rule

Catalogue tables and user Delta tables must share the same invariant.

A shared renderer or equivalent central DDL ownership should prevent one code path from reverting to `CREATE OR REPLACE TABLE`.

Tests must inspect generated bundles and fail if table DDL contains `CREATE OR REPLACE TABLE`.

---

# 15. Prepared current state

Build requires three broad inputs:

```text
parsed repository
current catalogue state
current target state
```

The parser deals only with repository files.

Workspace state readers retrieve the current physical and catalogue state required for:

- prune;
- validation of catalogue integrity;
- generation of build actions.

## 15.1 Catalogue reader

The catalogue reader should return the current catalogue representation required by reconciliation.

It should be capable of distinguishing:

- no catalogue structures;
- existing valid structures;
- partially missing structures;
- incompatible structures where validation exists.

The exact transport-neutral model is implementation-defined.

## 15.2 Target inventories

Target inventory must contain everything that Weaver documents could have created and that may now need to be pruned or checked for existence.

This includes the supported managed kinds, such as:

### Lakehouse

- Weaver-managed folders;
- generated file artefacts where applicable;
- Delta tables;
- schemas where Weaver manages them;
- supported views or aliases.

### Warehouse

- schemas;
- tables;
- views;
- other currently supported Warehouse Weaver document outputs.

The inventory does not need to model every arbitrary capability of Fabric.

It needs to model the physical object identities Weaver can create, retain, prune or reconcile.

## 15.3 Purpose

Target inventory serves at least two purposes.

### Prune

```text
physical object exists
+ no corresponding Weaver document exists
→ prune
```

### Catalogue integrity

```text
catalogue claims an object is installed
+ physical object no longer exists
→ catalogue signature is lying
→ remove it from trusted current state
```

---

# 16. Catalogue reconciliation

## 16.1 Purpose

Catalogue reconciliation creates a trustworthy starting catalogue for bundle generation.

It is a distinct, independently testable operation.

Conceptually:

```text
current catalogue
+ target inventories
→ ReconciledCatalogue
+ reconciliation delete DML
```

## 16.2 Responsibilities

Reconciliation must identify catalogue registrations whose corresponding physical objects no longer exist.

For each stale registered object:

```text
remove it from the effective reconciled catalogue
→ emit catalogue delete DML
```

The resulting `ReconciledCatalogue` contains only signatures and registrations that may be trusted for planning.

Its most important purpose is to prevent this false state:

```text
catalogue says object with signature X is installed
physical object is missing
builder trusts X and skips creation
```

Instead:

```text
catalogue says X is installed
physical object missing
→ discard that catalogue registration
→ bundle generator sees object as unregistered/missing
→ normal build may recreate it
```

## 16.3 Outputs

The exact Python representation is implementation-defined, but the operation must produce the equivalent of:

### Reconciled catalogue

- trusted catalogue rows;
- invalid rows removed;
- signatures safe for downstream use.

### Reconciliation DML

- deletes for stale root registry or installation rows;
- any explicit dependent deletes required where cascade is not physically enforced.

## 16.4 Independent testing

Reconciliation tests should cover:

```text
catalogue row exists
+ physical object exists
→ preserve row

catalogue row exists
+ physical object missing
→ remove from ReconciledCatalogue
→ emit delete DML

catalogue row absent
+ physical object exists
→ no reconciliation delete
```

The last case may still matter to prune or ordinary build, but it is not a stale catalogue claim.

## 16.5 Relationship to bundle generation

Bundle generation should not decide whether a catalogue signature is lying.

It receives the already reconciled result.

This makes reconciliation independently testable from bundle generation and avoids mixing current-state integrity with action planning.

## 16.6 Relationship to incremental build

Future incremental build may use trusted signatures from `ReconciledCatalogue` to decide what changed.

This branch does not implement that changed-signature selection.

---

# 17. Catalogue ownership and foreign keys

## 17.1 Required ownership model

One Weaver document creates one root registry or installation identity for its built result.

All catalogue metadata created from that Weaver document must be transitively owned by that root row.

The existing catalogue row-generation implementation should be used to determine the precise identities and relationships.

The plan does not prescribe new arbitrary keys where the implementation already makes the ownership obvious.

## 17.2 Foreign keys

Add foreign-key relationships across catalogue tables so deletion of a root installation or registry row propagates through dependent metadata.

This should cover catalogue structures such as:

- registry;
- table dictionary;
- column dictionary;
- foreign-key metadata;
- dependency metadata;
- other rows generated from a Weaver document.

Conceptual ownership:

```text
root registry / installation row
    └── object dictionary rows
        └── columns, keys, dependencies and related metadata
```

## 17.3 Cascade behaviour

Desired reconciliation behaviour:

```text
physical object missing
→ delete root registry/installation row
→ dependent catalogue metadata removed
```

Desired unbind behaviour:

```text
named physical target
→ delete its root installation rows
→ dependent catalogue metadata removed
```

Where Delta or the chosen catalogue technology does not physically enforce cascading foreign keys, Weaver must render equivalent ordered delete DML.

The architectural invariant is ownership and complete deletion, not dependence on a particular enforcement mechanism.

---

# 18. Build orchestration

## 18.1 Build from uploaded repository

Conceptual flow:

```text
resolve Workspace and Weaver Lakehouse
→ read Files/
→ parse_item_repository
→ resolve selected physical bindings
→ add implicit _weaver binding
→ read current catalogue
→ read selected target inventories
→ reconcile catalogue
→ generate build bundle
→ install bundle
→ publish catalogue DML
→ certify registry last
```

## 18.2 Build from local repository

The CLI may support a local-root build form that composes push and build.

Conceptual flow:

```text
resolve Workspace
→ parse local repository
→ push complete repository to Files/
→ load/parse uploaded repository
→ continue normal uploaded build workflow
```

The exact final command grammar may use a positional local root or an explicit option, but it must not reintroduce `--weaver-repository`.

## 18.3 Ordering

The effective action ordering is:

```text
ensure required schemas and catalogue structures
→ apply reconciliation catalogue deletes
→ apply prune and required drop actions
→ create/build physical objects
→ publish object dictionaries and installation rows
→ registry certification last
```

Existing planner DAG behaviour continues to order creation:

```text
dependencies before dependants
```

Declared schemas remain prerequisites.

Drops that are already safe to run in parallel may remain parallel. This branch does not introduce an unnecessary reverse-dependency drop planner unless existing behaviour requires it.

---

# 19. `build_item_repository`

## 19.1 Target responsibility

`build_item_repository` should orchestrate prepared build execution, not parse authored files or discover all state itself.

Conceptually it receives:

- a parsed `WeaverRepository`;
- resolved effective bindings;
- target inventories;
- a `ReconciledCatalogue`;
- reconciliation DML;
- execution/output context.

It then:

```text
generate bundle
→ install bundle
→ return build result
```

## 19.2 It must not

- accept an authored repository root as its primary input;
- materialise generated built-ins into authored source;
- call `parse_item_repository`;
- accept `catalogue=False`;
- parse CLI binding syntax;
- parse Workspace YAML;
- discover a Host;
- decide whether catalogue publication is enabled.

A higher application workflow composes parse, push, read, reconcile, plan and install.

---

# 20. `generate_item_build_bundle`

## 20.1 Conceptual inputs

The bundle generator receives the equivalent of:

```text
WeaverRepository
effective bindings
TargetInventories
ReconciledCatalogue
reconciliation delete DML
output/store context
prune policy
```

Exact Python type shapes are implementation-defined.

## 20.2 Responsibilities

The planner must:

- verify selected logical items exist;
- use the complete effective bindings including `_weaver`;
- project the selected repository dependency graph;
- compare repository declarations to target inventories for prune;
- consume trusted catalogue state;
- include reconciliation delete DML;
- emit non-destructive Delta creation;
- emit view replacement where appropriate;
- generate deterministic create/drop/prune actions;
- generate catalogue publication;
- preserve registry-last certification;
- write an inspectable deterministic bundle and repository snapshot.

## 20.3 It must not

- connect to Fabric;
- construct Workspace adapters;
- enumerate Lakehouse state;
- enumerate Warehouse state;
- read the catalogue;
- open Spark or SQL connections;
- parse CLI bindings;
- parse Workspace YAML;
- decide whether catalogue rows are trustworthy.

## 20.4 Prune

Existing prune semantics remain:

```text
physical Weaver-manageable object exists
+ object no longer exists in current item repository
→ drop it
```

Target inventory must therefore include everything Weaver documents could have created and that can be pruned.

This plan does not redesign currently working prune behaviour.

---

# 21. Catalogue publication

Catalogue publication is already the end of the successful build sequence and should remain so.

Required invariant:

```text
physical build succeeds
→ dictionary and installation DML succeeds
→ registry certification occurs last
```

A failed physical action or failed catalogue publication must not result in successful certification.

When the catalogue was empty or recreated, it records only what was successfully built in that operation.

When the catalogue already contains unrelated valid installations, a selective build must preserve them.

---

# 22. `weaver unbind`

## 22.1 Purpose

Remove catalogue state for explicitly named physical targets.

It does not reconcile by checking whether those targets still exist.

## 22.2 Semantics

For a selected typed physical target:

```text
find catalogue root rows belonging to target
→ delete them
→ propagate deletion to all dependent catalogue metadata
```

This applies regardless of whether the target currently exists.

It does not delete physical objects.

## 22.3 CLI

Use typed or otherwise unambiguous target selection.

A suitable initial form is:

```bash
weaver unbind \
  --workspace-config workspace.yml \
  --lakehouse Dev_Data \
  --warehouse Dev_Reporting
```

or an equivalent typed selector consistent with the rest of the CLI.

The important behaviour is explicit target-directed catalogue deletion.

## 22.4 Difference from reconciliation

### Unbind

```text
user names target
→ delete all associated catalogue rows
```

### Reconcile

```text
reader proves registered physical objects are missing
→ delete only stale catalogue rows
```

They may share catalogue delete rendering and ownership logic, but they are not the same operation.

---

# 23. `weaver wipe`

## 23.1 Purpose

Remove Weaver-managed artefacts from selected physical targets.

## 23.2 Behaviour

Wipe:

1. resolves selected typed physical targets;
2. removes Weaver-managed physical objects according to the existing wipe scope;
3. leaves unrelated unmanaged artefacts untouched unless the existing explicit wipe contract states otherwise;
4. removes catalogue state for the affected target through unbind semantics.

A user should not need to run `weaver unbind` immediately after a Weaver-managed wipe of the same target.

## 23.3 Weaver Lakehouse

`--weaver-lakehouse` identifies the control Lakehouse used to read and update catalogue state.

The Weaver Lakehouse itself is not wiped merely because it is supplied as the control target.

An explicit future operation may define wiping the Weaver Lakehouse.

## 23.4 Catalogue deletion after external target loss

If a target is deleted outside Weaver, catalogue reconciliation identifies the stale rows later.

That is distinct from wipe, where Weaver already knows exactly which target it removed and can unbind it directly.

---

# 24. Validation requirements

Perform validation before remote execution where practical.

Required validation includes:

- Workspace resolves from CLI or configuration;
- Workspace configuration represents one Workspace;
- Workspace type is supported;
- Weaver Lakehouse resolves where required;
- local root exists for push;
- local repository passes `parse_item_repository`;
- typed physical item syntax is valid;
- selected physical targets are configured where the CLI requires configuration membership;
- physical and logical binding types match;
- repeated bindings are not contradictory;
- one logical item resolves to no more than one physical target in one build;
- physical names may overlap across Lakehouse and Warehouse types;
- parallel worker values are valid;
- `--exists-ok` does not suppress incompatible structures.

Target existence itself may be validated naturally through the Fabric access operation. Weaver need not duplicate Fabric's item-existence checks as a separate architecture.

Errors must use Workspace terminology.

---

# 25. CLI consistency

## 25.1 Kebab case

Use:

```text
--workspace
--workspace-config
--environment
--weaver-lakehouse
--exists-ok
--parallel-workers
```

Do not use underscore-form CLI flags.

## 25.2 Repeatable singular selectors

Prefer:

```bash
--lakehouse Sales \
--lakehouse Finance \
--warehouse Reporting
```

rather than one plural list argument.

## 25.3 Typed binding selectors

Bindings use:

```text
Lakehouses/<physical-name>
Warehouses/<physical-name>
```

Logical item names use:

```text
Lakehouse/<logical-name>
Warehouse/<logical-name>
```

---

# 26. Complete desktop workflow

Using explicit CLI values:

```bash
weaver install \
  --workspace "My Workspace" \
  --environment weaver
```

```bash
weaver initialise \
  --workspace "My Workspace" \
  --environment weaver \
  --weaver-lakehouse Weaver \
  --exists-ok
```

```bash
weaver push ./ilg \
  --workspace "My Workspace" \
  --weaver-lakehouse Weaver
```

```bash
weaver build \
  --workspace "My Workspace" \
  --environment weaver \
  --weaver-lakehouse Weaver \
  --bind Lakehouses/Dev_Data \
  --bind Warehouses/Dev_Data
```

```bash
weaver wipe \
  --workspace "My Workspace" \
  --weaver-lakehouse Weaver \
  --lakehouse Dev_Data \
  --warehouse Dev_Data
```

```bash
weaver unbind \
  --workspace "My Workspace" \
  --weaver-lakehouse Weaver \
  --warehouse Dev_Reporting
```

Using a Workspace configuration:

```bash
weaver install \
  --workspace-config "./workspaces/Matthias Workspace.yml"
```

```bash
weaver initialise \
  --workspace-config "./workspaces/Matthias Workspace.yml" \
  --exists-ok
```

```bash
weaver push ./ilg \
  --workspace-config "./workspaces/Matthias Workspace.yml"
```

```bash
weaver build \
  --workspace-config "./workspaces/Matthias Workspace.yml" \
  --bind Lakehouses/Dev_Data \
  --bind Warehouses/Dev_Data
```

```bash
weaver wipe \
  --workspace-config "./workspaces/Matthias Workspace.yml" \
  --lakehouse Dev_Data \
  --warehouse Dev_Data
```

```bash
weaver unbind \
  --workspace-config "./workspaces/Matthias Workspace.yml" \
  --warehouse Dev_Reporting
```

---

# 27. Python and notebook API migration

## 27.1 Required renames

```text
Host → Workspace
FabricHost → FabricWorkspace
LocalHost → LocalWorkspace
host → workspace
host_name → workspace_name
hosts → workspaces
host_config → workspace_config
host_type → workspace_type
```

## 27.2 Public model

Use:

```python
FabricWorkspace(...)
```

or:

```python
LocalWorkspace(...)
```

The exact constructor remains an implementation choice.

## 27.3 Notebook parity

The notebook should demonstrate the same lifecycle:

```text
install
→ initialise
→ make the repository available directly under Files/
→ parse repository
→ build
→ wipe
→ unbind
```

A Fabric notebook cannot push files from the developer's desktop.

Therefore desktop `push` is replaced by manually uploading the repository contents directly under:

```text
Files/
```

After that point, notebook and desktop workflows use the same repository, binding, build, catalogue and reconciliation concepts.

## 27.4 Illustrative notebook cells

### Workspace

```python
from weaver import FabricWorkspace

workspace = FabricWorkspace(
    workspace="My Fabric Workspace",
    environment="weaver",
    weaver_lakehouse="Weaver",
)
```

### Install

```python
workspace.install()
```

### Initialise

```python
workspace.initialise(exists_ok=True)
```

### Parse uploaded repository

```python
repository = parse_item_repository(
    workspace.files_location()
)
```

The exact API may differ, but it must validate the uploaded `Files/` repository structure before build.

### Build

```python
workspace.build(
    bindings=[
        "Lakehouses/Example_Lakehouse",
    ],
)
```

or an equivalent stable API preserving typed physical binding semantics.

### Wipe

```python
workspace.wipe(
    lakehouses=["Example_Lakehouse"],
)
```

### Unbind

```python
workspace.unbind(
    lakehouses=["Example_Lakehouse"],
)
```

---

# 28. Example repository

Add a small executable example:

```text
examples/
└── lakehouse/
    ├── README.md
    ├── workspace.example.yml
    ├── Lakehouse/
    │   └── Example/
    │       ├── schemas/
    │       │   └── Example.yml
    │       └── <valid current Weaver document>
    └── notebooks/
        └── lakehouse_example.ipynb
```

The exact document filename and declaration form must follow current repository conventions.

The example must demonstrate:

- one logical Lakehouse;
- at least one schema;
- at least one Delta table;
- pure parsing;
- full push;
- physical binding;
- first build;
- repeated build preserving Delta data;
- prune where practical;
- wipe;
- catalogue recreation;
- unbind;
- Fabric notebook parity.

---

# 29. Installation documentation

Documentation must clearly distinguish:

## Python package installation

```bash
pip install weaverstack[cli]
```

## Contributor editable installation

```bash
pip install -e '.[dev]'
```

## Weaver runtime installation

```bash
weaver install --workspace-config workspace.yml
```

These are different operations.

`pip install` installs the Python package in the invoking environment.

`pip install -e '.[dev]'` installs a local source checkout for development.

`weaver install` publishes or installs the Weaver runtime into the selected Workspace Environment.

---

# 30. Implementation sequence

## Phase 1 — Workspace terminology and configuration

1. Rename public Host concepts to Workspace.
2. Add or migrate `FabricWorkspace` and `LocalWorkspace`.
3. Replace CLI flags:
   - `--host` → `--workspace`;
   - `--hosts` → `--workspace-config`.
4. Update configuration parsing.
5. Update errors, logs, fixtures and documentation.
6. Preserve temporary deprecated aliases only where migration requires them.

## Phase 2 — pure repository parser

1. Introduce `parse_item_repository`.
2. Move all repository-only parsing and validation under it.
3. Compose generated `_weaver` declarations in memory.
4. Stop writing built-ins into authored source as a parsing prerequisite.
5. Calculate document, item and repository signatures.
6. Add pure-Python parser tests.
7. Deprecate `read_weaver_repository`.

## Phase 3 — push

1. Add reusable push application operation.
2. Validate the complete local repository with `parse_item_repository`.
3. Upload the complete repository.
4. Use canonical destination `Files/`.
5. Add `weaver push <root>`.
6. Remove `--weaver-repository`.
7. Do not implement selective push.
8. Do not implement remote signature verification in this branch.

## Phase 4 — implicit `_weaver`

1. Ensure every parsed repository contains `Lakehouse/_weaver`.
2. Add the implicit physical binding to the Weaver Lakehouse.
3. Keep generated declarations package-owned.
4. Ensure catalogue schema and tables can be created during ordinary build.
5. Preserve `weaver initialise` as the Fabric-item creation/preparation operation.

## Phase 5 — non-destructive Delta creation

1. Locate every Delta table DDL renderer.
2. Replace `CREATE OR REPLACE TABLE` with `CREATE TABLE IF NOT EXISTS`.
3. Use the same invariant for `_weaver` and user tables.
4. Leave view replacement unchanged.
5. Add tests forbidding replace-style Delta table DDL.
6. Add tests proving repeat build preserves existing data.
7. Do not implement general schema evolution in this phase.

## Phase 6 — mandatory catalogue

1. Remove `catalogue` Boolean from build APIs.
2. Remove all `catalogue=False` call sites.
3. Make catalogue DML unconditional for ordinary builds.
4. Preserve registry certification last.
5. Redirect parser-only tests to `parse_item_repository`.
6. Redirect narrow executor tests to explicit bundles/actions.

## Phase 7 — state readers

1. Extract or formalise catalogue reading.
2. Extract or formalise Lakehouse inventory reading.
3. Extract or formalise Warehouse inventory reading.
4. Ensure inventory covers all Weaver-manageable object kinds required by prune.
5. Keep representations transport-neutral enough for local and Fabric execution.
6. Avoid prescribing unnecessary public type hierarchies.

## Phase 8 — catalogue reconciliation

1. Implement reconciliation from current catalogue plus target inventories.
2. Return a `ReconciledCatalogue`.
3. Return reconciliation delete DML.
4. Remove missing physical objects from trusted catalogue state.
5. Test reconciliation independently from bundle generation.
6. Do not implement changed-signature incremental selection.

## Phase 9 — catalogue foreign keys and ownership

1. Trace existing root registry/installation row generation.
2. Add foreign-key relationships from dependent catalogue metadata.
3. Add cascade semantics where supported.
4. Render ordered equivalent deletes where physical cascade is not enforced.
5. Ensure one root deletion removes all metadata originating from the Weaver document.

## Phase 10 — narrow bundle generation

1. Change bundle generation to consume parsed repository and prepared state.
2. Remove Workspace, resolver, Spark and SQL discovery from the planner.
3. Pass effective bindings.
4. Pass target inventories.
5. Pass `ReconciledCatalogue`.
6. Pass reconciliation delete DML.
7. Preserve current prune behaviour.
8. Preserve deterministic DAG creation ordering.
9. Preserve registry-last certification.

## Phase 11 — unbind

1. Add reusable target-directed unbind operation.
2. Delete catalogue rows for named physical targets regardless of physical existence.
3. Propagate deletion through catalogue ownership.
4. Expose through `weaver unbind`.
5. Keep it distinct from catalogue reconciliation.

## Phase 12 — wipe integration

1. Preserve existing wipe behaviour.
2. After known target deletion, invoke unbind semantics for those targets.
3. Do not require a subsequent manual unbind.
4. Use reconciliation only for state lost outside Weaver.

## Phase 13 — examples and documentation

1. Add executable Lakehouse example.
2. Add Workspace example configuration.
3. Add full desktop lifecycle README.
4. Add Fabric notebook equivalent.
5. Demonstrate repeated build data preservation.
6. Demonstrate catalogue structure recreation.
7. Demonstrate wipe and unbind.
8. Remove Host and `--weaver-repository` from all public material.

---

# 31. Test requirements

## 31.1 Parser purity

Prove that `parse_item_repository`:

- imports without PySpark;
- runs without Java;
- runs without Fabric credentials;
- performs no catalogue read;
- performs no target inspection;
- validates aliases;
- validates schemas;
- validates references;
- validates dependencies;
- detects cycles;
- calculates signatures;
- includes generated `_weaver`;
- does not mutate authored source.

## 31.2 Push

Prove that:

- invalid local repositories fail before upload;
- valid repositories upload completely;
- destination is `Files/`;
- the local root folder name is not inserted as an extra remote level;
- no `--weaver-repository` option remains;
- push performs no build;
- push performs no catalogue mutation;
- selective push is not exposed.

## 31.3 Workspace and binding

Prove that:

- Host terminology is absent from public CLI/help;
- one configuration describes one Workspace;
- physical names may overlap across types;
- type-mismatched bindings fail;
- configured defaults resolve correctly;
- inline overrides resolve correctly;
- one logical item cannot map to two physical targets in one build;
- implicit `_weaver` binding is always present.

## 31.4 Delta DDL

Prove that:

- every generated Delta table uses `CREATE TABLE IF NOT EXISTS`;
- no generated Delta table uses `CREATE OR REPLACE TABLE`;
- views retain replace semantics;
- repeated build preserves existing table rows;
- wipe can still remove tables explicitly.

## 31.5 Mandatory catalogue

Prove that:

- no build API exposes `catalogue=False`;
- every ordinary build includes catalogue publication;
- registry certification is last;
- failed physical actions prevent certification;
- failed catalogue publication prevents successful certification;
- selective build preserves unrelated existing catalogue rows.

## 31.6 Target inventory and prune

Prove that:

- inventory contains all supported Weaver-manageable object types;
- objects removed from the repository are pruned;
- unmanaged or unsupported physical capabilities are not accidentally treated as Weaver objects;
- existing prune behaviour remains intact.

## 31.7 Reconciliation

Prove that:

- valid catalogue rows remain;
- catalogue rows for missing physical objects are removed from `ReconciledCatalogue`;
- delete DML is generated for stale roots;
- dependent catalogue metadata is removed;
- physical objects with no catalogue row do not generate stale-catalogue deletes;
- bundle generation consumes reconciled state;
- planner does not re-evaluate whether signatures are lying.

## 31.8 Planner purity

Prove that `generate_item_build_bundle`:

- does not call Workspace APIs;
- does not construct a resolver;
- does not read catalogue state;
- does not inspect Lakehouse state;
- does not inspect Warehouse state;
- does not open Spark or SQL connections;
- is deterministic for identical inputs.

## 31.9 Unbind

Prove that:

- named target rows are deleted even when the target does not exist;
- valid unrelated targets remain;
- no physical target objects are dropped;
- dependent metadata is deleted through cascade or ordered DML.

## 31.10 Wipe

Prove that:

- selected Weaver-managed target objects are removed;
- affected catalogue state is unbound automatically;
- no immediate follow-up unbind is required;
- unrelated target and catalogue state remains.

## 31.11 Catalogue recreation

Prove that:

- an existing Weaver Lakehouse with missing catalogue tables can rebuild them;
- recreated catalogue structures use non-destructive DDL;
- only successfully rebuilt targets are registered;
- no false “catalogue completeness” status is introduced.

---

# 32. Documentation acceptance criteria

The implementation is not complete until documentation demonstrates:

## Desktop

```text
install Python package
→ weaver install
→ weaver initialise
→ weaver push ./repository
→ weaver build
→ repeated build preserving data
→ weaver wipe
→ weaver unbind
```

## Fabric notebook

```text
make Weaver package available
→ initialise Workspace/Weaver Lakehouse
→ manually upload repository directly under Files/
→ parse and validate
→ build
→ repeated build preserving data
→ wipe
→ unbind
```

All public examples use:

- Workspace terminology;
- `Files/` as the repository root;
- no `--weaver-repository`;
- no Host terminology;
- no `catalogue=False`.

---

# 33. Completion criteria

This refactor is complete when:

- Workspace replaces Host in all public interfaces;
- one Workspace configuration represents one Workspace;
- physical-centric target defaults and typed binding selection work;
- `parse_item_repository` is the canonical pure-Python boundary;
- generated `_weaver` is composed without mutating authored source;
- `weaver push ./ilg --weaver-lakehouse Weaver` validates and uploads the whole repository;
- uploaded contents land at `Files/Lakehouse` and `Files/Warehouse`;
- `--weaver-repository` no longer exists;
- all bound physical Fabric items are assumed to exist before build;
- `Lakehouse/_weaver` participates in every build;
- all Delta table creation is `CREATE TABLE IF NOT EXISTS`;
- ordinary build never implicitly replaces Delta data;
- `catalogue=False` no longer exists;
- every ordinary build publishes catalogue state;
- catalogue and target readers prepare current state;
- reconciliation returns an independently testable `ReconciledCatalogue`;
- reconciliation returns stale-row delete DML;
- missing physical objects invalidate lying catalogue signatures;
- catalogue foreign-key ownership supports complete deletion from root rows;
- bundle generation performs no remote discovery;
- target inventory supports current prune semantics;
- one logical item resolves to one physical target per invocation;
- future builds can replace the catalogue installation mapping for that logical item;
- `weaver unbind` removes rows for named targets regardless of target existence;
- catalogue reconciliation removes rows proven stale by physical inspection;
- wipe automatically unbinds targets it removes;
- the example repository and notebook demonstrate the complete lifecycle;
- incremental build selection remains a clearly separate next phase.
