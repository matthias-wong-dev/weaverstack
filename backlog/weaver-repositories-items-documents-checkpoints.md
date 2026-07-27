# Repository, Item and Document Re-architecture Checkpoints

## Status and use

This plan continues the construction recorded in
[`docs/journal.md`](../docs/journal.md). It supersedes the flat-repository and
single-target assumptions in the original step-by-step plan only for this
re-architecture; it does not rewrite that historical plan.

Each checkpoint is a review boundary. For each one:

1. read this checkpoint and the listed current files;
2. state what is ported and what is replaced;
3. raise any newly exposed decision and wait;
4. implement only the checkpoint;
5. update the journal in the same change;
6. present structure, behaviour and verification;
7. wait for approval before beginning the next checkpoint.

The target contract is
[`weaver-architecture-summary.md`](weaver-architecture-summary.md). The governing
build constraints remain
[`docs/build-philosophy.md`](../docs/build-philosophy.md).

## Checkpoint map

| Checkpoint | Outcome | Status |
|---|---|---|
| R0 | Accepted architecture and implementation seams are recorded | complete |
| R1 | Repository/item/document identity and vocabulary exist | complete |
| R2 | The item-oriented repository layout is read statically | complete |
| R3 | Exact-case logical references and `alias.yml` resolve | complete |
| R4 | Dependencies, graph and sparse projection are item-owned | complete |
| R5 | Build bundles bind and coordinate multiple logical items | complete |
| R6 | The catalogue is item-scoped and `_weaver` is built in | complete |
| R7 | Lakehouse ownership unifies Tables, Files, prune and wipe | complete |
| R8 | Public API, CLI and compatibility surface use the new model | pending |
| R9 | Local and Fabric verticals prove the architecture | pending |

---

## R0 — Record the architecture

### Outcome

The repository contains the accepted architecture, settled decisions and this
checkpoint sequence. Current-state documentation is clearly distinguished from
the target so no intermediate checkpoint claims behaviour that has not landed.

### Port

- the journal's implementation history;
- the build philosophy's frozen-bundle and explicit-target invariants;
- the catalogue document as a description of the current implementation.

### Replace

- the architecture summary's flat SES repository and one-target model;
- the attached draft's unresolved underscore, wipe, binding and alias wording.

### Verification

- documentation links resolve;
- terminology is internally consistent;
- `git diff --check` passes;
- no Python source changes.

---

## R1 — Introduce repository, item and document identity

### Outcome

The core value model can represent one exact-case repository containing any
number of typed logical items, each owning schemas and object documents. Folder is
an object kind inside a Lakehouse, not a target.

### Read

- `src/weaver/ses/metadata.py`
- `src/weaver/ses/source.py`
- `src/weaver/ses/repository.py`
- `src/weaver/targets.py`
- `tests/test_ses_metadata.py`
- `tests/test_core_boundary.py`

### Port

- validated metadata values and immutable dataclasses;
- language/kind routing for materialisation behaviour;
- exact filename/header/class agreement;
- the single Weaver error hierarchy.

### Replace

- `SesRepository` with the canonical `WeaverRepository` model;
- `SesDocument` with `WeaverDocument` for object declarations;
- node identity `target_kind:Schema.Object` with item-qualified identity;
- target-kind ownership with `WeaverItem` ownership;
- schema-as-repository-resource with schema-as-item-resource.

`SchemaDocument` remains distinct from `WeaverDocument`.

### Observable behaviour

- `Lakehouse/Raw/Sales.Customer` and
  `Warehouse/Reporting/Sales.Customer` are distinct;
- logical parsing and lookup require exact case;
- case-only duplicate declarations are rejected;
- repository name comes from the repository directory;
- no CLI, planner or catalogue migration occurs yet.

### Verification

Focused unit tests cover value parsing, equality, canonical rendering, invalid
item types, exact-case lookup and physical-name independence. Core remains
importable without Spark or Fabric dependencies.

---

## R2 — Read the item-oriented repository layout

### Outcome

Static discovery reads the target directory structure and assigns every source
and schema document to exactly one item without importing user code.

### Read

- `src/weaver/ses/repository.py`
- `src/weaver/ses/source.py`
- `src/weaver/ses/schemas.py`
- `src/weaver/store.py`
- repository-reader and end-to-end fixture tests

### Port

- Store-based traversal;
- UTF-8/BOM and line-ending-stable hashing;
- structural Python AST and SQL parsing;
- filename/header/class agreement;
- support-file snapshotting and signatures.

### Replace

- root-only object discovery with `ItemType/ItemName` discovery;
- `_schemas/` with item-owned `schemas/`;
- root helper treatment with Lakehouse item `lib/`;
- root Folder documents with Lakehouse item `Files/`;
- broad ignored-path rules with the single authored `_ignore/` keyword.

### Observable behaviour

- multiple Lakehouses and Warehouses are discovered;
- the same schema ID may be declared independently in several items;
- a Delta and Folder document may share a base ID;
- `_ignore/` contributes nothing to discovery, installation or signature;
- every other underscore path is processed normally;
- user-authored `__init__.py` is rejected;
- no module is imported or executed.

### Verification

An end-to-end neutral `Estate` fixture demonstrates two Lakehouses, two
Warehouses, item-owned schemas, Files, lib helpers, same-name Delta/Folder
documents and parked `_ignore` content.

---

## R3 — Resolve logical references and repository aliases

### Outcome

One exact-case logical-reference grammar resolves item-relative and item-qualified
schemas, objects and Files objects. `alias.yml` is parsed as a destination-keyed
repository declaration.

### Read

- `src/weaver/ses/references.py`
- `src/weaver/ses/metadata.py`
- `src/weaver/ses/repository.py`
- catalogue projection tests involving descriptions and aliases

### Port

- reference-chain following and cycle detection;
- `$$` literal-dollar escaping;
- `[Column]` suffixes;
- the distinction between descriptive reuse and graph dependency.

### Replace

- ambiguous same-name cross-target reference selection with exact item ownership;
- document-local `Warehouse alias`/`Lakehouse alias` headers with `alias.yml`;
- tolerant unresolved metadata references with hard validation errors;
- case-folded reference indexes with exact-case indexes.

### Observable behaviour

- short references resolve only in the current item;
- canonical references resolve across items;
- descriptions, lineage, column notes, foreign keys and hyperlink targets share
  one resolver;
- aliases map destination to source, allow any item types, allow one source at
  several destinations and reject duplicate destinations;
- physical names are invalid in `alias.yml`;
- aliases produce logical repository state only.

### Verification

Unit and fixture tests cover schemas, tables, Files objects, chains, cycles,
casing errors, duplicate destinations, one-to-many source use and alias/native
namespace collisions.

---

## R4 — Make dependencies, graph and projection item-owned

### Outcome

Every graph node belongs to an exact Weaver item. Python relative imports,
two-part logical SQL names, authored physical SQL names and alias destinations
produce the correct consumer-owned edges without executing source.

### Read

- `src/weaver/ses/dependencies.py`
- `src/weaver/ses/graph.py`
- `src/weaver/ses/repository.py`
- `src/weaver/build_bundle/planner.py` projection section
- dependency tests by dialect

### Port

- Python AST import extraction;
- SQL relation-position extraction and span-preserving rewrite support;
- physical three- and four-part reference capture;
- graph cycle detection, layering and dependency provenance;
- declared `Dependencies` replacing discovery where still supported.

### Replace

- string-prefix Python object detection with resolved item module identity;
- namespace partitions based only on target kind with item namespaces;
- `is_within_repository` with `is_within_item`;
- projection by bound target type with projection by exact bound item;
- the fixpoint that drops a bound consumer merely because its producer item is
  unbound.

### Observable behaviour

- `.Files`, `..` and `lib` imports classify correctly;
- an item-local import may resolve through an alias destination;
- `lib` imports create no object edge;
- two-part SQL names remain consumer-item logical names;
- three- and four-part names remain physical and unchanged;
- bound consumers may assume unbound producers are static;
- only documents in bound items are selected for physical work.

### Verification

The fixture proves Delta-to-Folder, Folder-to-Delta, Folder-to-Folder,
cross-item alias, authored three-part and unbound-producer cases. Cycles remain
errors across item boundaries.

---

## R5 — Bind and coordinate multiple items in one bundle

### Outcome

The manifest represents logical-item bindings explicitly and plans any non-empty
subset of repository items as one coordinated bundle. Installation remains
mechanical and every batch names one physical target.

### Read

- `src/weaver/build_bundle/targets.py`
- `src/weaver/build_bundle/models.py`
- `src/weaver/build_bundle/planner.py`
- `src/weaver/build_bundle/installer.py`
- `src/weaver/build_bundle/bundle.py`
- build philosophy §§2–11 and §16

### Port

- immutable plan/sequence/batch/action hierarchy;
- target-bound batches and explicit control-plane target;
- canonical bundle hashing and payload validation;
- barriers, omissions and installation reporting;
- executor mechanics and explicit destination resolution;
- coordinated physical work followed by a catalogue tail.

### Replace

- `TargetBindings(lakehouse, warehouse)` with logical-item binding mappings;
- `_single_binding` and the one-physical-side restriction;
- bound target ids derived only from physical kind/name;
- target-kind projection with exact item projection;
- one-target sequence construction with global graph layers containing
  item-specific batches.

### Observable behaviour

- at least one binding is required;
- most repository items may remain unbound;
- two items of one type may bind to different physical items;
- the same physical item may be supplied twice without a special prohibition;
- `prune=False` is the caller's explicit unsafe sharing escape hatch;
- a three-part authored reference may participate in a coordinated build;
- retained alias usage fails planning with
  `NotImplementedError("Alias usage is not yet supported")` before mutation;
- a generated bundle remains installable after its repository snapshot is
  unavailable.

### Verification

Manifest tests prove deterministic identity, logical-to-physical binding,
multi-batch barriers, sparse bindings, unbound static ancestors, alias refusal,
physical reference preservation and no install-time repository interpretation.

---

## R6 — Scope the catalogue by item and build in `_weaver`

### Outcome

The ten catalogue tables identify installations and objects by
`(repository, item_type, item_name)`. The built-in catalogue is the repository's
`Lakehouse/_weaver` item and is built through the ordinary path.

### Read

- `src/weaver/catalogue/tables.py`
- `src/weaver/catalogue/projection.py`
- `src/weaver/catalogue/render.py`
- `src/weaver/catalogue/reconcile.py`
- `src/weaver/catalogue/reader.py`
- `src/weaver/catalogue/builtin.py`
- `src/weaver/setup.py`
- `docs/catalogue.md`

### Port

- one authoritative table-definition source;
- generated built-in documents;
- typed deterministic DML;
- scoped delete plus idempotent merge;
- tolerant reads for specifically recognised absence;
- dictionary → installation → registry ordering;
- exact one result per catalogue action.

### Replace

- scope `(repository, target_type)` with
  `(repository, item_type, item_name)`;
- target-type alias ownership with destination-keyed `alias.yml` rows;
- repository-relative dependency scope with consumer-item scope;
- the standalone `_weaver` repository with built-in `Lakehouse/_weaver`;
- built-in flat resources with the item-oriented repository structure.

### Observable behaviour

- two items of the same type have independent rows;
- rebinding one item updates only its Installation row;
- `_.Alias` reproduces `alias.yml` destination/source pairs;
- `_.Dependency` records the consumer item and authored two-/three-part name;
- Registry is published only after every retained item's physical and dictionary
  work succeeds;
- catalogue evolution is a destructive rebuild, with no migration promise yet.

### Verification

Regeneration tests keep built-in source byte-for-byte aligned with table
definitions. Projection, reconciliation, bootstrap and local Spark tests cover
multi-item scope and prove one item cannot delete another item's rows.

---

## R7 — Unify Lakehouse Tables, Files, prune and wipe

### Outcome

Lakehouse ownership drives all physical operations. Folder remains a first-class
document/object but has no independent binding or installation scope.

### Read

- `src/weaver/build_bundle/planner.py` prune and action sections
- `src/weaver/build_bundle/executors/folder.py`
- `src/weaver/wipe.py`
- `src/weaver/targets.py`
- local and Fabric wipe/build fixtures

### Port

- frozen, reviewable prune actions;
- fail-closed target inventory;
- reserved control-plane protection;
- Lakehouse path/name resolution;
- full physical wipe confirmation and reporting;
- dependency-safe Warehouse drops.

### Replace

- FolderTarget routing with the owning Lakehouse binding;
- separate folder and Delta managed sets with one Lakehouse item plan;
- separate folder/Delta catalogue installation scope;
- CLI/core operations that imply a Folder is independently deployable.

### Observable behaviour

- one Lakehouse build creates and prunes Tables and Files;
- prune removes physical objects absent from that item's retained documents;
- `prune=False` emits no destructive reconciliation actions;
- Lakehouse wipe clears both physical areas;
- Warehouse wipe remains physical and explicit;
- rebinding leaves the old physical item untouched;
- cleanup of an old binding names the physical item, not catalogue history.

### Verification

The same behavioural fixtures run locally and in Fabric. They call the real build,
prune and wipe functions and independently inspect both Tables and Files.

---

## R8 — Move the public API and CLI to the new vocabulary

### Outcome

Public names, errors, help and serialisable command results consistently separate
logical Weaver items from physical targets. Legacy flat-layout use receives a
clear migration error rather than an inferred adapter.

### Read

- `src/weaver/__init__.py`
- `src/weaver_cli/main.py`
- `docs/cli-usage.md`
- `docs/ses-repository.md`
- public API and neutrality tests

### Port

- CLI as a thin adapter over core functions;
- typed physical target resolution;
- caller-owned credential policy;
- serialisable result structures;
- confirmation gates for destructive wipe.

### Replace

- SES terminology with Weaver repository/document terminology;
- generic lakehouse/warehouse/folder build flags with repeatable logical-item
  bindings;
- independent folder-target wipe/build options;
- public `Ses*` types, retaining compatibility aliases only where genuinely
  cheap and isolated;
- flat repository errors with a concrete layout migration message.

### Observable behaviour

- build input is one repository plus one or more item bindings;
- wipe still names typed physical items;
- no semantics move into `weaver_cli`;
- no default workspace, item, repository or environment names appear;
- old flat repositories fail with instructions for moving documents, schemas,
  Files and lib helpers.

### Verification

Core-boundary, CLI parser, help-text, serialisation and neutrality tests pass. The
base package remains importable without CLI, PySpark or Fabric credentials.

---

## R9 — Prove the complete architecture

### Outcome

One neutral end-to-end estate proves the repository/item/document model in the
local emulator, from a desktop against Fabric where appropriate, and with Weaver
running inside Fabric.

### Read

- `tests/conftest.py`
- `tests/fabric/conftest.py`
- existing mixed-estate and catalogue fixtures
- `docs/fabric-testing.md`
- `docs/local-setup.md`

### Port

- one shared local Spark session with Delta cache cleanup;
- one stable Fabric Weaver/target fixture set per run;
- independent physical assertions rather than assertions through the write
  context;
- the same behavioural body across local and Fabric execution.

### Replace

- flat fixtures with the item-oriented `Estate` fixture;
- one-target build assumptions with sparse and coordinated item bindings;
- tests that reproduce an operation with tests that call the real public
  function;
- stale documentation and examples after the code has landed.

### Required proof

The completed vertical covers:

1. two Lakehouses and two Warehouses;
2. several bound and unbound items;
3. Tables and Files sharing one Lakehouse binding;
4. same-name Delta and Folder documents;
5. item-owned schemas with repeated schema IDs across items;
6. exact-case logical references and hard failures;
7. item-local and canonical metadata references;
8. destination-keyed aliases in graph and catalogue;
9. explicit alias-use refusal before mutation;
10. a supported authored three-part coordinated dependency;
11. multi-item catalogue tail and registry-last certification;
12. combined Lakehouse prune and full physical wipe;
13. bundle independence from the source repository;
14. row 1 local behaviour, row 2 desktop-to-Fabric access where relevant, and
    row 3 installed Weaver inside Fabric;
15. catalogue table display names preserve their canonical PascalCase on Fabric,
    separately from Fabric's host-chosen lower-case physical Delta directory;
16. the Fabric harness reports active or queued Livy sessions before requesting a
    scarce capacity slot, using the sessions collection API where available.

### Completion condition

The journal, architecture summary, build philosophy, catalogue guide, repository
authoring guide, CLI guide and test fixtures all describe the same shipped model.
Physical alias behaviour remains a clearly named future checkpoint rather than a
hidden fallback.
