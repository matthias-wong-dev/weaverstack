# Repository fragments: one composition path

The agreed plan for composing repositories from multiple trees, shipping package-owned static content as repository fragments, and retiring the bespoke paths that currently inject content into a repository.

**Nothing has landed yet.** Delete this document once the work lands. For the current implementation, see [code-architecture.md](../code-architecture.md) and [weaver-repository.md](../weaver-repository.md).

A **fragment** is a directory tree in ordinary Weaver repository layout, contributed alongside the authored repository root.

A fragment may contain:

- a complete item;
- part of an item;
- a single declaration such as `Lakehouse/Sales/shortcuts.py`;
- package-owned programmables such as `Warehouse/Sales/programmables/_.Load.sql`.

Repository layout is the fragment grammar. Identity, ownership and dialect come from the path exactly as they do for authored content.

## The problem

Weaver currently has several ways for package-owned content to enter a repository.

| Path | Where | What it contributes |
| --- | --- | --- |
| Rendered builtin files | `catalogue/builtin.py` `render_item_sources`, merged inside `parse_item_repository` | The `Warehouse/_weaver` item: catalogue schemas and runtime table DDL |
| Per-item generated declarations | `etl.generated_item_files`, merged inside `parse_item_repository` | ETL schema documents and Folder documents |
| Synthesised runtime references | `declaration/item_dependencies.py` `_with_runtime_references` | `_.Bookmark`-family shortcut pairs and declarations |
| Generated dispatch procedures | `declaration/tsql_entry.py` and `etl._entry_artefacts` | `_.Load` and `_.Test` procedures generated separately for each Warehouse |

`planned_shortcuts` exists because runtime shortcut declarations are created outside the repository's ordinary shortcut collection. It holds authored shortcuts plus synthesised shortcuts so later planning can see both.

That means `shortcuts` and `planned_shortcuts` describe the same concept at different stages.

The other significant problem is `_.Load` and `_.Test`. Their generated bodies enumerate installed objects, so changing the contents of a Warehouse changes the dispatcher procedure even though dispatch itself has not changed.

The repository should contain declarations. Dispatch should resolve the requested object at call time.

## Destination

Repository construction becomes:

```text
READ       authored tree and applicable fragment trees
MERGE      declarations by identity
DERIVE     content-dependent declarations and implementations
ASSEMBLE   validate, sign, resolve dependencies, create WeaverRepository
```

There is no separate runtime-reference injection stage and no separate attachment stage.

Fragments are read through the same repository readers as authored content.

After merge there is one effective set of declarations.

### READ

Read:

1. the authored repository tree;
2. the package fragment trees that apply to each item.

Every declaration records its origin for diagnostics.

Reading a fragment does not require that the fragment be a valid repository by itself. A fragment may contain only one declaration. Whole-repository validation happens after merge.

### MERGE

Declarations are merged by Weaver identity.

Rules:

- new identity: add it;
- same identity from a later contributor: later contributor wins;
- duplicate identity within one contributor: error;
- case-only identity collision: error;
- package-sealed namespace violation: error.

For example, two `shortcuts.py` files do not merge as Python text. Each is parsed into shortcut declarations and those declarations merge by shortcut identity.

Origin is diagnostic information only. It does not affect signatures.

### DERIVE

Only content that genuinely depends on the merged repository is derived after merge.

Examples include:

- generated per-object load procedures;
- generated validation procedures;
- generated Folder/schema declarations where these remain content-dependent.

Static package-owned content should not be generated in Python merely because Weaver owns it. It should be shipped as fragment files.

### ASSEMBLE

Once contribution, merge and derivation are complete:

- validate declarations;
- calculate signatures;
- resolve shortcut pairs;
- resolve dependencies;
- construct dependency graphs;
- construct the final `WeaverRepository`.

Downstream code sees one repository.

## Package fragments

Proposed package layout:

```text
src/weaver/repo_fragments/
├── _weaver/
│   └── Warehouse/_weaver/
│       ├── schemas/_.yml
│       ├── _.Registry.sql
│       ├── _.Bookmark.sql
│       ├── _.Log.sql
│       ├── _.LoadStatus.sql
│       ├── _.LoadStatistic.sql
│       └── _.TestStatus.sql
│
├── lakehouse_runtime/
│   └── shortcuts.py
│
└── warehouse_runtime/
    ├── shortcuts.yml
    └── programmables/
        ├── _.Load.sql
        └── _.Test.sql
```

The exact grouping can change during implementation. The important rule is that these are ordinary repository declarations read by ordinary repository readers.

## Fragment application

Fragments apply by item type.

Every Lakehouse receives the Lakehouse runtime fragment.

Every Warehouse receives the Warehouse runtime fragment.

`Warehouse/_weaver` receives its catalogue fragment and does not receive runtime views over tables it owns directly.

This removes `item_presents_runtime_tables`.

Uniform runtime pointers on items that do not currently use them are acceptable.

## `_weaver`

The catalogue item is checked in as repository files rather than rendered into memory by `catalogue/builtin.py`.

The `_weaver` namespace remains sealed.

Any non-package contributor attempting to declare:

```text
Warehouse/_weaver/**
```

is refused.

The seal is enforced as a contributor/namespace rule rather than by special logic inside `parse_item_repository`.

## Programmables

Introduce an ordinary Warehouse declaration kind for stored procedures:

```text
Warehouse/<Item>/programmables/<Schema>.<Procedure>.sql
```

For example:

```text
Warehouse/Serving/programmables/_.Load.sql
Warehouse/Serving/programmables/_.Test.sql
Warehouse/Serving/programmables/Sales.RebuildSummary.sql
```

Read time requires exactly one:

```sql
CREATE PROCEDURE
```

or:

```sql
CREATE OR ALTER PROCEDURE
```

statement representing the declared procedure.

Unsupported SQL in `programmables/` fails during repository discovery rather than during installation.

A programmable remains a stored-procedure physical object. Its Registry role describes what Weaver uses it for.

| Instance | Origin | Role |
| --- | --- | --- |
| User procedure | authored | `programmable` |
| `[Load X.Y]` implementation | derived | `load` |
| Test procedure | derived | `test` |
| Assumption procedure | derived | `assumption` |
| `_.Load` | fragment | `programmable` |
| `_.Test` | fragment | `programmable` |

There is no `entry` role.

`ROLE_ENTRY` is deleted.

### Installation

Programmables install after the other physical actions in their item.

They do not require dependency edges between procedures. T-SQL deferred name resolution allows procedures to reference procedures that are installed later in the same batch of work.

Authored programmables may declare only into authorable item schemas.

Generated/package-controlled namespaces such as `_` and the ETL implementation schema remain sealed from authored declarations.

## Static `_.Load` and `_.Test`

`_.Load` and `_.Test` become checked-in programmables.

Their SQL does not enumerate the objects installed in the Warehouse.

### `_.Load`

`_.Load` receives an object name and dispatches to the corresponding generated implementation procedure.

Conceptually:

```text
exec _.[Load] @object_name = 'Sales.Customer'
        ↓
exec [Load Sales.Customer] ...
```

The dispatcher:

1. validates `@object_name` against the supported two-part identifier grammar;
2. constructs the implementation procedure name from the validated value;
3. calls it through one `sp_executesql` template;
4. receives the standard load outputs;
5. records `_.Log`, `_.LoadStatus` and bookmark state using the existing runtime contract;
6. rethrows errors after recording where the current contract requires it.

Invalid identifier text is refused before dynamic SQL is constructed.

A valid object name whose implementation does not exist fails naturally when SQL Server/Fabric attempts to execute the missing procedure.

The dispatcher does not require a Registry row before attempting dispatch.

That matters during recovery from a partially failed build: the implementation procedure may physically exist before its Registry certification has been published.

Registry may be used after execution to enrich runtime records where appropriate, but not as a prerequisite for dispatch.

### `_.Test`

`_.Test` follows the same model.

It dispatches dynamically to the requested validation implementation rather than containing a generated branch for every validation installed in the Warehouse.

Tests and assumptions currently have different output contracts. Keep separate execution templates if necessary rather than rebuilding object enumeration.

## Runtime references

Runtime references become fragment declarations.

Delete:

```text
_with_runtime_references
planned_shortcuts
```

A Lakehouse runtime shortcut or Warehouse runtime view is read and merged exactly like an authored shortcut declaration.

After repository construction:

```python
repository.shortcuts
```

contains the complete shortcut set.

`logical_shortcuts` remains the resolved logical source/destination representation used for dependency resolution, ordering and freshness.

There is no separate collection of "planned" shortcuts.

### Catalogue host

A Warehouse that physically hosts the catalogue owns the real runtime tables.

It must not create runtime views over those same tables.

The fragment-selection/composition rule therefore omits the Warehouse runtime pointer declarations where the bound item is the catalogue host.

This case is explicit and tested.

## Retired code

| Delete | Current location | Replacement |
| --- | --- | --- |
| `render_item_sources` / `item_repository_files` | `catalogue/builtin.py` | Checked-in `_weaver` fragment |
| Inline builtin merge | `declaration/repository.py` | Ordinary contributor read/merge |
| Authored `_weaver` special-case refusal inside parse | `declaration/repository.py` | Generic sealed-namespace rule |
| `_with_runtime_references` | `declaration/item_dependencies.py` | Runtime shortcut fragments |
| `planned_shortcuts` | `WeaverRepository` | `shortcuts` |
| `item_presents_runtime_tables` | `etl.py` | Item-type fragment selection |
| `generate_load_entry` | `declaration/tsql_entry.py` | Static `_.Load.sql` |
| `generate_test_entry` | `declaration/tsql_entry.py` | Static `_.Test.sql` |
| `_entry_artefact` / `_entry_artefacts` | `etl.py` | Ordinary programmable declarations |
| `TSQL_ENTRY_VERSION` | entry generation | Nothing |
| Entry-specific signature salts | `etl.py` | Ordinary declaration/file signatures |
| `ROLE_ENTRY` | Registry/runtime vocabulary | Nothing |

Surviving concepts:

- `logical_shortcuts`;
- generated per-object load procedures;
- generated validation procedures;
- generated Folder/schema declarations that genuinely depend on repository contents;
- the existing load/test/assumption roles;
- `programmable` for ordinary stored procedures.

## Workstreams

### W1. Split repository construction

Refactor repository parsing into explicit stages without changing behaviour:

```text
read
merge
derive
assemble
```

Add origin information to parsed declarations.

Fragments may be partial trees; whole-repository validation occurs after merge.

Guard:

- existing pure suite;
- existing build fixed-point tests;
- no signature changes for an unchanged repository.

### W2. Ship `_weaver` as files

Create the checked-in catalogue fragment under:

```text
src/weaver/repo_fragments/_weaver/
```

Replace the rendered builtin-file merge with normal fragment contribution.

Delete the old builtin renderer once parity is established.

Add a pure parity test ensuring the shipped runtime-table DDL agrees with the runtime reader/writer expectations in `catalogue/tables.py`.

### W3. Runtime pointer fragments

Create Lakehouse and Warehouse runtime shortcut/view fragments.

Select them by item type during repository construction.

Delete:

```text
_with_runtime_references
planned_shortcuts
item_presents_runtime_tables
```

After merge, runtime and authored shortcuts are indistinguishable to downstream code.

Cover explicitly:

- ordinary Lakehouse;
- ordinary Warehouse;
- catalogue-host Warehouse;
- logical dependency resolution;
- fixed-point rebuild.

### W4. Programmables

Add:

```text
Warehouse/<Item>/programmables/
```

Introduce the stored-procedure declaration/physical kind and ordinary reader.

Add discovery-time SQL validation.

Retype generated load/test/assumption procedures onto the same stored-procedure representation with their existing semantic roles.

Add authored programmable support.

Install programmables late within the item without procedure-to-procedure dependency edges.

### W5. Static dispatchers

Ship:

```text
_.Load.sql
_.Test.sql
```

inside the Warehouse runtime fragment as ordinary programmables.

Delete generated dispatch procedure construction and associated signature/version machinery.

Delete `ROLE_ENTRY`.

Verify:

- valid dispatch;
- invalid identifier refusal;
- missing implementation refusal;
- output parameter propagation;
- runtime logging/status;
- bookmark advancement;
- implementation callable when physical procedure exists but Registry certification is absent.

Run the Fabric acceptance estate for the behaviour change.

Add a focused marked Fabric test for direct:

```sql
exec _.[Load] ...
exec _.[Test] ...
```

against an installed Warehouse.

### W6. Documentation and deletion

Update:

- `code-architecture.md`;
- `weaver-repository.md`;
- `how-does-build-work.md`.

Describe:

- repository contributors;
- fragment selection;
- identity-level merge;
- derivation;
- programmables;
- static dispatch.

Delete this plan after the implementation and documentation are complete.

Order:

```text
W1 -> W2 -> W3 -> W4 -> W5 -> W6
```

Each workstream should leave the fast suite green.

## Fixed-point requirement

Moving a declaration from generated Python content to a fragment must not cause unnecessary rebuild churn.

For unchanged semantics:

```text
build
build again
```

must remain a fixed point.

Origin metadata must not enter declaration signatures.

Where existing signatures are based on declaration content rather than source file representation, preserve that rule.

## Risks

### Signature churn

Moving declarations between generation and fragment contribution could accidentally alter signatures.

Guard every workstream with fixed-point tests.

### Shared catalogue host

The catalogue Warehouse owns the real runtime tables and must not receive views with the same names.

Fragment selection must explicitly omit those pointer declarations for the catalogue host.

### Dynamic SQL

`_.Load` and `_.Test` construct implementation procedure names dynamically.

Object-name validation therefore has to happen before dynamic SQL construction.

The generated implementation procedure family must retain a stable output contract.

### Partial build state

Dispatch must not depend on Registry certification existing before the implementation can run.

A failed build may leave valid physical work ahead of Registry publication. The next build is expected to recover from that state, and standalone dispatch should not introduce a contradictory existence rule.

### Runtime-record identity

The static dispatcher no longer has generated knowledge of the logical item it belongs to.

Before W5 lands, verify exactly how `_.Log`, `_.LoadStatus`, `_.TestStatus` and bookmark writes obtain the required logical item identity when Registry certification is absent.

Do not silently weaken the runtime-state contract to make static dispatch easier.

## Deferred

### Fragment override policy

Contributor order determines replacement:

```text
later contributor wins
```

Do not add per-declaration priorities or additional override modes until a real consumer requires them.

### Authored programmable metadata

Start with SQL text plus identity/origin.

Do not add a metadata header format until a feature needs it.

### Test and assumption output convergence

`_.Test` may require separate templates for tests and assumptions if their output contracts remain different.

There is no need to force them into one SQL shape as part of this work.

## Not planned

### Text-level file merging

Do not merge `shortcuts.py`, YAML or SQL source text.

Parse declarations and merge by Weaver identity.

### Fragment class hierarchy

A fragment is repository content plus provenance/order.

Do not create a class hierarchy for fragments unless behaviour eventually requires one.

### Two dispatcher implementations

Do not keep generated enumeration for small Warehouses and dynamic dispatch for large ones.

There is one `_.Load` implementation and one `_.Test` implementation.

### Per-declaration priority

Contributor ordering controls overrides.

Do not add declaration-level priority fields.
