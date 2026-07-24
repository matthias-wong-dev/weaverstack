# Weaver Build Philosophy

## Status

This document records the governing philosophy of Weaver's build system.

It is not an implementation plan and it is not a description of one particular
version of the code. It defines the properties that implementations must
preserve as Weaver evolves across local Spark, Microsoft Fabric Lakehouse and
Warehouse targets.

Where an implementation conflicts with this document, the conflict should be
treated as an architectural decision requiring explicit resolution rather than
an incidental coding detail.

---

## 1. Build is a controlled transition of structure

A Weaver repository declares the desired structure of a data estate.

A build turns that declaration into physical structure:

- schemas;
- folders;
- Delta tables;
- views;
- procedures and other supported database objects.

A build does **not** populate that structure with source data.

The distinction is absolute:

> **Build creates and reconciles structure. Load moves and transforms data.**

Build must never call an object's `read()` implementation, inspect a CSV or
spreadsheet to discover its runtime data, execute merge policy, advance a
bookmark, or otherwise perform load work.

An empty Delta table created from a declared schema is a successful build.
Populating that table is a separate load operation.

This boundary prevents deployment from quietly becoming data movement and
allows structural changes to be reviewed, tested and installed independently
of operational data processing.

---

## 2. Interpretation happens once

The source repository is interpreted during bundle generation.

That interpretation includes:

- reading and validating SES source documents;
- resolving object identity and type;
- resolving target bindings;
- constructing the dependency graph;
- projecting the graph onto the supplied targets;
- compiling create and drop operations;
- establishing execution order;
- calculating payload hashes and bundle identity.

After this point, the repository is no longer an input to installation.

The intended flow is:

```text
SES repository
    ↓
repository interpretation
    ↓
target inspection and reconciliation
    ↓
frozen BuildBundle
    ↓
installation
```

The installer must never:

- reopen SES source documents;
- import SES Python classes;
- rediscover schemas or dependencies;
- infer omitted build metadata;
- re-evaluate target projection;
- decide which objects are stale;
- regenerate DDL;
- reinterpret author intent.

This creates a foundational invariant:

> **The source is planned once; the resulting plan is executed many times.**

The repository may be deleted after bundle generation and the certified bundle
must remain independently installable.

---

## 3. The bundle is the complete contract

A BuildBundle is not a hint to an intelligent deployment engine. It is the
complete, frozen description of the intended installation.

It must contain everything required to execute the build, including:

- fully bound target identity;
- ordered actions and barriers;
- exact create payloads;
- exact destructive payloads;
- exact paths for filesystem operations;
- action metadata needed for reporting;
- content hashes;
- a deterministic bundle identity;
- a readable summary of structural effects.

The bundle represents the contract between planning and execution:

```text
what was interpreted
=
what was reviewed
=
what was approved
=
what is executed
```

A bundle should be inspectable without executing it. A reviewer must be able to
see, before production installation:

- what will be created;
- what will be replaced;
- what will be dropped;
- what will be deleted;
- in what order;
- against which target.

The bundle is therefore both an execution artifact and an audit artifact.

---

## 4. Installation is deliberately unintelligent

The installer validates and executes the bundle. It does not make architectural
decisions.

Its responsibilities are intentionally narrow:

1. validate the bundle structure;
2. validate payload presence and hashes;
3. establish the declared target connections;
4. execute actions in their frozen order;
5. respect sequence barriers and failure policy;
6. record one result for every attempted action;
7. produce an installation report.

Executors should be mechanical. Typical executors include:

- execute this exact Spark SQL payload;
- execute this exact T-SQL payload;
- create this exact directory;
- delete this exact target-relative path.

An executor may resolve transport details required to perform an action, but it
must not change the meaning or scope of the action.

For example, a folder-delete executor may translate a frozen target-relative
path into the correct OneLake or local filesystem operation. It may not list the
parent directory and decide what else ought to be deleted.

This is a feature, not a limitation:

> **A production installer should be boring.**

---

## 5. Prune is planned and frozen

Prune is part of the build, and therefore part of the bundle.

The planner compares:

```text
current target structure
−
desired repository structure
=
stale target objects
```

It then compiles each stale object into an explicit destructive action.

Examples include:

```sql
DROP VIEW IF EXISTS `DWG`.`OldView`
```

```sql
DROP TABLE IF EXISTS `DWG`.`OldCustomer`
```

or a filesystem action containing the exact path:

```text
Files/Raw/OldCustomerCsv
```

The installer must not dynamically inventory the target and calculate a new
prune set at execution time.

Dynamic installation-time prune is unsafe because the deletion scope can be
expanded by a transient catalogue failure, incomplete registration, incorrect
target binding or executor defect. A supposedly empty catalogue could otherwise
be interpreted as permission to remove an entire physical estate.

Freezing prune moves this risk into an observable planning stage. A bad
inventory can still produce a bad plan, but the proposed destructive operations
are explicit, reviewable and rejectable before execution.

The governing rule is:

> **No object is dropped merely because an installer discovered it at runtime.**

Every destructive action must already exist in the certified bundle.

---

## 6. Target inspection is a planning concern

Real reconciliation requires knowledge of both desired state and current state.

Bundle generation may therefore inspect the target's structural metadata:

- schemas;
- registered tables;
- registered views;
- other supported catalogue objects;
- managed table directories;
- managed folder paths.

This inspection should use system catalogues, metadata APIs and directory
listings. It must not read business data merely to determine structure.

Target inspection is lightweight compared with installation or load. More
importantly, it belongs on the side of the architecture where decisions are
made and can be reviewed.

Target inspection must fail closed.

If the planner cannot establish a complete and trustworthy inventory, it must
not guess that missing objects are absent and must not emit destructive actions
from an uncertain comparison.

Acceptable outcomes are:

- fail bundle generation;
- generate a bundle with prune explicitly disabled, when the caller requested
  that mode;
- report the incomplete inventory clearly.

Silently falling back to installation-time discovery is not acceptable.

---

## 7. Declared structure is authoritative

Build structure comes from declarations, not from observing source data.

For a Delta table, the schema used to create the table must be supplied by the
SES declaration or by another explicitly supported compile-time declaration.

The builder must not infer a production table schema by reading:

- CSV files;
- Excel workbooks;
- JSON payloads;
- Parquet files;
- API responses;
- arbitrary Python return values.

Those sources are operationally unstable. Inference makes deployment behaviour
depend on whichever sample happens to be available during planning or
installation.

The default rule is:

```text
declared schema
→ compile empty table DDL

no declared schema
→ explicit unsupported/not-implemented result
```

Future schema inference may be introduced as an explicit, dependency-aware
feature, but it must never masquerade as declaration and must never occur
silently inside installation.

---

## 8. Dependencies determine order, not runtime discovery

The repository dependency graph is resolved before the bundle is written.

The bundle carries an execution structure such as:

```text
plan
└── sequences
    └── batches
        └── actions
```

Sequences are barriers. Later sequences do not begin until earlier structural
requirements have succeeded.

This supports cases such as:

```text
schema
→ Delta table
→ view
→ view on view
```

Prune order must likewise be explicit and dependency-safe. In general,
destructive operations proceed from dependants toward dependencies:

```text
views
→ tables
→ folders
→ schemas
```

Create operations proceed in the opposite structural direction.

The installer follows the encoded ordering. It does not reconstruct the graph.

---

## 9. Physical binding is explicit

Logical objects become installable only when bound to physical targets.

A bundle must identify the target sufficiently to prevent installation against
an unintended Lakehouse, Warehouse, workspace, local root or environment.

Names at one level must not accidentally resolve through mutable shared state
at another level. In particular, catalogue registration, current-session
defaults and attached-Lakehouse context must not be treated as reliable target
identity.

The planner and target adapter are responsible for producing actions whose
physical destination is unambiguous.

The installer should reject unresolved or incompatible bindings rather than
select a plausible default.

> **Convenience may shorten configuration, but it must not weaken target
> identity.**

---

## 10. Determinism is a safety property

Given the same:

- repository content;
- target bindings;
- trusted target inventory;
- Weaver build version and supported capabilities;

bundle generation should produce the same semantic actions and the same bundle
identity.

Canonical ordering and content hashing are not cosmetic. They enable:

- reliable review;
- reproducible installation;
- comparison between environments;
- certification and approval workflows;
- detection of payload tampering;
- diagnosis of what actually ran.

Timestamps, temporary paths and nondeterministic traversal order should not
alter the bundle's semantic identity.

Where target state differs, the reconciliation actions may legitimately differ.
That difference must be visible in the bundle rather than discovered during
installation.

---

## 11. Fail before mutation where possible

The builder and installer have different opportunities to fail.

Bundle generation should reject:

- invalid SES declarations;
- unresolved object identity;
- missing required schemas;
- unsupported object types;
- invalid dependency references;
- cycles where cycles are not supported;
- unbound required targets;
- incomplete target inventory for requested prune;
- payloads that cannot be compiled deterministically.

Before the first installation action, the installer should reject:

- malformed manifests;
- unsupported bundle versions;
- missing payloads;
- payload hash mismatches;
- incompatible executor capabilities;
- target identity mismatches.

Once mutation begins, failures must be reported precisely and later barriers
must not run when their prerequisites failed.

Early validation is valuable when it is authoritative. It must not become a
second speculative interpreter that rejects valid repository behaviour based on
heuristics.

---

## 12. Reports describe execution, not intention

The bundle states intention. The InstallationReport states what occurred.

For every action, the report should preserve:

- action identity;
- target;
- action kind;
- start and finish status;
- success, failure or skip;
- error details;
- relevant executor output;
- bundle identity.

The report must make partial installation visible. It should be possible to
distinguish:

- an action that was not selected;
- an action skipped because an earlier barrier failed;
- an action attempted and failed;
- an action completed successfully.

The report must not rewrite the plan after the fact. It records execution of the
frozen contract.

---

## 13. Build policy may evolve without weakening the boundaries

Some structural policies are intentionally versioned decisions rather than
eternal rules.

For example, an early implementation may use:

```sql
CREATE OR REPLACE TABLE
```

for every declared Delta table. A later implementation may add change
classification, schema reshaping, data-preservation policy or prohibited
rebuilds.

Those policies can evolve while preserving the deeper invariants:

- build does not load;
- the planner makes the decision;
- the exact chosen operation is frozen into the bundle;
- destructive consequences are visible before installation;
- the installer does not reinterpret policy.

Thus, replace-on-build may be acceptable as an explicit current policy. An
installer deciding at runtime whether to replace is not.

---

## 14. Local and Fabric execution share semantics

Local execution is not a separate product with approximately similar
behaviour. It is an implementation of the same build contract.

The same bundle model should support:

- local Spark installation;
- Fabric Spark through Livy;
- Fabric notebook execution;
- Warehouse execution through the supported SQL transport.

Transports and capabilities differ. Semantics should not.

A capability that is unavailable on one host should produce an explicit
unsupported result during planning or pre-installation validation. It should not
be simulated through behaviour that changes the meaning of the build.

Host-specific adapters may determine **how** an action runs. They do not
determine **what** the action means.

---

## 15. Tests protect invariants, not merely code paths

The most important tests demonstrate architectural properties.

The build system should prove that:

### The bundle is independent

Generate a bundle, remove or make unavailable the SES source repository, and
successfully install from the bundle alone.

### Build does not load

Use SES objects whose `read()` methods would fail if invoked. Building and
installing their structure must still succeed.

### Declared schema is used

Create an empty Delta table from the declared schema without reading sample
source data.

### Dependencies are frozen

Install table → view → view-on-view from the encoded action order without
reconstructing the repository graph.

### Prune is explicit

Show that every drop or delete operation appears in the bundle before
installation.

### Installation cannot broaden prune

Change or corrupt target catalogue visibility after bundle generation and prove
that the installer executes only the destructive actions already frozen.

### Payloads are immutable

Change a payload after certification and prove installation stops before any
mutation.

### Barriers are honoured

Cause an action to fail and prove dependent later sequences do not execute.

### Target binding is real

Install equivalent logical names into distinct targets and prove that neither
installation can mutate the other.

Tests should exercise Fabric behavior through the local emulator and in Fabric
wherever the capability exists. The behavioural assertion should remain the
same; only the fixture and transport should differ.

---

## 16. Architectural anti-patterns

The following are not harmless shortcuts. They violate the build model.

### Reopening the source during installation

```text
action names a Python class
→ installer imports class
→ installer reparses source document
```

This creates a second interpretation point and makes the bundle incomplete.

### Calling `read()` during build

This crosses the build/load boundary and makes structural deployment depend on
source-data availability.

### Inferring schema from current source data

This makes infrastructure vary according to transient operational input.

### Dynamic prune inside the installer

```text
installer lists target
→ installer decides what is stale
→ installer deletes it
```

This makes the reviewed bundle different from the executed plan and permits
runtime catalogue failures to broaden deletion scope.

### Relying on ambient catalogue or attached-target context

This permits identically named schemas or objects to resolve to the wrong
physical target.

### Hiding unsupported behaviour behind fallback

A clear `NotImplementedError` is safer than silently switching execution model,
schema source, target or language.

### Treating bundle payloads as templates

Payloads that require the installer to fill in semantic decisions are not
frozen payloads. Runtime substitution is acceptable only for strictly
transport-level values whose meaning was already bound and validated.

---

## 17. The standard of review

A reviewer of a BuildBundle should be able to answer:

1. Which physical targets will be changed?
2. Which objects will be created or replaced?
3. Which objects and paths will be removed?
4. Why is each destructive action present?
5. What is the dependency-safe execution order?
6. Which capabilities or branches were omitted, and why?
7. Can this artifact execute without the source repository?
8. Will execution perform exactly these actions and no others?

If those questions cannot be answered from the bundle and its summary, the
bundle is not complete enough.

---

## 18. Governing principles

The philosophy can be condensed into ten rules:

1. **Build creates structure; load moves data.**
2. **Interpret the repository once.**
3. **Make every decision before installation.**
4. **Freeze create and destructive actions alike.**
5. **Treat the bundle as the complete, reviewable contract.**
6. **Keep the installer mechanical and incapable of broadening scope.**
7. **Use declared structure rather than observed source data.**
8. **Bind physical targets explicitly.**
9. **Fail closed when inventory, identity or capability is uncertain.**
10. **Test the invariants that make production mistakes impossible.**

These constraints are intentionally strict because they encode accumulated
production experience. Their purpose is not ceremony. Their purpose is to move
risk earlier, make consequential actions visible, and prevent entire classes of
deployment failure by construction.
