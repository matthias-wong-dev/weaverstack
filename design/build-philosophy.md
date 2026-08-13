# Weaver Build Philosophy

## Purpose

This document defines the build-system invariants that implementations must
preserve. For the product lifecycle, see
[Weaver architecture](weaver-architecture.md); for code ownership, see
[Code architecture](code-architecture.md).

## Status

This document defines the build invariants across local Spark, Microsoft Fabric
Lakehouses, and Warehouses. It is a contract, not an implementation diary or a
step-by-step build guide.

An implementation that conflicts with these invariants requires an explicit
architectural decision.

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

The source repository is interpreted during bundle generation. In Fabric, the
session first materialises the installed OneLake repository into a driver-local
temporary directory. Every interpretation step then reads that one local copy;
the laptop is not part of the product build path.

That interpretation includes:

- reading and validating Weaver source documents;
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
Weaver repository
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

- reopen Weaver document source documents;
- import Weaver document Python classes;
- rediscover schemas or dependencies;
- infer omitted build metadata;
- re-evaluate target projection;
- decide which objects are stale;
- regenerate DDL;
- reinterpret author intent.

This creates a foundational invariant:

> **The source is planned once; the resulting plan is executed many times.**

The repository may be deleted after bundle generation and the certified bundle
must remain independently installable. Independence does not require every build
to upload an artefact: the ordinary development path installs directly from its
temporary local bundle in the same session.

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

The bundle is therefore both an execution artifact and an audit artifact. Its
ordinary representation is a temporary local directory. When a durable record
or handover is wanted, that directory is packaged after generation or
installation as one `<timestamp>.weaver.zip`; the persisted archive is optional,
not an intermediate remote filesystem required by every build.

---

## 4. Installation is mechanical

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

The installer is intentionally limited to executing the reviewed bundle.

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

Because the order is frozen too, it must be dependency-safe *by construction*
rather than by relying on the engine to cascade. Where a target offers no
cascading drop — a Warehouse has no `DROP SCHEMA … CASCADE` — the planner emits
dependants before dependencies: views, then the tables they read, then the schema
once it is empty. An engine capability may shorten the plan; it may not be the
reason the plan is correct.

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

## 7. Structure comes from declaration or from query shape, never from source data

Build structure has exactly two authoritative sources: an explicit **declared
schema**, or the **output shape of an object's own query**. Both are stable,
reviewable properties of the frozen repository. Neither reads business data.

The builder must never infer a production table schema by reading:

- CSV files;
- Excel workbooks;
- JSON payloads;
- Parquet files;
- API responses;
- arbitrary Python return values.

Those sources are operationally unstable. Inference from them makes deployment
behaviour depend on whichever sample happens to be available during planning or
installation. That inference is forbidden.

### 7.1 Declared schema

For a Python-backed Delta table, the schema used to create the table must be
supplied by the Weaver document declaration or another explicitly supported compile-time
declaration. There is no query to consult:

```text
declared schema
→ compile empty table DDL

no declared schema (Python-backed Delta)
→ explicit unsupported/not-implemented result
```

### 7.2 Query-shape inference is declared structure, not source-data inference

A SQL-backed table — Spark SQL against the Lakehouse, or T-SQL against the
Warehouse — may **declare** its schema or **omit** it. When it omits it, the
physical business columns are the output columns of its own query.

This is not the forbidden inference of §7's opening. The line is exact:

> **Query-shape inference reads a query's output *types*. Source-data inference
> reads operational *rows*.**

A query's result shape is a deterministic function of frozen repository text and
of the ancestor tables built earlier in the same bundle's barrier order. It does
not vary with whichever CSV or API response happens to be present. Running the
query in shape-only form — every `SELECT` guarded so it returns its columns and
no rows — reads structure, never data.

When a schema is declared on a SQL-backed table, the declaration controls the
physical types and the query is analysed only to validate case-insensitive
column-set equivalence. Declared types are deliberately allowed to be wider or
more stable than the query currently infers.

### 7.3 Inference is deferred into one self-contained install action

Query-shape inference happens **at install time, inside a single self-contained
action** — never at plan time, and never as a multi-round-trip exchange.

Not at plan time, because inferring a table's shape requires running its query,
which requires its ancestors to exist. Plan time has no built target; forcing
ancestors to be materialised before a bundle can be produced would destroy the
self-contained bundle the rest of this document depends on.

Not as a round-trip, because an installer that did build-shape → fetch-types →
generate-DDL → execute as separate exchanges would be doing planning work it is
forbidden to do (§4). Instead the action is one unit that carries its own
inference: the target engine — which alone authoritatively knows the query's
result types — is the *type oracle* that renders and executes the create in a
single pass. In T-SQL this is one script that shapes a `WHERE 1=0` temp table,
reads its column metadata back through a frozen type mapping, and executes the
generated `CREATE TABLE` server-side. Spark carries the same magic: one
executor resolves the query's `DataFrame` schema and creates the table without
returning to the planner. Different transports, identical contract.

The payload for such an action stays frozen and deterministic — the same query
text and schema mode always render the same table. The engine supplies types;
it makes no decision about *which* object is built or *what* it means. That is
still fixed in the bundle. A payload that let the installer choose scope, target
or meaning would be the forbidden template of §16; a payload that uses the
engine only as a type oracle for its one named table is not.

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

The graph is item-owned. Every document belongs to one logical Weaver item, and
the build projects by exact bound item rather than by the broad fact that an
object eventually uses Lakehouse or Warehouse machinery. Several items of one
type may exist in one repository and must remain distinguishable throughout the
plan.

Bindings are deliberately sparse. An unbound item is outside the physical scope
of that build; it is not a deletion. A bound consumer may declare an unbound
producer and treat it as static. That does not certify the producer and does not
guarantee operational availability: a structural action that needs the producer
may fail at the engine, while a declared-schema Python table can be built without
touching it and will discover absence only during load.

---

## 9. Physical binding is explicit

A logical Weaver item becomes installable only when bound to a physical target.
Its documents inherit that one binding. Folder and Delta are object kinds inside a
Lakehouse item, not independently bindable destinations.

At least one item must be bound, but no complete-repository binding is required.
The bundle records both the logical item and its physical target so two items of
the same Fabric type cannot collapse into one manifest identity.

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

- invalid Weaver document declarations;
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

Delta creation currently uses:

```sql
CREATE TABLE IF NOT EXISTS
```

for every declared Delta table. A later implementation may add change
classification, schema reshaping or prohibited rebuilds without returning to
replace-style creation as the ordinary path.

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

A capability that is unavailable on one workspace should produce an explicit
unsupported result during planning or pre-installation validation. It should not
be simulated through behaviour that changes the meaning of the build.

That rule also applies to logical features whose physical implementation has not
landed. Repository aliases may be valid declarations, graph edges and catalogue
rows before Weaver can materialise them. A retained action that uses such an
alias must fail explicitly before mutation; it must not run the consumer's
two-part name against whichever physical item happens to be bound.

Workspace-specific adapters may determine **how** an action runs. They do not
determine **what** the action means.

On Fabric, both phases run inside the session. The session copies the OneLake
repository once to its driver-local temporary filesystem, generates and installs
the bundle there, and removes the working files afterwards. An optional handover
archive makes the inverse trip as one file: copy locally, extract, validate and
install without reopening the source repository.

---

## 15. Tests protect invariants, not merely code paths

The most important tests demonstrate architectural properties.

The build system should prove that:

### The bundle is independent

Generate a bundle, remove or make unavailable the Weaver document source repository, and
successfully install from the bundle alone. The bundle carries frozen outputs
only — no copy of the source — so this is a property of the artefact rather than
a discipline the installer observes.

### Build does not load

Use Weaver document objects whose `read()` methods would fail if invoked. Building and
installing their structure must still succeed.

Installing an item's load *code* does not cross this line and is worth being
exact about. A deployed module and a generated procedure are objects that must
exist before a load can run; a build creates, signs and prunes them, and never
runs them. The test is unchanged — nothing a build does executes authored code.

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

This makes infrastructure vary according to transient operational input. It is
distinct from query-shape inference (§7.2), which reads a query's output types,
not its rows, and is deterministic across operational data.

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

A two-part `Schema.Object` in a generated statement is this anti-pattern, even
though it looks like the opposite of one. It carries no path and names no
Lakehouse — and therefore takes whichever Lakehouse the session happens to be
attached to. Since the session is attached to the Weaver Lakehouse, every
destination statement written that way aims at the control plane.

An object is instead named logically in the payload and resolved against the
batch's target at install time:

```sql
CREATE OR REPLACE VIEW {{object:DWG.ActiveCustomer}} AS
SELECT * FROM {{object:DWG.Customer}} WHERE IsActive
```

On Fabric that resolves to the native four-part name,
`workspace.lakehouse.schema.object`; the local emulator folds the Lakehouse into
its one namespace level. Either way the destination is stated, not inherited, and
one session can build several Lakehouses without switching what it is attached to.

### Collapsing item ownership into target kind

`Lakehouse/Raw` and `Lakehouse/Curated` are two logical items even though both use
Spark and OneLake. Projecting or cataloguing them merely as `lakehouse` recreates
the single-target architecture and permits one item to prune or certify the
other. Materialisation kind chooses an executor; item ownership chooses identity,
binding and scope.

### Hiding unsupported behaviour behind fallback

A clear `NotImplementedError` is safer than silently switching execution model,
schema source, target or language.

### Treating bundle payloads as templates

Payloads that require the installer to fill in semantic decisions are not
frozen payloads. Runtime substitution is acceptable only for strictly
transport-level values whose meaning was already bound and validated. A
self-contained query-shape action (§7.3) is not a template: it fixes the object,
its query and its schema mode, and uses the engine only as a type oracle for the
one table it names.

Resolving `{{object:Schema.Name}}` against the batch's target is likewise not a
template. The object, its schema, the statement and the item the batch targets are
all fixed before the bundle is written; what the installer supplies is only how
that already-chosen destination spells a name. Freezing the spelling instead would
cost §10: a Fabric name carries workspace and Lakehouse display names, so two
bundles of one repository generated against different environments would differ in
every payload rather than only in their target block — and comparison between
environments is one of the things canonical hashing exists for.

The same reasoning forbids freezing a schema's `LOCATION`. It is a *resolved
path*: on Fabric it embeds workspace and item ids, and locally it embeds whichever
directory the caller was using. A schema-creating action therefore names the
schema, and the destination decides how to make one.

---

## 17. Bundle review

A reviewer of a BuildBundle should be able to answer:

1. Which physical targets will be changed?
2. Which objects will be created or replaced?
3. Which objects and paths will be removed?
4. Why is each destructive action present?
5. What is the dependency-safe execution order?
6. Which capabilities or branches were omitted, and why?
7. Can this artifact execute without the source repository?
8. Will execution perform exactly these actions and no others?

The bundle and its summary must answer these questions before installation.

---

## 18. Principles

1. **Build creates structure and the code that will load it; load moves data.**
2. **Interpret the repository once.**
3. **Make every decision before installation.**
4. **Freeze create and destructive actions alike.**
5. **Treat the bundle as the complete, reviewable contract.**
6. **Keep the installer mechanical and incapable of broadening scope.**
7. **Use declared structure rather than observed source data.**
8. **Bind physical targets explicitly.**
9. **Fail closed when inventory, identity or capability is uncertain.**
10. **Test the invariants that make production mistakes impossible.**

Together, these principles make the interpreted, reviewed, and executed plan the
same artefact.
