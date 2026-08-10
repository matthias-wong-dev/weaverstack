# Weaver Runtime Architecture, Session Reuse, and Test-Factor Refactor

## Status

Proposed implementation plan for the next major Weaver architecture refactor.

This plan combines:

- the existing Weaver Session/interactivity direction;
- the current `BuildState` / `BuildBundle` / installer seams already present in the repository;
- a new first-class `Runner`;
- explicit Python-first representations and physical boundaries;
- a revised test taxonomy;
- mock / thin / full run-cycle fixtures;
- physical-estate reuse;
- targeted test migration;
- an explicit deletion plan so the refactor removes old architecture rather than layering over it;
- a functional `weaver session` CLI and pytest Session reuse in the same deliverable.

The richer interactivity and remote-execution decomposition work remains a later project. This deliverable establishes the architecture that makes that later work small and safe.

---

# 1. Goals

## 1.1 Primary architectural goal

Weaver should spend most of its time manipulating Python representations.

The physical world should be crossed deliberately and for a named reason:

- **installation** crosses through `InstallAction`s;
- **runtime data engineering** crosses through **Primitives**;
- **observation/persistence** crosses through explicit boundary methods;
- **Fabric resource discovery and execution transport** cross through `Session`.

The four central active objects are:

```text
Session
Builder
Installer
Runner
```

Everything else should be one of:

1. a Python representation;
2. a narrow adapter/boundary function;
3. an InstallAction implementation;
4. a runtime Primitive implementation.

Do not introduce another manager/coordinator/environment/runner abstraction unless one of these four demonstrably cannot own the behaviour.

## 1.2 Primary testing goal

A test should pay for the lowest physical boundary that its assertion actually proves.

In particular:

> A real run does not require a real build.

and:

> A run-cycle test does not require real data-engineering work unless that physical interaction is part of the claim.

This refactor must produce an observable test-speed improvement, not merely cleaner classes.

## 1.3 Primary developer-experience goal

The same reusable `Session` must power:

```text
weaver session
    wipe
    build
    load
    test
```

without repeatedly starting Livy, reacquiring Fabric authentication, or re-resolving the same Fabric item identities.

The same architecture must be reused by pytest.

---

# 2. Architectural principles

## 2.1 Python-first core

The central handovers are Python objects:

```text
Repository
BuildState
Catalogue
TargetInventory
BuildBundle
RunRequest
RunGraph
RunResult
InstallationReport
```

They should be constructible directly in tests.

A physical catalogue is not the Catalogue. It is one persistence representation of `Catalogue`.

A physical estate is not the `TargetInventory`. It is what a boundary reader observes to produce a `TargetInventory`.

A physical log is not Runner state. It is an optional persisted sink of Runner results/events.

## 2.2 Explicit physical boundaries

Higher-level domain code must not casually reach into:

- Fabric REST;
- OneLake;
- Spark catalogue state;
- TDS;
- Livy;
- physical catalogue Delta tables;
- `_ /Log`;
- filesystem/runtime deployment locations.

Those crossings occur through narrow boundary functions or Session capabilities.

## 2.3 Decisions happen before mutation

Build follows:

```text
Repository + BuildState
        ↓
      Builder
        ↓
    BuildBundle
        ↓
     Installer
        ↓
 physical mutation
```

`Installer` does not rediscover why an action should happen.

Runtime follows:

```text
Catalogue + RunRequest
        ↓
      Runner
        ↓
  RunGraph / state
        ↓
 Primitive dispatch
        ↓
 physical runtime effect
```

`Runner` does not read the repository and should depend on physical state only at actual execution boundaries.

## 2.4 Delete parallel architecture

Every new first-class abstraction added in this refactor must absorb or delete an existing competing abstraction.

No transitional parallel architecture should remain at the end of the project.

---

# 3. Core architecture

```text
                             PYTHON DOMAIN

Repository
    │
    │                    BuildState
    │             ┌──────────┴──────────┐
    │             │                     │
    │         Catalogue          TargetInventory
    │             │                     │
    └─────────────┴────────┬────────────┘
                           ▼
                        Builder
                           │
                           ▼
                      BuildBundle
                    + InstallActions
                           │
                           ▼
                       Installer
                           │
                       via Session
═══════════════════════════╪══════════════════════════════
                           ▼
                      physical estate
                           │
              ┌────────────┴────────────┐
              │                         │
        observe target            read catalogue
              │                         │
              ▼                         ▼
       TargetInventory               Catalogue
                                          │
                                          ▼
                                        Runner
                                ┌─────────┴─────────┐
                                │                   │
                             RunGraph           Run state
                                │                   │
                                └─────────┬─────────┘
                                          │
                                   invoke Primitive
                                          │
                                      via Session
══════════════════════════════════════════╪══════════════
                                          ▼
                               real runtime operation
                                          │
                                          ▼
                                      RunResult
                                          │
                                   optional boundary
                                          ▼
                                        _/Log
```

`Session` spans the physical edges by supplying:

- cached resolution;
- Store/path access;
- Fabric authentication;
- Spark/Livy;
- SQL/TDS;
- host-neutral execution capabilities;
- resource lifecycle;
- transport telemetry;
- reporting context.

`Session` does **not** own:

- Builder reconciliation state;
- BuildBundle state;
- Installer action state;
- Runner DAG/node state;
- the authoritative RunResult.

---

# 4. The four doers

# 4.1 Session

## Purpose

`Session` is Weaver's reusable execution scope.

It answers:

> Where am I running, what resources do I already have, what physical thing does this logical Fabric reference mean, and how do I execute work in this host?

It does **not** answer:

> What should be built?

or:

> What should run next in this DAG?

## Ownership

```text
Session
├── workspace/config
├── session id / lifetime
├── item/workspace resolution cache
├── Store/path resolution
├── reporting context
├── transport/resource timing
├── optional runtime resources
│   ├── Fabric authentication
│   ├── Livy
│   ├── Spark/current notebook runtime
│   └── SQL/TDS
└── host-neutral execution capabilities
    ├── execute_python(...)
    ├── execute_spark_sql(...)
    └── execute_tsql(...)
```

### Important addition to the existing Session plan

Fabric item resolution belongs to Session lifetime.

The current repository already caches resolution inside `FabricResolver` and `FabricSessionResolver`. The architectural change is to ensure that one reusable Session owns one resolver/cache for its lifetime rather than operations repeatedly recreating it.

The cache key must remain typed:

```text
(workspace, item_type, item_name)
```

because a Lakehouse and Warehouse may share a display name.

## Reporting ownership

The attached Session plan establishes Task / Step / Sub-step reporting on Session.

Keep that direction, but distinguish:

```text
Session reporting context
    = what is currently being presented/timed to the user

Runner state
    = canonical state of one runtime execution
```

Session may know:

```text
current Task
current Step
current Sub-step
presentation/timing history
transport timings
```

but it must not become the owner of:

```text
RunGraph
node readiness
node status
run result aggregation
BuildBundle planning state
```

This prevents the Session from becoming a second Runner.

## Base interface

Conceptually:

```python
class Session:
    workspace: WorkspaceConfig

    # Reporting context
    def task_started(...): ...
    def task_completed(...): ...
    def task_failed(...): ...

    def step_started(...): ...
    def step_completed(...): ...
    def step_failed(...): ...

    def substep_started(...): ...
    def substep_completed(...): ...
    def substep_failed(...): ...

    # Resolution
    def resolve_workspace(...): ...
    def resolve_item(...): ...

    # Host-neutral execution capabilities
    def execute_python(...): ...
    def execute_spark_sql(...): ...
    def execute_tsql(...): ...

    # Resource lifecycle
    def close(...): ...
```

Do not expose Livy/TDS as the interface used by domain code.

For Console:

```text
execute_python      → Livy
execute_spark_sql   → Livy
execute_tsql        → TDS
```

For Notebook:

```text
execute_python      → current process
execute_spark_sql   → spark.sql(...)
execute_tsql        → current Fabric SQL mechanism
```

Transport-specific methods remain private/internal:

```python
ConsoleSession._livy_submit(...)
ConsoleSession._sql_submit(...)
```

## Resource state

Each expensive resource should have explicit state:

```text
not_started
starting
ready
failed
closed
```

Concurrent requests for one resource must share the same startup/acquisition operation.

A SQL statement failure must not automatically destroy a healthy TDS resource.

A Spark statement failure must not automatically destroy a healthy Livy session.

A genuinely dead Livy resource may be reacquired according to bounded, observable recovery semantics.

## Session types

Required concrete host implementations:

```text
ConsoleSession
NotebookSession
```

Local Spark pytest execution should use the same Session contract through the narrowest existing local adapter rather than inventing a parallel test-only runtime architecture.

Do not add additional top-level Session types merely to match every test directory unless the runtime semantics genuinely differ.

---

# 4.2 Builder

## Purpose

Builder answers:

> Given repository intent and a Python snapshot of the current estate, what installation work should happen?

## Input

```python
Builder(
    repository=repository,
    state=BuildState(
        catalogue=catalogue,
        target_inventories=inventories,
    ),
    bindings=bindings,
)
```

## Output

```python
BuildBundle
```

## Required invariant

After its inputs have been supplied, Builder is pure Python.

Builder must not:

- query Fabric REST;
- read Spark catalogue state;
- inspect OneLake;
- connect to Warehouse;
- read physical catalogue Delta tables;
- perform physical installation.

## Existing repo fit

The current repository is already very close:

- `BuildState` already holds `Catalogue + TargetInventory`;
- bundle generation is already separated from installation;
- `build_item_repository()` explicitly describes a planner/executor seam.

This is primarily an ownership/naming refactor, not a rewrite of build semantics.

## Reconciliation

Reconciliation belongs on the decision side.

The preferred end-state is:

```text
Repository
+ BuildState
+ bindings
    ↓
 Builder
    ├── reconciliation
    ├── impact/fixed-point logic
    └── action generation
    ↓
BuildBundle
```

If moving reconciliation inside Builder causes unnecessary churn, it may remain a pure helper during migration, but it must not require physical access.

---

# 4.3 Installer

## Purpose

Installer answers:

> Execute this already-decided BuildBundle against the current Session.

## Interface

```python
report = Installer(session).install(bundle)
```

## Responsibilities

```text
validate bundle
resolve bundle targets through Session
walk sequences/batches
execute InstallActions
record action outcomes
apply install stop/continue rules
return InstallationReport
```

Installer must never:

- reopen repository source;
- resolve dependencies;
- decide which target should be selected;
- inspect the estate to decide whether the Builder was right;
- replan the bundle.

## Existing repo fit

The current `install_bundle()` implementation already states essentially this contract.

The refactor makes the contract first-class and replaces `InstallationEnvironment` with Session.

---

# 4.4 Runner

## Purpose

Runner answers:

> Given the installed runtime description in Catalogue and a request, what should run, when is it ready, what happened, and what is the final result?

Runner is one production runtime concept for:

- load;
- test;
- assumption;
- runtime barriers such as endpoint refresh.

Do not create separate `LoadRunner` and `TestRunner`.

## Input

```python
Runner(
    catalogue=catalogue,
    request=RunRequest(...),
)
```

## State owned by Runner

```text
Catalogue snapshot
RunRequest
InstalledEstate projection
RunGraph
resolved runtime nodes
node runtime states
ready/blocked/pending state
per-node result
per-node timing
messages/errors
run status
RunResult
events
```

## Interface

Planning must work without a Session:

```python
runner = Runner(catalogue, request)
runner.plan()

assert runner.graph == ...
```

Execution crosses outward only at Primitive dispatch:

```python
result = runner.run(
    session=session,
    dispatch=dispatch_primitive,
)
```

For pure tests:

```python
result = runner.run(
    session=None,
    dispatch=controlled_dispatch,
)
```

`dispatch` is a function/callable seam, not a new top-level `RunExecutor` doer.

## Runtime indirection

The Catalogue/Registry is the runtime indirection layer.

Production registry entries point at real installed runtime artefacts.

Run-cycle fixtures may point those same entries at deliberately trivial fixture artefacts.

Runner must not know the difference.

No:

```text
test_mode
fake_result
fixture_node
simulated_error
```

fields may leak into production Runner state.

---

# 5. Representations

Representations are inert Python state.

They should be:

- inspectable;
- serializable where useful;
- constructible directly in tests;
- free of hidden physical access.

## Repository

Developer-authored intent after parsing.

## Catalogue

What Weaver knows/manages about the installed estate.

The physical catalogue is one persistence form.

## TargetInventory

What Weaver observed physically at a target at a point in time.

It is a snapshot.

It does not mutate itself and does not expose installation methods.

## BuildState

```python
@dataclass(frozen=True)
class BuildState:
    catalogue: Catalogue
    target_inventories: Mapping[WeaverItemId, TargetInventory]
```

The authoritative Python handover from observation into Builder.

## BuildBundle

Canonical handover from Builder into Installer.

Contains:

```text
targets
ordered sequences/batches
InstallActions
payloads
dependencies
signatures
provenance
execution metadata
```

No desktop-specific bundle representation.

## RunRequest

Requested runtime scope and policy:

```text
operation kind / selected node roles
requested targets/names
fault tolerance
dry-run
other runtime policy
```

Avoid separate orchestration engines for load/test.

## RunGraph

Immutable/inspectable runtime topology owned by Runner.

This replaces `LoadPlan` as the primary runtime-plan concept while preserving the useful inspectable DAG representation.

## RunResult

Canonical in-memory result of one Runner execution.

Physical log publication is downstream of this object.

---

# 6. InstallAction

Rename the existing generic BuildAction abstraction to **InstallAction**.

It is not a generic action. It is an instruction emitted by Builder for Installer.

Examples:

```text
create_schema
build_table
build_view
build_folder
write_file
build_procedure
create_alias
drop_table
drop_view
drop_folder
prune_table
prune_view
prune_schema
prune_folder
refresh_sql_endpoint
publish_catalogue
publish_registry
```

## Direct execution seam

Preserve the existing architectural value of `execute_action()`, but make the naming explicit:

```python
execute_install_action(...)
```

This method executes exactly one InstallAction without:

- parsing a repository;
- reading the catalogue;
- generating a bundle;
- orchestrating bundle sequences;
- producing a whole installation report.

It is the direct physical seam for targeted install tests.

## InstallAction vs Primitive

Keep these concepts separate.

```text
InstallAction
    materialises/reconciles what the estate IS

Primitive
    performs runtime data-engineering behaviour
    against the installed estate
```

Do not create one generic "action/operation" abstraction spanning both.

---

# 7. Primitive

Primitive remains a narrow Weaver/data-engineering term.

A Primitive is:

> One installed runtime data-engineering thing executed directly, with its real semantics, and with no Runner orchestration needed to prove that primitive itself.

Examples:

```text
Python table load
Spark-SQL-backed table load
folder load
Warehouse load procedure
validation
assumption
endpoint refresh where treated as a runtime barrier/effect
```

Primitive does not mean:

```text
catalogue hydration
inventory readback
Fabric item resolution
log persistence
bundle serialization
```

Those are boundary crossings.

Primitive tests should continue to hit the real engine appropriate to the primitive.

---

# 8. Boundary crossings

Boundary crossings translate between Python state and external systems or provide physical access without embodying data-engineering semantics.

Examples:

```text
physical catalogue → Catalogue
physical target → TargetInventory

logical Fabric item → resolved Fabric Item
RunResult/events → physical _/Log

BuildBundle → archive
archive → BuildBundle

Session capability → Livy/TDS/notebook runtime
```

Prefer narrow functions/methods.

Do not automatically promote each boundary into another first-class service object.

Examples:

```python
read_catalogue(...)
read_target_inventory(...)
persist_run_log(...)
session.resolve_item(...)
session.execute_python(...)
```

---

# 9. Public operation flows

# 9.1 Build

Conceptually:

```python
def build(..., session=None):
    with use_or_create_session(session) as session:
        repository = prepare_repository(...)
        state = read_build_state(..., session=session)

        bundle = Builder(
            repository=repository,
            state=state,
            bindings=bindings,
        ).build()

        return Installer(session).install(bundle)
```

The combined convenience operation may remain public, but Builder and Installer must be independently callable internally.

## 9.2 Load

```python
def load(..., session=None):
    with use_or_create_session(session) as session:
        catalogue = read_catalogue(..., session=session)

        runner = Runner(
            catalogue=catalogue,
            request=RunRequest.load(...),
        )

        return runner.run(
            session=session,
            dispatch=dispatch_primitive,
        )
```

## 9.3 Test

```python
def test(..., session=None):
    with use_or_create_session(session) as session:
        catalogue = read_catalogue(..., session=session)

        runner = Runner(
            catalogue=catalogue,
            request=RunRequest.test(...),
        )

        return runner.run(
            session=session,
            dispatch=dispatch_primitive,
        )
```

## 9.4 Wipe

Wipe should also take/reuse Session so it receives:

- cached Fabric item resolution;
- Store/path context;
- Spark/Livy/TDS resources where required;
- common reporting/transport instrumentation.

Wipe does not need Builder or Runner.

---

# 10. Functional `weaver session` in this deliverable

The basic interactive Session CLI is part of this architecture deliverable.

Do not defer it to the later rich-interactivity project.

## CLI

```bash
weaver session
```

Example:

```text
$ weaver session

Weaver · Weaver Example
Starting Fabric resources...

weaver> wipe Lakehouse/Sales Warehouse/Reporting
...
weaver> build . --bind ...
...
weaver> load Lakehouse/Sales Warehouse/Reporting
...
weaver> test Lakehouse/Sales
...
weaver> exit
```

All commands reuse exactly one `ConsoleSession`.

## Requirements

The interactive shell must:

- create one ConsoleSession;
- reuse the existing CLI parser/dispatcher rather than inventing a second command grammar;
- execute `wipe`, `build`, `load`, and `test` through that same Session;
- preserve Session caches/resources across commands;
- survive ordinary command failures if Session resources remain healthy;
- close owned resources cleanly on exit.

## Startup policy

Following the attached Session design:

- prompt should become available immediately;
- Fabric auth and Livy startup may begin proactively/asynchronously;
- SQL/TDS may remain demand-driven initially;
- if an operation asks for a resource already starting, it waits on the same startup operation;
- duplicate resource acquisition is forbidden.

## Standalone CLI

Standalone commands still work:

```bash
weaver build ...
weaver wipe ...
weaver load ...
weaver test ...
```

Each creates a short-lived Session when no Session is injected.

Resource startup remains demand-driven for these one-shot commands.

## Explicitly deferred from this first deliverable

The shell does **not** need, yet:

- a TUI;
- keyboard navigation;
- rich live repainting;
- retry prompts;
- source-snippet panels;
- HTML notebook reporting;
- `compose`;
- decomposed host-driven install execution.

Those remain follow-on work.

---

# 11. Pytest Session reuse

Pytest must use the same Session architecture as product execution.

## Canonical Fabric fixture

Conceptually:

```python
@pytest.fixture(scope="session")
def weaver_session(...):
    with ConsoleSession(...) as session:
        yield session
```

Fabric integration tests reuse:

```text
one Session
one Fabric authentication context
one resolver/item cache
one Livy session when required
reused SQL/TDS capability where appropriate
```

## Marker-aware resource acquisition

```text
pure Python
    no Session physical resources

spark
    shared Spark-capable local Session/runtime
    no Fabric resources unless the claim requires Fabric

fabric
    shared ConsoleSession
    auth reused
    Livy reused
    SQL/TDS reused where safe

full integration
    reuse Session by default
    stronger isolation only when the claim explicitly requires it
```

## Explicit isolation

A fresh runtime must become an explicit fixture:

```python
fresh_weaver_session
```

or equivalent.

Fresh Session isolation is separate from estate isolation.

A test may require:

```text
fresh target state
same Session
```

and should not pay for new Livy/auth unless runtime isolation is the actual claim.

## Recovery semantics

Shared Session must distinguish:

```text
test/task failure
    → Session remains usable

statement failure
    → resource normally remains usable

resource failure
    → mark resource failed
    → bounded recovery/reacquisition

fatal Session failure
    → fail dependent tests clearly
```

Do not endlessly recreate dead resources and hide lifecycle defects.

## Diagnostics

Retain suite-level timings such as:

```text
Session lifetime
auth acquisition
Livy startup
Livy submissions
Livy execution/wait
TDS calls
Warehouse execution
endpoint refresh
item-resolution calls/cache hits
```

These should feed test-performance work without cluttering ordinary output.

---

# 12. Runner dispatch interface and run-cycle fixtures

The production Runner must interface with tests through the same dispatch seam it uses in production.

## Production

```python
runner.run(
    session=session,
    dispatch=dispatch_primitive,
)
```

`dispatch_primitive` is a narrow runtime function:

```text
RunNode
    ↓
primitive kind/runtime reference
    ↓
Session host-neutral capability
    ↓
real runtime artefact
```

It is not another doer.

## Mock

```python
runner.run(
    dispatch=controlled_dispatch,
)
```

`controlled_dispatch` returns configured outcomes immediately.

## Thin and full

Use the real `dispatch_primitive`.

The Catalogue Registry determines where runtime references point.

This is what makes synthetic runtime fixtures clean:

> Runner sees no distinction between a production runtime artefact and a deliberately trivial fixture artefact.

---

# 13. Run-cycle fixture family

Create an explicit run fixture family with three levels.

# 13.1 Mock run

## Purpose

Prove Runner state-machine/orchestration behaviour exhaustively and cheaply.

## Ingredients

```text
Python Catalogue
real Runner
controlled dispatch outcomes
no Spark
no Fabric
no physical catalogue
no physical log
no real runtime artefacts
```

## Canonical controlled outcomes

Fixture helpers should support at least:

```text
success
success with row counts
success with rejects
reported failure
Python exception
SQL-like exception
dispatch exception
malformed result contract
unsupported/skipped
endpoint-refresh failure
```

Do not emulate merge/data semantics.

The fixture returns a result or throws an error; that is all.

## Claims covered

```text
scope selection
DAG generation
dependency expansion
alias traversal
endpoint barrier insertion
ready-set calculation
ordering
dry-run
fail-fast
fault tolerance
pending/blocked/skipped states
status aggregation
result normalization
event generation
load/test/assumption node coexistence
future concurrency scheduler rules
```

These should run in milliseconds.

# 13.2 Thin real run

## Purpose

Exercise real dispatch and transport without paying for irrelevant data-engineering semantics.

## Ingredients

```text
synthetic Python Catalogue
Registry entries pointing at fixture artefacts
real Runner
real Session
real import/TDS/Livy/SQL transport
trivial fixture runtime artefacts
no business tables required
```

## Fixture runtime artefacts

Python examples:

```text
Success
    returns a normal successful LoadResult/TestResult

Failure
    returns a normal reported failure

PythonError
    raises RuntimeError

Malformed
    returns an invalid result contract
```

Warehouse examples:

```text
FixtureSuccess procedure
    sets normal output parameters

FixtureFailure procedure
    returns failure-shaped outputs where supported

FixtureSqlError procedure
    THROWs deliberately

FixtureNoisy procedure
    emits irrelevant result rows but valid named outputs
```

These fixture artefacts must not recreate Weaver's merge/update/delete logic.

Their purpose is controlled physical outcomes.

## Claims covered

```text
Runner → dispatch_primitive wiring
Session → Python/Livy wiring
Session → TDS wiring
exception normalization
named-output handling
malformed response handling
independent branch continuation across real transports
Session health after statement failure
```

# 13.3 Full real run

## Purpose

Prove interactions whose truth depends on real physical data-engineering effects over time.

## Ingredients

```text
Python Catalogue
real Runner
real Session
real runtime artefacts
minimal directly prepared physical targets
```

The physical target state may be established directly by fixtures.

A full real run does **not** imply Builder/Installer.

## Important full-run claims

Examples:

```text
Lakehouse load
    ↓
SQL endpoint refresh
    ↓
Warehouse consumer sees the new state
```

This cannot be proven purely and cannot be reduced to an isolated refresh primitive.

Likewise future concurrency:

```text
two genuine independent physical branches execute concurrently
one transport/primitive fails
the other genuinely completes
Runner settles dependent/independent state correctly
Session remains healthy where appropriate
```

## Full build/run journey is separate

Only a journey test proves:

```text
Repository
→ Builder
→ BuildBundle
→ Installer
→ physical Catalogue
→ hydrate Catalogue
→ Runner
→ real Primitive
```

---

# 14. Test taxonomy

Evolve the existing test-architecture taxonomy.

Directories and markers continue to describe **cost/environment**.

Claim suffixes describe **what is being proven**.

Recommended claims:

```text
declaration
representation
boundary
install
primitive
cycle
invariant
journey
```

## declaration

What authored/user-facing contracts accept/refuse.

Keep this category; it remains useful and already has substantial coverage.

## representation

Pure Python models and transformations.

Includes much of what is currently called:

```text
render
pure binding/planning
projection
reconciliation
graph construction
```

Examples:

```text
Repository → BuildBundle
Catalogue → InstalledEstate
Catalogue → RunGraph
result/status semantics
serialization contracts
SQL/text generation where engine execution is not the claim
```

## boundary

Python ↔ external/physical fidelity.

Examples:

```text
physical catalogue → Catalogue
physical target → TargetInventory
logical Fabric name → resolved item
BuildBundle archive round-trip
RunResult/event → log sink
```

Round-trip pairing is especially valuable:

```text
physical readback == Python fixture constructor
```

because it justifies using the Python fixture everywhere above it.

## install

One InstallAction genuinely executed against the relevant physical target.

This replaces generic "action" as the test-domain term.

Examples:

```text
create_schema InstallAction
build_table InstallAction
build_view InstallAction
create_alias InstallAction
refresh_endpoint InstallAction where install semantics are the claim
```

The existing action checklist becomes an **InstallAction checklist**.

## primitive

One runtime data-engineering Primitive executed directly.

Nothing orchestrates it.

Examples:

```text
Warehouse load primitive
Python table load primitive
folder primitive
validation primitive
assumption primitive
runtime endpoint refresh primitive
```

## cycle

Representations/state evolving through multiple transitions over time.

Examples:

```text
build cycle
run cycle
wipe/rebuild cycle
resource recovery cycle
```

"Cycle" replaces the overloaded "lifecycle" term.

A cycle is not automatically end-to-end.

## invariant

Architectural/system property enforced rather than trusted.

Examples:

```text
all InstallAction kinds have direct coverage
test naming/marker invariants
core import neutrality
public API invariants
```

## journey

Composition of major architecture parts.

Examples:

```text
repository → build → persisted catalogue → run
```

Journey tests should be few and should rarely be the first place a defect is detected.

---

# 15. Test naming conventions

Move toward:

```text
test_<subject>_<claim>.py
```

where `<claim>` is from the revised taxonomy.

Examples:

```text
test_builder_representation.py
test_catalogue_boundary.py
test_inventory_boundary.py
test_install_action_invariant.py
test_actions_delta_install.py
test_warehouse_load_primitive.py
test_run_cycle.py
test_build_cycle.py
test_local_lakehouse_journey.py
```

Function names for install coverage should explicitly say `install_action`:

```text
test_create_schema_install_action_creates_the_schema
test_build_table_install_action_creates_the_declared_columns
```

Update the checklist and invariant tests accordingly.

Migrate naming as files are touched rather than performing a giant no-value rename before behaviour changes.

---

# 16. Estate fixture architecture

The current blocker is that higher-level tests often reconstruct a physical estate merely to reach a lower seam.

Replace that with explicit fixture levels.

```text
Level 0
Python representation only

Level 1
mock run / pure Builder

Level 2
thin real runtime artefact/transport

Level 3
directly prepared physical target for Primitive/full-run claim

Level 4
installed estate produced by Builder + Installer

Level 5
journey crossing build + persisted catalogue + run
```

A test starts at the lowest level capable of answering its question.

## Canonical fixture families

```text
catalogue_factory
build_state_factory
target_inventory_factory
build_bundle_factory

controlled_run_outcomes
mock_run

thin_runtime_artefacts
thin_run

minimal_physical_target
full_run

installed_estate
journey_estate
```

Avoid one gigantic fixture that eagerly creates everything.

---

# 17. Estate reuse

Reuse is the default.

## Scope guidance

```text
Session/runtime resource
    session scoped

immutable installed estate
    module scoped

fixture runtime artefacts
    module/session scoped when names are unique and safe

scenario baseline
    module/scenario scoped where reset is cheap

mutation-specific estate
    function scoped only when isolation is the actual claim
```

## Rule

> Function-scoped full-estate setup requires an isolation reason.

If a test only reads the estate, it shares it.

If several tests assert properties of the same expensive transition, combine those assertions into one scenario or reuse the state.

## Remote transitions

Keep the current Fabric-economics principle:

> One remote state transition should produce one evidence payload; assertions about that moment stay local.

Extend it to setup:

> One expensive physical state transition should support all assertions about that resulting state before it is discarded.

---

# 18. Existing test refactor

# 18.1 Validation orchestration

Current pattern:

```text
author repository
→ real build
→ physical catalogue publication
→ install runtime artefacts
→ real load
→ finally invoke validation orchestration
```

Many assertions do not need any of those physical prerequisites.

Move to mock run-cycle coverage:

```text
every installed validation selected
one named validation selected
missing validation name
one failure does not stop independent validations
worst node determines run status
dry-run dispatches nothing
source/runtime selection
blocked/pending state
result aggregation
```

Keep Primitive coverage for genuine validation semantics:

```text
real Spark/DataFrame comparison
real Warehouse validation
counts/evidence
```

Add a small number of full real run tests with directly prepared data and runtime artefacts.

Keep very few true build→catalogue→run journeys.

# 18.2 Load orchestration

Move these kinds of claims to mock run-cycle tests:

```text
complete graph resolution
dry-run graph equals executed graph
dependency ordering
endpoint barrier insertion
primitive kind dispatch selection
blocked/pending/skipped classification
normalised per-node results
run status aggregation
```

Keep full real run tests for:

```text
downstream truly sees upstream physical data
folder output genuinely feeds another operation
endpoint refresh genuinely makes new state visible
future real concurrency/resource interaction
```

# 18.3 Catalogue build/lifecycle tests

Split into:

```text
representation
    pure projections/plans/reconciliation

boundary
    physical catalogue readback fidelity

cycle
    rebuild/prune/fixed-point transitions

journey
    only where full composition is the claim
```

Read-only assertions over one physical catalogue share one built estate.

# 18.4 Built-in catalogue tests

One physical built catalogue should support all read-only assertions about:

```text
table existence
columns
types
nullability
physical directories
publication/certification shape
```

Planner/action-shape claims move to pure representation tests.

Only actual rebuild/fixed-point transitions perform another build.

# 18.5 Warehouse Primitive tests

Do not fake genuine Warehouse Primitive semantics.

Keep real Fabric coverage for representative:

```text
insert
update/unchanged
delete
reject rollback/tolerance
identity
static/no-op
SQL-program execution
```

But consolidate duplicated expensive states.

Examples:

```text
"second run updates only changed row"
+
"unchanged row keeps update timestamp"

→ one second-run scenario
```

and:

```text
"tolerant rejection result"
+
"rejection evidence persisted"

→ one tolerant-rejection scenario
```

and:

```text
"static second run is no-op"
+
"intermediate tables cleaned"

→ one second-run scenario
```

## Retiring/delete fixture

The current function-scoped baseline repeatedly installs and first-loads the same object.

Replace with either:

- one sequential scenario;
- a shared baseline plus cheap direct reset;
- or a module-scoped installed object with per-case target-row reset.

Measure which is cheaper; do not assume rebuild is necessary.

# 18.6 Fabric alias/endpoints

Keep valuable real cycles.

Examples:

```text
producer mutation
→ endpoint refresh
→ Warehouse consumer sees new state

OneLake shortcut
→ source changes
→ consuming physical path remains coherent
```

These are exactly the claims Fabric should answer.

Reuse their expensive estate setup across all read-only assertions.

---

# 19. InstallAction test architecture

Rename the action checklist to reflect the domain.

Current concept:

```text
every action kind has a real execution test
```

New concept:

```text
every InstallAction kind that mutates a target
    has a direct install test on each materially different physical side
```

Catalogue publication InstallActions remain covered as catalogue boundary round trips where the persisted rows are the real claim.

The direct seam is:

```python
execute_install_action(...)
```

not:

```text
Repository → Builder → Bundle → Installer
```

when the assertion is merely "does this InstallAction work on the engine?"

---

# 20. Boundary fidelity tests

Maintain and strengthen paired fixture constructors.

For Catalogue:

```text
fixture Catalogue
        ↕
physical catalogue round-trip
```

For TargetInventory:

```text
fixture TargetInventory
        ↕
physical inventory reader
```

Once equality is proven, all Builder/Runner tests should construct the Python fixture directly.

Add equivalent narrow fidelity tests for any new serialized handover whose physical form matters.

---

# 21. Physical log decoupling

Runner state is authoritative in memory.

```text
Runner
├── events
├── node results
└── RunResult
```

Physical `_ /Log` is an optional boundary:

```text
RunResult/events
      ↓
   log sink
      ↓
    _/Log
```

Ordinary Runner correctness must not require a physical log.

Pure run-cycle tests therefore need no storage setup.

A small boundary suite proves log persistence.

A few journeys prove it composes with real runs.

Asynchronous log publication may be added later.

---

# 22. What we kill

This refactor is incomplete unless these competing abstractions/patterns disappear.

| Existing concept/pattern | End state |
|---|---|
| `InstallationEnvironment` | **DELETE**; Session owns these capabilities/resources |
| `LoadEnvironment` | **DELETE**; runtime dispatch uses Session |
| `LoadPlan` as the primary runtime ownership object | **ABSORB/DELETE**; Runner owns RunGraph |
| `ResolvedLoadPlan` as a lifecycle handoff | **DELETE**; Runner owns resolved node state |
| `execute_load_plan()` as orchestration API | **DELETE**; logic moves into `Runner.run()` |
| separate load/test orchestration engines | **DELETE**; one Runner |
| generic `BuildAction` naming | **RENAME** to `InstallAction` |
| generic `execute_action()` naming | **RENAME** to `execute_install_action()` |
| combined planner+installer responsibility in `build_item_repository()` | **SPLIT**; convenience wrapper may call Builder then Installer, but no hidden combined architecture |
| `install_bundle()` as the central architecture | **DEMOTE** to compatibility wrapper during migration, then remove internally in favour of `Installer.install()` |
| repeated operation-local resolver creation | **DELETE PATTERN**; one Session owns one resolver/cache |
| physical `_ /Log` as a Runner execution dependency | **DELETE DEPENDENCY** |
| old `lifecycle` test-claim terminology | **MIGRATE** to `cycle` |
| old `render` claim category | **FOLD** into `representation` |
| old `binding` claim category | **FOLD** into `representation` or `boundary` depending on whether physical access is involved |
| full-estate setup as the default orchestration fixture | **DELETE PATTERN** |
| test-only runtime orchestration path | **DELETE PATTERN**; pytest uses the real Session/Runner seams |

Things that remain:

```text
BuildState
Catalogue
TargetInventory
BuildBundle
InstallationReport
InstalledEstate or equivalent useful runtime projection
InstallAction models
InstallAction executor implementations
runtime Primitive implementations
FabricResolver / LocalResolver as Session-owned adapters
Store
physical catalogue/inventory readers
```

---

# 23. Module destination

Do not perform a giant package move before behaviour is stable, but the conceptual destination is:

```text
src/weaver/

  session/
    base.py
    console.py
    notebook.py

  build/
    state.py
    builder.py
    bundle.py
    install_actions.py
    installer.py
    inventory.py
    executors/

  catalogue/
    model.py
    persistence.py
    reconciliation.py

  run/
    request.py
    estate.py
    graph.py
    runner.py
    result.py
    events.py
    dispatch.py

  runtime/
    primitives/
```

This is a conceptual ownership map, not a requirement to rename every package in the first commit.

Avoid introducing duplicate modules while old ones remain indefinitely.

---

# 24. Implementation phases

# Phase 0 — Baseline and guardrails

Before changes:

1. record full pure/Spark/Fabric timing baselines;
2. capture `--durations`;
3. record Livy/TDS/endpoint-refresh telemetry;
4. identify current fixture scopes;
5. freeze behavioural expectations.

Current known baselines:

```text
Spark
247 passed
~11m13s

Fabric
82 passed
~19m32s
```

Add the deletion rule to `AGENTS.md` / architecture guidance:

> Every new first-class abstraction introduced by this project must absorb or delete the abstraction it replaces within the same migration phase.

# Phase 1 — Session foundation and resolver ownership

Implement/strengthen:

```text
Session
ConsoleSession
NotebookSession
```

Move into Session lifetime:

```text
workspace/config
resolver instance
typed item cache
Store/path context
auth state
Livy state
SQL/TDS state
transport timing
minimal reporting context
```

Keep host-neutral capabilities:

```text
execute_python
execute_spark_sql
execute_tsql
```

Do not decompose build/load execution yet.

# Phase 2 — Every operation accepts/reuses Session

Migrate:

```text
wipe
build
load
test
```

to an injectable Session.

Standalone calls create a short-lived Session.

Delete operation-local resolver/resource acquisition paths where replaced.

## Deliver `weaver session`

Add the persistent CLI shell now.

Required proof:

```text
one session
→ wipe
→ build
→ load
→ test
→ same Livy/session resources reused
```

Add one product-level test demonstrating resource identity/cache reuse.

## Pytest reuse

Introduce canonical session-scoped fixtures.

Migrate Fabric integration fixtures to reuse one ConsoleSession.

Keep physical estate isolation separate from Session isolation.

Measure immediate timing change.

# Phase 3 — Formalize Builder / Installer

Create thin first-class `Builder`.

Use current pure planning logic.

Rename:

```text
BuildAction → InstallAction
execute_action → execute_install_action
```

Create `Installer(session)` around current install semantics.

Delete `InstallationEnvironment`.

Split the combined build workflow so the canonical path is visibly:

```text
BuildState
→ Builder
→ BuildBundle
→ Installer
```

Keep public convenience wrappers only if they delegate cleanly.

# Phase 4 — Introduce Runner and remove load orchestration stack

Create one Runner for load/test/assumption.

Absorb:

```text
LoadPlan ownership
ResolvedLoadPlan
execute_load_plan
load execution state
test orchestration state
```

Preserve useful pure graph/InstalledEstate logic as representations/helpers.

Delete:

```text
LoadEnvironment
ResolvedLoadPlan
execute_load_plan
separate test orchestration engine
```

Runner planning must work from Catalogue alone.

Runner execution uses injected dispatch.

# Phase 5 — Boundary decoupling

Make explicit:

```text
physical catalogue → Catalogue
physical target → TargetInventory
RunResult/events → log sink
```

Remove physical log requirements from ordinary runs/tests.

Strengthen round-trip fidelity tests.

# Phase 6 — Run fixture family

Create:

```text
mock_run
thin_run
full_run
```

plus fixture constructors.

Implement controlled outcome matrix.

Create trivial Python/SQL fixture runtime artefacts for thin real runs.

Ensure Registry can point directly at these artefacts without production special cases.

# Phase 7 — Targeted test migration

Move orchestration claims out of Spark/Fabric where the physical engine is not part of the assertion.

Priority:

1. validation orchestration;
2. load orchestration;
3. catalogue planner/read-only lifecycle tests;
4. duplicated Warehouse scenarios;
5. alias/endpoint estate reuse.

Rename touched modules to revised taxonomy.

# Phase 8 — Estate reuse

Change expensive fixtures from function scope to module/session scope where state is immutable/recoverable.

Introduce cheap reset helpers where needed.

Combine multiple assertions over one expensive state transition.

Require explicit justification for fresh full-estate fixtures.

# Phase 9 — Final deletion and performance acceptance

Remove compatibility architecture that is no longer needed.

Run:

```text
pure
spark
fabric remote
hosted as appropriate
full integration
```

Compare timings and slowest tests.

Do not close the project merely because tests pass.

The performance shape must change.

---

# 25. Acceptance criteria

## Architecture

- [ ] Exactly four central doers: Session, Builder, Installer, Runner.
- [ ] Builder is pure after Repository + BuildState are supplied.
- [ ] Installer executes a BuildBundle without replanning.
- [ ] Runner plans from Catalogue without physical reads.
- [ ] Runner owns runtime DAG/state/results.
- [ ] Session owns reusable resources, resolution/cache and transport capabilities.
- [ ] Session does not own Runner node state.
- [ ] `Catalogue`, `TargetInventory`, `BuildState`, `BuildBundle`, `RunGraph`, `RunResult` are directly constructible Python representations.
- [ ] Physical log persistence is optional/downstream.
- [ ] Registry indirection can point runtime nodes at arbitrary real or fixture artefacts without special Runner logic.

## Deletion

- [ ] `InstallationEnvironment` gone.
- [ ] `LoadEnvironment` gone.
- [ ] `ResolvedLoadPlan` gone as a first-class handoff.
- [ ] `execute_load_plan()` gone as the orchestration API.
- [ ] LoadPlan no longer owns runtime lifecycle.
- [ ] Separate test orchestration engine gone.
- [ ] Generic BuildAction terminology migrated to InstallAction.
- [ ] Repeated resolver construction removed from migrated operations.
- [ ] No new compatibility layer left permanently beside its replacement.

## Session CLI

- [ ] `weaver session` starts one persistent ConsoleSession.
- [ ] prompt appears without blocking on all expensive resource startup.
- [ ] `wipe`, `build`, `load`, `test` run in the same Session.
- [ ] Fabric item-resolution cache survives across commands.
- [ ] Livy is started at most once while healthy.
- [ ] SQL/TDS is reused where safe.
- [ ] ordinary command failure does not discard a healthy Session.
- [ ] Session close releases owned resources.

## Pytest

- [ ] Fabric tests reuse one canonical Session fixture by default.
- [ ] local Spark tests use the same Session capability architecture without forcing Fabric.
- [ ] fresh runtime isolation is explicit.
- [ ] estate isolation is independent of Session isolation.
- [ ] transport/resource timing is available in diagnostics.

## Run fixtures

- [ ] mock run uses pure Catalogue + controlled outcomes.
- [ ] thin run uses real dispatch/transport and trivial fixture artefacts.
- [ ] full run uses real physical Primitives but does not require Builder.
- [ ] at least one journey proves Builder→Installer→physical Catalogue→Runner→Primitive.
- [ ] no production `test_mode` path exists.

## Test architecture

- [ ] taxonomy updated to declaration / representation / boundary / install / primitive / cycle / invariant / journey.
- [ ] action checklist becomes InstallAction checklist.
- [ ] orchestration claims migrate to cycle/representation tests.
- [ ] physical engine tests only remain where the engine/platform matters.
- [ ] immutable expensive estates are reused.
- [ ] repeated expensive state transitions are consolidated.

## Performance

Minimum target:

```text
Spark
< 8 minutes
stretch: ~5–7 minutes

Fabric
< 16 minutes
stretch: ~12–15 minutes
```

The exact headline number is secondary to the slow-test profile.

After the refactor, the slowest tests should mainly be recognisably valuable physical work:

```text
real Warehouse Primitive execution
real endpoint-refresh visibility cycles
real Fabric concurrency when added
full build/run journeys
```

The slow list should no longer be dominated by:

```text
build a whole estate
just to ask a Python orchestration question
```

---

# 26. Follow-on project: richer interactivity and decomposition

This architecture intentionally prepares but does not complete the later interactivity project.

After this refactor, the next project can add:

```text
Task / Step / Sub-step polished reporting
Console/Notebook presentation specialisation
structured source-aware errors
retry interaction
compose
async log publication
transport timing UI
host-driven decomposed Installer execution
finer-grained Runner remote dispatch
batch tuning
parallel Runner execution
real concurrency failure handling
```

The important difference from the original Session plan is that the hard ownership questions are already settled.

The follow-on does not need to invent:

```text
who owns the build decision?
who owns the run DAG?
who owns item-resolution cache?
who owns physical logs?
what is the runtime plan?
```

Those answers are:

```text
Builder      → build decision
Installer    → install execution
Runner       → run DAG/state/results
Session      → reusable physical execution context
boundary     → persistence/observation
```

Decomposition then becomes an execution-granularity problem rather than an architecture redesign.

---

# 27. Final mental model

## Four doers

```text
Session
    where/how

Builder
    what should be installed

Installer
    install the decision

Runner
    what runs next and what happened
```

## Representations

```text
Repository
Catalogue
TargetInventory
BuildState
BuildBundle
RunRequest
RunGraph
RunResult
InstallationReport
```

## Physical concepts

```text
InstallAction
    one installation instruction

Primitive
    one runtime data-engineering behaviour

Boundary
    Python ↔ external/physical translation or persistence
```

## Test concepts

```text
representation
    prove Python semantics

boundary
    prove physical/Python fidelity

install
    prove one InstallAction physically works

primitive
    prove one runtime data-engineering Primitive physically works

cycle
    prove state evolves correctly over time

journey
    prove major architecture parts compose
```

## Central rule

> Construct the Python handover directly unless the physical handover itself is what the test is proving.

and:

> Cross an expensive boundary only when that boundary is part of the assertion.

That is the architecture that lets Weaver become both easier to reason about and materially faster to develop.
