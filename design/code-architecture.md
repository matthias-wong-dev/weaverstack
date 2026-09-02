# Code architecture

## Purpose

This document explains how the repository implements the product architecture.
It complements [Weaver architecture](weaver-architecture.md), which defines
product behaviour and is authoritative for that behaviour.

## Responsibilities and handoffs

Weaver decides things in Python and performs physical work through a Session.
The repository keeps those responsibilities separate.

Planning, dependency selection, status tracking, and failure handling use
ordinary Python objects in memory. Access to a Lakehouse or Warehouse is confined
to named boundaries. The values passed between those responsibilities are plain
handoff objects.

The rest of this document identifies the handoffs between those responsibilities.

---

## The shape

```text
 WeaverRepository + BuildState  →  Builder  →  BuildBundle

    Catalogue + RunRequest      →  Runner   →   RunGraph

           BuildBundle / RunGraph  →  Installer / dispatch  →  Session
                                                                  │
                                                  TDS | OneLake | Livy | REST
```

Builders reconcile authored files with physical state. Runners trust the
installed catalogue and let dispatch report physical failures.

A `BuildBundle` describes everything a build intends to do. It can be printed,
compared, serialised, or used in a test before physical work begins.

---

## The four doers

Four components own runtime behaviour: `Session`, `Builder`, `Installer`, and
`Runner`.

```text
Session     supplies physical capabilities and resources
Builder     decides what should be installed
Installer   carries out a BuildBundle
Runner      decides what runs next, and records what happened
```

`Builder`, `Installer`, and `Runner` interpret Weaver semantics. `Session`
provides reusable physical capabilities and transport. `Session` does not
interpret dependency graphs; DAG selection and execution state belong to
`Runner`.

### Session

One process scope holding the expensive things: a credential, a resolver with an
item cache, a Livy session, a TDS connection per Warehouse, a store. It caches
these per workspace, so a `build` and the `load` that follows share them.

Callers ask for *capabilities*, never for transports:

```python
session.execute_python(program)  # here, or across to Fabric
session.execute_spark_sql(statement)  # this host's Spark, wherever it is
session.execute_tsql(statement, target)  # a Warehouse, over TDS
session.store(workspace)  # files
session.resolver(workspace)  # names → physical items
```

Domain code does not access `session.livy`; doing so would couple it to Fabric
transport.

Commands say up front what they will need, coarsely, and the Session starts those
in the background:

```text
weaver load Warehouse/Reporting   → auth, resolver, tds
weaver load Lakehouse/Sales       → auth, resolver, tds, onelake, livy
weaver load                       → auth, resolver, tds, onelake, livy
```

Commands declare the capabilities they may need. Preparation starts them in the
background, but a capability is acquired only when an executor uses it. A run
that declares `livy` and performs only T-SQL does not open a Spark session.

A Livy session is created against a Lakehouse, so a command that may want Spark
also offers the physical Lakehouses it was asked for, through
`Session.offer_spark_home`. That is a transport requirement and nothing more:
the Lakehouse is where the session lives, while every generated statement names
its own target in full. Nothing above the transport reads it, and a host that
already runs where Spark is ignores it.

A Session holds one Fabric workspace for its whole life, and an operation given a
borrowed one resolves that workspace as its base. A command's own configuration
travels beside the Session as *names*: `operation_workspace` applies `catalogue`
and `environment` on top of the base it found, so `load --catalogue
Warehouse/Curated` at a session prompt reads that catalogue without anything
about the Session changing. `weaver_cli` passes what its command line settled on,
and refuses a command naming a workspace other than the Session's rather than
resolving one it cannot run in.

### Builder

```text
WeaverRepository + BuildState  →  BuildBundle
```

Pure. It takes parsed authored files and a snapshot of the estate, and returns a
plan. It never reads Fabric, never opens a connection, never mutates anything.
Give it the same inputs twice and you get the same bundle twice.

That purity is why most build behaviour can be tested in milliseconds with no
tenant, no credentials and no JVM.

Build planning dispatches from the authored `WeaverItemId`. A Lakehouse item
calls Lakehouse prune, drop, schema, shortcut, document and endpoint planners. A
Warehouse item calls the corresponding Warehouse planners and has no endpoint
refresh step. Folder, Table and View describe object kind within an item.
Python, Spark SQL and T-SQL describe authored or executable language. They are
not build target categories.

### Installer

```text
BuildBundle  →  physical estate
```

`Installer` executes the actions already defined in a `BuildBundle`. It validates
the bundle, walks its sequences as barriers, calls the named executor for each
action, and records one result per action. Repository reading and dependency
resolution have already completed when installation begins.

### Runner

```text
RunState + RunRequest  →  RunResult
```

Selects nodes, builds the dependency graph, decides readiness, handles blocking
and fail-fast, and aggregates the result. One Runner serves both `weaver load`
and `weaver test`; what differs is which nodes are selected and which primitive
runs, not how a run behaves when one of them fails.

`RunRequest.items` is the requested scope. `InstalledDag` turns each item into
its physical target, which stays on every node for dispatch and barrier
placement, and selection is by item.

`Runner` delegates node execution through the `dispatch` callable. Production
execution supplies the estate-backed dispatcher; tests can supply a controlled
dispatcher for the same state machine.

What a run *records* is downstream of it, on purpose. A Runner with no catalogue
runs correctly; the operation that wants a durable record feeds each settled node
into the catalogue and flushes before reporting. Both rows go the same way,
because both are rows in the same `_` schema — but a lost `_.Log` row loses
evidence and a lost bookmark makes the next load re-read a window, so only the
second fails the operation.

---

## The representations

The handoff points, roughly in the order you meet them.

| | what it is |
|---|---|
| `WeaverRepository` | authored files, parsed and structurally checked — never executed |
| `Catalogue` | what Weaver installed, read from the catalogue Warehouse over TDS |
| `InstalledDag` | the managed installed graph `Catalogue.dag()` derives: nodes, resolved edges, topology |
| `TargetInventory` | what a physical target actually holds right now |
| `BuildState` | `Catalogue` + inventories, as one snapshot the Builder is handed |
| `BuildBundle` | the plan: sequences → batches → `InstallAction`s, plus frozen payloads |
| `RunRequest` | the items a run was asked for, and the policy it runs under |
| `RunState` | the catalogue a run plans against and records itself in, read once |
| `RunGraph` | the selected nodes and their edges |
| `Graph` | the one topology implementation: order, layers, ancestry, subgraphs |
| `RunResult` / reports | what happened, per node, per action |

The representations are plain values. A test can construct a `BuildState`
directly and assert a `RunGraph` without dispatching anything. `BuildBundle`
serialises to and from canonical `plan.yml`, so a bundle can be archived,
inspected, or executed by a different process from the one that planned it.

### One graph implementation, three topologies

`weaver.graph.Graph` owns the mechanics: known-node validation, self-dependency
and cycle refusal, deterministic ordering, layers, roots and leaves, direct
navigation, ancestors, descendants and subgraphs. Three models carry their own
node metadata and hand it the topology.

```text
                       weaver.graph.Graph
                       /       |             authored repository   installed estate   selected run
     dependency_graph      InstalledDag       RunGraph
     item_graph                 |
     item_layers          load_plan / test_plan / health
           |
         build
```

The authored graphs and the installed graph answer different questions. The
first is what the repository says depends on what, and build plans against it.
The second is what the managed estate currently contains and how those installed
nodes depend on one another.

`Catalogue.dag()` is an immutable derived view of catalogue rows already in
memory. It reads `_.Registry`, `_.Installation`, `_.Dependency`, `_.Shortcut`
and `_.TestDictionary`, and it is the one place a persisted
`dependency_reference` is interpreted: a Python import or a `Schema.Object`,
resolved against the consuming item's shortcuts. An import of `shortcuts`
resolves through the declaration of that name, so a folder shortcut and a table
sharing one `Schema.Object` stay apart. Load planning, validation planning and
health read the resolved graph and never a raw row.

A Test and an Assumption are ordinary terminal nodes. Nothing reads one, and
Weaver has no test-on-test ordering.

Two node kinds are refused when the graph is built: a managed cycle, and one
identity claimed by both an object and a validation. Two are recorded and left
to whichever operation touches them: a read that names nothing installed, and
two objects at one physical address.

`RunGraph` and `LoadDag` break ties on their own `sort_key`, through
`Graph.order(key=...)`, so a plan reads target by target.

### Carrying, not reconstructing

Values known during planning are carried forward rather than derived again during
execution.

The clearest case is source provenance. A build may involve hundreds of files, and
by the time an action fails the only spellings left are the deployed ones —
`_/Load/Tables/Sales__Customer.py`, `[_].[Load Sales.Customer]`. Neither is a file anyone
has open. So the authored path travels with the action:

```text
SourceDocument.relative_path
  → RuntimeArtefact.source_path
    → InstallAction.source_path
      → plan.yml, and back
        → ActionResult.source_path
          → "Source: Lakehouse/Sales/Tables/Sales__Customer.py"
```

The path cannot always be derived from the deployed artefact: a Spark SQL table
is authored as `.sql` and deployed as `.py` under a different name.

---

## The physical handoff

The following capabilities cross the boundary to a physical environment.

A `Session` decides how each capability is met, based on where the code is
running:

| | in a Fabric notebook | desktop → Fabric |
|---|---|---|
| `execute_python` | call it | submit over Livy |
| `execute_spark_sql`, `execute_spark_sql_batch` | `spark.sql(...)` | submit over Livy |
| `execute_tsql` | TDS | TDS |
| `store` | `notebookutils` | OneLake over HTTPS |
| `resolver` | Fabric REST | Fabric REST |

Every install action runs in the `Installer`, in whichever position that is. An
executor reaches for the capability its work needs and the Session decides what
performing it means:

```text
write_file            → OneLake        (the deployed Python tree; the bulk)
build_procedure       → TDS
refresh_sql_endpoint  → REST
create_schema, build_table, build_view, shortcut
                      → Session Spark SQL
```

So an action is not classified by where it has to run, and nothing on the far
side of an install imports Weaver. Statements belonging to *one* action travel
together — a `spark_sql_batch` payload, or the setup a `DESCRIBE QUERY` needs —
because they are one piece of work rather than because a submission is expensive.

### Runtime scopes held by name

A deployed Python primitive is a *module imported inside the Fabric session*, and
the object owning those imports — `RuntimeScope` — has to live where the imports
do. So it stays there and only its name crosses:

```text
desktop                        Fabric session

open_scope(run_id)     ──►     RuntimeScope.new(), stored under run_id
dispatch(run_id, A)    ──►     same scope → existing import path
dispatch(run_id, B)    ──►     same scope
close_scope(run_id)    ──►     scope.close(), forgotten
```

The registry is `weaver.runtime.session_scopes`, beside `RuntimeScope`, because a
scope's lifetime is the interpreter's. The two entry points a submitted statement
calls — `run_python_primitive` and `run_validation_primitive` — are named
functions in `weaver.run.entry`, so the wheel and the desktop name one thing and
a rename is an ordinary rename.

Each run has one scope, closed when the run ends or fails. Retaining a scope after
its run could let a later run import modules that a rebuild replaced.

---

## Why it is arranged this way

### Both positions run the same code

The runtime components do not depend on where they execute. Changing the
`Session` lets the same `Builder` and `Installer` run in a notebook or on a
desktop against the same workspace:

```text
in a notebook     Weaver runs in the session, and reaches everything from
                  inside it.

from a desktop    Weaver runs on your machine. Fabric is reached through Livy,
                  TDS, OneLake and REST — and each crossing carries a small,
                  clear script rather than an operation.
```

There is one `build`, one `load` and one `test`. What surrounds a build differs
between the positions — a desktop proves its items exist over REST first — but
the build itself is one path.

Its actions and the state it plans against are all statements, storage or TDS,
so nothing it submits imports Weaver and a desktop build needs no published
wheel.

One thing still crosses as a program: `weaver.run.entry`, for a run's deployed
Python primitives. A desktop `weaver load` therefore requires `weaver fabric environment publish` to
have published the current wheel; the version check reports when it has not:

```text
the Weaver published in <workspace> is older than this console ...
Publish the current wheel with `weaver fabric environment publish <environment> --workspace <workspace>`
```

An Environment reference is either `Environment` in the workload workspace or
`Workspace/Environment` in another workspace. Livy resolves the owning
workspace, attaches the Environment ID, and continues to create the session
against the workload workspace's Lakehouse. The cross-workspace acceptance test
uses the same `{"id": "<environment-id>"}` Environment details payload as a
local Environment. Fabric resolves the owning workspace from the item ID.

The entry points are few and stable, and each is a named function rather than
text inside a submitted body: a name is versioned, testable and greppable, and
widening this surface is the coupling that has caused Fabric failures.

### Testing lands where the logic is

Because the decisions are pure and the handoffs are values, most behaviour is
testable without a tenant:

```text
pytest                             pure Python, about a second, no JVM, no cloud
pytest -m "fabric and remote"      real workspace, no published wheel
pytest -m "fabric and hosted"      real workspace and the published wheel
pytest -m full_integration         the composed lifecycle journeys
```

The `@weaver_test(...)` scope says whether a Fabric test needs the wheel
published to the Environment; pytest markers are generated selection details.
A capability that behaves differently in the two execution positions requires
coverage in both.

Planning tests construct a `BuildState` and assert a `BuildBundle`. Fabric tests
cover behaviour that requires a real estate, including shortcut readability, bundle
ordering, and endpoint convergence.

Operational constraints:

- **A capability being available is not the same as it being acquired.** Building
  a context must not start a Spark session; the executor that uses one starts it.
  Get this wrong and plain `pytest` starts needing Java.
- **The Fabric capacity permits one concurrent Livy session.** Tests take the
  harness's session rather than opening their own, and Fabric runs are serial. A
  second session comes back `dead`, and the resulting errors look like anything
  but what they are.

### The physical work can be deferred, batched or parallelised

Because planning completes before physical work begins, the physical layer can be
reshaped without changing planning:

- batched — several Spark actions in one submission
- routed — files to OneLake, T-SQL to TDS, control to REST
- deferred — evidence written to the task log after the fact
- reordered within a barrier, where the physical semantics genuinely allow it

`BuildBundle` sequences are ordered and a batch's actions belong to one target.
Execution changes must preserve those boundaries.

Dependency ordering does not imply database lock independence. Concurrent T-SQL
within a batch caused deadlocks and snapshot-isolation aborts in a Warehouse. The
manifest describes Weaver ordering, not database locking, so changes to physical
execution require measurement.

---

## Where to look in the code

```text
weaver/sessions/         the Session contract, hosts, resources, capabilities
weaver/declaration/      parsing authored files into a WeaverRepository
weaver/build_bundle/     Builder, BuildBundle, Installer, executors
weaver/run/              Runner, RunGraph, RunState, dispatch, run evidence
weaver/catalogue/        what is installed, its tables, and how they persist
weaver/graph.py          deterministic DAG mechanics, shared by every topology
weaver/installed.py      the installed managed graph, derived from a Catalogue
weaver_cli/              argument parsing and rendering, and nothing else
```

Inside `weaver/catalogue/` the split is between what the catalogue *is* and how
it is stored:

```text
tables.py       the fixed shape: tables, columns, keys, public names, vocabularies
projection.py   what a repository declares, as rows
state.py        what is persisted, and the diff between the two
reconcile.py    which statements a build emits, and in what order
render.py       those statements as T-SQL
tsql.py         identifier quoting, literals and types — the only module that
                knows SQL syntax
connection.py   reading `_` over TDS, and what an absent table means
writer.py       where a catalogue's runtime writes go
flusher.py      batching them without waiting for the Warehouse — appended for
                history, merged on the key for current state
runtime_state.py  which current-state rows a build ends the life of, as intent
claims.py       which rows a document owns, and how a status row is keyed
```

`state.py` holds `Catalogue`, which owns catalogue I/O: it is selectively
materialised, it answers which installed object a physical name is, and runtime
rows are submitted, updated and flushed through it. It is live mutable state, not
a snapshot, and it reaches its Warehouse through a Session — borrowed when it was
handed one, owned and closed when it opened one for itself. What a target
physically holds is not its question; a `TargetInventory` answers that.

Three other modules decide *when*: `build_bundle/runtime_tables.py` for what a
build invalidates and which runtime tables a target presents, `run/record.py` for
what a settled unit of work leaves behind, and `runtime/anchor.py` for how an
authored object anchors itself to a catalogue by name.

Everything above `render.py` holds plain Python values under internal
snake-case keys. The public sentence-case names and the stored vocabularies the
`_` schema publishes exist only at that boundary.

The CLI resolves a workspace, calls the API, renders the result, and chooses an
exit code. `tests/test_core_boundary.py` prevents it from acquiring core
semantics.
