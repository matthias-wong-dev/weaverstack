# Code architecture

## Purpose

This document explains how the repository implements the product architecture.
It complements [Weaver architecture](weaver-architecture.md), which defines
product behaviour and is authoritative for that behaviour.

## Responsibilities and handoffs

Weaver decides things in Python and performs physical work through a Session.
The repository keeps those responsibilities separate.

Almost everything interesting — what to build, what order to build it in, what
still needs loading, what failed and why — happens as ordinary Python objects in
memory. Reaching a real Lakehouse or Warehouse happens at a small number of
named places. In between there are **handoff points**: plain values that one
part produces and another consumes.

The rest of this document identifies the handoffs between those responsibilities.

---

## The shape

```text
   authored files            physical estate
        │                          │
        ▼                          ▼
 WeaverRepository            BuildState / RunState        ← read once, at a boundary
        │                          │
        └───────────┬──────────────┘
                    ▼
              Builder / Runner                             ← pure Python decisions
                    │
                    ▼
        BuildBundle / RunGraph                             ← the handoff: a decision, written down
                    │
                    ▼
          Installer / dispatch                             ← does what the decision says
                    │
                    ▼
                 Session                                   ← the only thing that touches Fabric
                    │
        ┌───────┬───┴────┬────────┐
       TDS   OneLake    Livy     REST
```

Read from the top: two kinds of input arrive, a decision is made, the decision
is written down, and only then does anything physical happen.

The value of the middle layer is that it is *inspectable*. A `BuildBundle` is a
description of everything a build intends to do. You can print it, diff it,
serialise it, or hand it to a test — before a single byte moves.

---

## The four doers

Four objects do things. Nothing else does.

```text
Session     supplies physical capabilities and resources
Builder     decides what should be installed
Installer   carries out a BuildBundle
Runner      decides what runs next, and records what happened
```

They are deliberately few, and the boundary between them is the thing most worth
protecting. The rule that keeps it honest:

> **Builder, Installer and Runner own Weaver semantics. Session owns reusable
> physical capabilities and transport.**

A Session that learned what a DAG was would become a second Runner, and then two
things would disagree about a node's status. So Session answers *"how do I reach
Spark from here?"* and never *"what should run next?"*.

### Session

One process scope holding the expensive things: a credential, a resolver with an
item cache, a Livy session, a TDS connection per Warehouse, a store. It caches
these per workspace, so a `build` and the `load` that follows share them.

Callers ask for *capabilities*, never for transports:

```python
session.execute_python(program)          # here, or across to Fabric
session.execute_spark_sql(statement)     # this host's Spark, wherever it is
session.execute_tsql(statement, target)  # a Warehouse, over TDS
session.store(workspace)                 # files
session.resolver(workspace)              # names → physical items
```

`session.livy` is deliberately not part of that list. The day domain code writes
it is the day Weaver stops being able to run anywhere else.

Commands say up front what they will need, coarsely, and the Session starts those
in the background:

```text
weaver load Warehouse/Reporting   → auth, resolver, tds
weaver load Lakehouse/Sales       → auth, resolver, onelake, livy
```

The Session is *told*; it does not infer. And **preparing is not using** — a
declaration gives a head start to something that was coming anyway. A run that
declares `livy` and turns out to be all T-SQL opens no Spark session at all.

### Builder

```text
WeaverRepository + BuildState  →  BuildBundle
```

Pure. It takes parsed authored files and a snapshot of the estate, and returns a
plan. It never reads Fabric, never opens a connection, never mutates anything.
Give it the same inputs twice and you get the same bundle twice.

That purity is why most build behaviour can be tested in milliseconds with no
tenant, no credentials and no JVM.

### Installer

```text
BuildBundle  →  physical estate
```

The doer with no opinions. It validates the bundle, walks its sequences as
barriers, runs each action through the executor the bundle named, and records one
result per action. It never reopens the repository, resolves a dependency or
second-guesses the Builder — every such decision is already in the bundle, and an
Installer that could overrule one would make *"who decided?"* unanswerable.

### Runner

```text
RunState + RunRequest  →  RunResult
```

Selects nodes, builds the dependency graph, decides readiness, handles blocking
and fail-fast, and aggregates the result. One Runner serves both `weaver load`
and `weaver test`; what differs is which nodes are selected and which primitive
runs, not how a run behaves when one of them fails.

Execution leaves through exactly one hole: `dispatch`, a callable. Give it the
real one and nodes run against the estate; give it a controlled one and the whole
state machine is provable in milliseconds.

---

## The representations

The handoff points, roughly in the order you meet them.

| | what it is |
|---|---|
| `WeaverRepository` | authored files, parsed and structurally checked — never executed |
| `Catalogue` | what Weaver installed, read from the control Lakehouse |
| `TargetInventory` | what a physical target actually holds right now |
| `BuildState` | `Catalogue` + inventories, as one snapshot the Builder is handed |
| `BuildBundle` | the plan: sequences → batches → `InstallAction`s, plus frozen payloads |
| `RunState` | the same pair, for a run |
| `RunGraph` | the selected nodes and their edges |
| `RunResult` / reports | what happened, per node, per action |

Two properties matter more than the list.

**They are plain values.** A test constructs a `BuildState` directly and needs no
estate. A `RunGraph` can be asserted against without dispatching anything.

**They round-trip.** `BuildBundle` serialises to a canonical `plan.yml` and back,
which is what lets a bundle be archived, inspected, or executed by a different
process from the one that planned it.

### Carrying, not reconstructing

One habit runs through all of them: when something is known early, it is carried
forward rather than derived later.

The clearest case is source provenance. A build may involve hundreds of files, and
by the time an action fails the only spellings left are the deployed ones —
`_/Load/Sales__Customer.py`, `[_].[Load Sales.Customer]`. Neither is a file anyone
has open. So the authored path travels with the action:

```text
SourceDocument.relative_path
  → RuntimeArtefact.source_path
    → InstallAction.source_path
      → plan.yml, and back
        → ActionResult.source_path
          → "Source: Lakehouse/Sales/Sales__Customer.py"
```

Deriving it at the end would be a guess presented as evidence — and sometimes an
impossible one, since a Spark SQL table is authored as `.sql` and deployed as
`.py` under a different name.

---

## The physical handoff

Everything above is Python. Here is where it stops being Python.

A `Session` decides how each capability is met, based on where the code is
running:

| | notebook / emulator | desktop → Fabric |
|---|---|---|
| `execute_python` | call it | submit over Livy |
| `execute_spark_sql` | `spark.sql(...)` | submit over Livy |
| `execute_tsql` | TDS | TDS |
| `store` | filesystem / `notebookutils` | OneLake over HTTPS |
| `resolver` | local | Fabric REST |

So an install routes each action to the cheapest thing that can do it:

```text
write_file            → OneLake        (the deployed Python tree; the bulk)
build_procedure       → TDS
refresh_sql_endpoint  → REST
build_table, alias    → Livy           (genuine Spark, or a Spark read probe)
```

The pieces that need Spark cross; nothing else does. And when several actions in
one batch need Spark, they cross **together** — one submission carrying several
actions, each still reporting its own status, duration and failure. The semantic
unit stays the action; what gets batched is the physical effect.

### One thing that has to stay remote

A deployed Python primitive is a *module imported inside the Fabric session*, and
the object owning those imports — `RuntimeScope` — has to live where the imports
do. So it stays remote and only its name crosses:

```text
desktop                        Fabric session

begin_run(run_id)      ──►     RuntimeScope.new(), stored under run_id
dispatch(run_id, A)    ──►     same scope → existing import path
dispatch(run_id, B)    ──►     same scope
end_run(run_id)        ──►     scope.close(), forgotten
```

One scope per run, closed when the run ends — including when it fails. A scope
that outlived its run would let the next run import modules a rebuild had already
replaced.

---

## Why it is arranged this way

### Remote and local run the same code

The doers do not know where they are. Swap the Session and a build that ran in a
notebook runs from a laptop, against the same workspace, through the same
`Builder` and the same `Installer`. That is what makes the two supported
experiences — `pip install weaverstack` in a Fabric notebook, and `weaver build`
from a terminal — genuinely the same product rather than two implementations that
drift.

The destination is **either**, in the strong sense:

```text
in a notebook     Weaver runs in the session. Everything is local to it.

from a desktop    Weaver runs on your machine. Fabric is reached only through
                  Livy, TDS, OneLake and REST — and each crossing carries a
                  small, clear script rather than an operation.
```

Not a fast loop and a real one. Two complete ways to work, with the same code
underneath, and you pick whichever suits the moment.

**Where we actually are.** Close, not finished, and the measure is executor
parity: an action whose executor works in both positions is done; one that still
has to cross whole is not. `alias` is the current example — creating a shortcut
is a REST call that works from anywhere, but the readability wait still crosses
with it, and the executor's own comment says why the split was reverted and what
would prove it.

**One honest caveat**, and it is the other half of the gap. The far side of a
decomposed operation imports the published wheel — `weaver.run.remote` for a
Python primitive, `weaver.build_bundle.remote` for a Spark install action — so a
desktop build or load needs `weaver install` to have been run, and the version
check will tell you plainly if it hasn't:

```text
the Weaver published in <workspace> is older than this console ...
Publish the current wheel with `weaver install ...`
```

Shrinking that surface is worth doing; pretending it isn't there is not. Keeping
the remote entry points *few and stable* is the practical mitigation — one entry
point that runs whichever executor the bundle named, rather than an API shaped
like the executor registry.

### Testing lands where the logic is

Because the decisions are pure and the handoffs are values, most behaviour is
testable without a tenant:

```text
pytest                             pure Python, about a second, no JDK, no cloud
pytest -m spark                    local Spark and Delta, needs a JDK
pytest -m "fabric and remote"      real workspace, Weaver on this machine
pytest -m "fabric and hosted"      real workspace, Weaver as the published wheel
```

The `remote` / `hosted` pair is the parity question made selectable. Every Fabric
test states which position it runs in, so a capability proven in one and not the
other is a gap you can *see* rather than one someone has to remember to look for.

The suite is bottom-heavy. A planning bug should be caught by a test
that constructs a `BuildState` and asserts a `BuildBundle` — not by a
thirty-minute Fabric run. The expensive tests are for claims only a real estate
can make: that an alias really becomes readable, that a bundle's order is
*viable*, that the endpoint really catches up.

Operational constraints:

- **A capability being available is not the same as it being acquired.** Building
  a context must not start a Spark session; the executor that uses one starts it.
  Get this wrong and plain `pytest` starts needing Java.
- **The Fabric capacity permits one concurrent Livy session.** Tests take the
  harness's session rather than opening their own, and Fabric runs are serial. A
  second session comes back `dead`, and the resulting errors look like anything
  but what they are.

### The physical work can be deferred, batched or parallelised

This is the quiet payoff. Because every decision is settled *before* anything
physical happens, the physical layer is a stream of independent effects rather
than a series of choices. So it can be reshaped without touching the decision:

- batched — several Spark actions in one submission
- routed — files to OneLake, T-SQL to TDS, control to REST
- deferred — evidence written to the task log after the fact
- reordered within a barrier, where the physical semantics genuinely allow it

The barriers are what make this safe. A `BuildBundle`'s sequences are ordered and
a batch's actions belong to one target, so anything that reshapes execution has a
stated boundary it must not cross.

A caution from experience: *independent in dependency order is not independent in
lock order*. Running a batch's T-SQL concurrently looked safe by the manifest's
own contract and a real Warehouse answered with deadlocks and snapshot-isolation
aborts. The manifest describes Weaver's ordering, not the database's. Reshaping
the physical layer is allowed — but it is a measurement, not a deduction.

---

## Where to look in the code

```text
weaver/session/          the Session contract, hosts, resources, capabilities
weaver/declaration/      parsing authored files into a WeaverRepository
weaver/build_bundle/     Builder, BuildBundle, Installer, executors
weaver/run/              Runner, RunGraph, RunState, dispatch
weaver/catalogue/        the control plane: what is installed, and its tables
weaver_cli/              argument parsing and rendering, and nothing else
```

The CLI owns no semantics — it resolves a workspace, calls the API, renders what
comes back, and picks an exit code. A test reads its source to make sure it stays
that way, because that is the kind of rule that decays quietly.
