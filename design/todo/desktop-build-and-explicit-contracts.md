# Desktop build, and explicit contracts

Planned work, not implemented behaviour. This document is the agreed sequence
for five related changes and is expected to be deleted once they land. Nothing
here describes how Weaver currently works; for that, read
[code-architecture.md](../code-architecture.md).

The five:

1. Replace dynamic capability discovery with declared interfaces, where the
   discovery selects Weaver's own behaviour.
2. Retire `run/remote.py` as a concept.
3. Delete `needs_spark` and the install routing it drives.
4. Run the whole of `build` from a desktop, with no published wheel.
5. Extend the lifecycle journey to a cross-item estate.

Plus one documentation correction that the measurements below require.

## What the measurements changed

Two of these workstreams were planned around a transport cost that was measured
and found not to hold.

`_run_crossed` and [interactivity-baseline.md](../history/interactivity-baseline.md)
both record that crossing per install action cost about four seconds of overhead
each, and that six actions paid roughly twenty-four seconds of pure transport.
A probe against `PYTEST_WORKSPACE` on 13 August 2026 measured a warm Livy round
trip carrying a trivial statement at 0.59 s, over eight calls ranging 0.50 s to
0.71 s. Session start, paid once, was 37.9 s.

The repository's own later measurement agrees with that figure rather than with
the four-second one: the Run decomposition notes record per-node dispatch at
roughly a second apiece on the same mechanism.

### Measured

| Question | Result |
| --- | --- |
| Warm round trip, trivial statement | 0.59 s mean over 8 calls |
| Eight statements in one submission | 3.82 s, against 4.8 s submitted individually |
| Submission overhead, by difference | about 0.11 s per statement |
| Failure part-way through a batch | earlier results returned, with the error type |
| `DESCRIBE QUERY` against `field.dataType.simpleString()` | identical on 10 shapes, including `array<struct<inner_amount:decimal(9,3)>>`, `map<string,int>`, `decimal(18,2)` |
| Temporary view created in one submission | resolvable in the next |
| Identifier case set in one submission | resolvable under the same setting in the next |

### Inferred, and not yet measured

The remaining time in the four-second figure is attributed to work the far side
does per submission: `install_actions` constructs an `Installer` and re-resolves
every target the plan declared before running an action. That reading comes from
the code, not from a measurement.

Whether the desktop path is faster overall is also a prediction. Each Spark
statement becomes its own crossing where a batch previously shared one, traded
against far-side setup that a desktop path does not perform.

What is measured is that the wire costs 0.59 s, so whatever the rest of the
four seconds was, it was not transport.

## Phase A: vocabulary, and two free changes

**A1. Correct the test markers.** `remote` keeps its meaning of "no published
wheel is required". `hosted` means the wheel is required, whether or not the
orchestration runs here. A decomposed desktop operation that imports the wheel
on the far side is `hosted`. Update the marker table and prose in
[AGENTS.md](../../AGENTS.md), the looser wording in
[code-architecture.md](../code-architecture.md), and
[test-architecture.md](../test-architecture.md). Re-mark any test whose current
marker no longer describes what it needs.

This comes first because the later phases are described in these terms, and
because Phase C changes which tests need a wheel.

A1 does not need a Fabric run.

An earlier draft paired it with a second task: replacing
`hasattr(target, "lakehouse")` in `weaver/fabric/preflight.py` and
`weaver/operations.py` with the existing `physical_kind` helper. That was
attempted and reverted, because the premise was wrong. See [B0](#phase-b-one-run-scope-interface),
which is where the change belongs.

## Phase B: one run-scope interface

`RuntimeScope` owns imported-module state. `RemoteScope` names a scope in a
Fabric session and submits statements to reach it. They share no declared
interface, so `run/dispatch.py` decides which it holds by looking for methods.
When the attribute is absent the code takes the local path, which on a desktop
means importing a deployed module into the console.

**B0. Give the binding types a declared kind.** `weaver/fabric/preflight.py`
and `weaver/operations.py` ask `hasattr(target, "lakehouse")` to tell a bound
Lakehouse from a bound Warehouse. `physical_kind` does not answer it:
`binding.target` is a `LakehouseBinding` or a `WarehouseBinding` from
`build_bundle/targets.py`, which are a different type family from `DeltaTarget`
and `WarehouseTarget`, and `physical_kind` raises for them. The two binding
types carry their kind only inside `to_bound_target()`, and spell it
`"lakehouse"` where Fabric's item type is `"Lakehouse"`, so preflight needs a
translation as well as a declaration.

The change is to declare the kind on both binding types and translate to the
Fabric item type at the preflight boundary, which is the same kind of work as
the rest of this phase rather than a substitution.

Severity is low, and the plan's own test says so: there are exactly two binding
types, neither carries both attributes, and a third would fail with an
`AttributeError` on `.warehouse` rather than silently taking the wrong branch.
The fault is misdiagnosis, not silent misbehaviour.

**B1.** Declare `RunScope` with `dispatch_python`, `dispatch_validation` and
`close`. Add `DirectRunScope`, wrapping `RuntimeScope` so that module isolation
stays its only responsibility. `RemoteScope` becomes `FabricRunScope` and needs
no behavioural change; it already answers all three.

`RuntimeScope` does not gain the dispatch methods itself. Dispatch calls
`python_primitive` from `run/dispatch.py`, and `run` already imports `runtime`,
so that would close a cycle.

**B2.** Remove both `hasattr(scope, "dispatch_*")` branches. With one interface
the call is unconditional and the branch goes rather than becoming explicit.

**B3.** Replace the scope-or-callable `open_runtime` parameter with a lazy
holder exposing `get()` and `close()`. `close()` never opens, and is idempotent.
This absorbs `Runner.runtime_scope` and `Runner._close_runtime`, so a
Warehouse-only run opening no scope becomes one object's property rather than an
invariant maintained across two methods.

**B4.** Split `run/remote.py`. The scope registry moves to
`runtime/session_scopes.py` as `open_scope`, `get_scope` and `close_scope`; it
is interpreter-lifetime infrastructure and belongs beside `RuntimeScope`. The
two argument adapters remain named functions beside `dispatch.py`, renamed so
that no module claims there is a remote way to run. `getattr(remote, name)` in
`_submit` becomes a passed callable.

The adapters stay as functions rather than moving into generated statement text.
A named function is versioned, testable and greppable; widening the far-side
surface is the coupling that has already caused Fabric failures.

**B5.** Decide how the Session supplies position for scope selection. See
[open questions](#open-questions).

B1 to B3 are behaviour-preserving. B4 renames symbols the published wheel
exports, so it lands together with `weaver install`.

## Phase C: desktop install, by deletion

Every install executor can run in the desktop `Installer` and reach Spark
through `Session.execute_spark_sql`. With that true, no action needs to be
routed anywhere, and the routing machinery is removed rather than replaced.

The intended arrangement:

```text
Installer runs every action in this process
    OneLake actions        storage
    Warehouse actions      TDS
    control actions        REST
    Spark actions          Session.execute_spark_sql
                             in a Spark host: directly
                             from a desktop:  over Livy
```

**C1. Move `alias`.** One line. Its executor already reaches Spark through
`context.spark_sql`, and a desktop probe ran it end to end against a real
workspace, creating the shortcut over REST and reporting how long discovery
took. Its Fabric test is re-marked under A1.

**C2. Separate naming from execution in `SparkCatalogue`.** This is the
prerequisite for the rest, and the obstacle is not Spark. `expand`, `qualify`,
`qualified_schema` and `destination` need only the destination and are pure, but
`SparkCatalogue.__init__` raises when no session is supplied, so an executor
cannot be constructed on a desktop even when it only needs a name. Split it into
a destination-based naming value and an executing wrapper.

`alias` already works around this by reading `context.target.destination`
directly. The same separation is what the other four need.

**C3. Move `spark_schema`, `spark_sql` and `spark_sql_batch`.** Each runs
statements and needs no `DataFrame`. Keep batching where several statements are
one action. Do not keep cross-action batching: it saves about 0.11 s per
statement, and the four-second figure that justified it does not hold.

**C4. Move `spark_table`.** The executor takes two things from its `DataFrame`,
column names in order and `dataType.simpleString()` for each, and never collects
or writes it. `DESCRIBE QUERY` answers both identically.

```text
crossing 1   setup statements, then DESCRIBE QUERY, one submission,
             carrying exact_case. The setup builds temporary views the
             query reads, so they travel together.

in process   column names and types from the describe rows
             validate_build_columns   unchanged
             _physical_columns        unchanged
             _create_table_sql        unchanged

crossing 2   the rendered DDL, carrying exact_case
```

`exact_identifier_case` leaves the executor. The scope travels with each
statement instead, which is what `execute_spark_sql(exact_case=...)` is for.

Three details decide whether this is correct. `DESCRIBE QUERY` must return only
the query's columns. A query that fails analysis now fails on the far side, and
the resulting `InstallError` must still name the action and carry Spark's
message. And the create must be analysed under the same identifier case as the
query that shaped it.

**C5. Delete.** `needs_spark` and its five declarations, `_crosses`,
`_run_crossed`, `_crossed_result`, and `build_bundle/remote.py`. Neither
`tests/test_public_api.py` nor `tests/test_remote_program_invariant.py` names
`install_actions`, so it is not a pinned symbol.

`require_weaver` was to become conditional on the operation, and does not: the
premise was that install was `build`'s last far-side import, and it is not.
`read_build_state` reads the catalogue and the Lakehouse inventories through
`execute_python`, so a Fabric build still imports the published wheel — before
installing anything, and by design, because both phases decide against the
target's real state. Installing needs no wheel; building does. Decomposing the
two reads into Spark SQL and storage would change that, and is separate work.

`context.spark` stays. The emulator's alias path registers an external table
through it, and there it is always present. What is removed is the claim that
its absence should reroute an action.

`RemoteProgram` and `execute_python` stay. A deployed Python primitive is still
imported where Spark is, so `weaver load` continues to require the wheel.

**C6. Correct the transport attribution** in the `_run_crossed` docstring and in
the interactivity baseline, stating what was measured.

### Acceptance test for C4

One Fabric test, marked `fabric` and `remote`, running the real
`SparkTableExecutor` from the checkout against a real Lakehouse, with a single
observation crossing at the end. Its local twin runs the same body under `spark`.

| Case | Assertion |
| --- | --- |
| Complex types | a table declared with `decimal(18,2)`, `timestamp`, `array<struct<...>>` and `map<string,int>` is created with exactly those types |
| Temporary view setup | an instruction whose `setup` builds a view the query reads produces the right shape |
| Exact-case identifiers | `CustomerEnriched` with column `CustomerId` is stored case-preserved and is readable by the next action in the same build |
| Analysis failure | a query naming a missing column fails as `InstallError`, names the action, and carries Spark's message |
| Describe output | the result contains only the query's columns |

`DESCRIBE QUERY` has been probed against literals and expressions, not against a
Delta table Weaver created. This test closes that gap.

## Phase E: the lifecycle journey

**E1. Wire up the existing cross-item fixture.**
`tests/fixtures/cross-item-journey` is committed and referenced by nothing. It
holds `Lakehouse/Sales` and `Warehouse/Reporting` with an `alias.yml`, two
report views and a reconciliation test. Add a `SesFixture` for it in
`tests/support/build_envs.py`, extend `journey_claims.py` with Warehouse claims
and the endpoint-refresh barrier, and parametrise the Fabric journey on it.

That gives a Lakehouse to Warehouse journey with an alias between them.

**E2. Warehouse to Lakehouse alias.** Needed for a third hop, and not yet built.
The declaration layer models both directions: a `Warehouse alias` publishes a
Lakehouse object into the Warehouse, and a `Lakehouse alias` publishes a
Warehouse object into the Lakehouse. `lakehouse_alias` is parsed, validated and
surfaced on the source model, and no planner or executor consumes it.
`AliasExecutor` resolves both ends of a shortcut as a Lakehouse.

This needs planner support, an executor path that resolves a Warehouse source
and shortcuts to its OneLake storage, an emulator equivalent, and a decision
about the source's readiness barrier. Plan it separately.

**E3.** Extend the journey to three hops, after E2.

## Sequence

```text
A1 ──┬─→ C1 ───────────────────────────┐
     │                                  ├─→ C5 → C6
     ├─→ C2 ─→ C3 ──────────────────────┤
     │         C4 and its test ─────────┘
     ├─→ B0 B1 B2 B3 ─→ B4 with republish ─→ B5
     └─→ E1                                  E2 → E3
```

## Risks

**Rolling the wheel back after C5.** A new wheel with an old desktop fails,
because the old desktop calls `install_actions` and it is gone. The forward
direction is safe. Land C5 and the republish together.

**Symbol drift at B4.** Renaming what the wheel exports produces the version
mismatch message until `weaver install` runs. The failure names the fix, but B4
and the republish are one change.

**C4 coverage.** Feasibility is strongly indicated and not yet proven against a
Weaver-created Delta table. The acceptance test is what proves it.

## Open questions

**How the Session supplies position (B5).** With install routing deleted there
is one consumer of position rather than two, so there is no shared abstraction
to design. `session.open_runtime_scope()` would have the Session return a
`run`-shaped type, reversing a dependency that currently runs one way, so the
narrower option is preferred: the Session supplies position as a small value or
host in `weaver/session/`, and the run layer builds its scope from it.

**Far-side setup cost.** Two submissions settle it: one that constructs an
`Installer` and resolves a realistic target set, timed against an empty body.
Worth running before any performance claim is made for Phase C.

## Considered and not planned

**Result adapters at the Runner boundary.** Every result type reaching `_status`
was checked. `getattr(result, "rows_rejected", 0)` expresses a real difference
between result types: `LoadResult` carries the field, and `TestResult`,
`AssumptionResult` and `RunFailure` have no notion of rejected rows, for which
`FAILED` is the correct answer. `reports_outcome` is a contract check whose
failure raises `RESULT_CONTRACT_INVALID` naming the offending type. Both are
correct and documented.

**Unwrapping `_Deferred`.** It defers a cost rather than selecting behaviour,
and `spark_when_needed()` already decides `None` from position without acquiring
anything, so the `context.spark is None` guards still hold.

**Explicit install execution hosts.** Planned, then dropped: the measurements
removed the routing decision they were designed to express.

**Fast-path and optional-amenity lookups.** `getattr(store, "copy_to_local")`
falls back to a correct general implementation, and the optional `telemetry` and
`warn` lookups on a Session change only whether a diagnostic is emitted. The
test applied throughout this plan is what happens when the attribute is absent:
a correct fallback or a loud, accurate error is acceptable, and silently
selecting different behaviour is not.
