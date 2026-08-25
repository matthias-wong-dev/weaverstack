# The OneLake publication wait as a load graph node

A load waits at two cross-engine boundaries. One of them is a node in the graph
and one of them is not, and the asymmetry is an accident of where each was first
needed rather than a difference between them.

```text
Lakehouse producer  →  Warehouse consumer   refresh:<target>, a node
Warehouse producer  →  Lakehouse consumer   await_onelake, an attribute
```

The refresh is built by `_refresh_node` in `weaver.load_plan`, dispatched by
`_endpoint_refresh`, and placed behind every selected load in the refreshed
Lakehouse. The publication wait is a tuple of `OneLakeReadiness` hanging off the
Warehouse producer, and it runs inside `_warehouse_procedure` between the
procedure returning and the node settling.

## What the node shape gives

Four things, all from machinery the refresh node already uses.

**A visible step.** `node_label` in `weaver.run.runner` names a node for progress
output, and `_step_type` in `weaver.operations.load` names its step file. The
wait currently has neither, so a load that spent forty seconds waiting reports a
Warehouse load that took forty seconds.

**Its own timing.** `_node_substep` opens one frame per dispatched node. The wait
is charged to the producer today, which makes a load's recorded duration the sum
of two different things.

**Correct blame.** `load_status_row` derives the `_.LoadStatus` row from the
node's status. A publication timeout currently fails the Warehouse producer, so a
load whose T-SQL committed cleanly is recorded as an error. As a node, the
producer settles as succeeded, the barrier fails, and the consumers block through
`RunGraph.descendants` without new code.

**Evidence that the wait did anything.** A green run tells us little while the
wait is invisible: a downstream read that no longer faults may mean the barrier
worked, or may mean the lag did not happen that time. A node reports what it
observed and how long it took, which is the difference between a passing test and
a demonstrated mechanism.

## The baseline is the hard part

The wait needs the set of commits published *before* the load ran, and a node
placed after the producer cannot observe it. Three ways out:

1. **The producer observes and the barrier reads.** Needs a small run-scoped
   object threaded through dispatch. There is a precedent: the Runner already
   threads `open_runtime`, a run-scoped resource, into every dispatch.
2. **A second node before the producer observes.** Adds a node and still has to
   hand the observation forward, so it is option 1 with more parts.
3. **Derive the interval from commit timestamps.** Removes the baseline and
   replaces it with a comparison across two clocks, one of them Fabric's.

Option 1. The observation is a fact the producer is uniquely placed to record,
and the barrier is the only reader.

## The node's shape

``logical_id`` is ``None``, as the refresh node's is. ``_installed`` in
``weaver.run.record`` reads it to decide whether a node leaves catalogue state, so
a barrier carrying the producer's identity would write a second ``_.LoadStatus``
row for an object that already has one. The Warehouse table it waits on is carried
separately.

```text
node_id              publish:Warehouse/Serving/SERVE.Reporting
logical_id           None
physical_target      the Warehouse, so the Delta log is reachable
publication_of       the producer's document id, for the log path
publication_targets  the consuming shortcut destinations
produced_by          the producer's node id, for the ledger
```

## The baseline

A barrier runs after its producer and cannot observe what was published before.
The producer records it in a run-scoped ``PublicationLedger`` the Runner owns and
threads into dispatch, as it already threads ``open_runtime``. The ledger holds the
producer node ids a barrier follows, so a Warehouse-only load reads no Delta log:
the producer asks the ledger whether to observe, and the answer is no when nothing
waits on it.

The producer also records whether it moved rows. A barrier whose producer moved
none settles without reaching Spark.

## The change

- A `ONELAKE_PUBLICATION` primitive kind beside `ENDPOINT_REFRESH`, in
  `weaver.load_plan` and re-exported through `weaver.run.resolution`.
- `_publication_node(producer)` alongside `_refresh_node`, keyed on the producer
  so one Warehouse load has one barrier however many consumers read it.
- In `_select`, the `OneLakeReadiness` branch puts the barrier between producer
  and consumer instead of annotating the producer, the same substitution the
  refresh branch already makes.
- A `node_label` case, so the barrier prints as something a reader recognises.
- A dispatch branch calling `weaver.run.publication.await_publication`.
- The producer records its pre-load observation; the barrier reads it and decides
  whether there is anything to wait for.

## What it does not change

The proof itself. `weaver.run.publication` already owns the Delta-log inspection,
the publication interval, path construction and the readability probes, and the
node moves the call site rather than the mechanism. Selectivity is unchanged: a
barrier exists only where the planner found a Warehouse to Lakehouse shortcut
crossing, and a Warehouse-only load still reaches no Spark.
