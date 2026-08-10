# Runtime architecture refactor — performance acceptance

Measured against `design/todo/runtime-refactor-baseline.md`, recorded at
`df09551` before anything on this branch changed. Same machine, same commands:

```bash
pytest -q --durations=25
pytest -m spark -q --durations=25
pytest -m "fabric and remote" -q --durations=25
```

## Totals

| suite  | baseline    | final        | change | %      |
| ------ | ----------- | ------------ | ------ | ------ |
| pure   | 2059 in 85s | 2155 in 97s  | +12s   | +14.1% |
| spark  | 247 in 715s | 245 in 432s  | −283s  | −39.6% |
| fabric | 82 in 1270s | 94 in 829s   | −441s  | −34.7% |

Two of the three moved the right way, and the third is the trade.

**Pure** is slower because it absorbed 96 tests — the mock run cycle, the thin
run boundary, the Session resource cycle — and every one of them replaced
something that previously needed a JVM or a workspace. Per test it went from
41.3ms to 45.0ms; the rest is two new packages imported at collection.

**Spark** is 39.6% faster with two fewer tests, and neither was lost: run
aggregation and fail-isolation moved to the mock cycle, which proves them over
every combination rather than over the one an estate happens to produce.

**Fabric** is 34.7% faster while running twelve more tests — 15.5s to 8.8s per
test, −43%. None of that is anything running faster. Fabric time is Warehouse
round trips, and what changed is how many of them the suite asks for.

## Where the Fabric time went

The baseline named the target precisely: "a function-scoped baseline that
installs and first-loads the same object per case", and "two assertions about
one expensive second run, paid for twice". Both are gone.

```text
test_warehouse_load_primitive         ~500s → 211s
test_warehouse_sql_program_primitive  ~420s → 307s
```

Three changes, in order of what they were worth:

**A table and its procedure are installed once per module, not once per test.**
Installing them is not a claim any of those tests makes — it is the premise they
share — and a two-phase procedure install was the most expensive statement in
the file. Sequences reset by deleting rows instead, which is the same starting
state for every claim there.

**A sequence runs once, however many claims are about it.** "A second run
updates only what changed" and "an unchanged row keeps its original update time"
are two questions about one load-then-load-again. Each sequence now captures
what its claims need at the moment it finishes and hands back a snapshot —
capturing rather than querying later is what makes sharing one table safe, since
a query afterwards would describe whichever sequence ran last.

**The ordinary path runs as a chain**: seed, update, shrink, where each step is
the next one's starting state. Separately, the update case had to re-seed and
the shrink case had to re-seed and update again — three loads bought to reach
states two earlier loads had already produced. Rejection keeps its own
sequences, because refusing and tolerating are a different subject.

## Where the Fabric time still goes

```text
86.1s  setup  test_warehouse_load_primitive        two estates, six sequences
78.0s  setup  test_cross_item_alias                one estate, already built once
62.6s  setup  test_warehouse_sql_program_primitive ×3 — each a distinct retirement
```

Every one is now a sequence some claim genuinely needs. The three retirement
setups are the clearest remaining case, and they resist folding: each is a
*different* second load, and re-establishing the base is what makes each claim
read the same however the module is ordered.

## Where the Spark time went

None of it came from making anything faster. It came from not paying for
isolation nothing used, and from not executing the same run to ask several
questions about it.

```text
validation orchestration   ~220s → 29s   one estate, one whole-target run
local aliases               ~42s → 17s   one estate; the rebuild claim is idempotent
validation catalogue        ~63s → 37s   shared for the reads, fresh for the three that mutate
catalogue fidelity          ~44s → 44s   split: shared to read, per-test to publish
```

Two constraints shaped what could be shared, and both are properties of local
Lakehouses rather than of any test:

**A local Lakehouse folds to a Spark schema by name, and a process holds one
metastore.** So `shared_lakehouses` is named apart from the per-test pair —
without that, a module mixing the two has them writing into each other. It is
also why `test_multi_destination` still builds per test: its estates collide with
each other by construction, not by choice.

**A module that initialises a catalogue must drop it**, because the directory is
new each module while the schema name is not.

## The slowest things that remain

```text
17.42s  call   test_local_lakehouse_journey       the journey, and it should be the slowest
 8.81s  call   test_multi_destination             ×4 — cannot share, see above
 8.77s  setup  test_validation_orchestration      one build and load, for twenty claims
 8.57s  call   test_cross_item_alias_incremental  ×3 — each is a distinct build sequence
 7.13s  call   test_validation_catalogue          a delete-and-rebuild; mutation is the claim
 6.87s  setup  test_local_aliases                 one build, for six claims
```

Every entry is now either a genuine build sequence or one shared setup amortised
over a module. There is no remaining case of the same estate being built to ask
the same question twice.

The pure suite has no hot spot at all: its slowest entry is 1.53s and everything
below it is sub-second, so 2155 tests in 97s is fixture construction spread
evenly rather than anything worth attacking.

## What made it possible

The arithmetic did not change; what changed is that the claims moved to layers
that can answer them.

```text
tests/test_run_cycle.py        35 claims about the Runner, dispatch replaced, ~0.08s
tests/test_run_boundary.py     14 claims about the crossing, trivial artefacts, ~1.7s
```

The boundary layer is the one that did not exist before. A run reaches a
primitive when a catalogue says it is installed and the artefact is where the
catalogue says, so both are arranged directly and no build happens — the Registry
points at trivial artefacts exactly as it points at production ones. Loads need
no Spark session at all, because a trivial load never touches one.
