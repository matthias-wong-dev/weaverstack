# Runtime architecture refactor — performance acceptance

Measured against `design/todo/runtime-refactor-baseline.md`, recorded at
`df09551` before anything on this branch changed. Same machine, same commands:

```bash
pytest -q --durations=25
pytest -m spark -q --durations=25
pytest -m "fabric and remote" -q --durations=25
```

## Totals

| suite  | baseline        | final           | change  | %      |
| ------ | --------------- | --------------- | ------- | ------ |
| pure   | 2059 in 85s     | 2155 in 97s     | +12s    | +14.1% |
| spark  | 247 in 715s     | 245 in 432s     | −283s   | −39.6% |
| fabric | 82 in 1270s     | _pending_       |         |        |

The pure suite is slower and that is the trade being made. It absorbed 96 tests
— the whole mock run cycle, the thin run boundary, the Session resource cycle —
and each one replaced something that previously needed a JVM or a workspace. Per
test it went from 41.3ms to 45.0ms; the rest of the increase is the two new
packages being imported at collection.

The Spark suite is 39.6% faster with two fewer tests, and neither of the two was
lost: run aggregation and fail-isolation moved to the mock cycle, which proves
them over every combination rather than the one an estate happens to produce.

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
