# Test architecture

## Purpose

Weaver tests state what they prove, where they run, and which external Fabric
resources their claim needs. The same production Session telemetry used by
Weaver validates those declarations.

The organising rule is:

> Prove a claim at the cheapest layer that can answer it, and use Fabric only
> for behaviour that Fabric itself decides.

## One declaration model

Every test function has one `@weaver_test(...)` declaration:

```python
@weaver_test()
def test_a_plan_is_deterministic(): ...


@weaver_test(remote=True, resources={"tds"})
def test_a_warehouse_accepts_the_generated_statement(): ...


@weaver_test(integration=True, resources={"tds", "livy", "onelake"})
def test_the_lifecycle_composes(): ...
```

The declaration stores one scope and one resource set. It is the source of
truth for collection, selection, reporting, and resource validation. Pytest
scope and resource markers are generated from it and must not be written by
hand.

## Scopes

| declaration | meaning | selection |
|---|---|---|
| `@weaver_test()` | pure Python; no tenant | `pytest` |
| `@weaver_test(remote=True, ...)` | real Fabric, driven from this checkout; no published wheel | `pytest -m "fabric and remote"` |
| `@weaver_test(hosted=True, ...)` | requires Weaver published in the Fabric Environment | `pytest -m "fabric and hosted"` |
| `@weaver_test(integration=True, ...)` | a composed lifecycle journey | `pytest -m full_integration` |
| `@weaver_test(provision=True, ...)` | creates or deletes Fabric items | `pytest -m provision` |

Integration and provision are complete scopes. They do not also require a
remote or hosted flag.

The first question when placing a test is what its claim requires:

- Pure Python proves declarations, parsing, rendering, planning, selection,
  dispatch, reconciliation, and failure semantics.
- Remote tests prove a narrow platform boundary from the desktop position.
- Hosted tests prove behaviour that depends on the installed package running
  in Fabric.
- Integration proves that already-covered pieces compose.
- Provision proves Fabric item lifecycle operations.

## Resources

Scope describes where the test sits. Resources describe the external
boundaries needed by its claim. The vocabulary is closed:

| resource | crossing |
|---|---|
| `tds` | Warehouse SQL execution over TDS |
| `livy` | a Spark or Python submission through Livy |
| `onelake` | OneLake DFS storage access from outside Fabric |
| `rest` | Fabric control-plane REST operations |

Each declared resource also becomes a pytest selection marker. Declarations
with more than one resource carry every corresponding marker:

```bash
pytest -m "fabric and remote and tds"
pytest -m "fabric and rest"
pytest -m "fabric and not livy"
```

These markers select tests by their stated needs. Production telemetry remains
the semantic check that those needs exactly match the boundaries crossed by the
test body.

Ordinary timings are not resource events. Production boundaries call
`Session.telemetry.external(...)` explicitly, so a timing name cannot
accidentally become a resource declaration.

Warehouse work obtained with `Session.sql_executor()` remains Session-owned:
queries, scripts, result-set reads and procedure calls all emit TDS events.
Fabric test claims do not construct a desktop SQL executor directly, because
that connection has no active test Session to receive its telemetry.

For each test body, pytest compares the declared resources exactly with the
resources emitted by its registered Sessions:

```text
declared resources == observed resources
```

An undeclared crossing and an unused declaration both fail. Add a resource only
when it is necessary to prove the test's claim. A mismatch is evidence to
investigate: the test may combine claims, a fixture may do unrelated work, or
production may cross a boundary unnecessarily.

## Shared Session acquisition

The Fabric suite reuses one ConsoleSession, resolver/cache, Livy session, and
one TDS connection per Warehouse. Reuse must not make declarations depend on
test order.

Fixture acquisition is measured separately from claim-body enforcement. For
example, the first use of a Warehouse may resolve its item and SQL endpoint over
REST and establish a TDS capability. A later TDS test declares `{"tds"}` when
its claim performs only a query. It does not declare REST merely because shared
setup populated the cache.

Fixtures register Sessions explicitly. Pytest records an event offset after
fixture setup and another after the test body. Setup events remain in terminal
performance reporting; only body events participate in exact declaration
matching. Repeated REST resolution for the same target on later tests is a
Session caching defect, not a reason to widen declarations.

## Semantic attribution

Session telemetry combines the external resource boundary with the active
reporting context:

```text
Task
  Step
    Sub-step
      resource / operation / elapsed / failure
```

Callers establish Task, Step, and Sub-step. Low-level boundaries state only the
resource and operation. Work submitted to Session-owned background threads
captures the current telemetry context and restores it when the worker runs, so
resource acquisition and Warehouse flushing retain the meaning that caused
them.

## Reporting

At the end of a run, pytest reports:

- test counts by declared scope;
- counts of tests declaring each resource;
- shared Livy startup time;
- claim-body and fixture-setup cost by resource;
- the tests with the most external elapsed time;
- the most expensive Task / Step / Sub-step crossings;
- declaration match and mismatch counts.

Elapsed time is diagnostic, not a budget. Use the report to find unnecessary
crossings, unexpectedly rich fixtures, and repeated remote work. One remote
state transition should normally return one evidence payload, with assertions
performed locally against that payload.

## Repository layout and names

```text
tests/
  targeted/   narrow pure-Python seam tests and fixture constructors
  support/    shared declarations, observations, journeys, and harness helpers
  fabric/     tests that require a real Fabric workspace
```

Test modules use `test_<subject>_<claim>.py`. The claim suffix is one of:

| claim | module purpose |
|---|---|
| `declaration` | accepted and rejected contracts |
| `representation` | exact values or rendered output |
| `boundary` | behaviour at a stable collaborator or platform boundary |
| `install` | installation of a generated artefact |
| `primitive` | one executable primitive |
| `cycle` | related state transitions |
| `invariant` | a property mechanically kept true |
| `journey` | composed end-to-end behaviour |

`tests/test_test_architecture_invariant.py` enforces names, declarations,
generated markers, the closed resource vocabulary, valid scope combinations,
and removal of superseded machinery.

## What a test owns

A feature is not covered because its parts are tested. At least one test owns the
whole intended behaviour, at the cheapest layer that can prove it.

State that behaviour in the test's docstring, as `Intent:` and, where it helps,
`Proof:`. Write it as a product outcome rather than a mechanism:

```text
Intent: A corrected build self-heals from physical work a failed build left.
Intent: A Warehouse developer can load an installed object through _.Load.
```

rather than:

```text
Intent: generate_load_entry emits the expected SQL.
Intent: the fixture crosses TDS.
```

A narrow test may protect a narrow contract. It still says why that contract
matters.

Two rules follow, and they are about where a new test goes:

- When Fabric acceptance exposes a defect pure Python can model, strengthen the
  pure regression rather than adding another Fabric test.
- An isolated Fabric test names the Fabric-specific fact it establishes beyond
  the acceptance journey and the standalone developer journeys.

## Adding a test

1. Write the claim as one sentence and choose the cheapest scope that can prove
   it.
2. Name the module for its subject and claim type.
3. Add `@weaver_test(...)` with that one scope.
4. Use an existing Session fixture and ordinary Weaver behaviour.
5. Run the test and inspect observed telemetry before declaring resources.
6. Investigate every mismatch rather than broadening the declaration by
   default.
7. For Fabric work, use the fixed `PYTEST_WORKSPACE` estate and consolidate
   observations of one remote state into one payload.

The architecture invariant makes this declaration model repository-wide. A new
test without the wrapper, a handwritten managed marker, an unknown resource, or
retired declaration machinery fails the core suite.
