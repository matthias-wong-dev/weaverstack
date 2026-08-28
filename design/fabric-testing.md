# Fabric integration tests

## Purpose

The Fabric suite proves platform behaviour against a real workspace. It uses
the same public Weaver operations and production Session telemetry as normal
execution. Pure-Python tests remain the place for decisions that do not require
Fabric.

Fabric tests are opt-in and skip when the configured workspace cannot be
reached.

## Test declarations

Every test function declares its scope and external resource needs with
`@weaver_test(...)`:

```python
@weaver_test(remote=True, resources={"tds"})
def test_a_warehouse_query(...):
    ...


@weaver_test(hosted=True, resources={"livy"})
def test_an_installed_program_runs(...):
    ...

```

The declaration is the source of truth. Pytest generates `fabric`, `remote`,
`hosted`, `full_integration`, `tds`, `livy`, `onelake`, and `rest`
markers from it for selection; contributors do not maintain those markers
manually.

The scopes are:

| scope | use |
|---|---|
| `remote` | Weaver runs from this checkout against Fabric; no published wheel is needed |
| `hosted` | Weaver executes inside Fabric, normally from the checkout the suite injects |
| `integration` | a composed lifecycle journey |

Integration stands alone. It does not require an additional remote or hosted
declaration.

## Resource declarations

Where a test runs and which resources it needs are independent dimensions. The
closed external-resource vocabulary is:

- `tds` — Warehouse SQL over TDS;
- `livy` — remote Spark or Python submission through Livy;
- `onelake` — OneLake DFS storage access;
- `rest` — Fabric control-plane REST calls.

Production boundaries record these crossings explicitly on
`Session.telemetry`. At the end of each test body, pytest requires the observed
resource set to equal the declaration exactly. Unexpected use and unused
declarations both fail.

Only a completed claim is compared, so a skipped or failed test reports why
rather than reporting a mismatch.

Use ordinary Weaver behaviour in telemetry tests. A test that calls
`Session.execute_spark_sql`, for example, proves the normal operation crossed
Livy and retained its semantic attribution. Manually opening a telemetry event
does not prove the production path.

Warehouse primitives take their executor from `Session.sql_executor()`. The
returned capability records every TDS query, script and procedure call. Direct
`desktop_sql_executor()` construction belongs only to harness readiness or
reset work outside a test claim; test modules are mechanically prevented from
using it.

## Shared Session and attribution

One pytest run reuses:

- one ConsoleSession;
- one Fabric resolver and item cache;
- one Livy session;
- one TDS connection per Warehouse.

The cross-workspace Environment attachment primitive opens a session in its
consumer workspace. Its exclusive Livy fixture schedules it before any test
acquires the shared session. Fabric can reject a consumer-workspace session
after a session has run in the primary workspace, even after the first session's
scheduler reports it ended. The primitive closes before the suite starts its
shared primary-workspace session, then the fixture allows thirty seconds for
the cross-workspace capacity handoff.

Fixtures register the shared Session explicitly with the active test. Pytest
records telemetry offsets around the test body, so events from a previous test
cannot be attributed to a later one.

Shared acquisition is reported separately from resource-declaration
enforcement. The first Warehouse use may resolve the Warehouse and SQL endpoint
over REST before establishing the reusable TDS capability. A test whose claim
then performs a TDS query declares only `{"tds"}`. Later tests using the same
target should not repeat REST resolution; repetition indicates a cache defect.

Session-scoped and module-scoped fixture work remains visible in the setup-cost
section of the terminal report. It is not assigned to whichever test happens to
run first.

## Task, Step, and Sub-step context

External events inherit the Session's active Task, Step, and Sub-step. The
resulting event states both what crossed and why:

```text
Task: Build
Step: Install
Sub-step: Execute Spark SQL
Resource: livy
```

Session-owned asynchronous work captures this context when submitted and
restores it in the worker. This applies to resource acquisition and Warehouse
flushing, so later execution retains the semantic context that caused it.

## Workspace setup

Install the development dependencies and sign in with Azure CLI:

```bash
az login
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

The default fixed workspace is named `PYTEST_WORKSPACE`. Override it when
needed:

```bash
export WEAVER_FABRIC_WORKSPACE=PYTEST_WORKSPACE
export WEAVER_FABRIC_ENVIRONMENT=weaver
```

The suite expects these reusable items, each overridable through its matching
`WEAVER_PYTEST_<ROLE>` environment variable:

| item | type | role |
|---|---|---|
| `PYTEST_WEAVER` | Warehouse | Weaver catalogue |
| `PYTEST_LH_1` | Lakehouse | primary Lakehouse target |
| `PYTEST_LH_2` | Lakehouse | cross-item producer |
| `PYTEST_LH_3` | Lakehouse | cross-item consumer |
| `PYTEST_HOUSE` | Lakehouse | producer for a Warehouse shortcut |
| `PYTEST_WH_1` | Warehouse | Warehouse target |
| `PYTEST_STAGING` | Lakehouse | repositories and bundles, never a target |

A second workspace holds what a direct shortcut points at. It is not a Weaver
target workspace: nothing is built into it, and the tests that read it are
proving that no destructive operation reached it.

| Item | Type | Role |
|---|---|---|
| `PYTEST_WORKSPACE_EXT` | Workspace | external estate, `WEAVER_FABRIC_WORKSPACE_EXT` |
| `PYTEST_EXT_LH` | Lakehouse | external tables, schema and folder sentinels |
| `PYTEST_EXT_WH` | Warehouse | external T-SQL tables, read through OneLake |

Both hold two schemas, and the difference is load-bearing. `Reference` is never
mutated, so tests assert on its exact rows and bytes. `Source` is what the
acceptance journey mutates and restores, so a load can be shown to move data and
to leave the rest alone. `tests/support/external_estate.py` defines both.

Its contents are seeded by `tests/fabric/provision_estate.py`, which writes the
Delta tables through Spark and the Warehouse tables over TDS, so a fixture finds
them rather than filling them in.

Lakehouses must be schema-enabled. The suite empties fixed targets between
estate transitions. Nothing in it creates or deletes a Fabric item;
`tests/fabric/provision_estate.py` does that, run by hand.

The workspace capacity must be running. On a capacity that permits one Spark
session, close notebooks and other sessions before starting the suite. The
harness prints active or queued Spark sessions before requesting its shared
Livy session and never cancels them.

## Where the suite's Weaver comes from

Remote tests exercise the current checkout immediately. Hosted and integration
tests run Weaver inside Fabric, and the suite puts this checkout there itself.

At session start the harness builds one wheel from the checkout, stages it under
`PYTEST_STAGING/Files/injected_weaver`, and hands the Livy session a bootstrap
that extracts it on the driver and puts it first on `sys.path`. A Python change
therefore reaches a hosted test for the cost of a wheel build and an upload.
`tests/fabric/injected_weaver.py` holds it.

The Environment still has to exist and still carries the dependencies:

```bash
.venv/bin/weaver fabric environment publish weaver \
  --workspace PYTEST_WORKSPACE
```

Publish again when Weaver Python or its project dependencies change. Publication
keeps the Environment definition and foreign custom libraries. An Environment
holding a Weaver wheel as well is harmless. The bootstrap reads
`weaver.__file__` and fails the session unless the package came from the
extraction directory, so an Environment carrying a wheel of this same version
cannot satisfy it.

The staged artefact is a built wheel. Weaver reaches its bundled SQL templates
and `warehouse_type_mapping.yml` through `Path(__file__)`, so a packaging change
that dropped them from the wheel shows up here.

To run the published wheel, which is what exercises
`weaver fabric environment publish` end to end:

```bash
WEAVER_PYTEST_INJECT_WEAVER=0 .venv/bin/python -m pytest -m "fabric and hosted"
```

Do that before a release. In that mode a stale wheel behaves as it always did: a
hosted failure that looks like a change had no effect is usually one.
`tests/fabric/test_published_weaver_primitive.py` is the smoke test for it: the
published package imports, reports a version the Environment has published, and
resolves a Lakehouse. It skips in the ordinary injected mode, where the
Environment's published set says nothing about what the session is running.
Ordinary hosted runs execute one Weaver, the injected one.

`tests/fabric/test_environment_publish_preservation_primitive.py` exercises the
desktop publication command against the fixed Environment. It keeps a
pre-existing user package, the exported `environment.yml`, and published Spark
compute settings unchanged, confirms the Weaver wheel is present, and requires
the second publication to be a no-op.

A structural change to a table declaring `Prohibit rebuild: true` needs one
further step: reconciliation will not replace it, so an installed one keeps its
old shape until it is dropped. Weaver's own catalogue tables are the ones this
applies to, and dropping the `_` schema's tables is enough. The next build
recreates them.

## Running the strata

```bash
.venv/bin/python -m pytest
.venv/bin/python -m pytest -m "fabric and remote"
.venv/bin/python -m pytest -m "fabric and hosted"
.venv/bin/python -m pytest -m full_integration
.venv/bin/python -m pytest -m "fabric and remote and tds"
.venv/bin/python -m pytest -m "fabric and remote and livy"
.venv/bin/python -m pytest -m "fabric and remote and onelake"
.venv/bin/python -m pytest -m "fabric and remote and rest"
.venv/bin/python -m pytest -m "fabric and tds"
.venv/bin/python -m pytest -m "fabric and not livy"
```

Use `-s` when fixture phase timings and progress output are useful. Use a node
or module path with the same marker selection for a narrow rerun.

## Telemetry report

The terminal summary answers:

- which tests declared TDS, Livy, OneLake, and REST;
- which claim bodies actually crossed each resource;
- how much external time occurred in shared acquisition and fixture setup;
- which tests spent the most external time;
- which Task / Step / Sub-step caused each expensive crossing;
- how many resource declarations matched or mismatched.

Timings are evidence, not pass/fail budgets. When a test crosses an unexpected
resource, determine whether the declaration is wrong, the test combines claims,
a fixture performs unrelated work, or production makes an unnecessary crossing.
Do not add resources merely to silence the mismatch.

One Livy submission costs seconds. Gather all evidence about one remote state
transition in one payload and make assertions locally. Split submissions only
when the boundary between two moments is itself the claim.

## What provisioning an estate costs

Standing an estate up is the largest single thing this suite spends, and it
happens in fixtures. The harness records it in a ledger of its own, reported as
**Estate provisioning** in the terminal summary: how many times each phase ran and
what it crossed.

These are the *harness's* crossings, not a Weaver Session's, and they are reported
apart for that reason. A test's declared resources are compared with what its own
subject crossed in its claim body, and putting fixture plumbing into that
comparison would make every assertion-heavy test declare a resource its subject
never touched.

Measured over `-m "fabric or full_integration"` against `PYTEST_WORKSPACE`, a
1h 27m run of 187 tests:

```text
install the bundle        13 run(s)    600.8s  livy
generate the bundle       13 run(s)    160.7s  livy
reset the target           8 run(s)     87.6s  livy
stage the repository      11 run(s)     56.3s  onelake
total                                  905.0s
```

So one estate costs roughly a minute, thirteen of them were built, and
provisioning is **17% of the sweep**. The external crossings behind it:

```text
livy      476 op(s)   1677.9s
tds     1,860 op(s)   1066.4s     0.57s per operation
rest      435 op(s)    179.4s
onelake 1,016 op(s)    145.7s
```

Livy dominates, and the five heaviest acceptance-journey scenarios are about 29
minutes of the total. The journey drives one estate through build, load, test
and wipe against a real workspace, and that scope is what costs the 29 minutes.

**Sharing one installation across the modules that use the same repository is
the largest remaining saving, and it has been declined.** The obstacle is the
reset: a module gets its estate by emptying the target and the catalogue and
then installing into it, so sharing one installation means a reset that clears
the mutable data and the catalogue's runtime rows while leaving the structure and
the projected rows. That makes isolation a claim about execution order, and it
puts shared mutable state between modules. Module isolation stays explicit and
the suite pays for the builds. Revisit only with evidence from running the
modules in several orders and getting identical answers.

The same reasoning declined a catalogue reset scoped to the items the next module
binds. Clearing `_` and rebuilding it is expensive and understandable; ownership
and invalidation rules in the harness are neither.

What has been taken is the round trips inside those phases.
`_empty_the_catalogue` and `_forget_the_catalogue_schema` each dropped one object
per round trip and now build their drops with `string_agg` and run them with
`sp_executesql`, which is one round trip each. `_empty_the_catalogue` also takes
its executor from the Session, so it reuses the warm connection and its crossings
reach this ledger; a connection of its own emitted no Session telemetry, which is
why earlier readings understated the reset.

Then the estates themselves. `test_installed_estate_boundary.py` is three
modules that each built the same repository, and six modules whose claims the
acceptance journey or the core suite already make are gone. That took remote and
hosted from 38m34s to about 24 minutes. The figures in the ledger above were
measured at that point.

A second pass moved the remaining standalone Fabric machinery onto the
acceptance estate, which builds and loads anyway:

```text
gone                                     where the claim is now
test_load_orchestration_cycle.py         the acceptance load report: one endpoint
                                         refresh, its edges, and the Warehouse
                                         reading what the Lakehouse wrote
test_external_shortcut_journey.py        the acceptance estate reads the foreign
                                         workspace through a table, a folder and
                                         a schema shortcut, by loading through them
test_item_catalogue_fabric_boundary.py   the acceptance build's own catalogue:
                                         every table present, `_weaver` installed
                                         and certified table for table
test_shared_catalogue_host_boundary.py   a user's schema seeded in the catalogue
                                         Warehouse before the first build, read
                                         back after the build and the rebuild
```

`test_installed_estate_boundary.py` submitted one body twice and asked it two
questions; it now submits it once. Its run-decomposition section is gone, because
scope opening, reuse and closing are the core suite's and catalogue-driven load
composition is the acceptance journey's. Of its two File-key cases the tolerant
one stayed, because it shows the rejection, the publication and the change
document together.

`test_published_weaver_primitive.py` was five capability probes and a whole
installation. Every hosted test now runs an injected checkout inside Fabric and
proves those capabilities by using them, so what is left is one release-mode
smoke test for the published wheel.

The Delta keyed matrix is decided without a tenant. One representative refusal
runs against Spark, in `test_delta_keyed_refusal_primitive.py`, with no build and
no estate behind it.

What came back is `test_authored_object_attachment_primitive.py`. Fabric decides
what a session reports as its attachment and where it mounts it, so no local test
can settle it, and it costs one submission against the session the suite already
holds.

Measured after that pass, against `PYTEST_WORKSPACE`:

```text
pytest                          3,506 tests     9m 31s
pytest -m "fabric and remote"     116 tests    16m 07s
pytest -m "fabric and hosted"      25 tests     5m 30s
pytest -m full_integration         11 tests    36m 40s
```

Remote is the larger half of remote plus hosted, and TDS is where it goes: 710
operations and 497s of them, with the catalogue-upgrade builds at the top. Hosted
carries two bundle installs, 103s of the 5m 30s.

The journey's cost is its scope. Seven builds, five loads, four test runs and two
wipes over a four-item estate, and Livy is 1,368s of it. Its own largest scenario
is the failed build and its recovery, at 443s, which drives a failed build, a
repair, two more builds, a load and a test.

## Test estate hygiene

Fixed items reduce endpoint readiness variance and Fabric namespace churn.
Fixtures clean the part of the estate they use and report cleanup failures
without masking a test failure.

Tests must not depend on tenant-specific defaults outside this harness. Another
tenant can run the suite by supplying the workspace, Environment, and fixed item
names through the documented environment variables.
