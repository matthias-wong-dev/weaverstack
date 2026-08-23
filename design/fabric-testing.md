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


@weaver_test(provision=True, resources={"rest"})
def test_an_item_is_created_and_deleted(...):
    ...
```

The declaration is the source of truth. Pytest generates `fabric`, `remote`,
`hosted`, `full_integration`, `provision`, `tds`, `livy`, `onelake`, and `rest`
markers from it for selection; contributors do not maintain those markers
manually.

The scopes are:

| scope | use |
|---|---|
| `remote` | Weaver runs from this checkout against Fabric; no published wheel is needed |
| `hosted` | the claim requires Weaver published in the Fabric Environment |
| `integration` | a composed lifecycle journey |
| `provision` | Fabric item creation and deletion |

Integration and provision stand alone. They do not require an additional
remote or hosted declaration.

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

Its contents are seeded by `tests/fabric/provision_estate.py`, which writes the
Delta tables through Spark, so a fixture finds them rather than filling them in.

Lakehouses must be schema-enabled. The suite empties fixed targets between
estate transitions instead of recreating them. Provision tests separately
exercise create/delete lifecycle behaviour.

The workspace capacity must be running. On a capacity that permits one Spark
session, close notebooks and other sessions before starting the suite. The
harness prints active or queued Spark sessions before requesting its shared
Livy session and never cancels them.

## Publishing the hosted package

Remote tests exercise the current checkout immediately. Hosted and integration
tests require the current Weaver wheel in a Fabric Environment:

```bash
.venv/bin/weaver fabric environment publish weaver \
  --workspace PYTEST_WORKSPACE
```

Publish again when Weaver Python changes. A Livy session can run Spark SQL
without the installed package; only operations that import Weaver require the
Environment.

That includes the estate a hosted test builds for itself. `fabric_lakehouse_estate`
installs its bundle in the session, so a change to *generation* — the DDL a table
is created with, the catalogue DML a build publishes — reaches a hosted test only
after a republish. A hosted failure that looks like the change had no effect is
usually a stale wheel.

A structural change to a table declaring `Prohibit rebuild: true` needs one further
step: reconciliation will not replace it, so an installed one keeps its old shape
until it is dropped. Weaver's own catalogue tables are the ones this applies to,
and dropping the `_` schema's tables is enough — the next build recreates them.

## Running the strata

```bash
.venv/bin/python -m pytest
.venv/bin/python -m pytest -m "fabric and remote"
.venv/bin/python -m pytest -m "fabric and hosted"
.venv/bin/python -m pytest -m full_integration
.venv/bin/python -m pytest -m provision
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

Measured over four Lakehouse modules against `PYTEST_WORKSPACE`:

```text
stage the repository     ~10s   onelake
generate the bundle      14-21s livy
install the bundle       43-60s livy
```

So one estate costs roughly a minute, and the four modules that share a
repository fixture paid for three of them out of a 14-minute run — material, and
not dominant.

**Sharing the build is therefore worth doing and has not been done.** The obstacle
is the reset, not the build: a module currently gets its estate by *emptying the
target and the catalogue* and then installing into it, so sharing one installation
means replacing that with a reset that clears the mutable data and the catalogue's
runtime rows while leaving the structure and the projected rows. Whether that
preserves isolation is a claim about execution order, and it can only be settled by
running the modules in both orders. Until that has been done, the suite pays for
the builds.

Two things that were free have been taken. `test_run_decomposition_boundary.py`
no longer parametrises `weaver_repo_fixture` with the value it already defaults
to, which forced an estate of its own for nothing. And the provisioning cost is
now measured rather than inferred, which is what any further reduction has to
argue against.

## Test estate hygiene

Fixed items reduce endpoint readiness variance and Fabric namespace churn.
Fixtures clean the part of the estate they use and report cleanup failures
without masking a test failure. Interrupted provision tests may leave items
with the `weavertest_` prefix; these can be removed manually after confirming
they belong to the test run.

Tests must not depend on tenant-specific defaults outside this harness. Another
tenant can run the suite by supplying the workspace, Environment, and fixed item
names through the documented environment variables.
