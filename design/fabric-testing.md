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
| `PYTEST_HOUSE` | Lakehouse | Warehouse alias producer |
| `PYTEST_WH_1` | Warehouse | Warehouse target |

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
.venv/bin/weaver install \
  --workspace PYTEST_WORKSPACE \
  --environment weaver
```

Publish again when Weaver Python changes. A Livy session can run Spark SQL
without the installed package; only operations that import Weaver require the
Environment.

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

## Test estate hygiene

Fixed items reduce endpoint readiness variance and Fabric namespace churn.
Fixtures clean the part of the estate they use and report cleanup failures
without masking a test failure. Interrupted provision tests may leave items
with the `weavertest_` prefix; these can be removed manually after confirming
they belong to the test run.

Tests must not depend on tenant-specific defaults outside this harness. Another
tenant can run the suite by supplying the workspace, Environment, and fixed item
names through the documented environment variables.
