# Fabric integration tests

These touch a real workspace and a running capacity. They are **deselected by
default** and skip unless a workspace is named, so nobody runs them by accident
and nobody without a tenant is blocked.

## Once

```bash
brew install azure-cli                  # macOS
# Linux:   curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
# Windows: winget install Microsoft.AzureCLI
az login
pip install -e '.[test,cli]'
```

`[test]` is the suite without a JVM — core tests plus these Fabric ones, which
need credentials and HTTP but no Spark. `[cli]` adds `weaver install`, needed
below to publish the Environment. Use `[dev]` instead only if you also want
local Spark; it pulls in PySpark, which is a few hundred megabytes, builds from
source and does nothing without a JDK.

`az login` is the only authentication Weaver needs — see
[CLI usage](cli-usage.md#signing-in-to-azure) for what it does and why the
credential chain is pinned.

You need a Fabric workspace where the test identity can create and delete
Lakehouses and Warehouses and connect to Warehouse SQL. It can be empty; the
tests bring their own. The workspace capacity must already be running and
usable. Pytest never starts, resumes, suspends, or waits for capacity.

## Install Weaver once, whenever the code changes

The Fabric suite imports Weaver from a Fabric Environment — it does **not** copy
source into the workspace. Install (or update) that Environment whenever Weaver
Python changes:

```bash
weaver install --workspace <workspace> --environment weaver
```

This builds a wheel from the checkout, stages it and Weaver's dependencies, and
publishes. Publishing resolves the dependencies into the image and takes a few
minutes; the wheel itself uploads in about a second.

## Each session

Capacity is billed while it runs, so turn it on, work, turn it off.

```bash
weaver capacity resume  --resource-group <rg> --capacity-name <capacity>
weaver capacity status  --resource-group <rg> --capacity-name <capacity>

WEAVER_FABRIC_WORKSPACE=<workspace> WEAVER_FABRIC_ENVIRONMENT=weaver \
  .venv/bin/python -m pytest -m fabric

weaver capacity suspend --resource-group <rg> --capacity-name <capacity>
```

Resuming takes about half a minute and `resume` returns before the capacity is
`Active`, so `status` is the confirmation.

Without `WEAVER_FABRIC_WORKSPACE` the suite skips with a message saying so,
rather than failing. `WEAVER_FABRIC_ENVIRONMENT` defaults to `weaver`; the Livy
tests skip (rather than fail) if that Environment has no Weaver installed yet,
pointing at `weaver install`.

To run only the Warehouse SQL vertical and see its stage timings:

```bash
WEAVER_FABRIC_WORKSPACE=<workspace> WEAVER_FABRIC_ENVIRONMENT=weaver \
  .venv/bin/python -m pytest -m fabric -s tests/fabric/test_warehouse_wipe.py
```

## What the tests do to your workspace

They create their own Lakehouses, named `weavertest_<role>_<random>`, and a
disposable Warehouse named `Weaver_Pytest_<UTC timestamp>_<random>`. Every item
is deleted in a `finally`. Nothing pre-existing is touched.

If a run is interrupted, the prefix makes leftovers obvious and they can be
deleted from the workspace by hand. A Warehouse cleanup warning includes both
its name and item ID. Cleanup failures print a warning rather than raising, so a
tidy-up problem never masks a real test failure.

The Warehouse test deliberately crosses the boundary twice:

```text
desktop pytest creates and populates through mssql-python
→ installed Weaver wipes inside Environment-backed Livy
→ desktop pytest independently inspects the catalogue
```

Fixture population and catalogue inspection live under `tests/fabric`; they do
not select Fabric-native identity or duplicate the production wipe SQL.
Terminal output records item creation, endpoint readiness, first SQL
connection, first `select 1`, fixture population, Livy startup, Fabric wipe,
Warehouse deletion, and total fixture lifetime.

## The build bundle runs entirely in Fabric

`tests/fabric/test_build_bundle.py` runs the same four behavioural tests in
Fabric and its local emulator, selected by an indirect `build_env` parameter
(`local`/`fabric`). It is the reference for the Fabric-first rule: **both phases
of a build — *generate* and *install* — run in the target environment.** On
Fabric that is inside the Livy session, against the native Spark catalogue: the
test uploads the repository to the Weaver Lakehouse (the push), then a Livy
program calls `generate_build_bundle` in-session and another calls
`install_bundle`, so planning and installation both use the authoritative
catalogue. In the emulator the same two calls run in-process against local Spark.
The desktop's only job on Fabric is to push the repository and read results back
for assertions — it never plans.

The target Lakehouse is created **schema-enabled** so a managed
`CREATE TABLE Schema.Object` lands at `Tables/<schema>/<table>` and views bind by
name, and the session defaults to that target so two-part names resolve there.
See the journal's build-bundle log for the full Fabric contract these tests
established (`CREATE SCHEMA` over `CREATE DATABASE`, the reserved `dbo` schema, the
https/abfss bundle re-resolution, and `FabricStore` byte reads/writes).

## One environment fixture, two transports

The build tests share a single reusable harness so the same assertions run
locally and on Fabric, and so a Fabric run costs as little as possible.

`BuildEnv` (in `tests/fabric/conftest.py`) is a small record of callables —
`install_repo`, `generate`, `install`, `query`, `columns`, `seed_orphans` — with
the transport hidden behind them. A test body drives it and never mentions Livy,
Spark or ODBC. Three environments implement it:

| fixture | generation | installation | reads back with |
|---|---|---|---|
| `local_build_env` | in-process | in-process | local Spark |
| `fabric_build_env` | in the Livy session | in the Livy session | in-session Spark |
| `warehouse_estate` | in the Livy session | in the Livy session, over Weaver's Fabric-native SQL | desktop T-SQL (assertions only) |

Weaver is a Fabric tool, so **every** environment runs both phases where the code
is installed — there is no desktop-planned build. For a Warehouse that means
generation reads the target's system schema in-session through Weaver's own
`fabric_sql_executor` (the session identity) and compiles the prune into the
bundle there; installation runs the frozen T-SQL through the same connector. The
desktop's only jobs are uploading the SES repository and reading the catalogue
back for assertions — the latter through `desktop_sql_executor`, which is test
infrastructure and never part of what is under test.

Two rules keep the cost down and the setup in one place:

- **Environment setup lives only in `conftest`.** Tests never build a host,
  create a Lakehouse, start a session or clean a catalog. Which SES repository an
  environment installs is the `ses_fixture` parameter (paths in
  `tests/fabric/build_envs.py`), so one body can be pointed at another estate:
  `@pytest.mark.parametrize("ses_fixture", [SQL_TABLE_FIXTURE], indirect=True)`.
- **An estate is provisioned and installed once per module.** The module-scoped
  `lakehouse_estate` and `warehouse_estate` fixtures install one estate and hand
  every test in the module the same `InstalledEstate`, so a whole module of Fabric
  assertions costs one Lakehouse (or one Warehouse) and one install rather than
  one per test. `disposable_warehouse` is module-scoped for the same reason — a
  Warehouse takes minutes to provision. Use the function-scoped
  `fabric_build_env` only where a test genuinely needs a fresh target, as the
  prune and failure-path cases in `test_build_bundle.py` do.

`lakehouse_environments` (also in `build_envs.py`) is the marker that runs one
body against both Spark transports, so `-m spark` and `-m fabric` each select
their half.

### `weaver install` is a precondition of the Fabric suite

Every Fabric build test — Lakehouse and Warehouse alike — runs inside the Livy
session, which imports Weaver from the Environment. **The suite therefore tests
the wheel that was last published, not your working tree**, so any Weaver Python
change needs `weaver install` before it is exercised on Fabric. That is the point:
what is under test is Weaver-on-Fabric doing the job.

The one thing that legitimately skips a publish is a change whose effect is fully
determined *before* Fabric runs — nothing in the current build suite qualifies,
because generation itself happens in Fabric.

## The three test suites

| | command | needs |
|---|---|---|
| core | `pytest` | nothing — under a second |
| local Spark | `pytest -m spark` | a JDK and the `[spark]` extra |
| Fabric | `pytest -m fabric` | `az login`, a workspace, a running capacity |

The default run excludes both optional suites, so a contributor with neither a
JVM nor a tenant still gets a green build.

## Known Fabric behaviour

Things learned the hard way, kept here so they are not learned twice.

**OneLake does not support directory rename.** `PUT ?resource=directory` with
`x-ms-rename-source` returns `400 UnsupportedHeader`. Moving data on OneLake
means copying it — read the bytes and write them back — or `notebookutils.fs.mv`
from inside a session. `DELETE ?recursive=true` is supported.

This matters to Weaver's design: `Store.move_within_store` exists as a
first-class operation precisely so an implementation *can* choose a cheap
rename. On OneLake it cannot, and must copy.

**The Lakehouse SQL endpoint lags behind Delta schema changes.** After tables
appear or change, a Warehouse cross-database view can fail with
`Invalid object name` until the endpoint syncs. Force it with
`POST /v1/workspaces/{workspace}/sqlEndpoints/{endpoint}/refreshMetadata`,
taking the endpoint id from the Lakehouse's
`properties.sqlEndpointProperties.id`.

**Delta row counts without Spark.** Sum `numRecords` from `add.stats` across the
active files in `Tables/<schema>/<table>/_delta_log/*.json`, subtracting
`remove`d paths. Useful for asserting against Fabric without paying for a
session.

**A capacity resume is not instant.** About 30 seconds, and the ARM call returns
before the state changes. Poll `status`.
