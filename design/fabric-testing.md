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

Immediately before it requests its shared Livy session, the harness reads the
sessions collection for every Lakehouse in the workspace. It prints active or
queued scheduler/plugin/Livy states and the submitter when present. This catches
the common one-session-capacity case where an open notebook or leaked test would
otherwise make startup appear silently stuck. The check is read-only and never
cancels someone else's session.

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
program calls the public `build_uploaded_item_repository` workflow. That workflow copies
the OneLake repository once to the session driver's temporary filesystem, then
generates and installs from local files while target inspection still uses the
authoritative Fabric catalogue. In the emulator the same workflow runs
in-process against local Spark.
The desktop's only job on Fabric is to push the repository and read results back
for assertions — it never plans.

Both Lakehouses are created **schema-enabled**: the target so a managed table
lands at `Tables/<schema>/<table>` and views bind by name, and the Weaver
Lakehouse because the catalogue lives in a schema called `_` and a Lakehouse
without schemas cannot hold one.

## Cross-item aliases need two destinations

`tests/fabric/test_cross_item_alias.py` builds two Lakehouse items in one bundle,
where the consumer aliases a table the producer makes. It takes its own pair of
disposable Lakehouses (`fabric_alias_lakehouses`) rather than the shared target,
because a cross-item alias is the one thing a single destination cannot express.

Three things only a real workspace answers, and this is where they are answered:
a OneLake shortcut is a workspace API call rather than a file operation; the
shortcut has to be created after the table it points at exists; and a Lakehouse's
SQL analytics endpoint lags its Delta tables, so an item that mutated Delta is
closed by a refresh the emulator can only skip.

It also found the asynchrony: Fabric returns from the shortcut call before the
Lakehouse will accept the name as a relation, and the consumer's very next
statement failed with *"neither a view nor a table"*. The alias action now waits
for a real read to succeed before reporting success.

**The session attaches to the Weaver Lakehouse**, which is the production model —
the control plane is the fixed attachment, destinations are the variable data
plane. It used to attach to the *target*, and that made the suite structurally
unable to fail: a two-part `Schema.Object` happened to land in the right place,
and the assertion then read it back through the same session catalogue, so a
table written to the wrong Lakehouse would have been read from the wrong Lakehouse
and passed. Under the real attachment an unqualified name lands in the control
plane, so every statement has to name its Lakehouse — and so does every assertion.

`BuildEnv.query` takes the same `{{object:Schema.Name}}` form a payload uses and
resolves it against a named destination, defaulting to the target:

```python
build_env.query("SELECT count(*) AS n FROM {{object:DWG.Customer}}")
build_env.query("SELECT * FROM {{object:_.Registry}}",
                destination=build_env.weaver_destination)
```

See the master CLI plan for the Fabric contract these tests enforce, including
in-session generation, explicit target naming and catalogue-last certification.

## One environment fixture, two transports

The build tests share a single reusable harness so the same assertions run
locally and on Fabric, and so a Fabric run costs as little as possible.

`BuildEnv` (in `tests/fabric/conftest.py`) is a small record of callables —
`install_repo`, `generate`, `install`, `run_query`, `run_columns` and `seed_orphans`
— with the transport hidden behind them, plus the two destinations
the environment addresses. A test body drives it and never mentions Livy, Spark or
ODBC. Three environments implement it:

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
desktop's only jobs are uploading the Weaver document repository and reading the catalogue
back for assertions — the latter through `desktop_sql_executor`, which is test
infrastructure and never part of what is under test.

Two rules keep the cost down and the setup in one place:

- **Environment setup lives only in `conftest`.** Tests never build a workspace,
  create a Lakehouse, start a session or clean a catalog. Which Weaver document repository an
  environment installs is the `weaver_repo_fixture` parameter (paths in
  `tests/fabric/build_envs.py`), so one body can be pointed at another estate:
  `@pytest.mark.parametrize("weaver_repo_fixture", [SQL_TABLE_FIXTURE], indirect=True)`.
- **An estate is provisioned and installed once per module.** The module-scoped
  `lakehouse_estate` and `warehouse_estate` fixtures install one estate and hand
  every test in the module the same `InstalledEstate`, so a whole module of Fabric
  assertions costs one Lakehouse (or one Warehouse) and one install rather than
  one per test. `disposable_warehouse` is session-scoped for the same reason — a
  Warehouse takes minutes to provision the endpoint. Use the function-scoped
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

**A schema is a three-level name.** Under `spark_catalog`, a Fabric schema is
`workspace.lakehouse.schema`, so an object is four parts. One session can create,
read, drop and `MERGE` in any Lakehouse in the workspace by that name, and can
build a view in one Lakehouse over a table in another — no attaching, no
switching. `SHOW SCHEMAS IN `ws`.`lh`` is **not** supported, though: it encodes the
pair and looks it up as a schema, and a bare `SHOW SCHEMAS` answers only for the
attached Lakehouse. Enumerating another Lakehouse's schemas means reading its
`Tables/` area. `SHOW TABLES IN` a four-part schema works and includes views, so
tables are `SHOW TABLES` minus `SHOW VIEWS`. `spark.catalog.tableExists` and
`databaseExists` accept the qualified name; `listDatabases` and `listTables` do
not — they re-encode it and fail.

**Lakehouse table casing is creation-session policy, and the Warehouse is
case-sensitive.** With Fabric's default `spark.sql.caseSensitive=false`, even a
quoted `Sales.Customer` is registered and stored as `Sales.customer`. Weaver
temporarily enables case-sensitive analysis for its table-create DDL and restores
the session setting immediately, so current Weaver builds preserve `Customer` in
both the Spark catalogue and managed directory. If an older build registered a
case-only predecessor such as `customer`, the same DDL scope drops that
build-owned structure before creating the declared spelling; an ordinary rebuild therefore converges
existing Lakehouses as well as creating new ones correctly. A Fabric Warehouse
uses a case-sensitive collation, so inspect
`INFORMATION_SCHEMA.TABLES` on the Lakehouse SQL endpoint rather than guessing
either spelling or a sync delay.

Weaver passes a three- or four-part name through untouched, by design: the author
named a physical thing. Matching the endpoint's actual spelling is therefore the
author's job.

**A Lakehouse SQL endpoint exposes tables, not Spark views.** `Sales.ActiveCustomer`
is a Spark-catalogue object; it is queryable from Spark in any Lakehouse and is
simply absent from the endpoint. A Warehouse object cannot read one.

**An unqualified name lands in the attached Lakehouse, silently.**
`CREATE TABLE DWG.Customer` in a session attached to Lakehouse A creates
`Weaver.A.DWG.Customer` with no error, whatever the caller meant. This is why
generated payloads name their objects logically and the installer resolves them.

**Delta row counts without Spark.** Sum `numRecords` from `add.stats` across the
active files in `Tables/<schema>/<table>/_delta_log/*.json`, subtracting
`remove`d paths. Useful for asserting against Fabric without paying for a
session.

**A capacity resume is not instant.** About 30 seconds, and the ARM call returns
before the state changes. Poll `status`.
