# Fabric integration tests

## Purpose

This document explains the real-Fabric test environment, marker selection, and
the evidence each test tier provides.

These tests use a real workspace and running capacity. They are deselected by
default and skip unless a workspace is configured.

## Once

```bash
brew install azure-cli                  # macOS
# Linux:   curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
# Windows: winget install Microsoft.AzureCLI
az login
pip install -e '.[dev]'
```

`[dev]` adds pytest and ruff. Everything else — the Fabric transports, the
`weaver` command, `weaver install` — is in the package itself. Fabric supplies
Spark where authored runtime code executes, so nothing here needs a JVM.

`az login` is the only authentication Weaver needs — see
[CLI usage](cli-usage.md#signing-in-to-azure) for what it does and why the
credential chain is pinned.

You need a Fabric workspace where the test identity can create and delete
Lakehouses and Warehouses and connect to Warehouse SQL. The workspace capacity
must already be running and usable. Pytest never starts, resumes, suspends, or
waits for capacity.

## Provision the fixed estate, once

The suite reuses a fixed set of items rather than creating disposable ones per
run.

Fixed items avoid provisioning variance, endpoint-wait tail risk, and namespace
churn. The suite is dominated by bundle generation and installation round trips
through Livy; reduce those cycles to improve run time.

Create these once, in the workspace you will point the suite at:

| Item | Type | Role |
|---|---|---|
| `PYTEST_WEAVER` | Lakehouse | the Weaver Lakehouse — the control plane |
| `PYTEST_LH_1` | Lakehouse | destination target |
| `PYTEST_LH_2` | Lakehouse | cross-item alias producer |
| `PYTEST_LH_3` | Lakehouse | cross-item alias consumer |
| `PYTEST_HOUSE` | Lakehouse | second alias producer, for the Warehouse case |
| `PYTEST_WH_1` | Warehouse | Warehouse destination |

Every Lakehouse must be **schema-enabled** (`creationPayload.enableSchemas`),
because a managed table has to land at `Tables/<schema>/<table>` and the
catalogue lives in a schema called `_`.

> A Warehouse **cannot** share a display name with a Lakehouse. A Lakehouse
> generates a `SQLEndpoint` facet of the same name, so the name is already taken
> and Fabric answers `ItemDisplayNameAlreadyInUse`. The only cross-type name
> sharing in Fabric is a Lakehouse and *its own* endpoint.

`scripts/` has no provisioning helper; the REST calls are three lines each
(`POST workspaces/{id}/lakehouses`, `POST workspaces/{id}/warehouses`) and the
whole estate builds in about fifteen seconds.

Isolation comes from **emptying** these between runs, not from replacing them.
That is the same reconciliation the build itself performs, so the cleaning path is
exercised rather than bypassed — but it does mean residue is possible here in a
way it never is locally, where every target is a fresh temporary directory. And
emptying is not free: it is part of why reuse did not make the suite faster.

Residue is not inert, either. A producer whose table already matches is correctly
*not* rebuilt, so a test asserting build order then finds no build action in the
plan. `fabric_empty_lakehouse` exists for exactly that case; ask for it wherever
freshness is the premise.

## Naming the estate

The estate is permanent and the suite knows its names, so a run needs no
environment at all. Everything is still overridable, so another tenant runs the
suite with its own items:

```bash
export WEAVER_FABRIC_WORKSPACE=PYTEST_WORKSPACE   # default: PYTEST_WORKSPACE
export WEAVER_FABRIC_ENVIRONMENT=weaver           # default: weaver
```

Every marker says *what a test needs*, and the one that matters most for day-to-day
work is whether a **wheel publish** is one of those things:

```bash
pytest -m fabric            # every test against a real Fabric workspace
pytest -m "fabric and remote" # no publish
pytest -m "fabric and hosted" # needs the published wheel
pytest -m full_integration  # the Fabric lifecycle journey
pytest -m provision         # Fabric creating and deleting items
```

Every Fabric test carries `fabric` and exactly one of the two. `remote` versus
`hosted` is about **whether a publish is required**, not about whether Livy is
involved and not about where the orchestration runs. Creating a OneLake shortcut
is a REST call, refreshing an endpoint is another, wiping a Lakehouse is
directory removal, and a Warehouse is reached over TDS, all of which work from
this checkout against a real workspace. Even a Spark body is `remote` if it does
not import Weaver, which is why starting a Livy session does not assert the
install: `LivySession.ensure_weaver` is called by the crossing that submits a
program, and by nothing else.

What earns `hosted` is needing the installed package. That covers tests whose
subject *is* the wheel: that it acquires its own capabilities from the
session's identity, that the Environment carries it, that it bootstraps its own
catalogue, proven once per capability in
`tests/fabric/test_published_weaver.py`. It also covers a decomposed operation
that orchestrates from this checkout and imports the wheel on the far side,
because the publish is what such a test costs whoever runs it.

Item **lifecycle** tests — creating and deleting Lakehouses — are separated for a
different reason.
They are separated because they exercise Fabric's resource management rather
than Weaver's and change rarely, while their create/delete churn slows every run
of the code actually under development. `create_lakehouse` is still required
platform integration — ordinary `weaver build` ensures the control Lakehouse — so this says
*when* to run the cover, not that it is unnecessary.

The item names above are defaults; each has a matching
`WEAVER_PYTEST_<ROLE>` override.

**Put the suite's workspace on its own capacity if you can.** A trial capacity
works and costs nothing — it is not an Azure resource, so it cannot bill a
subscription — and it keeps the suite off whatever capacity real work uses. On a
small shared capacity (an F2 permits one Spark session at a time) a long test run
and a scheduled job contend for the same slot.

## Install Weaver when you want to test the installed Weaver

The hosted position needs it, including the Fabric lifecycle journey, because
its subject *is* the installed package. `pytest -m "fabric and remote"` needs no
publish at all, which is the point: a five-minute publish should not sit between
you and finding out a REST request body is wrong.

Weaver is imported from a Fabric Environment — nothing is copied into the
workspace. Install (or update) that Environment when Weaver Python changes and
you want the wheel-backed tiers to reflect it:

```bash
weaver install --workspace <workspace> --environment weaver
```

This builds a wheel from the checkout, stages it and Weaver's dependencies, and
publishes. Publishing resolves the dependencies into the image and takes a few
minutes; the wheel itself uploads in about a second.

## Each session

Capacity is billed while it runs, so turn it on, work, turn it off.

```bash
weaver fabric capacity resume  --resource-group <rg> --capacity-name <capacity>
weaver fabric capacity status  --resource-group <rg> --capacity-name <capacity>

.venv/bin/python -m pytest -m "fabric and remote" # no publish needed

# and, when the installed package is what you want to exercise:
weaver install --workspace <workspace> --environment weaver
.venv/bin/python -m pytest -m "fabric and hosted"
.venv/bin/python -m pytest -m full_integration

weaver fabric capacity suspend --resource-group <rg> --capacity-name <capacity>
```

Leave a gap between the wheel-backed tiers: a capacity often allows one Spark
session, and a run starting seconds after another released its slot will find the
session queued rather than idle.

Resuming takes about half a minute and `resume` returns before the capacity is
`Active`, so `status` is the confirmation.

The suite runs against `PYTEST_WORKSPACE` unless `WEAVER_FABRIC_WORKSPACE` names
another, and skips with the reason if that workspace cannot be reached rather
than failing. `WEAVER_FABRIC_ENVIRONMENT` defaults to `weaver`. The session no longer asserts
that the Environment carries a usable Weaver: a body that wants Weaver imports it
and fails on its own terms, which is both more precise and what lets a test use
Spark without needing a publish.

Immediately before it requests its shared Livy session, the harness reads the
sessions collection for every Lakehouse in the workspace. It prints active or
queued scheduler/plugin/Livy states and the submitter when present. This catches
the common one-session-capacity case where an open notebook or leaked test would
otherwise make startup appear silently stuck. The check is read-only and never
cancels someone else's session.

To run only the Warehouse SQL vertical and see its stage timings:

```bash
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

## Both phases plan against real state

The Fabric build contexts are the reference for the Fabric-first rule: **both
phases of a build — *generate* and *install* — decide against the target
environment's real state.** With Weaver running in Fabric that state is right
there, so the test places an explicit repository source under a test-only
OneLake location and its Livy programs parse it, generate the bundle and install
it, all inside the session against the native Spark catalogue.

From a desktop the same state is read across first — the catalogue and a
Lakehouse's views as Spark SQL, its objects as storage, a Warehouse over TDS —
and planning happens here against what came back. What differs is where the
process runs, not what it plans against.

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
closed by a refresh.

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
build_env.query(
    "SELECT * FROM {{object:_.Registry}}", destination=build_env.weaver_destination
)
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
ODBC. Two environments implement it:

| fixture | generation | installation | reads back with |
|---|---|---|---|
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
  `tests/support/build_envs.py`), so one body can be pointed at another estate:
  `@pytest.mark.parametrize("weaver_repo_fixture", [SQL_TABLE_FIXTURE], indirect=True)`.
- **An estate is provisioned and installed once per module.** The module-scoped
  `lakehouse_estate` and `warehouse_estate` fixtures install one estate and hand
  every test in the module the same `InstalledEstate`, so a whole module of Fabric
  assertions costs one Lakehouse (or one Warehouse) and one install rather than
  one per test. `disposable_warehouse` is session-scoped for the same reason — a
  Warehouse takes minutes to provision the endpoint. Use the function-scoped
  `fabric_build_env` only where a test genuinely needs a fresh target, as the
  prune and failure-path cases in `test_build_bundle.py` do.

Local Spark and Fabric keep separate test modules and fixtures. They may share a
claim helper or declaration fixture, but no test body is parametrised across the
two transports: each proves only the engine or deployment facts its transport
can honestly answer.

### `weaver install` is a precondition of two tiers, not of the suite

`fabric and hosted`, including the Fabric `full_integration` journey, tests the
wheel last published to the Environment rather than the working tree.

It does not apply to `-m "fabric and remote"`, which drives real Fabric from this checkout.
The bundle is generated here, in pure Python, so the actions that run are provably
the ones the build produces; then only the part that must be remote is run — an
action through `execute_action`, a wipe, a REST call. A change to Weaver Python
is exercised there immediately, with no publish.

The distinction to hold on to is **whether a published wheel is required**, not
whether Livy is involved and not where the orchestration runs. A Spark body that
does not import Weaver needs a session, not a published package. An operation
that orchestrates from this checkout and imports the wheel on the far side is
`hosted`, because the publish is what it costs.

## The test tiers

The test-layer selection rule and directory conventions are defined in
[Test architecture](test-architecture.md). This section records the Fabric
preconditions for those tiers.

| | command | needs |
|---|---|---|
| core | `pytest` | nothing — a couple of minutes |
| Fabric, remote | `pytest -m "fabric and remote"` | `az login`, a workspace, a running capacity |
| Fabric, hosted | `pytest -m "fabric and hosted"` | the above **and** `weaver install` |
| the journeys | `pytest -m full_integration` | the above |

The default run excludes every optional tier, so a contributor without a tenant
still gets a green build. The first two are the development loop and neither
publishes anything.

Marker and directory agree: the Fabric tiers collect only `tests/fabric`. That is
enforced by where a test lives rather than by convention — a module placed there
loads the Fabric conftest, and with it a workspace, a credential and a session,
whatever its marker says.

## Fabric platform behaviour

**OneLake does not support directory rename.** `PUT ?resource=directory` with
`x-ms-rename-source` returns `400 UnsupportedHeader`. Moving data on OneLake
means copying it — read the bytes and write them back — or `notebookutils.fs.mv`
from inside a session. `DELETE ?recursive=true` is supported.

This matters to Weaver's design: `Store.move_within_store` exists as a
first-class operation precisely so an implementation *can* choose a cheap
rename. On OneLake it cannot, and must copy.

**A long desktop operation outlives its connections.** A build polls the Livy
API for as long as the build takes, and over ten minutes a single connection is
likely to be refused outright — `WinError 10061` on Windows — while the Spark
work it is watching carries on unaffected.

`_call` therefore retries with a short backoff, and what it may retry depends on
how far the request got. A read can always be repeated. Anything else only when
the connection was never established, which urllib3 reports as
`NewConnectionError` or `ConnectTimeoutError`: the server has not seen the
request, so sending it again cannot run a statement twice. A failure after the
request left this machine is reported rather than repeated, because whether
Fabric acted on it is no longer knowable.

Found by the desktop journey, which failed four times in a row at four different
points before this.

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
