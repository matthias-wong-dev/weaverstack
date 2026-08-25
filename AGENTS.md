# Weaverstack Agent Guide

Guidance for coding agents working on weaverstack itself.

## Repository role

`weaverstack` is a data-engineering runtime for Microsoft Fabric built around a
central catalogue. The Weaver catalogue is Weaver's operational metadata, held
under the `_` schema of a configured Fabric Warehouse. That Warehouse may be
Weaver's own, or one already holding a user's schemas; Weaver owns `_` in it and
nothing else. Destination Lakehouses and Warehouses hold materialised output only.

The distribution is `weaverstack`. The import is `weaver`.

## The sibling `weaver` repository is reference-only

The repositories sit side by side:

```text
dwg-platform/
├── weaver/        reference implementation. DO NOT MODIFY
└── weaverstack/   this repository
```

Consult `weaver` for proven algorithms, Fabric/OneLake/Spark/Warehouse edge
cases, Weaver document fixtures and behavioural intent. Never change it as part
of weaverstack work, and never import from it. Where the two disagree,
[design/weaver-architecture.md](design/weaver-architecture.md) is authoritative.

Reference baseline: `a97ba8a0b00dd66dff1b2c5e818403694562fd30`, the plan's
reviewed snapshot. The sibling checkout has since advanced. Confirm which
revision you are reading before treating it as the baseline.

## Implementation authority

Two documents, answering different questions.
[design/weaver-architecture.md](design/weaver-architecture.md) describes what
Weaver is: repository structure, documents, build, bundle, installation, the
Weaver catalogue, the command lifecycle.
[design/code-architecture.md](design/code-architecture.md) describes how this
repository is arranged to deliver it: the four doers, the representations they
hand each other, and where anything physical happens. Read the first for
behaviour, the second before moving code between layers.

The underlying system has run in production on SQL Server for years, and the
sibling `weaver` implementation works on Fabric. Port the proven algorithms.
Spend design attention on the control plane, which is the new part.

## The shape

```text
Workspace       one Fabric workspace configuration

Session         ConsoleSession   desktop → Fabric
                NotebookSession  already in Fabric
                TestSession      records the same contract

build           resolve request → read BuildState → Builder → Installer
load / test     resolve request → read RunState   → Runner

Fabric          Resolver, REST, OneLake, Livy, TDS
```

There is one workspace type, one build, one place a workspace is resolved, and
one conversion into the physical target vocabulary. Anything more complicated
needs a concrete reason.

## The core abstraction

Weaver runs inside Microsoft Fabric, and the resources are always in Fabric. What
varies is where Weaver's own code runs:

| | code runs | position |
|---|---|---|
| 1 | on a desktop | the desktop position: Weaver runs locally and reaches in over Livy, TDS, OneLake and REST |
| 2 | in Fabric | the in-Fabric position: `pip install weaverstack` in a notebook |

Both are complete ways to work.

**The rule: the Session handles TDS, REST, Livy and OneLake execution, picking
the right path for the host.** An operation calls the Session. It does not test
where it is running.

```text
Session in a notebook   native Spark, notebookutils, TDS
Session on a desktop    Livy, OneLake over HTTPS, TDS, REST
```

`use_or_create_session` in `weaver.sessions.host` picks the host once, per
workspace. Above that, `build`, `load`, `test` and `wipe` are the same code in
both positions.

The fast suite runs against a `TestSession`: a real implementation of the Session
contract that records what a host was asked to do and does not interpret it. It
models no Spark, no Delta and no Fabric catalogue, so nothing it proves can be
true only of a fake.

A `Workspace` identifies the workspace the resources live in. It says nothing
about whether access happens through desktop HTTP clients or inside a session. A
build bundle binds target kind, item identifiers and the display names that
four-part Spark naming uses. It carries no discriminator for where Weaver runs.

Storage has two parts. Keep them separate.

In-session execution, the store Weaver uses where it runs:

| execution | store |
|---|---|
| Fabric session | `FabricStore` over `notebookutils.fs` |

Cross-boundary access, a desktop caller reaching into a workspace:

| caller | destination | client |
|---|---|---|
| CLI | Fabric workspace | `OneLakeDfsClient` |
| Fabric integration tests | Fabric workspace | `OneLakeDfsClient` |

`OneLakeDfsClient` (ADLS Gen2 DFS over HTTPS) is how the desktop crosses in,
constructed explicitly by the caller that crosses. Inside Fabric, `store_for`
returns the session-native `FabricStore`. From a desktop that construction fails
and does not substitute DFS.

`FilesystemStore` is named for its transport. A build reads its repository
through one wherever it runs, because every incoming source tree is copied to a
temporary filesystem snapshot before parsing. See `_temp_copy` in
`weaver.build_bundle.workflow`. The copy is unconditional, including for a source
already on this filesystem, so a build never reads a tree the caller can still
edit underneath it.

Above resolution and the store, no code tests which host it is on. Code that does
means the abstraction is broken. Fix it in the factories, or in the CLI that does
the crossing.

**Credential choice belongs to the caller.** Core accepts an injected credential
and otherwise uses the library default without pinning the chain. The CLI and the
Fabric test infrastructure call `prefer_cli_credential()` themselves. Importing
or using the core imposes no credential choice.

### Fabric is the reference

Weaver is Fabric-first. The behaviour that must be right is the behaviour inside
Fabric. Without a tenant the fast suite may decide: render, plan, reconcile. It
must not model what Fabric would answer.

For anything with two phases, as the build bundle has generate then install, both
phases decide against the target environment's real state. Inside Fabric that
state is the native Spark catalogue, in the session. From a desktop it is read
across first and then planned against, which is what `read_build_state` does
before the Builder is handed anything.

The invariant is about the state, not the location. A planner given the real
catalogue and the real inventories reaches the same bundle wherever its process
runs. A planner given `None` does not: bundle generation that could not see
catalogue views could not prune them.

### Two positions, both first-class

A user can open a Fabric notebook, `pip install weaverstack`, and work. That is
the product, and it is what distinguishes Weaver from tools that require an
orchestration environment of their own.

The other half is everything driveable from a desktop, with Fabric reached
through Livy, TDS, OneLake and REST. Each crossing carries a small clear script.
It is one product in two positions, because the doers do not know which one they
are in.

There is one `build`, one `load` and one `test`. Every build action runs in the
`Installer` wherever that is, and the state a build plans against is read the same
way: the catalogue over TDS, a Lakehouse's views over Spark SQL, a Lakehouse's
objects from storage, a Warehouse over TDS. A desktop `weaver build` therefore
needs no published wheel, because nothing it submits imports Weaver, and no
Fabric Environment either, because its Spark statements run on the workspace
default. `load`, `test` and `install` ask for `--environment`.

Because the catalogue is a Warehouse, a Warehouse-only workflow performs zero
Livy submissions. Catalogue reads, publication, `_.Log` writes and `_.Bookmark`
reads and writes must never be the reason a Spark session starts.

What crosses as a program is a run's Python primitives, which are deployed
modules imported where Spark is. `weaver load` therefore requires the published
wheel.

A Fabric test that runs Weaver on the laptop tests the desktop position, not the
in-Fabric one. That is what the `remote` and `hosted` markers are for, and why a
capability is not proven until both are green.

Both positions are delivered by publishing Weaver into a Fabric Environment:

```bash
weaver fabric environment publish <environment> --workspace <workspace>
```

That builds a wheel from the checkout, stages it and Weaver's dependencies, and
publishes. A Livy session, and a Fabric notebook, then attaches that Environment
through `environment` on the workspace and imports the installed package. Nothing
is copied into the workspace. Republish whenever Weaver Python changes. An
unchanged source tree builds the same version and the publish is skipped.

### What this means when you add a feature

Ask, in order:

1. Can what it decides be tested without a tenant, against a `TestSession`?
2. Does it work against a Fabric workspace from the desktop?
3. Does it work with Weaver running inside Fabric?

Answer all three with tests that call the real function. Test code that
reproduces what the function would have done proves nothing: the first Fabric
suite deleted files through the store directly and looked like it was testing
`wipe`.

## Architecture invariants

Enforced by `tests/test_core_boundary.py`:

- **Core never imports the CLI.** `weaver_cli` parses arguments and prints. A
  core import of it would put a desktop concern inside the package a Fabric
  Environment runs. The dependency goes one way, CLI → core.
- **The core is importable without PySpark and without Fabric credentials.**
  PySpark, `azure-identity` and `mssql-python` are lazy imports confined to the
  modules that execute against those systems.
- **One error hierarchy.** Everything derives from `weaver.errors.WeaverError`,
  including CLI errors. Add a subclass when the operation that raises it lands.
- **The CLI owns no semantics.** It parses arguments and prints results. Command
  functions return plain serialisable structures.

Enforceable as the corresponding code lands:

- **Static discovery.** Discovery never imports object modules.
- **Objects never mutate the target.** `read()` proposes. Weaver owns mutation,
  CRUD accounting, staging and logging.
- **A runtime artefact is known by its role.** Planning reads `object_role` from
  the Registry row, or asks the repository what it claimed during a build where
  nothing is installed yet. A file or a stored procedure does not imply a load
  artefact: a Test compiles to a module and a procedure of its own, and a Test
  that inferred its way into the load DAG would be run by `weaver load`. See
  [validation](design/validation.md).
- **Validation declares. It does not materialise.** A Test and an Assumption
  carry an item's ordinary `Schema.Object` identity and are held apart from the
  documents an item materialises, so having an identity does not route one into
  table or view DDL.
- **Every target is named, not inherited.** No destination Lakehouse is assumed
  to be attached to the notebook, and that covers names as well as paths. A
  generated statement says which Lakehouse it means, as the native four-part
  `workspace.lakehouse.schema.object`, rendered when the bundle is generated. A
  bare `Schema.Object` resolves through whatever the session is attached to,
  which is ambient context. `[_].[Registry]` is two parts because a Warehouse
  connection reaches one database.

  One narrow exception, bounded by the same rule.
  `weaver.lakehouse.default_lakehouse` reads a notebook's own attachment, so a
  developer writing an object interactively does not have to resolve their one
  Lakehouse by hand. It converts the attachment into an explicit `Lakehouse`
  value at construction, and fails when there is nothing attached. From that
  point nothing is inherited. Two-part naming is permitted only for the Lakehouse
  that inference produced, where the session's catalogue is the destination.
  Every other `Lakehouse` carries a resolved destination or names no object.
- **Level-three identity is workspace + type + name.** An item name is unique per
  type, not across types: a Lakehouse and its generated SQL endpoint share a
  display name. Resolution is typed. The slot supplies the type, so a
  `DeltaTarget` is a Lakehouse and a `WarehouseTarget` a Warehouse, and core never
  asks the workspace what a bare name is. A destructive operation must not depend
  on name inference.
- **The central catalogue is authoritative.** No target-local catalogue, no
  target-local runtime, no target-local logging authority.
- **Certification is per object.** Before a rebuild, the selected objects and
  their descendants stop being certified. Each returns only after it builds.

## Retiring an abstraction

When a refactor introduces a first-class Weaver abstraction, the abstraction it
supersedes is absorbed or deleted in the same refactor. Do not leave parallel
environments, plans, runners, coordinators or execution paths standing beside
their replacements, unless an explicit migration boundary requires it and only
until that migration lands.

These are gone, and named here so a retirement stays retired:

```text
alias.yml and external.yml         InstallationEnvironment
Alias as a user-facing concept     LocalWorkspace
LoadEnvironment                    LocalResolver
LoadPlan as the runtime owner      FabricWorkspace (there is one Workspace)
ResolvedLoadPlan                   SparkNaming / SparkDestination
execute_load_plan orchestration    is_fabric
separate load/test engines         a per-position build
old/new action terminology         build_uploaded_item_repository
operation-local resource ownership update_catalogue / @update_catalogue
Bookmark-specific build plumbing   a bespoke write per runtime table
```

**Who records is the interface.** A lower execution primitive never writes
operational catalogue state. A run records centrally, and a standalone wrapper
records synchronously. In Python that is `_load()` against `load()`, and `read()`
against `run()`. In T-SQL it is `_.[Load X.Y]` and `_.[Test X.Y]` against `_.Load`
and `_.Test`. Nothing takes a parameter about it.

`tests/test_fabric_only_invariant.py`, `tests/test_public_api_invariant.py` and
`tests/test_remote_program_invariant.py` name them and fail if one comes back.

Temporary compatibility while intermediate commits land is fine. Obsolete
architecture left layered underneath the new architecture is not.

See [the code architecture](design/code-architecture.md) for what replaced them.

## Environment neutrality

Weaverstack contains no defaults for product, workspace, Lakehouse, Warehouse,
endpoint, repository or notebook names, no production endpoints and no local
platform paths. Allowed defaults are generic technical values: Fabric API URLs,
auth scopes, Livy version, timeouts, polling intervals, parallelism.

This covers examples, docstrings and test fixtures as well as code paths. Use
neutral item names such as `Sales`, `Inventory` and `Reporting`.

**One exception: the Fabric integration harness.** `tests/fabric` names a fixed
workspace and a fixed set of items (`PYTEST_WORKSPACE`, `PYTEST_WEAVER`,
`PYTEST_LH_*`, `PYTEST_WH_*`) instead of generating disposable ones. The rule
exists so no product behaviour depends on a name from one tenant. These names are
neither product behaviour nor tenant-specific, and every one is overridable by
environment variable, so another tenant runs the suite by exporting its own.

Fixed items remove variance. Creating an item is quick; what reuse removes is the
tail risk of an unbounded endpoint wait, which the harness tolerates ten minutes
for, and the artifact churn that makes Fabric's namespace resolver intermittently
report `Artifact not found` for an item that exists. The suite's cost is bundle
generate and install round trips through Livy.

Item lifecycle cover, creating and deleting Lakehouses, is marked `provision` and
opted into separately from ordinary Fabric work. It exercises Fabric's resource
management, changes rarely, and its create and delete churn would slow every run
of the code under development.

### One state transition, one evidence payload

A Livy call is an architectural decision. One submission costs seconds; the
statements inside it cost almost nothing. So:

> A remote state transition produces one evidence payload. Assertions stay local.

Gather every question about one moment into one body, submit it once, and assert
against what comes back:

```python
seen = env.observe(
    queries={"tables": "SHOW TABLES IN {{schema:DWG}}"},
    schemas={"dwg": "DWG", "weaver_dwg": ("DWG", env.weaver_destination)},
)
assert {"customer", "order"} <= seen.values("tables", "tableName")
assert not seen.schema("weaver_dwg")
```

instead of a call per question:

```python
assert env.query("SHOW TABLES IN ...")  # one round trip
assert env.schema_exists("DWG")  # another
assert not env.schema_exists("DWG", weaver)  # another
```

One payload is cheaper, and it is more accurate. Separate calls interrogate a
mutable remote estate at several instants, so "the estate after prune" becomes
several claims about several moments, and a later transition can make an earlier
assertion pass on state that no longer exists. Keep the payload on the step it
belongs to (`step.observation`) instead of re-reading later.

Split calls where the boundary between them is the subject: before against after
a build or refresh, a failure stopping later work, a repository mutated between
generation and installation, prune or wipe changing the estate. The protocol
tests in `test_livy_import_primitive.py` show both halves of that judgement.

The helpers live in `tests/support/observation.py`. Session telemetry reports the
real external crossings and elapsed time without imposing a call-count or time
budget.

### Test declarations

Every test function uses `@weaver_test(...)`. The declaration holds one scope and
the external resources the test's claim needs. It generates pytest markers for
selection. Managed markers are never written by hand.

```bash
pytest                        # pure Python, no JVM and no tenant
pytest -m "fabric and remote" # no published wheel needed
pytest -m "fabric and hosted" # needs the wheel published to the Environment
pytest -m full_integration    # composed lifecycle journeys
pytest -m provision           # Fabric item lifecycle
```

The scope is one of core, remote, hosted, integration, or provision. Integration
and provision need no additional position flag. Resources are a separate closed
vocabulary: `tds`, `livy`, `onelake`, `rest`.

Pytest compares declared resources exactly with claim-body events from the test's
registered Sessions. Fixture acquisition is reported separately, so the first TDS
capability may resolve an endpoint over REST without every TDS test declaring
REST. A repeated lookup for the same cached target is a defect.

Session telemetry carries Task, Step, and Sub-step attribution. Session-owned
asynchronous work captures that context when it is submitted and restores it when
the worker crosses the resource boundary.

A journey is the most expensive scope and should rarely be where a defect is
found first. Syntax, selection, planning, action rendering, execution and
reconciliation are all proven below it.

Isolation comes from emptying an item, not from having a new one. The cleaning
path is therefore load-bearing and asserted: residue is possible in a real
workspace in a way it never was on a fresh `tmp_path`.

Weaver has no opinion about data architecture. Folder, Delta and SQL are
materialisation forms, not tiers. `T0`/`T1`/`T2` naming is house jargon and is
rejected by `tests/test_neutrality_invariant.py`. Widely-understood naming such
as bronze/silver/gold is fine where it helps.

## Writing

See [CLAUDE.md](CLAUDE.md) for the register and the six rules that define it.
In short: describe, do not argue. State what a thing is, name the real components
(TDS, Livy, OneLake, a Delta commit, a 403), and stop. No em dashes, no emphasis,
no counterfactuals, no mental states ascribed to code.

CLI help says what a command or option does. Errors state the condition and, when
there is one, the next action. Detailed diagnostics belong in logs.

Comments explain a constraint, an invariant, platform behaviour, or an edge case.
Docstrings state a callable's purpose and its non-obvious contract. A function
docstring runs one to three sentences, a module or class docstring about ten
lines, a comment block three. Anything longer belongs in `design/`.

Design docs explain the system for maintainers. When behaviour changes, update
the relevant design document instead of adding a competing explanation elsewhere.

### Terminology

Use the established name for each public concept: Workspace, Environment, Weaver
catalogue, catalogue Warehouse, Lakehouse, Warehouse, target, logical target,
physical target, repository, catalogue, registry, session, composition, build,
load, test, assumption. Do not invent synonyms in UI text when a defined term
exists.

### GitHub publishing

Use the GitHub CLI for branch, push and pull-request work. Check `gh auth status`
before publishing. On Windows, if `gh` is not on `PATH`, use
`C:\Program Files\GitHub CLI\gh.exe`. Ask for re-authentication when its saved
token is invalid.

## Layout

```text
weaverstack/
├── pyproject.toml
├── AGENTS.md
├── src/
│   ├── weaver/       the core framework
│   └── weaver_cli/   the optional desktop CLI
└── tests/
```

## Dependencies

Base install is minimal. A dependency is declared when the feature that first
needs it lands. See the comment in `pyproject.toml`.

## Development

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest              # core only, no JVM and no tenant
.venv/bin/weaver --help
```

`pip install weaverstack` installs the CLI and the Fabric transports. It does not
install PySpark and needs no JDK. Fabric supplies Spark where authored runtime
code executes, and a desktop reaches Spark through the Session.
