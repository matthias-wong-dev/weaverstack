# Weaverstack Agent Guide

Guidance for coding agents working **on weaverstack itself**.

## Repository role

`weaverstack` is a data-engineering runtime for Microsoft Fabric built around a
central catalogue. The **Weaver catalogue** is Weaver's package-owned
operational metadata, under the `_` schema of a configured Fabric Warehouse —
which may be Weaver's own or one already holding a user's schemas, since Weaver
owns `_` there and nothing else. Destination Lakehouses and Warehouses hold only
materialised output.

The distribution is `weaverstack`; the import is `weaver`.

## The sibling `weaver` repository is reference-only

The repositories sit side by side:

```text
dwg-platform/
├── weaver/        reference implementation — DO NOT MODIFY
└── weaverstack/   this repository
```

`weaver` is consulted for proven algorithms, Fabric/OneLake/Spark/Warehouse edge
cases, Weaver document fixtures and behavioural intent. Never change it as part of
weaverstack work, and never import from it. Where the two disagree,
[design/weaver-architecture.md](design/weaver-architecture.md) is authoritative.

Reference baseline: `a97ba8a0b00dd66dff1b2c5e818403694562fd30` (the plan's
reviewed snapshot). The sibling checkout has since advanced; confirm which
revision you are reading before treating it as the baseline.

## Implementation authority

Two documents, and they answer different questions.
[design/weaver-architecture.md](design/weaver-architecture.md) is what Weaver
*is* — repository structure, documents, build, bundle, installation, the Weaver
Lakehouse, the command lifecycle. [design/code-architecture.md](design/code-architecture.md)
is how *this repository* is arranged to deliver it — the four doers, the
representations they hand each other, and where anything physical happens. Read
the first for behaviour, the second before moving code between layers.

The underlying system has run in production on SQL Server for years and
the sibling `weaver` implementation proved it works on Fabric. This is
implementation, not invention: port proven algorithms rather than re-deriving
them, and spend design attention on the control plane, which is the genuinely
new part.

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

Anything substantially more complicated than that needs a concrete reason to
exist. There is one workspace type, one build, one place a workspace is
resolved, and one conversion into the physical target vocabulary.

## The core abstraction

This is the thing that is hard to hold in your head, and the thing most likely
to be got wrong by someone reading only the code in front of them.

**Weaver is a system that runs inside Microsoft Fabric.** Fabric is its one
workspace. What varies is not where the resources are — they are always in
Fabric — but where Weaver's own code runs:

| | code runs | what it is |
|---|---|---|
| 1 | on a desktop | **the desktop position** — Weaver here, reaching in over Livy, TDS, OneLake and REST |
| 2 | **in Fabric** | **the in-Fabric position** — `pip install weaverstack` in a notebook |

Both are meant to be complete ways to work, not a fast loop and a real one.

**The foundational rule:** *an operation never works out where it is. It asks a
Session for a capability, and the Session knows.*

```text
Session in a notebook   → call it here: native Spark, notebookutils, TDS
Session on a desktop    → cross for it: Livy, OneLake over HTTPS, TDS, REST
```

`use_or_create_session` in `weaver.sessions.host` picks the host once, per
workspace. Above that, `build`, `load`, `test` and `wipe` are the same code in
both, which is the property the whole arrangement exists to keep.

The fast suite decides against a `TestSession`: a real implementation of the
Session contract that records what a host was asked to do and never interprets
it. It models no Spark, no Delta and no Fabric catalogue, so nothing it proves
can be true only of a fake.

A `Workspace` identifies the workspace the resources live in. It does **not**
say whether access happens through desktop HTTP clients or inside a session. A
build bundle binds target kind, item identifiers and the display names four-part
Spark naming is spelled with — never a discriminator for where Weaver is running.

So the storage picture has two parts, and they must not be conflated:

*In-session execution* — the store Weaver uses where it runs:

| execution | store |
|---|---|
| Fabric session | `FabricStore` over `notebookutils.fs` |

*Cross-boundary access* — a desktop caller reaching into a workspace:

| caller | destination | client |
|---|---|---|
| CLI | Fabric workspace | `OneLakeDfsClient` |
| Fabric integration tests | Fabric workspace | `OneLakeDfsClient` |

`OneLakeDfsClient` (ADLS Gen2 DFS over HTTPS) is how the desktop crosses in,
constructed explicitly by the caller that crosses. Inside Fabric, `store_for`
returns the session-native `FabricStore`; from a desktop that construction fails
rather than silently substituting DFS.

`FilesystemStore` is named for the transport it is: a build reads its repository
through one wherever it runs, because every incoming source tree is copied to a
temporary filesystem snapshot before it is parsed. That copy is unconditional —
see `_temp_copy` in `weaver.build_bundle.workflow`. A source already on this
filesystem is copied too, so a build never reads a tree the caller can still
edit underneath it.

Above resolution and the store, nothing knows which host it is running on. Code
that asks means the abstraction is being broken; the fix belongs in the
factories, or in the CLI that does the crossing.

**Credential choice is a caller's policy, not the core's.** Core accepts an
injected credential and otherwise uses the library default without pinning the
chain. The CLI and the Fabric test infrastructure call `prefer_cli_credential()`
themselves; importing or using the core imposes no credential choice.


### Fabric is the reference

**Weaver is Fabric-first.** The behaviour that must be right is the behaviour
*inside* Fabric. What the fast suite may do without a tenant is *decide* —
render, plan, reconcile — and what it must never do is model what Fabric would
answer.

Concretely, for anything with two phases (as the build bundle has *generate* then
*install*): **both phases decide against the target environment's real state.**
Inside Fabric that state is right there — the native Spark catalogue, in the
session. From a desktop it has to be *read across first*, and then planned
against, which is what `read_build_state` does before the Builder is handed
anything.

The invariant is about the *state*, not the location. A planner given the real
catalogue and the real inventories reaches the same bundle wherever its process
happens to be; a planner given `None` does not, and once did — bundle generation
that could not see catalogue views could not prune them. Planning blind is the
failure this guards against.

### Two positions, both first-class

A user can open a Fabric notebook, `pip install weaverstack`, and work. That is
the product, and it is what distinguishes Weaver from tools that demand an
orchestration environment of their own.

The other half is **everything driveable from a desktop**, with Fabric reached
through Livy, TDS, OneLake and REST, each crossing carrying a small clear script
rather than an operation. Not two products — one, in two positions, because the
doers do not know which one they are in.

There is one `build`, one `load` and one `test`. Every build action runs in the
`Installer` wherever that is, and the state a build plans against is read the
same way — the catalogue is TDS, a Lakehouse's views are Spark SQL, a
Lakehouse's objects are storage, a Warehouse is TDS. So a desktop `weaver build`
needs no published wheel: nothing it submits imports Weaver.

Because the catalogue is a Warehouse, a Warehouse-only workflow performs **zero
Livy submissions**. Catalogue reads, publication and `_.Log` writes must never be
the reason a Spark session starts.

What crosses as a program is a run's Python primitives, which are deployed
modules imported where Spark is. `weaver load` therefore requires the published
wheel.

**A Fabric test that runs Weaver on the laptop tests the desktop position, not
the in-Fabric one** — that is what the `remote` and `hosted` markers are for, and
why a capability is not proven until both are green.

Both positions are delivered by installing Weaver into a Fabric Environment:
`weaver install --workspace <ws> --environment <env>` builds a wheel from the checkout,
stages it and Weaver's dependencies, and publishes. A Livy session (and a Fabric
notebook) then attaches that Environment via `environment` on the workspace and
imports the installed package — nothing is copied into the workspace. Rerun
`weaver install` whenever Weaver Python changes; an unchanged source tree builds
the same version and the install skips the republish.

### What this means when you add a feature

Ask, in order:

1. Can what it *decides* be tested without a tenant, against a `TestSession`?
2. Does it work against a Fabric workspace from the desktop?
3. Does it work with Weaver *running inside* Fabric?

Answer all three, and answer them with tests that call the real function —
not with test code that reproduces what the function would have done. That
mistake has already been made once here: the first Fabric suite deleted files
through the store directly and looked like it was testing `wipe`.

## Architecture invariants

These are enforced by `tests/test_core_boundary.py`:

- **Core never imports the CLI.** `weaver_cli` parses arguments and prints;
  a core import of it would put a desktop concern inside the package a Fabric
  Environment runs. The dependency goes one way, CLI → core.
- **The core is importable without PySpark and without Fabric credentials.**
  PySpark, `azure-identity` and `mssql-python` are lazy imports confined to the
  modules that execute against those systems.
- **One error hierarchy.** Everything derives from `weaver.errors.WeaverError`,
  including CLI errors. Add a subclass when the operation that raises it lands.
- **The CLI owns no semantics.** It parses arguments and prints results. Command
  functions return plain serialisable structures.

These become enforceable as the corresponding code lands:

- **Static discovery.** Discovery never imports object modules.
- **Objects never mutate the target.** `read()` proposes; Weaver owns mutation,
  CRUD accounting, staging and logging.
- **A runtime artefact is known by its role, not its shape.** A file or a stored
  procedure used to mean "load artefact" because a load layer installed the only
  files and procedures there were. A Test compiles to a module and a procedure of
  its own, so planning reads `object_role` from the Registry row — or asks the
  repository what it claimed, during a build where nothing is installed yet. A
  Test that inferred its way into the load DAG would be run by `weaver load`. See
  [validation](design/validation.md).
- **Validation declares; it does not materialise.** A Test and an Assumption
  carry an item's ordinary `Schema.Object` identity and are held apart from the
  documents an item materialises, so nothing routes one into table or view DDL on
  the strength of it having an identity.
- **Every target is named, not inherited.** No destination Lakehouse is assumed to
  be attached to the notebook, and that covers *names* as well as paths: a
  generated statement says which Lakehouse it means, as the native four-part
  `workspace.lakehouse.schema.object`, rendered when the bundle is generated. A
  bare `Schema.Object` resolves through whatever the session happens to be
  attached to, so it is the ambient-context anti-pattern in disguise. The
  catalogue is the one exception and only because it is not one: `[_].[Registry]`
  is two parts because a Warehouse connection reaches one database.

  There is one narrow exception, and it is bounded by the same rule.
  `weaver.lakehouse.default_lakehouse` reads a notebook's *own* attachment, so a
  developer writing an object interactively does not have to resolve their one
  Lakehouse by hand. It converts the attachment into an explicit `Lakehouse`
  value at construction, and fails when there is nothing attached rather than
  guessing; from that point on nothing is inherited. Two-part naming is
  permitted only for the Lakehouse that inference produced, because there the
  session's catalogue *is* the destination. Every other `Lakehouse` carries a
  resolved destination or refuses to name an object at all.
- **Level-three identity is workspace + type + name.** An item name is unique per
  *type*, not across types — a Lakehouse and its generated SQL endpoint share a
  display name. Resolution is typed: the slot supplies the type (a `DeltaTarget`
  is a Lakehouse, a `WarehouseTarget` a Warehouse), so core never asks the
  workspace what a bare name "is". A destructive operation must not depend on
  name inference.
- **The central catalogue is authoritative.** No target-local catalogue, no
  target-local runtime, no target-local logging authority.
- **Certification is per object.** Before a rebuild, the selected objects and
  their descendants stop being certified; each returns only after it builds.

## Replacing an abstraction removes the one it replaces

When a refactor introduces a first-class Weaver abstraction, the abstraction it
supersedes is absorbed or deleted **in the same refactor**. Do not leave parallel
environments, plans, runners, coordinators or execution paths standing beside
their replacements unless an explicit, temporary migration boundary requires it —
and then only until that migration lands.

These are gone rather than deprecated, and named here so a retirement stays
retired once nobody remembers why the name was a problem:

```text
InstallationEnvironment            LocalWorkspace
LoadEnvironment                    LocalResolver
LoadPlan as the runtime owner      FabricWorkspace (there is one Workspace)
ResolvedLoadPlan                   SparkNaming / SparkDestination
execute_load_plan orchestration    is_fabric
separate load/test engines         a per-position build
old/new action terminology         build_uploaded_item_repository
operation-local resource ownership
```

`tests/test_fabric_only_invariant.py`, `tests/test_public_api.py` and
`tests/test_remote_program_invariant.py` name them and fail if one comes back.

Temporary compatibility while intermediate commits land is fine. Obsolete
architecture left layered underneath the new architecture is not.

See [the code architecture](design/code-architecture.md) for what replaced them.

## Environment neutrality

Weaverstack must contain no defaults for product, workspace, Lakehouse,
Warehouse, endpoint, repository or notebook names, no production endpoints and
no local platform paths. Allowed defaults are generic technical values (Fabric
API URLs, auth scopes, Livy version, timeouts, polling intervals, parallelism).

This covers **examples, docstrings and test fixtures**, not just code paths. Use
neutral item names — `Sales`, `Inventory`, `Reporting`.

**One deliberate exception: the Fabric integration harness.** `tests/fabric`
names a fixed workspace and a fixed set of items (`PYTEST_WORKSPACE`,
`PYTEST_WEAVER`, `PYTEST_LH_*`, `PYTEST_WH_*`) rather than generating disposable
ones. The rule exists so no *product* behaviour depends on a name from one
tenant; these names are neither product behaviour nor tenant-specific, and every
one is overridable by environment variable, so another tenant runs the suite by
exporting its own.

Fixed items remove *variance* rather than time. Creating an item is quick; what
reuse removes is the tail risk — an endpoint wait that is unbounded, and which
the harness tolerates ten minutes for — and the artifact churn that makes
Fabric's namespace resolver intermittently report `Artifact not found` for an
item that demonstrably exists. The suite's cost is bundle generate/install
round trips through Livy.

Item *lifecycle* cover — creating and deleting Lakehouses — is marked
`provision` and opted into separately from ordinary Fabric work. It exercises Fabric's
resource management rather than Weaver's, changes rarely, and its create/delete
churn would otherwise slow every run of the code actually under development.

### One state transition, one evidence payload

Because the suite's cost is Livy round trips, **a Livy call is an architectural
decision, not an implementation detail.** One submission costs seconds; the
statements inside it cost almost nothing. So the rule is:

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

rather than a call per question:

```python
assert env.query("SHOW TABLES IN ...")  # one round trip
assert env.schema_exists("DWG")  # another
assert not env.schema_exists("DWG", weaver)  # another
```

This is cheaper, but the reason it is *better* is accuracy. Separate calls
interrogate a **mutable remote estate** at several instants, so "the estate after
prune" becomes several claims about several moments — and a later transition can
make an earlier assertion pass on state that no longer exists. One payload is one
observation of one moment, which is what the assertion says it is. Keep it on the
step it belongs to (`step.observation`) rather than re-reading later.

Split calls only where the *boundary between them* is the subject: before versus
after a build or refresh, a failure stopping later work, a repository mutated
between generation and installation, prune or wipe changing the estate. The
protocol tests in `test_livy_import.py` show both halves of that judgement.

The helpers live in `tests/support/observation.py`. Nothing hides the call: every
submission is counted by `tests/fabric/livy_telemetry.py` and pytest prints a
breakdown at the end of the run.

```text
================================ Livy transport ================================
Livy calls: 31
Livy elapsed: 124.8s (plus 62.0s session startup)

By phase:
  generate and install: 12 calls / 84.1s
  observe install: 1 calls / 4.2s
...
```

No test asserts a call count. A number that has to be edited whenever a probe
legitimately changes teaches the suite to raise the budget rather than ask why;
the summary already puts a regression in front of whoever caused it.

### Markers

Each marker is opted into by name, and none implies another.

```bash
pytest                      # pure Python, no JVM and no tenant
pytest -m fabric            # every test against a real Fabric workspace
pytest -m "fabric and remote" # no published wheel needed
pytest -m "fabric and hosted" # needs the wheel published to the Environment
pytest -m full_integration  # the lifecycle journeys, one per position
pytest -m provision         # Fabric item lifecycle
```

Every marker says *what a test needs*:

| marker | needs |
|---|---|
| `fabric` | a workspace; carried by every Fabric test |
| `remote` | a workspace, and no published wheel |
| `hosted` | a workspace **and** the wheel published to the Environment |
| `full_integration` | a composed lifecycle journey |
| `provision` | creates and deletes Fabric items |

`remote` and `hosted` are the distinction that keeps the loop legible, and they
say whether a published wheel is required. Not whether Livy is involved, and not
where the orchestration runs: a decomposed desktop operation orchestrates here
*and* imports the wheel on the far side, so it is `hosted`. A Spark body that
does not import Weaver needs a session, not a published package, which is why
starting a Livy session asserts neither: `LivySession.ensure_weaver` is called
by the crossing that submits a program, and by nothing else. Creating a shortcut,
refreshing an endpoint and wiping a Lakehouse are all REST or storage, so they
run from the checkout against the real workspace and stay `remote`.

Position is worth recording, but it belongs in a test's docstring. A marker says
what a run costs, and the cost of `hosted` is a five-minute publish.

`full_integration` is the lifecycle journeys alone — one per position, since
composing is a claim about a position and not a claim a position can borrow.
A journey is the most expensive thing in the suite and should **rarely be where
a defect is found for the first time**: syntax, selection, planning, action
rendering, execution and reconciliation are all meant to be proven below it.
Making them run by exception keeps the routine Fabric run about components.

Isolation therefore comes from **emptying** an item rather than from having a new
one. That is not a weaker guarantee, but it is a different one, so the cleaning
path is load-bearing and asserted rather than assumed: residue is possible in a
real workspace in a way it never was on a fresh `tmp_path`.

Weaver also has no opinion about data architecture: Folder, Delta and SQL are
materialisation forms, not tiers. `T0`/`T1`/`T2` naming is house jargon and is
rejected by `tests/test_neutrality.py`; widely-understood naming such as
bronze/silver/gold is fine where it aids a reader.

## Writing

### User-facing text

User-facing text is product copy. Write neutral, respectful, ordinary technical
English. State what happened and, when known, what the developer can do next.

Do not lecture, scold, argue, joke or write as though correcting the developer.
Avoid describing something as obvious, simple, merely or the whole point. Avoid
“this is not X; it is Y” unless the distinction is necessary.

CLI help says what a command or option does; it does not explain implementation
rationale. Errors should be short enough to scan and specific enough to act on.
Detailed diagnostics belong in logs.

### Comments and docstrings

Comments explain non-obvious constraints, invariants, platform behaviour, edge
cases, or why an apparently simpler implementation is unsafe. Do not narrate
clear code, defend an implementation to an imagined reviewer, or put
architecture essays or implementation history in comments.

Prefer one plain sentence. Use two when the consequence matters. If an
explanation needs a paragraph, consider putting it in `design/`.

Docstrings describe a callable's purpose and non-obvious contract. They do not
contain design essays or implementation history.

### Design documentation

Design docs explain the system for maintainers. Prefer direct technical
explanation over rhetorical argument. When behaviour changes, update the
relevant design document rather than adding a competing explanation elsewhere.

### Terminology

Use the established name for each public concept. In particular: Workspace,
Environment, Weaver catalogue, catalogue Warehouse, Lakehouse, Warehouse,
target, logical target, physical target, repository, catalogue, registry,
session, composition, build, load, test and assumption. Do not invent synonyms in UI text when a defined term
already exists.

### GitHub publishing

Use GitHub CLI for branch, push and pull-request work. Check `gh auth status`
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

Base install is deliberately minimal. A dependency is declared when the feature
that first needs it lands, not in advance. See the comment in `pyproject.toml`.

## Development

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest              # core only — no JVM, no tenant
.venv/bin/weaver --help
```

`pip install weaverstack` installs the CLI and the Fabric transports. It does
not install PySpark and needs no JDK: Fabric supplies Spark where authored
runtime code executes, and a desktop reaches Spark through the Session.


