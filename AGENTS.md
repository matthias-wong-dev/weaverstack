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
of weaverstack work, and never import from it. Public behaviour is documented
at [docs.weaverstack.dev](https://docs.weaverstack.dev); this repository's
source and tests are authoritative for its implementation.

Reference baseline: `a97ba8a0b00dd66dff1b2c5e818403694562fd30`, the plan's
reviewed snapshot. The sibling checkout has since advanced. Confirm which
revision you are reading before treating it as the baseline.

## Implementation authority

[The public documentation](https://docs.weaverstack.dev) owns supported product
behaviour: Weaver documents, operations, catalogue state and command lifecycle.
This file names the repository's implementation boundaries and invariants. The
source and tests prove how those boundaries are delivered; keep them aligned
when moving code between layers.

The underlying system has run in production on SQL Server for years, and the
sibling `weaver` implementation works on Fabric. Port the proven algorithms.
Spend design attention on the control plane, which is the new part.

## The shape

```text
Workspace       one Fabric workspace configuration

Session         ConsoleSession   desktop → Fabric
                NotebookSession  already in Fabric
                TestSession      records the same contract

initialise      resolve request → read the workspace's items → create the
                missing ones → write the project → optionally publish the Environment

build           resolve request → read BuildState → Builder → MutationExecutor
load / test     resolve request → read RunState   → Runner
health          resolve request → read Catalogue  → HealthReport
doctor          authenticate → list workspaces → discover items → probe OneLake, TDS and Spark

Fabric          Resolver, REST, OneLake, Livy, TDS
```

There is one workspace type, one build, one place a workspace is resolved, one
conversion into the physical target vocabulary, one implementation of graph
mechanics, and one installed graph. Anything more complicated needs a concrete
reason.

`initialise` is the only operation that creates a Fabric item. A build's
preflight reads and never creates, so the two do not overlap. A command naming
no workspace and inheriting none reads `workspace-config.yml` in the directory
it was run from, which is the last resort in `weaver.config.resolve_workspace`.

`weaver.graph.Graph` is the topology. The authored repository graphs, the
installed estate graph and the runtime graph each carry their own node metadata
and hand ordering, layers, ancestry and subgraphs to it. `Catalogue.dag()`
derives the installed managed graph from catalogue rows already in memory, and
it is the one place a persisted `dependency_reference` is interpreted. Load
planning, validation planning and health read it.

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
and otherwise uses the library default without pinning the chain. Importing or
using the core imposes no credential choice.

The desktop CLI installs one for its process through
`weaver.fabric.auth.use_credential`, so it reaches the clients an operation
constructs for itself as well as the ones it is handed. What it installs is
`desktop_credential()`: the Azure CLI where it can issue a token, and browser
sign-in where it cannot, as one `ChainedTokenCredential` built once per process.
Reusing a browser sign-in in a later process needs the platform's secure token
cache and the `AuthenticationRecord` naming the cached account, so `BrowserSignIn`
keeps the second in `~/.weaver/authentication-record.json`. An unencrypted cache
is never asked for. The Fabric suite installs the same `desktop_credential()`
chain, so a sign-in performed by `weaver doctor` runs it.

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
mutation executor wherever that is, and the state a build plans against is read the same
way: the catalogue over TDS, a Lakehouse's views over Spark SQL, a Lakehouse's
objects from storage, a Warehouse over TDS. A desktop `weaver build` therefore
needs no published wheel, because its Spark SQL and TableBuilder submissions
import no Weaver, and no Fabric Environment either, because they run on the
workspace default. `load` and `test` ask for `--environment`. `install` asks
for nothing: a bundle carries the workspace, the catalogue, the Environment and
the Lakehouse a Spark session attaches to, frozen when it was generated, and the
Session that installs it supplies credentials and transport and no decision.

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

That is the product. The pytest suite reaches the same place by another route:
it builds one wheel from the checkout, stages it in `PYTEST_STAGING` and puts it
on the Livy session's `sys.path`, so a Python change reaches a hosted test
without a publish. `WEAVER_PYTEST_INJECT_WEAVER=0` runs the published wheel
instead, which is what holds the product route to account.

### What this means when you add a feature

Ask, in order:

1. Can what it decides be tested without a tenant, against a `TestSession`?
2. Does it work against a Fabric workspace from the desktop?
3. Does it work with Weaver running inside Fabric?

Answer all three with tests that call the real function. Test code that
reproduces what the function would have done proves nothing: the first Fabric
suite deleted files through the store directly and looked like it was testing
`wipe`.

## Archived Lakehouse installation

Build emits a format-5 `MutationPlan` and optional payload bytes. Build selection,
omissions, repository identity, runtime state and target changes are metadata in
the plan's frozen Build envelope. The directory codec rejects older bundles with
regeneration guidance. `BuildBundle` stores an artifact; execution consumes its
plan and payload bytes through `Session.execute_mutation`.

The Session owns execution routing. `ConsoleSession` executes a plan with no
frozen Spark attachment through the shared native executor, reaching TDS,
OneLake and REST from the desktop, so it starts no Spark session. A plan that
attaches Spark uses `execute_mutation_remote(plan, payloads=None)`, which submits
the whole plan once to Fabric. Native and remote execution both use
`MutationExecutor` and the existing physical executors.

The internal carrier contains the canonical plan, optional payloads and matching
Weaver runtime sources and static resources. It validates all payload hashes
before staging. The carrier and every member have SHA-256 identities. Extraction
validates inventory, paths, file kinds and content before creating the private
tree. The expanded-size bound is 128 MiB; an oversized carrier is refused before
submission. The generated bootstrap imports the extracted runtime under a
process-shared namespace lock, drains execution, and restores borrowed modules
and import paths.

The carrier is staged in the plan's Spark-home Lakehouse, under
`Files/_weaver_carriers/<invocation-id>/`, so a Lakehouse plan needs no other
item. That area is transport, not a plan target: it is outside plan identity and
physical scopes, and prune does not inventory it. The bootstrap copies the
carrier into private storage before any action runs. Every invocation removes its
directory when it returns, whatever the outcome, and removes the area once no
other carrier is in it.

Runtime dependencies and the pinned Delta writer are checked before mutation.
The archive retains direct-Delta creation, supported Views, catalogue settlement
and load-artifact installation. Build does not run loads or validations.
One Livy mutation submission has retries disabled and an allowance equal to the
action count times the statement timeout. Its final result is a byte count and
hash for the complete invocation report stored in OneLake. The desktop validates
the result identity and full action inventory before producing the Build report.

An ambiguous submission or lost result marks the invocation uncertain, and it is
never replayed. The next ordinary Build reads the actual catalogue and physical
inventory and converges. A carrier cleanup failure is recorded on the invocation
and does not change its report.

## Physical mutation contract

Every format-5 action supplies `depends_on` and `settle_after`, including empty
lists for roots. Sequence and batch nesting collect the actions; sequences also
provide presentation grouping. The planner freezes both edge sets, and execution
uses those links.

`depends_on` requires successful predecessors. `settle_after` imposes order after
a known terminal outcome, including failure or dependency blocking. Pending and
uncertain outcomes do not settle an ordering edge, and unsuccessful actions
provide no success evidence. Typed results, certification and required completion
follow success paths. The union of both edge sets must be acyclic.

Build stages and item layers order presentation only. Each planner declares,
per action, the keys it provides and the keys it `requires` or `follows`
(`weaver.build_bundle.dependencies`), and `enumerate_stages` compiles them into
`depends_on` and `settle_after`. A key nothing in the plan provides is already
satisfied by the target and adds no edge. The edges are the real physical
dependencies:

```text
decertify → reset runtime state → every physical root
schema ─→ table ─→ dependent view          drop consumer ─→ drop producer ─→ rebuild
shortcut create ─→ readiness ─→ consumer   source object ─→ shortcut create
Lakehouse mutations ··→ refresh start ─→ refresh await ─→ listed ─→ endpoint readers
folder ─→ runtime file                     object ─→ Warehouse procedure
every physical success sink ─→ physical gate ─→ catalogue publication ─→ Registry
```

`··→` is `settle_after`: a refresh reflects whatever the mutations left. A
completed refresh does not mean the endpoint lists a new table yet, so a
Warehouse waits until it lists every Lakehouse table it reads.
Publication certifies objects, not endpoint metadata, so the physical gate
excludes refreshes and publication runs beside them. The Build completes only
once every refresh it started is current, so the next operation reads a current
endpoint. A known
failure blocks only its dependents; independent branches continue, and
publication, which needs every physical success, does not run. The final gate
over every success sink is the required completion.

Platform limits are resources, not edges. An action names the capability it
occupies: `warehouse:<item>` for TDS, `spark` for Spark SQL and table creation,
`onelake:<item>` for storage, `shortcuts:<item>` for the shortcut API. The
Workspace's `execution.build` sets each capability's limit
(`warehouse_concurrency` per Warehouse, `spark_concurrency`,
`onelake_concurrency`, `shortcut_concurrency`) for Build, Wipe and Mirror, and
`weaver.sessions.archive_runtime.execution_capacity` applies it. The defaults
suit a mid-sized capacity; an F64 sustains twice as much. The limits travel
with an invocation, outside plan identity. They throttle execution only; an
ordering the plan needs is an edge, never a low limit. Each Warehouse lane
leases its own pooled connection, and four concurrent DDL lanes ran without
conflict in Fabric. Ready T-SQL actions on one Warehouse share round trips
spread across its free lanes, each action in its own `TRY`/`CATCH` with its own
outcome; Fabric refuses `SET XACT_ABORT`. Waiting work holds no resource.
`spark_table` actions with authored setup share an exclusion, because their
temporary views are session-scoped. The identifier-case scope is shared by
concurrent statements in one mode and exclusive between modes.

Slow Fabric convergence yields. Shortcut creation submits once and returns
`Waiting` while a source is still reaching OneLake; readiness and name release
are polled the same way, and the SQL endpoint refresh is a typed start/await
operation. A waiting state is plain data, because the invocation ledger records
it. `execute_install_action`, which runs one action alone, resumes it in place.

A `MutationPlan` owns tuples and recursively frozen envelope mappings. Construction
and decoding use the same structural validation and `weaver.graph.Graph`.
Both edge sets, typed result references, required completion, resource exclusions,
write scopes and protected scopes participate in canonical identity. Edge order
is canonical. A nonempty `bundle_id` must match the plan's computed identity at
shared validation. Empty identity is allowed for drafting; bundle validation
requires a sealed identity, and the writer seals drafts before writing. The codec
retains `plan.yml`, `payload/`, binary bytes, SHA-256 checks and manifest-last writes.

`DriverContract` declares an extension's payload and result types and whether it
starts or settles an asynchronous operation. Validation requires causal typed
references and successful settlement before declared certification and required
completion. `PhysicalScope.path` is canonical target-relative intent. Shared
validation rejects whitespace padding in any path component, unsafe relative paths
and padded physical item/workspace IDs before scope comparisons; execution does
not repair them.
The empty scope path covers the whole target. `PhysicalScope` comparisons use item
kind, item ID and effective workspace ID across manifest aliases. A target's
explicit workspace ID takes
precedence; a target with no workspace descriptor uses the execution workspace ID.
Workspace display names provide no physical identity. When either workspace is
unresolved, equal kind/item IDs are potentially overlapping. Destructive writes
must respect protected scopes, and overlapping writers need enforced ordering or
a common exclusion. Different known workspace IDs, item IDs, kinds or disjoint
paths retain distinct scopes.

The internal `MutationExecutor` validates sealed plans and all payload bytes before
physical driver preflight and admission. `execute` accepts a `MutationPlan` and
explicit payload bytes. Build
callers load the plan and payloads from their bundle before execution. Wipe and
Mirror callers supply their planner's plan. Persistence and transport stay with
the caller. The transitional `physical_driver` binds the existing Build executors.
The scheduler uses the frozen edge sets through stable ready counts and
bounded runtime lanes. Pending continuations release workers and execution
permits. Each action owns its declared exclusions through Pending and releases
them on known completion. Operation leases remain attached to acknowledged
starters until settlement. Settlers can access their operation's leases while
their own exclusions serialize shared writers. Uncertainty retains the affected
leases. Typed runtime drivers match the frozen
contracts. Bound physical adapters call the existing executors through supplied
contexts and capability requirements; a shared connection, Session or inner pool
that no per-action resource owns takes one driver lane.

Reports retain action-keyed success, known failure, dependency blocking,
not-dispatched and uncertain outcomes in frozen action order. A refused request
is a known failure. A lost response after a request may have been sent raises
`weaver.errors.OutcomeUnknown`, and its action is uncertain: the mutation may
have been applied, so it settles nothing, and the next Build reconciles it. Independent work
continues by default; fail-fast and cancellation stop new admissions and drain
running work. Idle waits observe cancellation through event interruption on the
default clock or bounded sleeps on supplied clocks. Blocking driver calls must
drain through their own contracts. Supported cancellation requires a
driver-confirmed outcome. A valid known failure retains its error and settlement
evidence after deadline expiry; late success cannot certify completion.
The report's ledger is in-memory evidence for one invocation. Runtime clocks, handles and invocation IDs remain outside
plan identity. Catalogue-free physical plans bind a Workspace without a catalogue.

Real Fabric qualification covers desktop plan execution, binary payload delivery,
physical actions and failures, lost-response uncertainty without replay, and
ordinary Build convergence. Local executor and bootstrap tests establish their
own boundaries, not Fabric readiness.

## Load scheduling

A load's `Runner` dispatches every node whose upstream has settled, within
`Lanes`: four Warehouse procedures per Warehouse, and four Python primitives.
Python primitives that start together go to the host together
(`dispatch_python_many`), and each runs in a Spark session of its own within
the one Spark application, so settings and temporary views stay its own. From
the desktop they cross as one Livy statement, because a Livy session runs its
statements one at a time. Every node is decided as a serial run decides it, in
graph order once its upstream has settled, and every settlement and catalogue
write happens in the thread running the run. Without fault tolerance a failure
starts nothing more: running nodes finish and settle, and nodes not yet started
stay pending. Two concurrent load commands are separate writers of the same
catalogue tables, which a Warehouse can refuse as an update conflict. A test run
schedules the same way: Warehouse validations take Warehouse lanes, and Lakehouse
validations that start together go to the host together
(`dispatch_validations_many`). A finding never stops the rest.

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
  the [validation reference](https://docs.weaverstack.dev/reference/weaver-documents/test/).
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
- **Delta protocol minima are creation policy.** Lakehouse Table declarations
  accept `Delta minReaderVersion` and `Delta minWriterVersion`. Authored values
  participate in declaration signatures. Omitted values resolve to Weaver's
  current defaults and are frozen into creation payloads. A default-only upgrade
  leaves unchanged installed Tables on their existing protocol. Both direct and
  Spark creation use reader 3/writer 7 by default; schema features are per Table.
  Explicit minima may be raised by features required by that Table. The direct
  profile verifies the committed protocol, schema and properties before publishing.
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
Installer(workspace=...)           weaver install --workspace
the `provision` test scope          disposable-Lakehouse fixtures
ROLE_ENTRY and entry artefacts     generate_load_entry / generate_test_entry
planned_shortcuts                  _with_runtime_references
per-part per-item declaration      generated_item_files
  indexes in RepositoryPart
InstalledEstate                    InstalledObject / InstalledDependency
_validation_dependencies           InstalledValidation.dependencies
weaver.declaration.graph           a per-model topological sort
stale_shortcut_destinations        delete-then-create shortcut replacement
environment_packages               resolve_wheel_closure / plan_requirements
Weaver's own dependency resolver   EnvironmentPackageConflict
SUPPORTED_FABRIC_RUNTIMES          per-runtime wheel ABI selection
initialise --no-input              a wipe dry run as its own preflight
a per-command interaction check    workspace-config as the wiped estate
weaver.test(strict=True)           format-4 bundles / compile_legacy_build
the public Installer               partial-batch archive routing
DurableJournal / MutationJournal   checkpoint recovery and receipts
ArchiveStaging / select_staging    a separate carrier Lakehouse
procedural Wipe and Mirror runs    create_onelake_shortcuts / await_addressable
per-batch settlement chains        the native-session lane
```

The `provision` scope went when the suite moved to fixed items. Standing the
estate up is `tests/fabric/provision_estate.py`, run by hand, and no test creates
or deletes an item.

Interaction is one CLI-wide policy. `--non-interactive` is the only spelling,
`weaver_cli.interaction` is the only place a terminal, a keypress or a
confirmation is read, and `--yes` grants authorisation and nothing else.

A wipe plans before it acts. `plan_wipe` settles the estate and the catalogue
disposition, `wipe_mutation_plan` freezes each target's destructive scope into a
MutationPlan, and `wipe` executes it through the Session. The physical mechanics
in `physical_wipe` know nothing about authorisation or estate discovery; they
enumerate inside the frozen scope when an action runs. A Warehouse is one
dynamic-SQL action. A Lakehouse area detaches its shortcuts, waits for OneLake to
release their paths, and is swept only after a successful detach. Targets are
independent; a removed catalogue or an unbind follows them all. The estate an
unscoped wipe empties is what `_.Installation` records. Where the workspace
configuration has `targets:`, every recorded installation must be bound there to
the same physical item, or the wipe refuses and asks for named targets. Named
targets are emptied exactly as named.

A mirror plans before it acts too. `check_mirror` proves the source and refuses
unsafe destinations, then `mirror_mutation_plan` reads what the mirror needs,
source code definitions, case-exact source paths and the deployed load tree,
and compiles one plan: the destination catalogue is emptied and built against its
known-empty state, while each item's destination is emptied and reconstructed,
and each reconstructed Lakehouse's SQL endpoint is refreshed before a Warehouse
reads through it. A refreshed endpoint can still be listing new shortcut
tables, so the Warehouse waits until it sees every object it reads. The fork, the record of what each item borrows and each item's
binding are one transaction, last, after every reconstruction. Until it commits
the destination catalogue records no installation, so it never claims the
source's items.

One disposition, one meaning. `REMOVE` takes the catalogue last, `UNBIND` keeps
it and deletes its claims for the targets emptied and is never handed it as a
target, `LEAVE` is the absence of one and is refused over a catalogue that
resolved, and `PHYSICAL_ONLY` empties exactly what it is named and reads no
catalogue. A command line reaches the first two. `PHYSICAL_ONLY` is internal,
for an operation emptying one physical item, and mirror is its caller.
`WipePlan.describe()` reads the target list and the claims, so its catalogue
line says what execution does.

`wipe` takes a settled plan or the arguments to build one. A planning argument
beside `plan=` is refused, because a wipe is destructive and an argument that
reads as changing the plan would change nothing. `session` and `dry_run` say how
an execution runs and travel with either.

`_.Load` and `_.Test` are checked-in `.sql` under `src/weaver/fragments/`, read by
`read_repository_fragment` like the catalogue declaration and the standard
per-item schema and folder documents. Static Weaver-owned repository content is a
fragment; nothing renders it from Python.

**Who records is the interface.** A lower execution primitive never writes
operational catalogue state. A run records centrally, and a standalone wrapper
records synchronously. In Python that is `_load()` against `load()`, and `read()`
against `run()`. In T-SQL it is `_.[Load X.Y]` and `_.[Test X.Y]` against `_.Load`
and `_.Test`. Nothing takes a parameter about it.

`tests/test_fabric_only_invariant.py`, `tests/test_public_api_invariant.py` and
`tests/test_remote_program_invariant.py` name them and fail if one comes back.

Temporary compatibility while intermediate commits land is fine. Obsolete
architecture left layered underneath the new architecture is not.

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

Nothing in the suite creates or deletes a Fabric item.
`tests/fabric/provision_estate.py` does that, run by hand to stand the estate up
on a new tenant. It reuses what is already there and deletes nothing.

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
pytest -m "fabric and hosted" # injects checkout wheel; no publish needed
pytest -m full_integration    # injects checkout wheel; no publish needed
```

The routine Fabric run is `pytest -m "fabric and not full_integration"`. The
release run is `pytest -m fabric --runslow`: it adds the acceptance journey,
which is most of the suite's Fabric time, and the `slow` tests, whose claims a
routine test covers closely but not exactly. Run it before a release and when a
change alters how build, load, test and wipe compose.

The scope is one of core, remote, hosted or integration. Integration needs no
additional position flag. Resources are a separate closed vocabulary: `tds`,
`livy`, `onelake`, `rest`.

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

Read [PROSE.md](PROSE.md) before changing user-facing text, documentation,
docstrings or source comments. It is the repository's source of truth for prose.

### Terminology

Use the established name for each public concept: Workspace, Environment, Weaver
catalogue, catalogue Warehouse, Lakehouse, Warehouse, target, logical target,
physical target, project, project folder, source, repository, catalogue,
registry, session, workflow, build, load, test, assumption. Do not invent
synonyms in UI text when a defined term exists.

A project is what a user authors. `project_folder` is the local directory
holding it, and user-facing text calls that a project folder. `source` is a
build's input, which may be a folder or an `abfss` location. `repository` and
`WeaverRepository` are the parsed authored model. Git repository means Git.

### GitHub publishing

Use the GitHub CLI for branch, push and pull-request work. Check `gh auth status`
before publishing. On Windows, if `gh` is not on `PATH`, use
`C:\Program Files\GitHub CLI\gh.exe`. Ask for re-authentication when its saved
token is invalid.

## Layout

```text
weaverstack/
├── pyproject.toml
├── VERSION           the one authored version
├── AGENTS.md
├── src/
│   ├── weaver/       the core framework
│   └── weaver_cli/   the optional desktop CLI
├── tools/            release and website generators
└── tests/
```

## Versioning

`VERSION` holds the release line and is the only authored version in the
repository. A build derives the wheel version from it: an ordinary checkout
gets `0.9.0.dev<fingerprint>`, and a clean checkout tagged `v0.9.0` gets
`0.9.0`. A tag on a checkout whose `VERSION` says something else is a hard
build failure, so a tag can never move the release line.

Releasing is one command, and it publishes nothing itself:

```bash
python tools/release.py
```

It tags `v<VERSION>` and pushes. Pushing that tag is the release event, and
GitHub Actions checks the tag against `VERSION`, builds, verifies both
artefacts carry that exact version, publishes to PyPI and creates the GitHub
Release.

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
