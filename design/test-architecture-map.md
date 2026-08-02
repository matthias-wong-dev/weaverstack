# Test architecture map

What each layer claims, where it is claimed, and what is not claimed yet.

This exists to be reviewed for **gaps**. The interesting column is the last one.

## Layout

Marker, directory, fixture and transport describe the same thing. That is not
tidiness: a module under `tests/fabric` that answered to `-m spark` loaded the
Fabric conftest, and with it a workspace, a credential and a session — so the
two names described different sets and neither was honest.

| command | needs | runtime |
|---|---|---|
| `pytest` | nothing | 14s |
| `pytest -m spark` | a JDK | ~7m |
| `pytest -m fabric` | a workspace | ~1m |
| `pytest -m published_weaver` | a workspace **and a published wheel** | ~2m |
| `pytest -m full_integration` | a workspace and a published wheel | ~8m |
| `pytest -m provisioning` | a workspace | — |

The first three are the development loop. **None of them publishes anything**,
which is the point: a five-minute `weaver install` used to sit between a
developer and finding out a REST body was malformed.

## What the wheel actually has to prove

Weaver's executors are one implementation; what differs between a desktop run and
a notebook is how capabilities are *acquired*. Prove that once per capability and
no feature has to re-prove it — which is what lets `-m fabric` drive real Fabric
from the checkout.

Writing those probes corrected the model. "Same implementation, different
acquisition" holds for only half of it:

| capability | desktop | in a session |
|---|---|---|
| SQL | injected `desktop_sql_executor` | `sql_for` on the session identity — *same class* |
| Spark | injected session | the session's own — *same class* |
| store | `OneLakeDfsClient` | `FabricStore` over `notebookutils.fs` — **different class** |
| resolution | `FabricResolver` + `DefaultAzureCredential` | `FabricSessionResolver` over `notebookutils` — **different class** |

A session has no CLI, no IMDS and no environment variables, so the desktop
credential chain simply fails there; REST is reached on a
`notebookutils.credentials` token. The bottom two get probes of their own because
for them a desktop test proves a *different class*, not the same one wired
differently.

The argument also rests on executors never branching on where they run.
`tests/targeted/test_executor_parity.py` asserts that rather than trusting it.

```text
tests/
  targeted/    pure Python, by seam — the narrow fixture constructors live here
  support/     shared harness: build env, observation, Livy ledger, claims
  spark/       local Spark, incl. boundary/ for interface fidelity
  fabric/      a real workspace, and nothing that does not need one
```

`tests/support` holds what belongs to neither transport — the build environment a
test drives, and the claims that two transports both make. A module that spans
both is a thin wrapper over a shared claim, not a parametrised module: a
parametrised one can only be honest about one of its markers.

## The intended structure

```text
pure Python            most behavioural confidence, no JVM, under a second
    ↓
local Spark            where Delta or catalogue execution is the claim
    ↓
targeted Fabric        one narrow question a workspace alone can answer
    ↓
one full journey       composition only — never the first sight of a defect
    ↓
provisioning           opt-in, changes rarely
```

Markers are peers; none implies another. See the table above for what each runs.

## The three states, and what compares them

Every build decision runs against prepared, transport-neutral objects:

```text
Catalogue.from_repository(...)   what the source says should be   — desired
read_catalogue_state(...)        what is persisted                — current
TargetInventory                  what is physically there         — reality
```

Reconciliation removes catalogue claims that reality disproves. The diff decides
how the persisted catalogue should move toward the one the repository describes:

```python
changes = current.diff(desired)      # reports new/changed/unchanged/removed
dml = changes.render_dml(binding)    # statements from `desired` alone
```

**The asymmetry is the design.** `current` informs the report; `desired` alone
drives the statements. A row-level delete would look equivalent and would not be
— a partial or wrongly-scoped read returns *fewer* rows in `current`, so fewer
deletes, and obsolete claims would survive indefinitely with nothing to notice.
Scoped against what `desired` claims, the pair is correct against any prior
state, including one the reader never saw. `test_catalogue_diff.py` asserts it:
three catalogues that disagree completely render byte-identical statements.

`Catalogue.from_repository` is production, not a fixture, and that is the point —
a projection the build itself uses cannot drift, whereas a fixture listing the
rows a repository ought to produce must be updated by hand every time an artefact
is added, and is wrong the first time someone forgets. It carries no binding: no
target name, no Weaver version, no Installation row, no epoch. The one exception
is `target_kind`, because an alias is registered as what it physically is.

Everything between them and the bundle is pure Python — no session, no store.
That is what makes the interesting logic cheap to prove: incremental selection,
alias staleness, reconciliation, prune and item planning all read these two and
nothing else.

They are populated three ways, and the class is the same one every time:

| | catalogue | inventory |
|---|---|---|
| production | `read_catalogue_state` over Spark | `read_lakehouse_inventory` / `read_warehouse_inventory` |
| fixture | `FixtureCatalogue.from_registry_rows(...)` | `target_inventory(...)` |
| repository | `FixtureCatalogue.from_repository(...)` | `FixtureInventory.from_repository(...)` |

The repository constructors give the **"already built, nothing changed"** state —
the premise of every incremental and prune claim, which previously cost a real
build to reach.

This is not a fake. It is the production class begun further along, exactly as
installing a frozen bundle begins further along than building one from a
repository. What each test then proves is the logic itself; what remains for
Spark and Fabric is the **fidelity of the boundary** — does a real read produce
the same object a fixture builds.

**The from-repository constructors live on test subclasses, not the production
classes**, and the asymmetry is deliberate. A wrong inventory *degrades a
decision* — prune removes nothing, a schema is skipped. A wrong catalogue
*forges a guarantee*: a Registry row means work succeeded, written last in a
build, with `uncertified` existing to withhold rows for work that was not done. A
production method manufacturing rows from declarations would be a way to forge
that, and something would eventually call it on a build path.

## Transfer state

The new suite lives in `tests/targeted/`. Old tests remain as reference and are
discarded **claim by claim**: an old test goes when a new test asserts its claim
at the lowest layer that can answer it. Old tests passing proves the refactor is
sound; it does **not** prove the claim moved. Nothing has been deleted yet.

Measured across every marker:

| command | tests | runtime | needs |
|---|---|---|---|
| `pytest` (incl. 163 targeted) | 1249 | 15s | nothing |
| `pytest -m spark` (incl. 21 boundary) | 98 | ~7m | a JDK |
| `pytest -m fabric` | 39 | ~4m | a workspace |
| `pytest -m published_weaver` | 13 | ~4.5m | a workspace **and the wheel** |
| `pytest -m full_integration` | 1 | ~8m | a workspace and the wheel |

Fabric transport, over the course of this work:

| | Livy calls | wall |
|---|---|---|
| start | 23 | 16m37s |
| after the alias and catalogue rewrites | 12 | 6m39s |
| after the wheel split | **6** | **~4m, and no publish** |

Where it went, and none of it from asserting less:

| module | before | after |
|---|---|---|
| `test_warehouse_build.py` | 4 calls / 195s | retired — a Warehouse is TDS |
| `test_cross_item_alias.py` | 10 calls / 486s | 3 calls, driven from the checkout |
| `test_warehouse_wipe.py` | wheel-gated | 6s, no session |
| `test_item_catalogue_fabric.py` | 2 calls / 150s | 1 call; the wipe half moved out |

The pattern is the same each time. The bundle is generated on the desktop, in
pure Python, so what runs against Fabric is provably what the build produces; and
only the part that *must* be remote is run — an action, an install, a wipe —
rather than the estate around it.

## Layer by layer

### Done — new targeted tests

| claim | entry point | file | not yet asserted |
|---|---|---|---|
| a declaration becomes one action + payload; id, executor, filename, hash, determinism | `render_document_build_action` | `test_document_action.py` | alias destinations; a document whose DDL raises |
| one action runs with installer result semantics; failures are data; statements reach the engine resolved | `execute_action` | `test_action_execution.py` | `spark_table`, `spark_schema`, `folder`, `alias`, `sql_endpoint_refresh` executors |
| new / unchanged / changed; descendant propagation; selection bounds the walk; prohibit-rebuild | `determine_impact`, `select_build` | `test_incremental_impact.py` | stale aliases; removed objects; cross-item propagation |
| one item's stages and their order: prune → drop → schema → build → refresh → load | `plan_item_build` | `test_item_plan.py` | alias stages; uncertified aliases |
| desired state; item scoping; what prune spares, on both physical sides | `managed_sets`, `item_prune_stage` | `test_prune.py` | alias destinations retained |
| the diff into removals; schema-drop folding; T-SQL escaping; determinism | `render_inventory_prune` | `test_inventory_prune.py` | — |
| every action kind is executed somewhere, or deferred with a reason | the action kinds themselves | `test_action_checklist.py` | — |
| which schemas an item needs; alias namespaces; per-side payloads | `item_schema_stage` | `test_schema_stage.py` | — |
| generate-and-install from prepared state; failure semantics; archives | `build_item_repository` | `test_build_workflow.py` | — |
| a claim confirmed, disproved, or held about an item with no inventory; malformed Registry rows | `reconcile_catalogue_state` | `test_reconciliation.py` | dictionary-table claim rules in depth |
| which sources own load artefacts; path and procedure identities; signature salts | `load_artefacts` | `test_load_artefacts.py` | — |
| the load layer's position, its frozen actions, and claim-driven removal | `item_load_stages` | `test_load_plan.py` | — |
| a build converges on what the source declares — from a correct estate, from nothing, from damage, and after a deletion | `generate_item_build_bundle`, `TargetInventory.update_using` | `test_convergence.py` (`-k converges`) | — |
| every action that touches a target is declared, and every declaration runs | `target_changes` | `test_build_intent.py` | — |
| every artefact kind reaches the catalogue, and every instance of one | `Catalogue.from_repository` | `test_catalogue_projection.py` | — |

### Covered by old tests, not yet re-homed

| claim | entry point | old file | judgement |
|---|---|---|---|
| repository parsing, identity, signatures, dependency graphs | `parse_item_repository` | `test_item_repository.py` + 15 more | already strong; re-home selectively, low priority |
| table and view DDL, T-SQL shaping, quoting, types | `source.create_ddl()` | `test_declaration_create_ddl.py`, `test_declaration_tsql_ddl.py` | already strong; leave |
| whole-bundle assembly, ordering, catalogue stages, bundle identity | `generate_item_build_bundle` | `test_item_build_planner.py` (884 lines) | keep as the whole-planner claim; it should shrink as item-level claims move down |
| installer sequencing, barriers, failure semantics, reporting | `install_bundle` | `test_build_installer.py` | re-home onto `single_action_bundle`; currently builds richer bundles than the claim needs |

### Gaps — asserted nowhere narrow

These are the plan's zero-coverage seams. Some are reached *indirectly* through
`plan_item_build`, which means a failure in them surfaces as an item-planning
failure rather than naming itself.

| seam | state |
|---|---|
| `read_catalogue_state` | Spark-boundary claim; must see a complete catalogue, not a Registry-only one |
| inventory readers | `read_lakehouse_inventory` / `read_warehouse_inventory` unasserted against fakes |
| dictionary-table claim rules | still only in `tests/test_catalogue_state.py`, which is why that file cannot retire |

Closed since this file was written: `render_inventory_prune`
(`test_inventory_prune.py`), `item_schema_stage` (`test_schema_stage.py`) and
`build_item_repository` (`test_build_workflow.py`) — the last of which no test had
named at all. Each was reached only through something larger, so a defect in it
and a defect in its caller failed the same test the same way.

Two of them turned up behaviour worth having written down. The Lakehouse folds
objects into a doomed schema's drop while the Warehouse must drop each by name
first, because T-SQL will not drop a schema that still holds objects — the two
look interchangeable from a distance. And every whole-plan assertion has to be
scoped to an item, because a repository always carries Weaver's own builtin
catalogue item and it is correctly built alongside.

### Boundary fidelity — the Spark and Fabric job now

With the logic proven above, what those layers owe is narrower: **does a real
read produce the same object a fixture builds?**

| boundary | claim | state |
|---|---|---|
| `read_catalogue_state` | a real catalogue reads back into a `Catalogue`; incompatible shapes rejected | partial — `test_item_catalogue.py` covers shape; `test_catalogue_fidelity.py` round-trips load artefacts, whose identities no two-part grammar can express |
| `read_lakehouse_inventory` | a real Lakehouse reads back into a `TargetInventory` matching what a build left | covered — `test_inventory_fidelity.py`, including the deployed runtime tree file by file |
| `read_warehouse_inventory` | same, over TDS | **gap** |
| genuine DDL | one Weaver document actually builds, and the object has the declared physical types | covered — `spark/boundary/test_actions_delta.py`, `fabric/test_actions_warehouse.py` |
| a whole bundle | the physical sequence executes against real Fabric, in manifest order, leaving the declared estate | covered — `fabric/test_bundle_can_install.py`, one Livy session |

The round-trip pairing is the strongest form and does not exist yet: build from a
repository, read the inventory back, and assert it equals
`FixtureInventory.from_repository(...)`. That single test would justify every
pure-Python prune claim that uses the fixture constructor.

### Fabric — not yet converted

The `-m fabric` runtime is **97% three modules**, each using the whole
orchestrator to ask a narrow platform question. `execute_action` now exists to
replace them; the conversion has not been done.

| module | Livy | what it actually asks | conversion |
|---|---|---|---|
| `test_cross_item_alias.py` | 10 calls / 486s | is a shortcut really created, is it left alone on rebuild, does the endpoint refresh publish it | render + execute the alias action; observe once — **not yet done** |
| ~~`test_warehouse_build.py`~~ | ~~4 calls / 195s~~ | — | **done**: replaced by `test_warehouse_boundary.py`, 10 tests / 7s / **zero Livy** |
| `test_item_catalogue_fabric.py` | 2 calls / 150s | catalogue build, prune and wipe in-session | genuinely session work; reduce, do not remove |

### Discarded, claim by claim

`test_warehouse_build.py` was the first module retired. Every claim was re-homed
before it went, which is the rule: an old test passing proves the refactor is
sound, not that its claim moved.

`test_local_persisted_view.py` was the second, and for a different reason: it
was a **spike**, proving Spark *could* carry a view over a view before Weaver
built one. The claim now belongs to a real document and a real action
(`test_build_view_action_creates_a_view_over_another_view`), which is what the
spike was written to anticipate.

| its claim | where it lives now |
|---|---|
| tables built empty | `test_warehouse_boundary.py` |
| declared types survive | `test_warehouse_boundary.py` |
| PK and audit columns not nullable | `test_warehouse_boundary.py` |
| objects present in the catalogue | `test_warehouse_boundary.py` (inventory fidelity) |
| a dimension gets a bigint surrogate | `test_warehouse_boundary.py` |
| prune removes unmanaged, spares managed | `test_warehouse_boundary.py` (executed, not just planned) |
| **dependency ordering** | `test_item_plan.py` — **pure Python; never needed Fabric** |

Everything already probe-shaped — `test_livy_import.py`,
`test_authored_object_attachment.py`, `test_warehouse_wipe.py`,
`test_shared_wipe.py`, and all five consolidated observations — costs **~30
seconds of Livy between them**.

One claim in `test_warehouse_build.py` needs no Fabric whatever:
`test_every_object_is_built_in_dependency_order` reads `bundle.plan.actions()`.

## A proposal, assessed: `from_repository` in production, and catalogue diffing

Two halves, and they do not get the same answer.

### Promoting `from_repository` — sound, and worth doing

`project_item_installation` already *is* "the catalogue this repository should
produce": it projects every catalogue table — Registry, the dictionaries,
Installation — from the declarations. It simply returns a `CatalogueProjection`
rather than a `Catalogue`.

Those are the same idea with two types. Unifying them would make
`Catalogue.from_repository(...)` production code with a real job — *desired*
state — and that also dissolves the objection that kept it on a test subclass. A
constructor computing what *should* be installed forges nothing; the danger was
only ever in manufacturing rows that claim work *succeeded*.

The test-side constructor would then be a thin wrapper, or disappear.

### Diffing two catalogues to produce DML — no, and the reason is load-bearing

Catalogue DML is deliberately **not** derived from reading the catalogue. Per
`weaver/catalogue/reconcile.py`: the delete keeps exactly the keys the projection
claims and the merge is idempotent, so the pair is correct against any prior
state — *including one the planner could not see*.

Making the builder diff current against desired would reintroduce precisely the
failure mode that design prevents: **a failed or partial read would widen the
deletion scope.** A catalogue read that silently returned fewer rows would
produce a delete for rows that should have been kept, in the authoritative
record, with nothing to catch it.

Note also that `reconcile.compare(...)` already exists — new, changed, unchanged
and removed — precisely so a reviewer can see what a bundle will change *without
any statement depending on it*. The comparison is already there; keeping it out
of the DML path is the point, not an omission.

So: promote the constructor, keep the rendering projection-driven. The symmetry
with prune is tempting and false — prune diffs against physical state it must
observe, while the catalogue is the authority and needs no permission from its
own prior contents.

## What only Fabric has caught, and why

Worth recording, because each one names a shape of gap rather than a bug.

**A Warehouse prune dropping the `_` schema it had just created.** Every pure
caller of `item_prune_stage` used a *Lakehouse* estate, and the two sides are not
symmetric: a Lakehouse's generated `_` is a folder *document*, so it reaches the
keep-set through the ordinary document path, while a Warehouse's `_` is a schema
no document declares. The defect could only exist on the side nothing covered.

It was reachable at all because the keep-set's load half arrived as a *defaulted*
argument — production passed it, a direct caller did not, and a seam with a
destructive default has two behaviours of which the suite exercised one. The
stage now derives it, so there is nothing to forget, and `test_prune.py` has a
Warehouse estate.

The general lesson: **an asymmetry between the two physical sides is where a
Lakehouse-only fixture stops being representative.** Prune, schemas and inventory
all behave differently across that line.

**Would the fixed-point test have caught it?** No — and the reason is worth
keeping. `test_convergence.py` composes the three states and asserts a build finds
nothing to do, which is the strongest whole-plan property available. It passes
with that defect reintroduced, because *the planner passed the argument
correctly*. The bug lived in the seam's **default**, on a path only a direct
caller took.

A defaulted argument is two contracts. A composed test can only ever prove the
one the composition uses. That is why the more important half of the fix was
removing the parameter rather than adding coverage: with the value derived
inside, there is one path, and breaking the derivation now fails
`test_convergence.py` *and* `test_prune.py` together — verified by doing it.

The rule this suggests: **a seam with a destructive default cannot be covered
from above.** Either the default goes, or the seam is tested directly on every
shape it accepts.

**A `snapshot=` keyword in a Livy body.** Not a coverage gap of the same kind —
that code is a *string* sent to a Fabric session and executes only against the
installed wheel, so no pure test could run it and no import check could see it. It
is the one category where `-m published_weaver` is the first possible sight of the
defect, and it argues for grepping test *bodies* after any signature change rather
than trusting a mechanical rewrite.

## The action checklist

```text
pytest --collect-only -q tests/targeted/test_action_checklist.py
```

lists every action Weaver can perform against a target and the test that executes
it, one `[<kind>-<test>]` per line. The test names carry both halves —
`test_<kind>_action_<what it proves>` — so the list reads as a checklist without
giving up the claim.

`test_action_checklist.py` holds the estate to it: every kind the product defines
is either covered, naming its test, or deferred with a reason. A new kind cannot
arrive unnoticed, and a renamed test cannot leave the list pointing at nothing.

See [how-to-add-an-artefact.md](how-to-add-an-artefact.md) for the six steps and
the order their tests fail in.

## Adding an artefact: what fails, and in what order

The suite is arranged so a new artefact type is caught by a sequence of tests
rather than one, each naming a different thing left undone.

| what is missing | what fails |
|---|---|
| the catalogue does not register it | `test_catalogue_from_repository_has_all_artefacts` |
| a build emits no action for it | `test_converges_from_nothing_to_the_declared_estate` |
| an action is emitted but declares no effect | `test_every_action_that_touches_a_target_is_declared` |
| the effect is declared but no action performs it | `test_every_declared_change_names_an_action_that_runs` |
| the inventory cannot see it | `test_inventory_fidelity.py`, and every claim about it becomes vacuous |

The order matters as much as the coverage: the first failure names the artefact,
not a symptom several layers downstream. Verified by doing it — adding a member
to `OBJECT_TYPES` fails only the first; suppressing the load layer's declarations
fails the third and, in consequence, convergence.

## Conventions

- Take the narrowest fixture that can answer the question. Reaching for a richer
  one is the smell `tests/targeted/factories.py` exists to remove.
- A Fabric state transition produces **one** evidence payload; assertions stay
  local. See `tests/fabric/observation.py`.
- No test asserts a Livy call count. The ledger prints a breakdown instead —
  `tests/fabric/livy_telemetry.py`.
- Pure-Python tests must not request Spark fixtures; Spark tests must not request
  Fabric fixtures. Currently a convention, not enforced.
