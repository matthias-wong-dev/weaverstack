# Test architecture map

What each layer claims, where it is claimed, and what is not claimed yet.

This exists to be reviewed for **gaps**. The interesting column is the last one.

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

Markers are peers; none implies another.

```bash
pytest                      # pure Python
pytest -m spark             # local Spark/Delta
pytest -m fabric            # targeted Fabric probes
pytest -m full_integration  # the lifecycle journey, both transports
pytest -m provisioning      # Fabric item lifecycle
```

## The two prepared states

Every build decision runs against two prepared, transport-neutral objects:

```text
Catalogue         what Weaver certifies as installed  (rows + registered)
TargetInventory   what is physically there            (schemas/tables/views/folders)
```

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

| | tests | runtime |
|---|---|---|
| `pytest` (pure Python, incl. 68 targeted) | 1154 | 11s |
| `pytest -m spark` (incl. 13 boundary) | 89 | 5m33s |
| `pytest -m fabric` | 51 | ~15m |
| `pytest -m full_integration` (both transports) | 2 | 9m23s |

Fabric transport, before and after this work:

| | Livy calls | Livy elapsed | wall |
|---|---|---|---|
| before | 23 | 852s | 16m37s |
| after | 19 | 661s | 13m28s |

The journey alone is 14 calls / 446s of that, and 408s of *those* are the eight
generate-and-install submissions — genuine build work, not transport. Merging
submissions would save the round trips (~4s each), not the builds.

## Layer by layer

### Done — new targeted tests

| claim | entry point | file | not yet asserted |
|---|---|---|---|
| a declaration becomes one action + payload; id, executor, filename, hash, determinism | `render_document_build_action` | `test_document_action.py` | alias destinations; a document whose DDL raises |
| one action runs with installer result semantics; failures are data; statements reach the engine resolved | `execute_action` | `test_action_execution.py` | `spark_table`, `spark_schema`, `folder`, `alias`, `sql_endpoint_refresh` executors |
| new / unchanged / changed; descendant propagation; selection bounds the walk; prohibit-rebuild | `determine_impact`, `select_build` | `test_incremental_impact.py` | stale aliases; removed objects; cross-item propagation |
| one item's stages and their order: prune → drop → schema → build → refresh | `plan_item_build` | `test_item_plan.py` | alias stages; Warehouse item planning; uncertified aliases |
| desired state; the diff into removals; item scoping; what prune spares | `managed_sets`, `item_prune_stage` | `test_prune.py` | `render_inventory_prune` called directly; empty-parent cleanup; alias destinations retained |
| a claim confirmed, disproved, or held about an item with no inventory; malformed Registry rows | `reconcile_catalogue_state` | `test_reconciliation.py` | dictionary-table claim rules in depth |

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
| `render_inventory_prune` | reached through `item_prune_stage`, never called directly |
| `item_schema_stage` | indirect only — bracket escaping, Lakehouse vs Warehouse, alias-derived schemas |
| `item_drop_stages` | partial — ordering and bad-type covered; per-kind drop rendering is not |
| alias planning | `plan_item_aliases`, `stale_alias_destinations` — old tests only |
| `build_item_repository` | zero references anywhere |

### Boundary fidelity — the Spark and Fabric job now

With the logic proven above, what those layers owe is narrower: **does a real
read produce the same object a fixture builds?**

| boundary | claim | state |
|---|---|---|
| `read_catalogue_state` | a real catalogue reads back into a `Catalogue`; incompatible shapes rejected | partial — `test_item_catalogue.py` covers shape, not round-trip |
| `read_lakehouse_inventory` | a real Lakehouse reads back into a `TargetInventory` matching what a build left | **gap** |
| `read_warehouse_inventory` | same, over TDS | **gap** |
| genuine DDL | one Weaver document actually builds, and the object has the declared physical types | covered by `-m spark` and `test_warehouse_build.py` |

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

## Conventions

- Take the narrowest fixture that can answer the question. Reaching for a richer
  one is the smell `tests/targeted/factories.py` exists to remove.
- A Fabric state transition produces **one** evidence payload; assertions stay
  local. See `tests/fabric/observation.py`.
- No test asserts a Livy call count. The ledger prints a breakdown instead —
  `tests/fabric/livy_telemetry.py`.
- Pure-Python tests must not request Spark fixtures; Spark tests must not request
  Fabric fixtures. Currently a convention, not enforced.
