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

## Transfer state

The new suite lives in `tests/targeted/`. Old tests remain as reference and are
discarded **claim by claim**: an old test goes when a new test asserts its claim
at the lowest layer that can answer it. Old tests passing proves the refactor is
sound; it does **not** prove the claim moved. Nothing has been deleted yet.

| | tests | runtime |
|---|---|---|
| `tests/targeted/` (new) | 48 | 1.8s |
| whole default suite (new + old) | 1134 | 11s |
| `pytest -m fabric` | 46 | 19m31s |
| `pytest -m full_integration -k fabric` | 1 | ~8m |

## Layer by layer

### Done — new targeted tests

| claim | entry point | file | not yet asserted |
|---|---|---|---|
| a declaration becomes one action + payload; id, executor, filename, hash, determinism | `render_document_build_action` | `test_document_action.py` | alias destinations; a document whose DDL raises |
| one action runs with installer result semantics; failures are data; statements reach the engine resolved | `execute_action` | `test_action_execution.py` | `spark_table`, `spark_schema`, `folder`, `alias`, `sql_endpoint_refresh` executors |
| new / unchanged / changed; descendant propagation; selection bounds the walk; prohibit-rebuild | `determine_impact`, `select_build` | `test_incremental_impact.py` | stale aliases; removed objects; cross-item propagation |
| one item's stages and their order: prune → drop → schema → build → refresh | `plan_item_build` | `test_item_plan.py` | alias stages; Warehouse item planning; uncertified aliases |

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
| `managed_sets` | indirect only — desired-state computation never asserted alone |
| `render_inventory_prune` | indirect only — the diff that decides removals |
| `item_prune_stage` | indirect only — item scoping and action packaging |
| `item_schema_stage` | indirect only — bracket escaping, Lakehouse vs Warehouse, alias-derived schemas |
| `item_drop_stages` | partial — ordering and bad-type covered; per-kind drop rendering is not |
| `reconcile_catalogue_state` | old tests exist; not yet expressible as one Registry row + one inventory |
| `read_catalogue_state` | Spark-boundary claim; must see a complete catalogue, not a Registry-only one |
| inventory readers | `read_lakehouse_inventory`, `read_warehouse_inventory` unasserted against fakes |
| `build_item_repository` | zero references anywhere |

### Fabric — not yet converted

The `-m fabric` runtime is **97% three modules**, each using the whole
orchestrator to ask a narrow platform question. `execute_action` now exists to
replace them; the conversion has not been done.

| module | Livy | what it actually asks | conversion |
|---|---|---|---|
| `test_cross_item_alias.py` | 10 calls / 533s | is a shortcut really created, is it left alone on rebuild, does the endpoint refresh publish it | render + execute the alias action; observe once |
| `test_warehouse_build.py` | 4 calls / 266s | does Fabric accept Weaver's T-SQL and produce these physical types | execute rendered DDL over TDS — **needs no Livy at all** |
| `test_item_catalogue_fabric.py` | 2 calls / 196s | catalogue build, prune and wipe in-session | genuinely session work; reduce, do not remove |

Everything already probe-shaped — `test_livy_import.py`,
`test_authored_object_attachment.py`, `test_warehouse_wipe.py`,
`test_shared_wipe.py`, and all five consolidated observations — costs **~30
seconds of Livy between them**.

One claim in `test_warehouse_build.py` needs no Fabric whatever:
`test_every_object_is_built_in_dependency_order` reads `bundle.plan.actions()`.

## Conventions

- Take the narrowest fixture that can answer the question. Reaching for a richer
  one is the smell `tests/targeted/factories.py` exists to remove.
- A Fabric state transition produces **one** evidence payload; assertions stay
  local. See `tests/fabric/observation.py`.
- No test asserts a Livy call count. The ledger prints a breakdown instead —
  `tests/fabric/livy_telemetry.py`.
- Pure-Python tests must not request Spark fixtures; Spark tests must not request
  Fabric fixtures. Currently a convention, not enforced.
