# Weaver Backlog

## Core build and execution

1. **Implement basic ETL for a single Weaver item**
   - Done. Load artefacts are claimed, registered, signed, built, pruned and
     deployed to `Files/_/Load`; a Warehouse table gets a generated procedure and
     every Lakehouse table gets a deployed Python module. A Spark-SQL-authored
     table is *compiled* into a `SparkSqlTable` module rather than into a load
     program of its own, so the Delta load lifecycle exists once instead of once
     per authoring language.
   - `TSQL_LOAD_VERSION` and `SPARK_LOAD_VERSION` salt the artefact signatures,
     so replacing a generated body rebuilds exactly what it renders.
2. **Implement ETL across Weaver items using catalogue dependencies**
   - Done: `weaver.load([...])` reads the installed catalogue, reverses the
     build's physical bindings, and executes the physical load DAG — including
     alias crossings and the endpoint-refresh barriers they need.
3. **Add ETL unit tests and document the execution assumptions**
4. **Implement bookmarks for incremental loading**

### Follow-ups from load orchestration

- **Done — two runtime trees, one module name.** Deployed modules now import
  under a context-qualified package (`_weaver_runtime.<item>__<target>.lib.dates`),
  so the context is part of the `sys.modules` key rather than only of the search.
  Two Lakehouses each deploying a `lib/dates.py` get their own, concurrently.
  See `weaver.runtime.python_context`. A side effect worth knowing: a deployed
  module is now a submodule of its context package, so the relative imports the
  parser used to accept and the load used to reject (`from .Files.X import X`)
  resolve correctly.
- **Done — a mount outliving what is done to OneLake behind it.** Weaver mounts
  with `fileCacheTimeout=0`, which is the real repair rather than invalidation:
  dropping `_MOUNTS[spark_root]` leaves the host mount in place and
  `getMountPath()` recovers the same stale view. The `ENOTEMPTY` from
  `new_staging_folder` is gone, and staging reset and cleanup additionally carry
  a bounded retry for transient remote-filesystem behaviour. Proved by
  `tests/fabric/test_onelake_mount_contract.py`, which stages through the mount,
  wipes over DFS, and resets through the same mount in the same Livy session.
- **A build does not converge from "catalogue wiped, estate intact".** Losing the
  catalogue while the physical objects survive leaves every declared object
  looking *new*, so the planner emits a create for something already there and
  the executor correctly refuses to clobber it. `test_artefact_lifecycle.py`
  covers damage in the other direction — objects deleted, catalogue intact — and
  nothing covers this one. It is reachable in practice: a control plane restored
  from an older backup, or a `_` schema dropped by hand. The repair is arguably
  that an object the catalogue does not certify but the inventory *does* see
  should be rebuilt rather than created, which is the same information prune
  already reads. Found by wiping the control plane in the Fabric harness.
- **`Has load artefact: false` for internal objects.** `_.Log` and `_.Load` are
  declared folders that nothing loads, and they are excluded from orchestration
  today only because the built-in item and generated documents produce no load
  artefacts at all. Saying so in the declaration would make the exclusion a
  property of the object rather than a consequence of where it came from.
- **Parallel execution.** The executor asks the graph what is ready and runs it,
  so the shape is there; only the sequential loop would change. Runtime
  isolation was built for it — different contexts are independently importable
  at the same time, and that is asserted under threads.
- **Object-level load selection**, retries, bookmarks, resumable runs and
  cancellation — all deliberately out of scope for this phase.

## Platform support

5. **Support semantic models**
6. **Support Fabric SQL Database**
   - Reuse as much of the Warehouse T-SQL implementation as possible.
   - Isolate the remaining target-specific behaviour.

## Performance and distribution

7. **Optimise repository push to OneLake**
8. **Optimise build planning and execution times**
9. **Publish WeaverStack to PyPI**
    - Support ordinary `pip install weaverstack`.
    - Ensure installation works cleanly within Microsoft Fabric environments.

## Developer and user experience

10. **Build a Weaver GUI over the catalogue**
    - Run as a Fabric custom web application.
    - Provide workspace, item, document, dependency, build-status and catalogue views.

11. **Generate HTML documentation into `docs/`**
    - Publish through GitHub Pages.
    - Include repository structure, Weaver documents, bindings, aliases, build behaviour, ETL and catalogue concepts.

12. **Create built-in examples**
    - A minimal single-item example.
    - A realistic multi-item example covering aliases, dependencies, ETL, bookmarks and multiple Fabric target types.
