# Weaver Backlog

## Core build and execution

1. **Implement basic ETL for a single Weaver item**
   - The lifecycle is in place: load artefacts are claimed, registered, signed,
     built, pruned and deployed to `Files/_/Load`, with a generated procedure per
     Warehouse table. What remains is the generated bodies themselves, and
     running them.
   - `SPARK_ETL_TEMPLATE_VERSION` and `TSQL_ETL_TEMPLATE_VERSION` exist so
     replacing a proxy body rebuilds exactly what it renders.
2. **Implement ETL across Weaver items using catalogue dependencies**
   - Done: `weaver.load([...])` reads the installed catalogue, reverses the
     build's physical bindings, and executes the physical load DAG — including
     alias crossings and the endpoint-refresh barriers they need.
3. **Add ETL unit tests and document the execution assumptions**
4. **Implement bookmarks for incremental loading**

### Follow-ups from load orchestration

- **A deployed module's imports must be absolute, and nothing says so.** The
  deployed tree flattens the item root to the runtime root, so a top-level
  module has no parent package and `from .Files.X import X` cannot resolve —
  while the repository parser accepts it happily. A build therefore succeeds and
  the load fails, which is the wrong end to find out. Either the parser refuses
  a relative import, or the deployed tree gains the package structure to honour
  one. Two shipped fixtures had this and had never been executed.
- **Two runtime trees, one module name.** `_import_deployed` puts the runtime
  root first on `sys.path` and loads by exact file, so the module *it*
  dispatches is unambiguous — but a module that module imports resolves through
  `sys.modules`, which a second Lakehouse's tree could already have populated
  under the same name. One estate per process is the normal case and this is not
  it.
- **A mount outlives what is done to OneLake behind it.** Weaver reaches a
  Lakehouse's Files area two ways — `abfss://` over DFS for storage work, and a
  `synfs` mount for authored code that calls `open()` — and `weaver.lakehouse`
  caches the mount in `_MOUNTS` for the life of the session, deliberately,
  because Fabric refuses a second mount of the same root. So anything that
  changes OneLake *outside* the mount is not necessarily visible through it: a
  wipe over DFS, another session's write, a shortcut created by REST.

  Reproducibly seen as `OSError: [Errno 39] Directory not empty` from
  `new_staging_folder`, which resets a folder's staging with `shutil.rmtree` then
  `mkdir(exist_ok=False)`. Correct on a POSIX filesystem; over a stale mount the
  `rmdir` at the end of `rmtree` meets entries that are already gone on DFS.
  Only appears when the same folder object is loaded twice into one Lakehouse in
  one session with a DFS wipe between — the Fabric suite, where the Livy session
  spans every module.

  Two candidate repairs and they are not the same fix: make the staging reset
  tolerant (empty rather than replace, and retry), or give the mount a way to be
  invalidated when Weaver itself changes OneLake behind it. The second is the
  real one, and it is not only a test concern — the same window exists for any
  session that wipes and then loads.

  Not reproducible locally, where storage *is* a filesystem, there is no mount,
  and deletes are immediate. The folder primitive itself is proven
  (`test_developer_load_primitive`); this is about how staging is issued.
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
- **Parallel execution.** The executor asks the graph what is ready and runs it,
  so the shape is there; only the sequential loop would change.
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
